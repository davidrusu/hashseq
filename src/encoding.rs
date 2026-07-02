use std::collections::{BTreeMap, BTreeSet, HashMap};

use rustc_hash::{FxHashMap, FxHashSet};

use crate::hashseq::{CausalRemove, Loc};
use crate::run::FirstOp;
use crate::{Anchor, HashNode, HashSeq, Id, NodeIdx, Op, Payload, Run};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    UnexpectedEof,
    InvalidVarint,
    InvalidUtf8,
    InvalidOpTag(u8),
    EmptyRun,
    InvalidIdIndex(usize),
    /// The bytes decode but are not the canonical encoding of their op set
    /// (strict acceptance mode, ENCODING_SPEC.md).
    NotCanonical,
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::UnexpectedEof => write!(f, "unexpected end of input"),
            DecodeError::InvalidVarint => write!(f, "invalid varint encoding"),
            DecodeError::InvalidUtf8 => write!(f, "invalid UTF-8 encoding"),
            DecodeError::InvalidOpTag(tag) => write!(f, "invalid operation tag: {}", tag),
            DecodeError::EmptyRun => write!(f, "run string cannot be empty"),
            DecodeError::InvalidIdIndex(idx) => write!(f, "invalid ID index: {}", idx),
            DecodeError::NotCanonical => write!(f, "bytes are not a canonical snapshot"),
        }
    }
}

impl std::error::Error for DecodeError {}

/// Byte cursor for decoding: threads the position through the
/// `fn(&[u8]) -> (value, consumed)` decoders below via `step`.
struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn step<T>(
        &mut self,
        f: impl FnOnce(&[u8]) -> Result<(T, usize), DecodeError>,
    ) -> Result<T, DecodeError> {
        let (v, n) = f(&self.bytes[self.pos..])?;
        self.pos += n;
        Ok(v)
    }

    fn byte(&mut self) -> Result<u8, DecodeError> {
        let b = *self.bytes.get(self.pos).ok_or(DecodeError::UnexpectedEof)?;
        self.pos += 1;
        Ok(b)
    }
}

// Reference-position codecs: the standalone node form writes refs as raw ids,
// the snapshot orphan section writes them positionally. One layout, two codecs.
type PutRef<'f> = dyn FnMut(&Id, &mut Vec<u8>) + 'f;
type PutRefSet<'f> = dyn FnMut(&BTreeSet<Id>, &mut Vec<u8>) + 'f;
type GetRef<'f> = dyn FnMut(&mut Cursor) -> Result<Id, DecodeError> + 'f;
type GetRefSet<'f> = dyn FnMut(&mut Cursor) -> Result<BTreeSet<Id>, DecodeError> + 'f;

// Stream framing tags — the transport tag space (distinct from the GRAMMAR
// kind tags, which live inside node preimages).
const TAG_RUN: u8 = 0x00;
pub(crate) const TAG_INSERT: u8 = 0x01;
pub(crate) const TAG_REMOVE: u8 = 0x02;
pub(crate) const TAG_MOVE: u8 = 0x03;
pub(crate) const TAG_PUT: u8 = 0x04;
pub(crate) const TAG_MARK: u8 = 0x05;

// --- Varint (LEB128) encoding/decoding ---

pub fn encode_varint(mut value: usize, buf: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
}

pub fn decode_varint(bytes: &[u8]) -> Result<(usize, usize), DecodeError> {
    let mut result: usize = 0;
    let mut shift = 0;
    let mut pos = 0;

    loop {
        if pos >= bytes.len() {
            return Err(DecodeError::UnexpectedEof);
        }
        let byte = bytes[pos];
        pos += 1;

        result |= ((byte & 0x7F) as usize) << shift;
        if byte & 0x80 == 0 {
            return Ok((result, pos));
        }
        shift += 7;
        if shift >= 64 {
            return Err(DecodeError::InvalidVarint);
        }
    }
}

// --- Id encoding/decoding ---

pub fn encode_id(id: &Id, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&id.0);
}

pub fn decode_id(bytes: &[u8]) -> Result<(Id, usize), DecodeError> {
    if bytes.len() < 32 {
        return Err(DecodeError::UnexpectedEof);
    }
    let mut id = [0u8; 32];
    id.copy_from_slice(&bytes[..32]);
    Ok((Id(id), 32))
}

// --- UTF-8 char encoding/decoding ---

pub fn encode_utf8_char(ch: char, buf: &mut Vec<u8>) {
    let mut tmp = [0u8; 4];
    let encoded = ch.encode_utf8(&mut tmp);
    buf.extend_from_slice(encoded.as_bytes());
}

pub fn decode_utf8_char(bytes: &[u8]) -> Result<(char, usize), DecodeError> {
    if bytes.is_empty() {
        return Err(DecodeError::UnexpectedEof);
    }

    // Determine UTF-8 character length from first byte
    let len = match bytes[0] {
        0x00..=0x7F => 1,
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        0xF0..=0xF7 => 4,
        _ => return Err(DecodeError::InvalidUtf8),
    };

    if bytes.len() < len {
        return Err(DecodeError::UnexpectedEof);
    }

    let s = std::str::from_utf8(&bytes[..len]).map_err(|_| DecodeError::InvalidUtf8)?;
    let ch = s.chars().next().ok_or(DecodeError::InvalidUtf8)?;
    Ok((ch, len))
}

// --- String encoding/decoding ---

pub fn encode_string(s: &str, buf: &mut Vec<u8>) {
    encode_varint(s.len(), buf);
    buf.extend_from_slice(s.as_bytes());
}

pub fn decode_string(bytes: &[u8]) -> Result<(String, usize), DecodeError> {
    let (len, varint_size) = decode_varint(bytes)?;
    let bytes = &bytes[varint_size..];

    if bytes.len() < len {
        return Err(DecodeError::UnexpectedEof);
    }

    let s = std::str::from_utf8(&bytes[..len]).map_err(|_| DecodeError::InvalidUtf8)?;
    Ok((s.to_string(), varint_size + len))
}

// --- Id set encoding/decoding ---

pub fn encode_id_set(ids: &BTreeSet<Id>, buf: &mut Vec<u8>) {
    encode_varint(ids.len(), buf);
    for id in ids {
        encode_id(id, buf);
    }
}

pub fn decode_id_set(bytes: &[u8]) -> Result<(BTreeSet<Id>, usize), DecodeError> {
    let (len, mut pos) = decode_varint(bytes)?;
    let mut ids = BTreeSet::new();

    for _ in 0..len {
        let (id, id_size) = decode_id(&bytes[pos..])?;
        ids.insert(id);
        pos += id_size;
    }

    Ok((ids, pos))
}

// --- Run encoding/decoding ---

// First-op tag for wire-format runs. Internal-storage runs in encode_hashseq
// always use After and omit the tag for compactness.
const RUN_OP_AFTER: u8 = 0x00;
const RUN_OP_BEFORE: u8 = 0x01;

pub fn encode_run(run: &Run, buf: &mut Vec<u8>) {
    buf.push(match run.first_op {
        crate::run::FirstOp::After => RUN_OP_AFTER,
        crate::run::FirstOp::Before => RUN_OP_BEFORE,
    });
    encode_id(&run.anchor, buf);
    encode_id_set(&run.first_extra_deps, buf);
    encode_string(&run.run, buf);
    // Interior extra-deps: varint count + (varint offset, id_set) entries,
    // ascending offsets (BTreeMap iteration order).
    encode_varint(run.interior_extra_deps.len(), buf);
    for (offset, deps) in &run.interior_extra_deps {
        encode_varint(*offset, buf);
        encode_id_set(deps, buf);
    }
}

/// Decode a run body after its first-op tag: `anchor ‖ first_extra_deps ‖
/// text ‖ interior deps`. Shared by the standalone form (raw ids) and the
/// snapshot run block (positional refs).
fn decode_run_with(
    c: &mut Cursor,
    first_op: crate::run::FirstOp,
    ref_: &mut GetRef,
    ref_set: &mut GetRefSet,
) -> Result<Run, DecodeError> {
    let anchor = ref_(c)?;
    let first_extra_deps = ref_set(c)?;
    let text = c.step(decode_string)?;
    let num_interior = c.step(decode_varint)?;
    let mut interior_extra_deps = BTreeMap::new();
    for _ in 0..num_interior {
        let offset = c.step(decode_varint)?;
        interior_extra_deps.insert(offset, ref_set(c)?);
    }
    Run::from_text(anchor, first_op, first_extra_deps, &text, interior_extra_deps)
        .ok_or(DecodeError::EmptyRun)
}

pub fn decode_run(bytes: &[u8]) -> Result<(Run, usize), DecodeError> {
    let mut c = Cursor { bytes, pos: 0 };
    let first_op = match c.byte()? {
        RUN_OP_AFTER => crate::run::FirstOp::After,
        RUN_OP_BEFORE => crate::run::FirstOp::Before,
        other => return Err(DecodeError::InvalidOpTag(other)),
    };
    let run = decode_run_with(
        &mut c,
        first_op,
        &mut |c| c.step(decode_id),
        &mut |c| c.step(decode_id_set),
    )?;
    Ok((run, c.pos))
}

// --- HashNode wire + preimage encoding/decoding ---

/// Wire form of a value payload (GRAMMAR_SPEC.md Part B "value elision"):
/// `0x00 len artifact_bytes` inline iff the canonical artifact encoding is at
/// or below the hash size (a rule, not a choice); `0x01 id` otherwise. The
/// preimage always hashes the value id, so this never changes identity.
pub fn encode_payload(p: &Payload, buf: &mut Vec<u8>) {
    match p {
        Payload::Char(c) => {
            let mut tmp = [0u8; 5];
            tmp[0] = crate::value::VK_CHAR;
            let n = c.encode_utf8(&mut tmp[1..]).len();
            buf.push(0x00);
            encode_varint(1 + n, buf);
            buf.extend_from_slice(&tmp[..1 + n]);
        }
        Payload::Id(id) => {
            buf.push(0x01);
            encode_id(id, buf);
        }
    }
}

pub fn decode_payload(bytes: &[u8]) -> Result<(Payload, usize), DecodeError> {
    let (&form, rest) = bytes.split_first().ok_or(DecodeError::UnexpectedEof)?;
    match form {
        0x00 => {
            let (len, mut pos) = decode_varint(rest)?;
            if len > 32 {
                // inline is mandatory-iff-small; larger inline is malformed
                return Err(DecodeError::InvalidOpTag(form));
            }
            let artifact = rest.get(pos..pos + len).ok_or(DecodeError::UnexpectedEof)?;
            pos += len;
            let payload = match crate::value::Value::decode(artifact) {
                Some(crate::value::Value::Char(c)) => Payload::Char(c),
                // Any other (or unknown) inline artifact: commit by derived id.
                _ => Payload::Id(crate::value::value_id_of_bytes(artifact)),
            };
            Ok((payload, 1 + pos))
        }
        0x01 => {
            let (id, n) = decode_id(rest)?;
            Ok((Payload::Id(id), 1 + n))
        }
        other => Err(DecodeError::InvalidOpTag(other)),
    }
}

/// The Part A canonical preimage (GRAMMAR_SPEC.md identity grammar):
/// `kind ‖ ref_count ‖ refs ‖ body_len ‖ body`, value fields always by id.
/// This is the reference encoder that `HashNode::id`'s streaming hasher is
/// locked to by test.
pub fn encode_node_preimage(node: &HashNode, buf: &mut Vec<u8>) {
    use crate::hash_node::{KIND_INSERT, KIND_MARK, KIND_MOVE, KIND_PUT, KIND_REMOVE};

    let mut refs: Vec<Id> = node.iter_refs().copied().collect();
    refs.sort_unstable();
    refs.dedup();
    let ref_idx =
        |id: &Id| -> usize { refs.binary_search(id).expect("named id is in the refs table") };
    let subset_idxs = |set: &BTreeSet<Id>| -> Vec<usize> {
        let mut idxs = Vec::with_capacity(set.len());
        let mut i = 0usize;
        for want in set {
            while refs[i] != *want {
                i += 1;
            }
            idxs.push(i);
            i += 1;
        }
        idxs
    };
    let varint_len = |mut v: usize| -> usize {
        let mut n = 1;
        while v >= 0x80 {
            v >>= 7;
            n += 1;
        }
        n
    };

    let kind = match &node.op {
        Op::Insert { .. } => KIND_INSERT,
        Op::Remove(_) => KIND_REMOVE,
        Op::Move { .. } => KIND_MOVE,
        Op::Put { .. } => KIND_PUT,
        Op::Mark { .. } => KIND_MARK,
    };
    buf.push(kind);
    encode_varint(refs.len(), buf);
    for r in &refs {
        encode_id(r, buf);
    }

    match &node.op {
        Op::Insert { at, payload } => {
            let packed = (ref_idx(at.id()) << 1) | at.side_bit();
            encode_varint(varint_len(packed) + 32, buf); // body_len
            encode_varint(packed, buf);
            encode_id(&payload.value_id(), buf);
        }
        Op::Remove(targets) => {
            let idxs = subset_idxs(targets);
            let body_len =
                varint_len(idxs.len()) + idxs.iter().map(|&i| varint_len(i)).sum::<usize>();
            encode_varint(body_len, buf);
            encode_varint(idxs.len(), buf);
            for i in idxs {
                encode_varint(i, buf);
            }
        }
        Op::Move {
            target,
            to,
            overwrites,
        } => {
            let t = ref_idx(target);
            let packed = (ref_idx(to.id()) << 1) | to.side_bit();
            let idxs = subset_idxs(overwrites);
            let body_len = varint_len(t)
                + varint_len(packed)
                + varint_len(idxs.len())
                + idxs.iter().map(|&i| varint_len(i)).sum::<usize>();
            encode_varint(body_len, buf);
            encode_varint(t, buf);
            encode_varint(packed, buf);
            encode_varint(idxs.len(), buf);
            for i in idxs {
                encode_varint(i, buf);
            }
        }
        Op::Put {
            key,
            value,
            overwrites,
        } => {
            let idxs = subset_idxs(overwrites);
            let body_len =
                64 + varint_len(idxs.len()) + idxs.iter().map(|&i| varint_len(i)).sum::<usize>();
            encode_varint(body_len, buf);
            encode_id(key, buf);
            encode_id(value, buf);
            encode_varint(idxs.len(), buf);
            for i in idxs {
                encode_varint(i, buf);
            }
        }
        Op::Mark {
            start,
            end,
            kind_v,
            value,
            overwrites,
        } => {
            let sp = (ref_idx(start.id()) << 1) | start.side_bit();
            let ep = (ref_idx(end.id()) << 1) | end.side_bit();
            let idxs = subset_idxs(overwrites);
            let body_len = varint_len(sp)
                + varint_len(ep)
                + 64
                + varint_len(idxs.len())
                + idxs.iter().map(|&i| varint_len(i)).sum::<usize>();
            encode_varint(body_len, buf);
            encode_varint(sp, buf);
            encode_varint(ep, buf);
            encode_id(kind_v, buf);
            encode_id(value, buf);
            encode_varint(idxs.len(), buf);
            for i in idxs {
                encode_varint(i, buf);
            }
        }
    }
}

/// One wire layout for a whole node — `tag ‖ pins ‖ per-kind fields` — shared
/// by the standalone form (refs as raw ids) and the snapshot orphan section
/// (refs positional). Reference fields go through `ref_`/`ref_set`; value
/// fields (payload, Put key/value, Mark kind/value) always ride raw — values
/// are not references.
fn encode_node_with(node: &HashNode, buf: &mut Vec<u8>, ref_: &mut PutRef, ref_set: &mut PutRefSet) {
    fn anchor(a: &Anchor, buf: &mut Vec<u8>, ref_: &mut PutRef) {
        buf.push(a.side_bit() as u8);
        ref_(a.id(), buf);
    }
    match &node.op {
        Op::Insert { at, payload } => {
            buf.push(TAG_INSERT);
            ref_set(&node.pins, buf);
            anchor(at, buf, ref_);
            encode_payload(payload, buf);
        }
        Op::Remove(targets) => {
            buf.push(TAG_REMOVE);
            ref_set(&node.pins, buf);
            ref_set(targets, buf);
        }
        Op::Move {
            target,
            to,
            overwrites,
        } => {
            buf.push(TAG_MOVE);
            ref_set(&node.pins, buf);
            ref_(target, buf);
            anchor(to, buf, ref_);
            ref_set(overwrites, buf);
        }
        Op::Put {
            key,
            value,
            overwrites,
        } => {
            buf.push(TAG_PUT);
            ref_set(&node.pins, buf);
            encode_id(key, buf);
            encode_id(value, buf);
            ref_set(overwrites, buf);
        }
        Op::Mark {
            start,
            end,
            kind_v,
            value,
            overwrites,
        } => {
            buf.push(TAG_MARK);
            ref_set(&node.pins, buf);
            anchor(start, buf, ref_);
            anchor(end, buf, ref_);
            encode_id(kind_v, buf);
            encode_id(value, buf);
            ref_set(overwrites, buf);
        }
    }
}

/// Standalone wire form of a node (full ids; payload in elision form).
/// Used for batches and anywhere a node travels outside a snapshot stream.
pub fn encode_hash_node(node: &HashNode, buf: &mut Vec<u8>) {
    encode_node_with(
        node,
        buf,
        &mut |id, buf| encode_id(id, buf),
        &mut |ids, buf| encode_id_set(ids, buf),
    );
}

/// Inverse of `encode_node_with`: decode a node body after its `tag`.
fn decode_node_with(
    tag: u8,
    c: &mut Cursor,
    ref_: &mut GetRef,
    ref_set: &mut GetRefSet,
) -> Result<HashNode, DecodeError> {
    fn anchor(c: &mut Cursor, ref_: &mut GetRef) -> Result<Anchor, DecodeError> {
        match c.byte()? {
            0 => Ok(Anchor::Before(ref_(c)?)),
            1 => Ok(Anchor::After(ref_(c)?)),
            other => Err(DecodeError::InvalidOpTag(other)),
        }
    }
    let pins = ref_set(c)?;
    let op = match tag {
        TAG_INSERT => Op::Insert {
            at: anchor(c, ref_)?,
            payload: c.step(decode_payload)?,
        },
        TAG_REMOVE => Op::Remove(ref_set(c)?),
        TAG_MOVE => Op::Move {
            target: ref_(c)?,
            to: anchor(c, ref_)?,
            overwrites: ref_set(c)?,
        },
        TAG_PUT => Op::Put {
            key: c.step(decode_id)?,
            value: c.step(decode_id)?,
            overwrites: ref_set(c)?,
        },
        TAG_MARK => Op::Mark {
            start: anchor(c, ref_)?,
            end: anchor(c, ref_)?,
            kind_v: c.step(decode_id)?,
            value: c.step(decode_id)?,
            overwrites: ref_set(c)?,
        },
        other => return Err(DecodeError::InvalidOpTag(other)),
    };
    Ok(HashNode { pins, op })
}

// --- Unified operation type for batch encoding ---

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncodableOp {
    Run(Run),
    Node(HashNode),
}

pub fn encode_op(op: &EncodableOp, buf: &mut Vec<u8>) {
    match op {
        EncodableOp::Run(run) => {
            buf.push(TAG_RUN);
            encode_run(run, buf);
        }
        EncodableOp::Node(node) => encode_hash_node(node, buf),
    }
}

pub fn decode_op(bytes: &[u8]) -> Result<(EncodableOp, usize), DecodeError> {
    let mut c = Cursor { bytes, pos: 0 };
    let tag = c.byte()?;
    let op = if tag == TAG_RUN {
        EncodableOp::Run(c.step(decode_run)?)
    } else {
        let node = decode_node_with(
            tag,
            &mut c,
            &mut |c| c.step(decode_id),
            &mut |c| c.step(decode_id_set),
        )?;
        EncodableOp::Node(node)
    };
    Ok((op, c.pos))
}

// --- HashSeq encoding/decoding ---
//
// Runs and removes share one dependency-ordered stream of tagged *blocks*.
// Every block exposes a list of ids — a run exposes its element ids, a remove
// block exposes its remove-op ids — and any id can be referenced positionally
// by its rank *within its kind*: `(run_rank, offset)` for a run element,
// `(remove_rank, offset)` for a remove op. This lets a run reference a remove
// it typed across (and a remove reference the run it deletes) without spending
// a 32-byte dictionary entry, while keeping both index spaces compact so the
// millions of remove targets in a delete-heavy doc don't pay for interleaving.
// Only ids no ref can resolve positionally — the origin, orphan deps on unknown
// nodes, and refs broken by a dependency cycle — fall back to the dictionary.
//
// Format: [origin][id_dict][blocks][orphans]

// Block tags (the blocks section only; orphans reuse the TAG_* op tags).
const BLK_RUN_AFTER: u8 = 0;
const BLK_RUN_BEFORE: u8 = 1;
const BLK_REMOVE_FWD: u8 = 2;
const BLK_REMOVE_BWD: u8 = 3;
const BLK_REMOVE_SINGLE: u8 = 4;
const BLK_REMOVE_OTHER: u8 = 5;

/// Encode a HashSeq to a compact byte representation.
///
/// Format:
/// - [origin: 32 bytes]               the document id (implicit dict entry 0)
/// - [num_ids: varint][id_1..id_n: 32 bytes each]
/// - [num_blocks][blocks...]          each block is `u8 tag` then, by tag:
///   - RUN_AFTER/BEFORE: { ref anchor, ref_set first_extra_deps, string,
///     varint n, n × (varint offset, ref_set) interior deps }; exposes its
///     element ids in order.
///   - REMOVE_FWD/BWD: { ref_set first_extra_deps, varint target_run_rank,
///     varint start, varint end } — a chain of single removes over a contiguous
///     span of that run's elements; exposes the link ids.
///   - REMOVE_SINGLE: { ref_set extra_deps, ref target }; exposes the link id.
///   - REMOVE_OTHER: { ref_set extra_deps, varint n_segments, segments } —
///     multi-target removes; exposes the remove-op id. Each segment's head
///     reuses the ref low bits: `r1` run-element range (rank, off, +count varint)
///     coalescing the consecutive elements a `remove_batch` deletes; `00` dict
///     singleton; `10` remove-op singleton.
/// - [num_orphans][orphans...]        tagged HashNodes with ref-encoded IDs
///
/// A `ref` is a varint whose low bits select the form, keeping the common
/// run-element ref at the cheapest 1-bit cost: `r1` → run element
/// `(run_rank << 1) | 1` + a varint offset; `00` → dictionary `dict_idx << 2`;
/// `10` → remove op `(remove_rank << 2) | 2` + a varint offset.
///
/// Blocks are emitted in one interleaved Kahn order (min-id tie-break) so a run
/// can reference a remove and vice-versa. Every remove → (run it targets) is a
/// *hard* edge with no dictionary fallback, so a remove never precedes a run it
/// deletes; the hard graph is bipartite (remove → run), hence acyclic, so an
/// order always exists. Soft edges (anchors, extra_deps) may cycle — concurrent
/// typists, or a run typed across a delete of its own element — and are broken
/// by force-emitting the smallest-id block whose hard edges are satisfied; its
/// unresolved soft refs fall back to the dictionary.
pub fn encode_hashseq(seq: &HashSeq) -> Vec<u8> {
    let origin = seq.origin();

    // A block emitted to the wire. Each exposes `exposed` (the ids it
    // produces, in offset order); references to those ids resolve to
    // `(this block's emit pos, offset)`.
    enum Payload {
        Run(usize),
        RemoveSpan {
            backwards: bool,
            first_extra_deps: BTreeSet<Id>,
            target_run: usize,
            start: usize,
            end: usize,
        },
        Single {
            extra_deps: BTreeSet<Id>,
            target: Id,
        },
        Other {
            extra_deps: BTreeSet<Id>,
            targets: Vec<Id>,
        },
    }
    struct Block {
        head_id: Id,
        exposed: Vec<Id>,
        payload: Payload,
    }

    // --- Canonical block derivation (a pure function of the op set) ---
    // Stored chain grouping is an arrival-order artifact (under concurrency,
    // whichever extension applied first extended the stored run); blocks
    // must not depend on it — deriving them from the op set is what makes
    // equal op sets encode to identical bytes (ENCODING_SPEC.md).
    //
    // Canonical insert chains follow the fast-path relation — element x
    // continues element p's chain iff x's op is `Insert{After(p)}` — with
    // the fork rule: the smallest-id extender continues the chain, every
    // other extender heads its own block. Chains extend *through* interior
    // extra-deps (typing across a delete never splits a canonical run).
    struct CanonRun {
        first_op: FirstOp,
        anchor: Id,
        first_deps: BTreeSet<Id>,
        text: String,
        interior: BTreeMap<usize, BTreeSet<Id>>,
        elements: Vec<NodeIdx>,
    }

    // Smallest-id After-child of an element. Stored-interior elements never
    // carry explicit afters (forks split the stored run), so their stored
    // successor is their only extender; at stored tails the Id-ordered
    // `afters` set decides (move-op siblings are not extenders).
    let smallest_after_child = |p: NodeIdx| -> Option<NodeIdx> {
        if let Loc::Run { run, pos } = seq.loc_of(p) {
            let r = &seq.runs[&run];
            if (pos as usize) + 1 < r.elements.len() {
                return Some(r.elements[pos as usize + 1]);
            }
        }
        seq.afters
            .get(&p)
            .into_iter()
            .flatten()
            .find(|a| matches!(seq.loc_of(*a), Loc::Run { .. }))
    };
    // Pins of one insert element, from wherever its stored run keeps them.
    let elem_pins = |e: NodeIdx| -> BTreeSet<Id> {
        let Loc::Run { run, pos } = seq.loc_of(e) else {
            unreachable!("insert elements live in runs")
        };
        let r = &seq.runs[&run];
        if pos == 0 {
            r.first_extra_deps.to_id_set(&seq.ids)
        } else {
            r.interior_extra_deps
                .get(&(pos as usize))
                .map(|d| d.to_id_set(&seq.ids))
                .unwrap_or_default()
        }
    };
    // One element's anchor: (side, anchor id, anchor element if it is one).
    let elem_anchor = |e: NodeIdx| -> (FirstOp, Id, Option<NodeIdx>) {
        let Loc::Run { run, pos } = seq.loc_of(e) else {
            unreachable!("insert elements live in runs")
        };
        if pos > 0 {
            let p = seq.runs[&run].elements[pos as usize - 1];
            (FirstOp::After, seq.id_of(p), Some(p))
        } else {
            let r = &seq.runs[&run];
            let anchor_elem = seq
                .idx_of(&r.anchor)
                .filter(|a| matches!(seq.loc_of(*a), Loc::Run { .. }));
            (r.first_op, r.anchor, anchor_elem)
        }
    };

    let mut canon_runs: Vec<CanonRun> = Vec::new();
    // element handle -> (canonical run index, offset); u32::MAX = unset.
    let mut elem_canon: Vec<(u32, u32)> = vec![(u32::MAX, 0); seq.ids.len()];
    for stored in seq.runs.values() {
        for &e in &stored.elements {
            let (first_op, anchor, anchor_elem) = elem_anchor(e);
            let continues = first_op == FirstOp::After
                && anchor_elem.is_some_and(|p| smallest_after_child(p) == Some(e));
            if continues {
                continue; // an interior member of some canonical chain
            }
            // e heads a canonical run: walk smallest-child extensions.
            let ci = canon_runs.len() as u32;
            let mut elements: Vec<NodeIdx> = Vec::new();
            let mut text = String::new();
            let mut interior = BTreeMap::new();
            let mut cur = e;
            loop {
                elem_canon[cur.0 as usize] = (ci, elements.len() as u32);
                if !elements.is_empty() {
                    let pins = elem_pins(cur);
                    if !pins.is_empty() {
                        interior.insert(elements.len(), pins);
                    }
                }
                elements.push(cur);
                text.push(seq.char_at(cur));
                match smallest_after_child(cur) {
                    Some(next) => cur = next,
                    None => break,
                }
            }
            canon_runs.push(CanonRun {
                first_op,
                anchor,
                first_deps: elem_pins(e),
                text,
                interior,
                elements,
            });
        }
    }

    let mut blocks: Vec<Block> = Vec::new();
    for (ci, cr) in canon_runs.iter().enumerate() {
        let exposed: Vec<Id> = cr.elements.iter().map(|e| seq.id_of(*e)).collect();
        blocks.push(Block {
            head_id: exposed[0],
            exposed,
            payload: Payload::Run(ci),
        });
    }

    // Canonical remove chains: link r2 continues r1 iff r2's pins are
    // exactly `{id(r1)}` — the relation decode synthesizes — with the same
    // smallest-id fork rule. The derivation may join chains a replica
    // stored apart and split where a smaller-id contender chains the same
    // link; a link that loses its fork heads a chain whose first deps are
    // `{predecessor}`, which reconstructs identically.
    // link -> (stored chain key, index within it)
    let mut link_pos: FxHashMap<NodeIdx, (NodeIdx, usize)> = FxHashMap::default();
    for (&key, chain) in &seq.remove_runs {
        for (i, &l) in chain.links.iter().enumerate() {
            link_pos.insert(l, (key, i));
        }
    }
    // link r -> stored-chain heads whose first deps are exactly {id(r)}.
    let mut heads_pinning: FxHashMap<NodeIdx, Vec<NodeIdx>> = FxHashMap::default();
    for chain in seq.remove_runs.values() {
        let deps: Vec<Id> = chain.first_extra_deps.iter_ids(&seq.ids).collect();
        if let [d] = deps[..]
            && let Some(di) = seq.idx_of(&d)
            && link_pos.contains_key(&di)
        {
            heads_pinning.entry(di).or_default().push(chain.links[0]);
        }
    }
    let next_link = |r: NodeIdx| -> Option<NodeIdx> {
        let (key, i) = link_pos[&r];
        let stored_next = seq.remove_runs[&key].links.get(i + 1).copied();
        let contenders = heads_pinning.get(&r).into_iter().flatten().copied();
        stored_next
            .into_iter()
            .chain(contenders)
            .min_by_key(|l| seq.id_of(*l))
    };
    // Does r canonically continue its pinned predecessor?
    let link_continues = |r: NodeIdx| -> bool {
        let (key, i) = link_pos[&r];
        let parent = if i > 0 {
            Some(seq.remove_runs[&key].links[i - 1])
        } else {
            let deps: Vec<Id> = seq.remove_runs[&key].first_extra_deps.iter_ids(&seq.ids).collect();
            match deps[..] {
                [d] => seq.idx_of(&d).filter(|di| link_pos.contains_key(di)),
                _ => None,
            }
        };
        parent.is_some_and(|p| next_link(p) == Some(r))
    };

    // Remove blocks: each canonical chain is segmented into maximal spans
    // contiguous within one canonical run. Multi-link spans become
    // remove-runs; isolated links become singles; non-element targets
    // become others. Segments after the first synthesize extra_deps =
    // {previous remove id} — exactly the deps those links carry, so decode
    // reconstructs identical nodes.
    let elem_of = |idx: NodeIdx| -> Option<(usize, usize)> {
        let (ci, off) = elem_canon[idx.0 as usize];
        (ci != u32::MAX).then_some((ci as usize, off as usize))
    };

    let mut canon_chains: Vec<(Vec<NodeIdx>, Vec<NodeIdx>, BTreeSet<Id>)> = Vec::new();
    for stored in seq.remove_runs.values() {
        for (i, &l) in stored.links.iter().enumerate() {
            if link_continues(l) {
                continue;
            }
            let first_deps = if i > 0 {
                BTreeSet::from_iter([seq.id_of(stored.links[i - 1])])
            } else {
                stored.first_extra_deps.to_id_set(&seq.ids)
            };
            let mut links = Vec::new();
            let mut targets = Vec::new();
            let mut cur = l;
            loop {
                let (key, j) = link_pos[&cur];
                links.push(cur);
                targets.push(seq.remove_runs[&key].targets[j]);
                match next_link(cur) {
                    Some(next) => cur = next,
                    None => break,
                }
            }
            canon_chains.push((links, targets, first_deps));
        }
    }
    canon_chains.sort_by_key(|(links, _, _)| seq.id_of(links[0]));
    for (links, targets, first_deps) in &canon_chains {
        let mut i = 0;
        while i < targets.len() {
            let deps = if i == 0 {
                first_deps.clone()
            } else {
                BTreeSet::from_iter([seq.id_of(links[i - 1])])
            };
            match elem_of(targets[i]) {
                Some((run_head, elem_idx)) => {
                    // Greedy span: stay in the same canonical run, stepping
                    // ±1 in a consistent direction.
                    let mut j = i + 1;
                    let mut backwards = None;
                    let mut last = elem_idx;
                    while j < targets.len() {
                        let Some((r2, e2)) = elem_of(targets[j]) else {
                            break;
                        };
                        if r2 != run_head {
                            break;
                        }
                        let step = match backwards {
                            None if e2 == last + 1 => Some(false),
                            None if e2 + 1 == last => Some(true),
                            Some(false) if e2 == last + 1 => Some(false),
                            Some(true) if e2 + 1 == last => Some(true),
                            _ => break,
                        };
                        backwards = step;
                        last = e2;
                        j += 1;
                    }
                    let exposed: Vec<Id> = links[i..j].iter().map(|l| seq.id_of(*l)).collect();
                    if j - i > 1 {
                        blocks.push(Block {
                            head_id: exposed[0],
                            exposed,
                            payload: Payload::RemoveSpan {
                                backwards: backwards == Some(true),
                                first_extra_deps: deps,
                                target_run: run_head,
                                start: elem_idx,
                                end: last,
                            },
                        });
                    } else {
                        blocks.push(Block {
                            head_id: exposed[0],
                            exposed,
                            payload: Payload::Single {
                                extra_deps: deps,
                                target: seq.id_of(targets[i]),
                            },
                        });
                    }
                    i = j;
                }
                None => {
                    let id = seq.id_of(links[i]);
                    blocks.push(Block {
                        head_id: id,
                        exposed: vec![id],
                        payload: Payload::Other {
                            extra_deps: deps,
                            targets: vec![seq.id_of(targets[i])],
                        },
                    });
                    i += 1;
                }
            }
        }
    }

    // Multi-target removes (identity-preserving — decode rebuilds the exact
    // Op::Remove set so the node id survives).
    let mut multi_removes: Vec<(NodeIdx, &CausalRemove)> =
        seq.remove_nodes.iter().map(|(i, r)| (*i, r)).collect();
    multi_removes.sort_by_key(|(idx, _)| seq.id_of(*idx));
    for (idx, remove) in &multi_removes {
        let id = seq.id_of(*idx);
        let targets = remove.nodes.iter().map(|t| seq.id_of(*t)).collect();
        blocks.push(Block {
            head_id: id,
            exposed: vec![id],
            payload: Payload::Other {
                extra_deps: remove.pins.to_id_set(&seq.ids),
                targets,
            },
        });
    }

    // Sort blocks by head id so provisional index == id order: BTreeSet<usize>
    // frontiers below are then deterministic, and the min-id tie-break is just
    // the smallest index.
    blocks.sort_by_key(|b| b.head_id);
    let nb = blocks.len();

    // id -> (block, offset) that exposes it.
    let mut producer: FxHashMap<Id, (usize, usize)> = FxHashMap::default();
    for (b, block) in blocks.iter().enumerate() {
        for (off, id) in block.exposed.iter().enumerate() {
            producer.insert(*id, (b, off));
        }
    }
    // canonical run index -> block index (remove-span target edges + emit).
    let mut run_block: FxHashMap<usize, usize> = FxHashMap::default();
    for (b, block) in blocks.iter().enumerate() {
        if let Payload::Run(ci) = block.payload {
            run_block.insert(ci, b);
        }
    }

    // --- Dependency-order the blocks ---
    // Runs and removes interleave in one emit order so a run can positionally
    // reference a remove it typed across (run→remove) and a remove can reference
    // an earlier remove (remove→remove) — both used to spend a dictionary entry.
    // But positional refs address an id by its rank *within its kind*
    // (`run_rank` / `remove_rank`), not by global emit position, so a remove
    // target still indexes into the compact `0..num_runs` range regardless of
    // interleaving — no varint inflation on the millions of target refs.
    //
    // `hard_indeg` counts the unbreakable edges — every remove → (run it
    // targets). A remove's targets are addressed with no dictionary fallback, so
    // a remove must never be emitted before a run it deletes. This also pins
    // wide multi-target removes behind all their targets, so one cyclic
    // reference can't force a remove ahead of the hundreds of innocent runs it
    // points at (dumping every such target into the dict). The hard graph is
    // bipartite (remove → run only), hence acyclic, so a valid order always
    // exists; only soft edges (refs that *can* fall back to the dict — anchors,
    // extra_deps) are broken to resolve a cycle.
    let is_run = |b: usize| matches!(blocks[b].payload, Payload::Run(_));

    // Every id-valued position in a block body; `hard` marks remove→target
    // refs (encoded with no dictionary fallback). A RemoveSpan's target is a
    // raw run rank, not an id ref — its hard edge is added separately below.
    let visit_refs = |block: &Block, f: &mut dyn FnMut(&Id, bool)| match &block.payload {
        Payload::Run(ci) => {
            let cr = &canon_runs[*ci];
            f(&cr.anchor, false);
            for id in &cr.first_deps {
                f(id, false);
            }
            for deps in cr.interior.values() {
                for id in deps {
                    f(id, false);
                }
            }
        }
        Payload::RemoveSpan {
            first_extra_deps, ..
        } => {
            for id in first_extra_deps {
                f(id, false);
            }
        }
        Payload::Single { extra_deps, target } => {
            for id in extra_deps {
                f(id, false);
            }
            f(target, true);
        }
        Payload::Other { extra_deps, targets } => {
            for id in extra_deps {
                f(id, false);
            }
            for t in targets {
                f(t, true);
            }
        }
    };

    let mut indeg = vec![0usize; nb];
    let mut hard_indeg = vec![0usize; nb];
    let mut children: Vec<Vec<(usize, bool)>> = vec![Vec::new(); nb];
    {
        let mut soft: FxHashSet<usize> = FxHashSet::default();
        let mut hard: FxHashSet<usize> = FxHashSet::default();
        for (b, block) in blocks.iter().enumerate() {
            soft.clear();
            hard.clear();
            visit_refs(block, &mut |id, is_hard| {
                if let Some(&(pb, _)) = producer.get(id)
                    && pb != b
                {
                    if is_hard { &mut hard } else { &mut soft }.insert(pb);
                }
            });
            if let Payload::RemoveSpan { target_run, .. } = &block.payload
                && let Some(&tb) = run_block.get(target_run)
                && tb != b
            {
                hard.insert(tb);
            }
            for h in &hard {
                soft.remove(h); // a parent referenced both ways counts as hard
            }
            hard_indeg[b] = hard.len();
            indeg[b] = soft.len() + hard.len();
            for &p in &hard {
                children[p].push((b, true));
            }
            for &p in &soft {
                children[p].push((b, false));
            }
        }
    }

    let mut emit_pos = vec![usize::MAX; nb]; // block index -> emit position
    let mut order: Vec<usize> = Vec::with_capacity(nb);
    {
        let mut ready: BTreeSet<usize> = (0..nb).filter(|&i| indeg[i] == 0).collect();
        let mut blocked: BTreeSet<usize> = (0..nb).filter(|&i| indeg[i] != 0).collect();
        while order.len() < nb {
            let i = match ready.pop_first() {
                Some(i) => i,
                // Soft cycle: force-emit the smallest-id block whose hard edges
                // are satisfied (one always exists — the hard graph is acyclic).
                None => {
                    let i = *blocked
                        .iter()
                        .find(|&&i| hard_indeg[i] == 0)
                        .expect("hard-edge DAG always has a free block");
                    blocked.remove(&i);
                    i
                }
            };
            emit_pos[i] = order.len();
            order.push(i);
            for &(c, hard) in &children[i] {
                if emit_pos[c] != usize::MAX {
                    continue;
                }
                indeg[c] -= 1;
                if hard {
                    hard_indeg[c] -= 1;
                }
                if indeg[c] == 0 && blocked.remove(&c) {
                    ready.insert(c);
                }
            }
        }
    }

    // Rank of each block within its kind, in emit order: positional refs address
    // a run element by (run_rank, offset) and a remove id by (remove_rank,
    // offset), keeping both index spaces compact.
    let mut run_rank = vec![usize::MAX; nb];
    let mut remove_rank = vec![usize::MAX; nb];
    {
        let mut nr = 0;
        let mut nrm = 0;
        for &b in &order {
            if is_run(b) {
                run_rank[b] = nr;
                nr += 1;
            } else {
                remove_rank[b] = nrm;
                nrm += 1;
            }
        }
    }

    const NO_LIMIT: usize = usize::MAX;
    // Resolve an id to an earlier-emitted block: (is_run, within-kind rank,
    // offset). `limit` is the referencing block's emit position; a ref may only
    // point at a strictly-earlier block.
    let resolve = |id: &Id, limit: usize| -> Option<(bool, usize, usize)> {
        let &(pb, off) = producer.get(id)?;
        if emit_pos[pb] >= limit {
            return None;
        }
        if is_run(pb) {
            Some((true, run_rank[pb], off))
        } else {
            Some((false, remove_rank[pb], off))
        }
    };

    // The trailing node section carries everything that is not a block:
    // parked orphans, applied move ops (placement registers), and gated
    // (quarantined) nodes — all as tagged nodes with ref-encoded ids,
    // sorted by node id for deterministic bytes.
    let orphans: Vec<HashNode> = {
        let mut nodes: Vec<(Id, HashNode)> = seq
            .delivery
            .held()
            .map(|(id, node)| (*id, node.clone()))
            .collect();
        for (idx, mv) in &seq.move_nodes {
            nodes.push((seq.id_of(*idx), seq.move_node(*idx, mv)));
        }
        for (idx, mk) in &seq.mark_nodes {
            nodes.push((seq.id_of(*idx), seq.mark_node(mk)));
        }
        nodes.sort_by_key(|(id, _)| *id);
        nodes.into_iter().map(|(_, node)| node).collect()
    };

    // --- Build the ID dictionary ---
    // Only ids no ref can resolve positionally: the origin, refs to a block
    // not yet emitted (cycle-broken), same-block refs, and orphan deps on
    // unknown nodes. Remove-span targets are raw block indices (never dict).
    let mut id_set: BTreeSet<Id> = BTreeSet::new();
    {
        let mut note = |id: &Id, limit: usize| {
            if resolve(id, limit).is_none() {
                id_set.insert(*id);
            }
        };
        for (b, block) in blocks.iter().enumerate() {
            let limit = emit_pos[b];
            visit_refs(block, &mut |id, _| note(id, limit));
        }
        // Orphan refs are exactly `refs(u)` — value fields (Put key/value,
        // Mark kind/value) are commitments, not refs, and ride raw 32 B.
        for orphan in &orphans {
            for id in orphan.iter_refs() {
                note(id, NO_LIMIT);
            }
        }
    }

    // The origin is the implicit first dictionary entry: written once in the
    // header, refs to it cost a 1-byte dict ref.
    id_set.remove(&origin);
    let id_list: Vec<Id> = std::iter::once(origin).chain(id_set).collect();
    let id_to_idx: HashMap<Id, usize> =
        id_list.iter().enumerate().map(|(i, id)| (*id, i)).collect();

    // --- Emit ---
    let mut buf = Vec::new();

    encode_id(&origin, &mut buf);
    encode_varint(id_list.len() - 1, &mut buf);
    for id in &id_list[1..] {
        encode_id(id, &mut buf);
    }

    // Ref forms (low bits pick the form, keeping the common run-element ref at
    // the cheapest 1-bit cost): `xxx1` run element (rank<<1|1) + offset;
    // `xx00` dict (idx<<2); `xx10` remove (rank<<2|2) + offset.
    let encode_ref = |id: &Id, limit: usize, buf: &mut Vec<u8>| match resolve(id, limit) {
        Some((true, rank, off)) => {
            encode_varint((rank << 1) | 1, buf);
            encode_varint(off, buf);
        }
        Some((false, rank, off)) => {
            encode_varint((rank << 2) | 0b10, buf);
            encode_varint(off, buf);
        }
        None => encode_varint(id_to_idx[id] << 2, buf),
    };
    let encode_ref_set = |ids: &BTreeSet<Id>, limit: usize, buf: &mut Vec<u8>| {
        encode_varint(ids.len(), buf);
        for id in ids {
            encode_ref(id, limit, buf);
        }
    };

    encode_varint(nb, &mut buf);
    for (pe, &bi) in order.iter().enumerate() {
        let block = &blocks[bi];
        match &block.payload {
            Payload::Run(ci) => {
                let cr = &canon_runs[*ci];
                buf.push(match cr.first_op {
                    FirstOp::After => BLK_RUN_AFTER,
                    FirstOp::Before => BLK_RUN_BEFORE,
                });
                encode_ref(&cr.anchor, pe, &mut buf);
                encode_ref_set(&cr.first_deps, pe, &mut buf);
                encode_string(&cr.text, &mut buf);
                encode_varint(cr.interior.len(), &mut buf);
                for (offset, deps) in &cr.interior {
                    encode_varint(*offset, &mut buf);
                    encode_ref_set(deps, pe, &mut buf);
                }
            }
            Payload::RemoveSpan {
                backwards,
                first_extra_deps,
                target_run,
                start,
                end,
            } => {
                buf.push(if *backwards {
                    BLK_REMOVE_BWD
                } else {
                    BLK_REMOVE_FWD
                });
                encode_ref_set(first_extra_deps, pe, &mut buf);
                // Hard edge guarantees the target run is already emitted; address
                // it by run rank (the same compact space run-element refs use).
                encode_varint(run_rank[run_block[target_run]], &mut buf);
                encode_varint(*start, &mut buf);
                encode_varint(*end, &mut buf);
            }
            Payload::Single { extra_deps, target } => {
                buf.push(BLK_REMOVE_SINGLE);
                encode_ref_set(extra_deps, pe, &mut buf);
                encode_ref(target, pe, &mut buf);
            }
            Payload::Other { extra_deps, targets } => {
                buf.push(BLK_REMOVE_OTHER);
                encode_ref_set(extra_deps, pe, &mut buf);
                // Targets are a set (order is free), and a `remove_batch` deletes
                // a contiguous span — so most targets are consecutive elements of
                // one run. Coalesce those into (run_rank, start, count) ranges;
                // remove-op and non-element targets stay singletons. Sort each
                // group for determinism and contiguity.
                let mut elems: Vec<(usize, usize)> = Vec::new(); // (run_rank, off)
                let mut remove_singles: Vec<(usize, usize)> = Vec::new(); // (rank, off)
                let mut dicts: Vec<usize> = Vec::new();
                for t in targets {
                    match resolve(t, pe) {
                        Some((true, rank, off)) => elems.push((rank, off)),
                        Some((false, rank, off)) => remove_singles.push((rank, off)),
                        None => dicts.push(id_to_idx[t]),
                    }
                }
                elems.sort_unstable();
                remove_singles.sort_unstable();
                dicts.sort_unstable();
                // (run_rank, start_off, count) ranges over consecutive elements.
                let mut ranges: Vec<(usize, usize, usize)> = Vec::new();
                for (rank, off) in elems {
                    match ranges.last_mut() {
                        Some(last) if last.0 == rank && last.1 + last.2 == off => last.2 += 1,
                        _ => ranges.push((rank, off, 1)),
                    }
                }
                encode_varint(ranges.len() + remove_singles.len() + dicts.len(), &mut buf);
                // Each segment's head reuses the ref low bits; run-element heads
                // carry a trailing count (decode mirrors this).
                for (rank, start, count) in &ranges {
                    encode_varint((rank << 1) | 1, &mut buf);
                    encode_varint(*start, &mut buf);
                    encode_varint(*count, &mut buf);
                }
                for (rank, off) in &remove_singles {
                    encode_varint((rank << 2) | 0b10, &mut buf);
                    encode_varint(*off, &mut buf);
                }
                for idx in &dicts {
                    encode_varint(idx << 2, &mut buf);
                }
            }
        }
    }

    // Orphans and gated nodes: the shared node layout with ref-encoded IDs.
    encode_varint(orphans.len(), &mut buf);
    for orphan in &orphans {
        encode_node_with(
            orphan,
            &mut buf,
            &mut |id, buf| encode_ref(id, NO_LIMIT, buf),
            &mut |ids, buf| encode_ref_set(ids, NO_LIMIT, buf),
        );
    }

    buf
}

/// Decoded ids addressable by positional refs, by within-kind rank in emit
/// order: a run block appends its element ids to `runs`, a remove block its
/// remove-op ids to `removes`.
struct Ranks {
    runs: Vec<Vec<Id>>,
    removes: Vec<Vec<Id>>,
}

impl Ranks {
    fn run_elem(&self, rank: usize, off: usize) -> Result<Id, DecodeError> {
        self.runs
            .get(rank)
            .and_then(|e| e.get(off))
            .copied()
            .ok_or(DecodeError::InvalidIdIndex(rank))
    }
}

/// Resolve a wire `ref`: `xxx1` run element (rank, offset); `xx00` dictionary
/// index; `xx10` remove op (rank, offset).
fn decode_ref(c: &mut Cursor, id_list: &[Id], ranks: &Ranks) -> Result<Id, DecodeError> {
    let v = c.step(decode_varint)?;
    ref_from_head(v, c, id_list, ranks)
}

/// The tail of a ref whose head varint is already consumed (`BLK_REMOVE_OTHER`
/// segment heads reuse the ref forms, with run-element heads meaning a range).
fn ref_from_head(v: usize, c: &mut Cursor, id_list: &[Id], ranks: &Ranks) -> Result<Id, DecodeError> {
    if v & 1 == 1 {
        let off = c.step(decode_varint)?;
        ranks.run_elem(v >> 1, off)
    } else if v & 2 == 0 {
        id_list
            .get(v >> 2)
            .copied()
            .ok_or(DecodeError::InvalidIdIndex(v >> 2))
    } else {
        let off = c.step(decode_varint)?;
        ranks
            .removes
            .get(v >> 2)
            .and_then(|e| e.get(off))
            .copied()
            .ok_or(DecodeError::InvalidIdIndex(v >> 2))
    }
}

fn decode_ref_set(c: &mut Cursor, id_list: &[Id], ranks: &Ranks) -> Result<BTreeSet<Id>, DecodeError> {
    let count = c.step(decode_varint)?;
    let mut ids = BTreeSet::new();
    for _ in 0..count {
        ids.insert(decode_ref(c, id_list, ranks)?);
    }
    Ok(ids)
}

/// Strict acceptance (ENCODING_SPEC.md): decode, then verify the bytes are
/// the canonical encoding of the decoded op set by re-encoding. Only
/// strict-verified bytes may be cached, deduped, or fingerprinted as
/// canonical; `decode_hashseq` alone is transport mode (any well-formed
/// stream decodes — ops are self-certifying — but the bytes carry no
/// canonical status).
pub fn decode_hashseq_strict(bytes: &[u8]) -> Result<HashSeq, DecodeError> {
    let seq = decode_hashseq(bytes)?;
    if encode_hashseq(&seq) != bytes {
        return Err(DecodeError::NotCanonical);
    }
    Ok(seq)
}

/// Decode a HashSeq from its byte representation.
pub fn decode_hashseq(bytes: &[u8]) -> Result<HashSeq, DecodeError> {
    let mut c = Cursor { bytes, pos: 0 };

    // Origin header (also the implicit first dictionary entry)
    let origin = c.step(decode_id)?;
    let num_ids = c.step(decode_varint)?;
    let mut id_list: Vec<Id> = Vec::with_capacity(num_ids + 1);
    id_list.push(origin);
    for _ in 0..num_ids {
        id_list.push(c.step(decode_id)?);
    }

    let mut seq = HashSeq::new(origin);
    // Positional refs index `ranks` by within-kind rank (the ref's low bits
    // pick which kind); refs always point at an earlier block of that kind.
    let mut ranks = Ranks {
        runs: Vec::new(),
        removes: Vec::new(),
    };

    let num_blocks = c.step(decode_varint)?;
    for _ in 0..num_blocks {
        let tag = c.byte()?;
        match tag {
            BLK_RUN_AFTER | BLK_RUN_BEFORE => {
                let first_op = if tag == BLK_RUN_AFTER {
                    crate::run::FirstOp::After
                } else {
                    crate::run::FirstOp::Before
                };
                let run = decode_run_with(
                    &mut c,
                    first_op,
                    &mut |c| decode_ref(c, &id_list, &ranks),
                    &mut |c| decode_ref_set(c, &id_list, &ranks),
                )?;
                ranks.runs.push(run.elements.clone());
                // `from_text` computed the element ids from the wire content —
                // the authoritative derivation, so apply without rehashing.
                for (id, node) in run.decompress_with_ids() {
                    seq.apply_with_id(id, node);
                }
            }
            BLK_REMOVE_FWD | BLK_REMOVE_BWD => {
                // A chain of single-element removes over a contiguous span of
                // the target run's elements, each depending on the previous.
                // Forward walks the span ascending (delete-key bursts),
                // backward descending (backspace bursts).
                let first_extra_deps = decode_ref_set(&mut c, &id_list, &ranks)?;
                let target_run = c.step(decode_varint)?;
                let start_idx = c.step(decode_varint)?;
                let end_idx = c.step(decode_varint)?;

                let span: Vec<usize> = if tag == BLK_REMOVE_BWD {
                    (end_idx..=start_idx).rev().collect()
                } else {
                    (start_idx..=end_idx).collect()
                };
                let mut exposed = Vec::with_capacity(span.len());
                let mut prev_remove_id: Option<Id> = None;
                for elem_idx in span {
                    let removed_id = ranks.run_elem(target_run, elem_idx)?;
                    let pins = match prev_remove_id {
                        Some(prev_id) => BTreeSet::from_iter([prev_id]),
                        None => first_extra_deps.clone(),
                    };
                    let node = HashNode {
                        pins,
                        op: Op::Remove(BTreeSet::from_iter([removed_id])),
                    };
                    let id = node.id();
                    prev_remove_id = Some(id);
                    exposed.push(id);
                    seq.apply_with_id(id, node);
                }
                ranks.removes.push(exposed);
            }
            BLK_REMOVE_SINGLE => {
                let pins = decode_ref_set(&mut c, &id_list, &ranks)?;
                let target = decode_ref(&mut c, &id_list, &ranks)?;
                let node = HashNode {
                    pins,
                    op: Op::Remove(std::iter::once(target).collect()),
                };
                let id = node.id();
                ranks.removes.push(vec![id]);
                seq.apply_with_id(id, node);
            }
            BLK_REMOVE_OTHER => {
                let pins = decode_ref_set(&mut c, &id_list, &ranks)?;
                let num_segments = c.step(decode_varint)?;
                let mut targets = BTreeSet::new();
                // Each segment's head reuses the ref low bits (mirrors emit):
                // a run-element head means a range (rank, off, count); dict and
                // remove-op heads decode as plain ref tails.
                for _ in 0..num_segments {
                    let head = c.step(decode_varint)?;
                    if head & 1 == 1 {
                        let off = c.step(decode_varint)?;
                        let count = c.step(decode_varint)?;
                        for i in off..off + count {
                            targets.insert(ranks.run_elem(head >> 1, i)?);
                        }
                    } else {
                        targets.insert(ref_from_head(head, &mut c, &id_list, &ranks)?);
                    }
                }
                let node = HashNode {
                    pins,
                    op: Op::Remove(targets),
                };
                let id = node.id();
                ranks.removes.push(vec![id]);
                seq.apply_with_id(id, node);
            }
            other => return Err(DecodeError::InvalidOpTag(other)),
        }
    }

    // Orphans and gated nodes: the shared node layout with ref-encoded IDs.
    let num_orphans = c.step(decode_varint)?;
    for _ in 0..num_orphans {
        let tag = c.byte()?;
        let node = decode_node_with(
            tag,
            &mut c,
            &mut |c| decode_ref(c, &id_list, &ranks),
            &mut |c| decode_ref_set(c, &id_list, &ranks),
        )?;
        seq.apply(node);
    }

    Ok(seq)
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck_macros::quickcheck;

    fn test_id(n: u8) -> Id {
        let mut id = [0u8; 32];
        id[0] = n;
        Id(id)
    }

    #[test]
    fn test_varint_roundtrip() {
        for value in [0, 1, 127, 128, 255, 256, 16383, 16384, usize::MAX / 2] {
            let mut buf = Vec::new();
            encode_varint(value, &mut buf);
            let (decoded, size) = decode_varint(&buf).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(size, buf.len());
        }
    }

    #[test]
    fn test_id_roundtrip() {
        let id = test_id(42);
        let mut buf = Vec::new();
        encode_id(&id, &mut buf);
        assert_eq!(buf.len(), 32);

        let (decoded, size) = decode_id(&buf).unwrap();
        assert_eq!(decoded, id);
        assert_eq!(size, 32);
    }

    #[test]
    fn test_utf8_char_roundtrip() {
        for ch in ['a', 'z', '\u{00e9}', '\u{1f600}', '\u{4e2d}'] {
            let mut buf = Vec::new();
            encode_utf8_char(ch, &mut buf);
            let (decoded, size) = decode_utf8_char(&buf).unwrap();
            assert_eq!(decoded, ch);
            assert_eq!(size, buf.len());
        }
    }

    #[test]
    fn test_string_roundtrip() {
        for s in ["", "hello", "hello world", "\u{1f600}\u{1f601}"] {
            let mut buf = Vec::new();
            encode_string(s, &mut buf);
            let (decoded, size) = decode_string(&buf).unwrap();
            assert_eq!(decoded, s);
            assert_eq!(size, buf.len());
        }
    }

    #[test]
    fn test_run_roundtrip() {
        let anchor = test_id(0);
        let mut run = Run::new(anchor, BTreeSet::new(), 'a');
        run.extend('b');
        run.extend('c');

        let mut buf = Vec::new();
        encode_run(&run, &mut buf);

        let (decoded, size) = decode_run(&buf).unwrap();
        assert_eq!(decoded, run);
        assert_eq!(size, buf.len());
    }

    #[test]
    fn test_run_with_deps_roundtrip() {
        let anchor = test_id(0);
        let mut deps = BTreeSet::new();
        deps.insert(test_id(1));
        deps.insert(test_id(2));

        let mut run = Run::new(anchor, deps, 'x');
        run.extend('y');

        let mut buf = Vec::new();
        encode_run(&run, &mut buf);

        let (decoded, size) = decode_run(&buf).unwrap();
        assert_eq!(decoded, run);
        assert_eq!(size, buf.len());
    }

    fn node_roundtrips(pins: impl IntoIterator<Item = Id>, op: Op) {
        let node = HashNode {
            pins: pins.into_iter().collect(),
            op,
        };
        let mut buf = Vec::new();
        encode_hash_node(&node, &mut buf);
        let (decoded, size) = decode_op(&buf).unwrap();
        assert_eq!(decoded, EncodableOp::Node(node));
        assert_eq!(size, buf.len());
    }

    /// Every op kind's standalone wire form roundtrips through the shared
    /// node layout, with and without pins.
    #[test]
    fn test_standalone_node_forms_roundtrip() {
        node_roundtrips([], Op::insert_after(Id::default(), 'a'));
        node_roundtrips([test_id(9)], Op::insert_before(test_id(5), 'z'));
        node_roundtrips(
            [],
            Op::Remove([test_id(1), test_id(2), test_id(3)].into()),
        );
        node_roundtrips(
            [test_id(9)],
            Op::Move {
                target: test_id(1),
                to: Anchor::After(test_id(2)),
                overwrites: [test_id(3)].into(),
            },
        );
        node_roundtrips(
            [],
            Op::Put {
                key: test_id(4),
                value: test_id(5),
                overwrites: [test_id(6), test_id(7)].into(),
            },
        );
        node_roundtrips(
            [test_id(9)],
            Op::Mark {
                start: Anchor::Before(test_id(1)),
                end: Anchor::After(test_id(2)),
                kind_v: test_id(3),
                value: test_id(4),
                overwrites: [].into(),
            },
        );
    }

    #[test]
    fn test_unicode_run() {
        let anchor = test_id(0);
        let mut run = Run::new(anchor, BTreeSet::new(), '\u{1f600}');
        run.extend('\u{4e2d}');
        run.extend('\u{00e9}');

        let mut buf = Vec::new();
        encode_run(&run, &mut buf);

        let (decoded, size) = decode_run(&buf).unwrap();
        assert_eq!(decoded, run);
        assert_eq!(size, buf.len());
    }

    #[quickcheck]
    fn prop_run_roundtrip(run: Run) -> bool {
        let mut buf = Vec::new();
        encode_run(&run, &mut buf);

        let (decoded, size) = decode_run(&buf).unwrap();
        size == buf.len() && decoded == run
    }

    // --- HashSeq encoder roundtrip tests ---

    #[test]
    fn test_hashseq_empty_roundtrip() {
        let seq = HashSeq::default();
        let encoded = encode_hashseq(&seq);
        let decoded = decode_hashseq(&encoded).unwrap();

        assert_eq!(
            seq.iter().collect::<String>(),
            decoded.iter().collect::<String>()
        );
        assert_eq!(seq, decoded);
    }

    #[test]
    fn test_hashseq_simple_roundtrip() {
        let mut seq = HashSeq::default();
        seq.insert(0, 'h');
        seq.insert(1, 'e');
        seq.insert(2, 'l');
        seq.insert(3, 'l');
        seq.insert(4, 'o');

        let encoded = encode_hashseq(&seq);
        let decoded = decode_hashseq(&encoded).unwrap();

        assert_eq!(decoded.iter().collect::<String>(), "hello");
        assert_eq!(seq, decoded);
    }

    #[test]
    fn test_hashseq_with_removes_roundtrip() {
        let mut seq = HashSeq::default();
        seq.insert(0, 'a');
        seq.insert(1, 'b');
        seq.insert(2, 'c');
        seq.remove(1);

        let encoded = encode_hashseq(&seq);
        let decoded = decode_hashseq(&encoded).unwrap();

        assert_eq!(decoded.iter().collect::<String>(), "ac");
        assert_eq!(seq, decoded);
    }

    #[test]
    fn test_hashseq_batch_insert_roundtrip() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "hello world".chars());

        let encoded = encode_hashseq(&seq);
        let decoded = decode_hashseq(&encoded).unwrap();

        assert_eq!(decoded.iter().collect::<String>(), "hello world");
        assert_eq!(seq, decoded);
    }

    #[test]
    fn test_hashseq_complex_roundtrip() {
        let mut seq = HashSeq::default();

        seq.insert_batch(0, "hello".chars());
        seq.insert(0, 'X');
        seq.insert(6, 'Y');
        seq.remove(3);

        let original_str: String = seq.iter().collect();

        let encoded = encode_hashseq(&seq);
        let decoded = decode_hashseq(&encoded).unwrap();

        assert_eq!(decoded.iter().collect::<String>(), original_str);
        assert_eq!(seq, decoded);
    }

    /// A backspace burst — chains the OpRef encoder compresses heavily.
    #[test]
    fn test_hashseq_backspace_chain() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "abcdefghij".chars());
        for _ in 0..5 {
            seq.remove(seq.len() - 1);
        }

        let original_str: String = seq.iter().collect();
        assert_eq!(original_str, "abcde");

        let encoded = encode_hashseq(&seq);
        let decoded = decode_hashseq(&encoded).unwrap();

        assert_eq!(decoded.iter().collect::<String>(), original_str);
        assert_eq!(seq, decoded);
    }

    /// Two replicas editing concurrently with periodic cross-merges: each
    /// side's runs carry deps on the other's elements, producing exactly the
    /// run-level reference cycles the encoder's dependency ordering must
    /// break deterministically (force-emit + dict fallback).
    #[quickcheck]
    fn prop_roundtrip_after_merge(a: Vec<(bool, u8, char)>, b: Vec<(bool, u8, char)>) -> bool {
        fn apply(seq: &mut HashSeq, ops: &[(bool, u8, char)]) {
            for &(is_insert, idx, ch) in ops {
                let idx = idx as usize;
                if is_insert {
                    seq.insert(idx.min(seq.len()), ch);
                } else if !seq.is_empty() {
                    seq.remove(idx.min(seq.len() - 1));
                }
            }
        }
        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();
        for (chunk_a, chunk_b) in a.chunks(3).zip(b.chunks(3)) {
            apply(&mut seq_a, chunk_a);
            apply(&mut seq_b, chunk_b);
            seq_a.merge(seq_b.clone());
            seq_b.merge(seq_a.clone());
        }

        let encoded = encode_hashseq(&seq_a);
        let decoded = decode_hashseq(&encoded).unwrap();
        // Blocks derive from the op set, never replica storage, so the
        // encoding is byte-canonical: the decoded copy (whose stored chain
        // grouping may differ) re-encodes to identical bytes, and the two
        // replicas (equal op sets, different arrival orders) encode
        // identically too.
        let encoded2 = encode_hashseq(&decoded);
        decoded == seq_a
            && decoded.iter().collect::<String>() == seq_a.iter().collect::<String>()
            && encoded2 == encoded
            && encode_hashseq(&seq_b) == encoded
            && decode_hashseq_strict(&encoded).is_ok()
    }

    /// The canonical snapshot vector (owed by GRAMMAR_SPEC.md once Part B
    /// normalized): one concurrent document's exact bytes, locked by hash.
    /// A change to block derivation or the stream grammar is a
    /// canonical-form change and must update this hash *knowingly* —
    /// unlike the Part A identity vectors, which never change.
    #[test]
    fn canonical_snapshot_vector() {
        let mut a = HashSeq::default();
        let mut b = HashSeq::default();
        a.insert_batch(0, "hello world".chars());
        b.merge(a.clone());
        a.insert(5, '!');
        b.remove_batch(0, 3);
        a.merge(b.clone());
        b.merge(a.clone());
        assert_eq!(a, b);
        // A move, a splice-anchored insert, and a mark, for trailing-section
        // coverage.
        let e0 = a.id_at(0).unwrap();
        let last = a.id_at(a.len() - 1).unwrap();
        let mv = a.move_element(e0, crate::Anchor::After(last));
        a.apply(HashNode {
            pins: BTreeSet::new(),
            op: Op::insert_after(mv.id(), 'x'),
        });
        let s0 = a.id_at(0).unwrap();
        let s2 = a.id_at(2).unwrap();
        a.mark_range(
            crate::Anchor::Before(s0),
            crate::Anchor::After(s2),
            crate::value::Value::String("bold".into()).value_id(),
            crate::value::Value::Bool(true).value_id(),
        );
        b.merge(a.clone());

        let bytes = encode_hashseq(&a);
        assert_eq!(encode_hashseq(&b), bytes, "equal sets, equal bytes");
        assert_eq!(
            blake3::hash(&bytes).to_hex().as_str(),
            "bedb884305a836b82a0164b96c8b37a61f59313a7c55e94ca00d33242af8acaf",
            "canonical snapshot bytes moved — bump knowingly"
        );
    }

    /// Strict mode rejects well-formed but noncanonical bytes: a two-char
    /// typing chain encoded as two single-element run blocks decodes to the
    /// same op set the canonical single-block form does — transport mode
    /// accepts it, strict mode does not.
    #[test]
    fn strict_decode_rejects_noncanonical_grouping() {
        let mut noncanon = Vec::new();
        encode_id(&Id::default(), &mut noncanon); // origin header
        encode_varint(0, &mut noncanon); // empty dict
        encode_varint(2, &mut noncanon); // two blocks
        // block 0: run "a" anchored at the origin (dict entry 0)
        noncanon.push(BLK_RUN_AFTER);
        encode_varint(0 << 2, &mut noncanon); // dict ref 0
        encode_varint(0, &mut noncanon); // no first deps
        encode_string("a", &mut noncanon);
        encode_varint(0, &mut noncanon); // no interior deps
        // block 1: run "b" anchored After(run 0, elem 0) — the canonical
        // form would extend block 0 instead (sole extender = smallest).
        noncanon.push(BLK_RUN_AFTER);
        encode_varint(1, &mut noncanon); // run-element ref (rank 0, ...)
        encode_varint(0, &mut noncanon); // ... offset 0
        encode_varint(0, &mut noncanon);
        encode_string("b", &mut noncanon);
        encode_varint(0, &mut noncanon);
        encode_varint(0, &mut noncanon); // no orphans

        let decoded = decode_hashseq(&noncanon).expect("transport mode accepts");
        assert_eq!(decoded.iter().collect::<String>(), "ab");
        assert_eq!(
            decode_hashseq_strict(&noncanon),
            Err(DecodeError::NotCanonical)
        );
        // The canonical bytes for the same op set: one two-element run.
        let canonical = encode_hashseq(&decoded);
        assert_ne!(canonical, noncanon);
        let strict = decode_hashseq_strict(&canonical).expect("canonical bytes verify");
        assert_eq!(strict, decoded);
    }

    /// Equal op sets encode to identical bytes even when replicas stored
    /// their chains differently: a fork whose smaller-id extender arrived
    /// second is grouped by id, not arrival.
    #[test]
    fn encoding_is_storage_independent() {
        // Replica 1: type "ab", then a concurrent fork "c" after 'a' arrives.
        let mut r1 = HashSeq::default();
        r1.insert_batch(0, "ab".chars());
        let a = r1.id_at(0).unwrap();
        let fork = HashNode {
            pins: BTreeSet::new(),
            op: Op::insert_after(a, 'c'),
        };
        let mut r2 = HashSeq::default();
        // Replica 2 sees the fork before the run continuation.
        for (id, node) in r1.all_nodes().into_iter().take(1) {
            r2.apply_with_id(id, node); // 'a'
        }
        r2.apply(fork.clone());
        for (id, node) in r1.all_nodes() {
            r2.apply_with_id(id, node); // 'b' (and 'a' dedup)
        }
        r1.apply(fork);

        assert_eq!(r1, r2);
        assert_eq!(encode_hashseq(&r1), encode_hashseq(&r2));
    }

    /// Deterministic cycle shape: two concurrent runs that each extend with
    /// an interior dep on the other's element.
    #[test]
    fn roundtrip_with_run_level_dep_cycle() {
        let mut a = HashSeq::default();
        let mut b = HashSeq::default();
        a.insert(0, 'a');
        b.insert(0, 'b');
        a.merge(b.clone());
        b.merge(a.clone());
        assert_eq!(a, b);

        // Extend the trailing run with a dep on the other run's tip (insert
        // at end), then extend the leading run likewise (insert at pos 1).
        a.insert(2, 'x');
        a.insert(1, 'y');

        let encoded = encode_hashseq(&a);
        let decoded = decode_hashseq(&encoded).unwrap();
        assert_eq!(decoded, a);
        assert_eq!(
            decoded.iter().collect::<String>(),
            a.iter().collect::<String>()
        );
        assert_eq!(encode_hashseq(&decoded), encoded);
    }

    /// Drive a seq from fuzz ops: (insert?, index, char), indices clamped.
    fn seq_from_ops(ops: &[(bool, u8, char)]) -> HashSeq {
        let mut seq = HashSeq::default();
        for &(is_insert, idx, ch) in ops {
            let idx = idx as usize;
            if is_insert {
                let at = if seq.is_empty() { 0 } else { idx % (seq.len() + 1) };
                seq.insert(at, ch);
            } else if !seq.is_empty() {
                seq.remove(idx % seq.len());
            }
        }
        seq
    }

    #[quickcheck]
    fn prop_hashseq_roundtrip_preserves_content(ops: Vec<(bool, u8, char)>) -> bool {
        let seq = seq_from_ops(&ops);
        let decoded = decode_hashseq(&encode_hashseq(&seq)).unwrap();
        seq.iter().collect::<String>() == decoded.iter().collect::<String>()
    }

    #[quickcheck]
    fn prop_hashseq_roundtrip_preserves_equality(ops: Vec<(bool, u8, char)>) -> bool {
        let seq = seq_from_ops(&ops);
        seq == decode_hashseq(&encode_hashseq(&seq)).unwrap()
    }

    /// Encoding the same logical state twice must produce bit-identical bytes,
    /// even when the underlying `HashMap`s have different randomization seeds.
    /// Decoded seqs use fresh `HashMap`s, so this round-trip is a real cross-seed test.
    #[quickcheck]
    fn prop_encoding_is_deterministic(ops: Vec<(bool, u8, char)>) -> bool {
        let seq = seq_from_ops(&ops);
        let first = encode_hashseq(&seq);
        let second = encode_hashseq(&decode_hashseq(&first).unwrap());
        first == second
    }

    #[quickcheck]
    fn prop_hashseq_batch_roundtrip(text: String, remove_indices: Vec<u8>) -> bool {
        let mut seq = HashSeq::default();

        if !text.is_empty() {
            seq.insert_batch(0, text.chars());

            for idx in remove_indices {
                if !seq.is_empty() {
                    let remove_idx = idx as usize % seq.len();
                    seq.remove(remove_idx);
                }
            }
        }

        let original_str: String = seq.iter().collect();

        let encoded = encode_hashseq(&seq);
        let decoded = decode_hashseq(&encoded).unwrap();

        original_str == decoded.iter().collect::<String>() && seq == decoded
    }
}

#[cfg(test)]
mod remove_roundtrip {
    use super::*;
    use quickcheck_macros::quickcheck;

    /// Regression: multi-target removes used to be decomposed into N single
    /// removes at encode time, changing their node identity — any later op
    /// depending on the original remove id would orphan forever after decode.
    #[test]
    fn test_multi_target_remove_roundtrip_identity() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "abc".chars());
        seq.remove_batch(0, 2); // one Remove node targeting two elements
        seq.insert(0, 'x'); // depends on the multi-target remove's id

        let encoded = encode_hashseq(&seq);
        let decoded = decode_hashseq(&encoded).unwrap();

        assert_eq!(
            decoded.iter().collect::<String>(),
            seq.iter().collect::<String>(),
        );
        assert_eq!(seq, decoded, "tips must survive the roundtrip");
    }

    /// Backspace bursts coalesce into a single in-memory RemoveRun.
    #[test]
    fn test_backspace_burst_coalesces() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "abcdefgh".chars());
        for _ in 0..5 {
            seq.remove(seq.len() - 1);
        }
        assert_eq!(seq.iter().collect::<String>(), "abc");
        assert_eq!(seq.remove_runs.len(), 1, "one chain for the whole burst");
        let chain = seq.remove_runs.values().next().unwrap();
        assert_eq!(chain.len(), 5);
        assert!(seq.remove_nodes.is_empty(), "no standalone removes");

        // The chain decompresses to the exact nodes a peer would have seen.
        let mut other = HashSeq::default();
        other.merge(seq.clone());
        assert_eq!(other, seq);
    }

    /// Roundtrip with mixed single and batch removes (exercises remove chains,
    /// the other-removes section, and ops depending on remove ids).
    #[quickcheck]
    fn prop_batch_remove_roundtrip(text: String, edits: Vec<(u8, u8, bool)>) -> bool {
        let mut seq = HashSeq::default();
        if !text.is_empty() {
            seq.insert_batch(0, text.chars());
        }
        for (idx, amount, insert_after) in edits {
            if !seq.is_empty() {
                let idx = idx as usize % seq.len();
                let amount = 1 + amount as usize % 5;
                seq.remove_batch(idx, amount);
            }
            if insert_after {
                let pos = if seq.is_empty() {
                    0
                } else {
                    idx as usize % (seq.len() + 1)
                };
                seq.insert(pos, 'x');
            }
        }

        let encoded = encode_hashseq(&seq);
        let decoded = decode_hashseq(&encoded).unwrap();

        decoded.iter().collect::<String>() == seq.iter().collect::<String>() && seq == decoded
    }
}
