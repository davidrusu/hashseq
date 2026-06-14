use std::collections::{BTreeMap, BTreeSet, HashMap};

use rustc_hash::{FxHashMap, FxHashSet};

use crate::hashseq::{CausalRemove, Loc, RemoveRun};
use crate::{HashNode, HashSeq, Id, NodeIdx, Op, Run};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    UnexpectedEof,
    InvalidVarint,
    InvalidUtf8,
    InvalidOpTag(u8),
    EmptyRun,
    InvalidIdIndex(usize),
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
        }
    }
}

impl std::error::Error for DecodeError {}

// Operation type tags (used for batch encoding, orphans, and — via
// `HashNode::id` — the node-id hash preimage; see hash_node.rs).
const TAG_RUN: u8 = 0x00;
pub(crate) const TAG_INSERT_AFTER: u8 = 0x01;
pub(crate) const TAG_INSERT_BEFORE: u8 = 0x02;
pub(crate) const TAG_REMOVE: u8 = 0x03;

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

pub fn decode_run(bytes: &[u8]) -> Result<(Run, usize), DecodeError> {
    if bytes.is_empty() {
        return Err(DecodeError::UnexpectedEof);
    }
    let mut pos = 0;
    let first_op_tag = bytes[pos];
    pos += 1;

    let (anchor, id_size) = decode_id(&bytes[pos..])?;
    pos += id_size;

    let (first_extra_deps, deps_size) = decode_id_set(&bytes[pos..])?;
    pos += deps_size;

    let (run_str, str_size) = decode_string(&bytes[pos..])?;
    pos += str_size;

    let (num_interior, size) = decode_varint(&bytes[pos..])?;
    pos += size;
    let mut interior_extra_deps = BTreeMap::new();
    for _ in 0..num_interior {
        let (offset, size) = decode_varint(&bytes[pos..])?;
        pos += size;
        let (deps, size) = decode_id_set(&bytes[pos..])?;
        pos += size;
        interior_extra_deps.insert(offset, deps);
    }

    let first_op = match first_op_tag {
        RUN_OP_AFTER => crate::run::FirstOp::After,
        RUN_OP_BEFORE => crate::run::FirstOp::Before,
        _ => return Err(DecodeError::InvalidOpTag(first_op_tag)),
    };
    let run = Run::from_text(
        anchor,
        first_op,
        first_extra_deps,
        &run_str,
        interior_extra_deps,
    )
    .ok_or(DecodeError::EmptyRun)?;

    Ok((run, pos))
}

// --- HashNode (InsertAfter, InsertBefore, Remove) encoding/decoding ---

pub fn encode_hash_node(node: &HashNode, buf: &mut Vec<u8>) {
    match &node.op {
        Op::InsertAfter(id, ch) => {
            buf.push(TAG_INSERT_AFTER);
            encode_id_set(&node.extra_dependencies, buf);
            encode_id(id, buf);
            encode_utf8_char(*ch, buf);
        }
        Op::InsertBefore(id, ch) => {
            buf.push(TAG_INSERT_BEFORE);
            encode_id_set(&node.extra_dependencies, buf);
            encode_id(id, buf);
            encode_utf8_char(*ch, buf);
        }
        Op::Remove(ids) => {
            buf.push(TAG_REMOVE);
            encode_id_set(&node.extra_dependencies, buf);
            encode_varint(ids.len(), buf);
            for id in ids {
                encode_id(id, buf);
            }
        }
    }
}

fn decode_insert_after(bytes: &[u8]) -> Result<(HashNode, usize), DecodeError> {
    let mut pos = 0;

    let (extra_deps, deps_size) = decode_id_set(bytes)?;
    pos += deps_size;

    let (after_id, id_size) = decode_id(&bytes[pos..])?;
    pos += id_size;

    let (ch, ch_size) = decode_utf8_char(&bytes[pos..])?;
    pos += ch_size;

    Ok((
        HashNode {
            extra_dependencies: extra_deps,
            op: Op::InsertAfter(after_id, ch),
        },
        pos,
    ))
}

fn decode_insert_before(bytes: &[u8]) -> Result<(HashNode, usize), DecodeError> {
    let mut pos = 0;

    let (extra_deps, deps_size) = decode_id_set(bytes)?;
    pos += deps_size;

    let (before_id, id_size) = decode_id(&bytes[pos..])?;
    pos += id_size;

    let (ch, ch_size) = decode_utf8_char(&bytes[pos..])?;
    pos += ch_size;

    Ok((
        HashNode {
            extra_dependencies: extra_deps,
            op: Op::InsertBefore(before_id, ch),
        },
        pos,
    ))
}

fn decode_remove(bytes: &[u8]) -> Result<(HashNode, usize), DecodeError> {
    let mut pos = 0;

    let (extra_deps, deps_size) = decode_id_set(bytes)?;
    pos += deps_size;

    let (remove_len, varint_size) = decode_varint(&bytes[pos..])?;
    pos += varint_size;

    let mut remove_ids = BTreeSet::new();
    for _ in 0..remove_len {
        let (id, id_size) = decode_id(&bytes[pos..])?;
        remove_ids.insert(id);
        pos += id_size;
    }

    Ok((
        HashNode {
            extra_dependencies: extra_deps,
            op: Op::Remove(remove_ids),
        },
        pos,
    ))
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
    if bytes.is_empty() {
        return Err(DecodeError::UnexpectedEof);
    }

    let tag = bytes[0];
    let bytes = &bytes[1..];

    match tag {
        TAG_RUN => {
            let (run, size) = decode_run(bytes)?;
            Ok((EncodableOp::Run(run), 1 + size))
        }
        TAG_INSERT_BEFORE => {
            let (node, size) = decode_insert_before(bytes)?;
            Ok((EncodableOp::Node(node), 1 + size))
        }
        TAG_REMOVE => {
            let (node, size) = decode_remove(bytes)?;
            Ok((EncodableOp::Node(node), 1 + size))
        }
        TAG_INSERT_AFTER => {
            let (node, size) = decode_insert_after(bytes)?;
            Ok((EncodableOp::Node(node), 1 + size))
        }
        _ => Err(DecodeError::InvalidOpTag(tag)),
    }
}

// --- Batch encoding/decoding ---

pub fn encode_batch(ops: &[EncodableOp]) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_varint(ops.len(), &mut buf);
    for op in ops {
        encode_op(op, &mut buf);
    }
    buf
}

pub fn decode_batch(bytes: &[u8]) -> Result<Vec<EncodableOp>, DecodeError> {
    let (count, mut pos) = decode_varint(bytes)?;
    let mut ops = Vec::with_capacity(count);

    for _ in 0..count {
        let (op, size) = decode_op(&bytes[pos..])?;
        ops.push(op);
        pos += size;
    }

    Ok(ops)
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
///   - REMOVE_OTHER: { ref_set extra_deps, varint n, n × ref target } —
///     multi-target / non-element removes; exposes the remove-op id.
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
        Run(NodeIdx),
        RemoveSpan {
            backwards: bool,
            first_extra_deps: BTreeSet<Id>,
            target_run: NodeIdx,
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

    let mut blocks: Vec<Block> = Vec::new();

    // Run blocks.
    for (head, run) in &seq.runs {
        let exposed: Vec<Id> = run.elements.iter().map(|e| seq.id_of(*e)).collect();
        blocks.push(Block {
            head_id: exposed[0],
            exposed,
            payload: Payload::Run(*head),
        });
    }

    // Remove blocks. In-memory `RemoveRun` chains are already what the wire
    // wants; each chain is segmented into maximal spans contiguous within one
    // encoded run (run splits since removal can break a chain across runs).
    // Multi-char spans become remove-runs; isolated links become singles;
    // non-element targets become others. Segments after the first synthesize
    // extra_deps = {previous remove id} — exactly the deps those links carry,
    // so decode reconstructs identical nodes.
    let elem_of = |idx: NodeIdx| -> Option<(NodeIdx, usize)> {
        match seq.loc_of(idx) {
            Loc::Run { run, pos } => Some((run, pos as usize)),
            _ => None,
        }
    };

    let mut chains: Vec<&RemoveRun> = seq.remove_runs.values().collect();
    chains.sort_by_key(|c| c.links.first().map(|l| seq.id_of(*l)));
    for chain in &chains {
        let mut i = 0;
        while i < chain.targets.len() {
            let deps = if i == 0 {
                chain.first_extra_deps.clone()
            } else {
                BTreeSet::from_iter([seq.id_of(chain.links[i - 1])])
            };
            match elem_of(chain.targets[i]) {
                Some((run_head, elem_idx)) => {
                    // Greedy span: stay in the same encoded run, stepping ±1 in
                    // a consistent direction.
                    let mut j = i + 1;
                    let mut backwards = None;
                    let mut last = elem_idx;
                    while j < chain.targets.len() {
                        let Some((r2, e2)) = elem_of(chain.targets[j]) else {
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
                    let exposed: Vec<Id> =
                        chain.links[i..j].iter().map(|l| seq.id_of(*l)).collect();
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
                                target: seq.id_of(chain.targets[i]),
                            },
                        });
                    }
                    i = j;
                }
                None => {
                    let id = seq.id_of(chain.links[i]);
                    blocks.push(Block {
                        head_id: id,
                        exposed: vec![id],
                        payload: Payload::Other {
                            extra_deps: deps,
                            targets: vec![seq.id_of(chain.targets[i])],
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
                extra_deps: remove.extra_dependencies.clone(),
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
    // run head -> block index (remove-span target edges and emit).
    let mut run_block: FxHashMap<NodeIdx, usize> = FxHashMap::default();
    for (b, block) in blocks.iter().enumerate() {
        if let Payload::Run(head) = block.payload {
            run_block.insert(head, b);
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

    let mut indeg = vec![0usize; nb];
    let mut hard_indeg = vec![0usize; nb];
    let mut children: Vec<Vec<(usize, bool)>> = vec![Vec::new(); nb];
    {
        fn note(parents: &mut FxHashSet<usize>, producer: &FxHashMap<Id, (usize, usize)>, b: usize, id: &Id) {
            if let Some(&(pb, _)) = producer.get(id)
                && pb != b
            {
                parents.insert(pb);
            }
        }
        let mut soft: FxHashSet<usize> = FxHashSet::default();
        let mut hard: FxHashSet<usize> = FxHashSet::default();
        for (b, block) in blocks.iter().enumerate() {
            soft.clear();
            hard.clear();
            match &block.payload {
                Payload::Run(head) => {
                    let run = &seq.runs[head];
                    note(&mut soft, &producer, b, &run.anchor);
                    for id in &run.first_extra_deps {
                        note(&mut soft, &producer, b, id);
                    }
                    for deps in run.interior_extra_deps.values() {
                        for id in deps {
                            note(&mut soft, &producer, b, id);
                        }
                    }
                }
                Payload::RemoveSpan {
                    first_extra_deps,
                    target_run,
                    ..
                } => {
                    for id in first_extra_deps {
                        note(&mut soft, &producer, b, id);
                    }
                    if let Some(&tb) = run_block.get(target_run)
                        && tb != b
                    {
                        hard.insert(tb);
                    }
                }
                Payload::Single { extra_deps, target } => {
                    for id in extra_deps {
                        note(&mut soft, &producer, b, id);
                    }
                    note(&mut hard, &producer, b, target);
                }
                Payload::Other { extra_deps, targets } => {
                    for id in extra_deps {
                        note(&mut soft, &producer, b, id);
                    }
                    for t in targets {
                        note(&mut hard, &producer, b, t);
                    }
                }
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

    // Orphans are parked as (id, node) keyed by missing dep; sort by the
    // precomputed node id for deterministic bytes (no rehashing).
    let orphans: Vec<&HashNode> = {
        let mut parked: Vec<(&Id, &HashNode)> = seq
            .orphaned
            .values()
            .flatten()
            .map(|(id, node)| (id, node))
            .collect();
        parked.sort_by_key(|(id, _)| **id);
        parked.into_iter().map(|(_, node)| node).collect()
    };

    // --- Build the ID dictionary ---
    // Only ids no ref can resolve positionally: the origin, refs to a block
    // not yet emitted (cycle-broken), same-block refs, and orphan deps on
    // unknown nodes. Remove-span targets are raw block indices (never dict).
    let mut id_set: BTreeSet<Id> = BTreeSet::new();
    {
        let note = |id: &Id, limit: usize, id_set: &mut BTreeSet<Id>| {
            if resolve(id, limit).is_none() {
                id_set.insert(*id);
            }
        };
        for (b, block) in blocks.iter().enumerate() {
            let limit = emit_pos[b];
            match &block.payload {
                Payload::Run(head) => {
                    let run = &seq.runs[head];
                    note(&run.anchor, limit, &mut id_set);
                    for id in &run.first_extra_deps {
                        note(id, limit, &mut id_set);
                    }
                    for deps in run.interior_extra_deps.values() {
                        for id in deps {
                            note(id, limit, &mut id_set);
                        }
                    }
                }
                Payload::RemoveSpan {
                    first_extra_deps, ..
                } => {
                    for id in first_extra_deps {
                        note(id, limit, &mut id_set);
                    }
                }
                Payload::Single { extra_deps, target } => {
                    for id in extra_deps {
                        note(id, limit, &mut id_set);
                    }
                    note(target, limit, &mut id_set);
                }
                Payload::Other { extra_deps, targets } => {
                    for id in extra_deps {
                        note(id, limit, &mut id_set);
                    }
                    for t in targets {
                        note(t, limit, &mut id_set);
                    }
                }
            }
        }
        for orphan in &orphans {
            for dep in &orphan.extra_dependencies {
                note(dep, NO_LIMIT, &mut id_set);
            }
            match &orphan.op {
                Op::InsertAfter(id, _) | Op::InsertBefore(id, _) => {
                    note(id, NO_LIMIT, &mut id_set);
                }
                Op::Remove(ids) => {
                    for id in ids {
                        note(id, NO_LIMIT, &mut id_set);
                    }
                }
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
            Payload::Run(head) => {
                let run = &seq.runs[head];
                buf.push(match run.first_op {
                    crate::run::FirstOp::After => BLK_RUN_AFTER,
                    crate::run::FirstOp::Before => BLK_RUN_BEFORE,
                });
                encode_ref(&run.anchor, pe, &mut buf);
                encode_ref_set(&run.first_extra_deps, pe, &mut buf);
                encode_string(&run.text, &mut buf);
                encode_varint(run.interior_extra_deps.len(), &mut buf);
                for (offset, deps) in &run.interior_extra_deps {
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
                encode_varint(targets.len(), &mut buf);
                for t in targets {
                    encode_ref(t, pe, &mut buf);
                }
            }
        }
    }

    // Orphans (tagged, with ref-encoded IDs)
    encode_varint(orphans.len(), &mut buf);
    for orphan in &orphans {
        match &orphan.op {
            Op::InsertAfter(id, ch) => {
                buf.push(TAG_INSERT_AFTER);
                encode_ref_set(&orphan.extra_dependencies, NO_LIMIT, &mut buf);
                encode_ref(id, NO_LIMIT, &mut buf);
                encode_utf8_char(*ch, &mut buf);
            }
            Op::InsertBefore(id, ch) => {
                buf.push(TAG_INSERT_BEFORE);
                encode_ref_set(&orphan.extra_dependencies, NO_LIMIT, &mut buf);
                encode_ref(id, NO_LIMIT, &mut buf);
                encode_utf8_char(*ch, &mut buf);
            }
            Op::Remove(ids) => {
                buf.push(TAG_REMOVE);
                encode_ref_set(&orphan.extra_dependencies, NO_LIMIT, &mut buf);
                encode_varint(ids.len(), &mut buf);
                for id in ids {
                    encode_ref(id, NO_LIMIT, &mut buf);
                }
            }
        }
    }

    buf
}

/// Resolve a wire `ref`: low bit 1 = positional (run_idx, elem_idx) into the
/// already-decoded run elements; low bit 0 = dictionary index.
fn decode_ref(
    bytes: &[u8],
    id_list: &[Id],
    run_elems: &[Vec<Id>],
    remove_ids: &[Vec<Id>],
) -> Result<(Id, usize), DecodeError> {
    let (v, mut size) = decode_varint(bytes)?;
    if v & 1 == 1 {
        // run element: (run_rank, offset)
        let rank = v >> 1;
        let (off, s) = decode_varint(&bytes[size..])?;
        size += s;
        let id = run_elems
            .get(rank)
            .and_then(|e| e.get(off))
            .copied()
            .ok_or(DecodeError::InvalidIdIndex(rank))?;
        Ok((id, size))
    } else if v & 2 == 0 {
        // dictionary index
        let idx = v >> 2;
        let id = id_list
            .get(idx)
            .copied()
            .ok_or(DecodeError::InvalidIdIndex(idx))?;
        Ok((id, size))
    } else {
        // remove op: (remove_rank, offset)
        let rank = v >> 2;
        let (off, s) = decode_varint(&bytes[size..])?;
        size += s;
        let id = remove_ids
            .get(rank)
            .and_then(|e| e.get(off))
            .copied()
            .ok_or(DecodeError::InvalidIdIndex(rank))?;
        Ok((id, size))
    }
}

fn decode_ref_set(
    bytes: &[u8],
    id_list: &[Id],
    run_elems: &[Vec<Id>],
    remove_ids: &[Vec<Id>],
) -> Result<(BTreeSet<Id>, usize), DecodeError> {
    let (count, mut pos) = decode_varint(bytes)?;
    let mut ids = BTreeSet::new();
    for _ in 0..count {
        let (id, size) = decode_ref(&bytes[pos..], id_list, run_elems, remove_ids)?;
        ids.insert(id);
        pos += size;
    }
    Ok((ids, pos))
}

/// Decode a HashSeq from its byte representation.
pub fn decode_hashseq(bytes: &[u8]) -> Result<HashSeq, DecodeError> {
    let mut pos = 0;

    // Origin header (also the implicit first dictionary entry)
    let (origin, size) = decode_id(bytes)?;
    pos += size;

    // Read dictionary
    let (num_ids, size) = decode_varint(&bytes[pos..])?;
    pos += size;

    let mut id_list: Vec<Id> = Vec::with_capacity(num_ids + 1);
    id_list.push(origin);
    for _ in 0..num_ids {
        let (id, size) = decode_id(&bytes[pos..])?;
        id_list.push(id);
        pos += size;
    }

    let mut seq = HashSeq::new(origin);
    // Decoded ids by within-kind rank, in emit order: a run block appends its
    // element ids to `run_elems`, a remove block its remove-op ids to
    // `remove_ids`. Positional refs index one of these arrays (the ref's low
    // bits pick which); refs always point at an earlier block of that kind.
    let mut run_elems: Vec<Vec<Id>> = Vec::new();
    let mut remove_ids: Vec<Vec<Id>> = Vec::new();

    let (num_blocks, size) = decode_varint(&bytes[pos..])?;
    pos += size;
    for _ in 0..num_blocks {
        if pos >= bytes.len() {
            return Err(DecodeError::UnexpectedEof);
        }
        let tag = bytes[pos];
        pos += 1;
        match tag {
            BLK_RUN_AFTER | BLK_RUN_BEFORE => {
                let first_op = if tag == BLK_RUN_AFTER {
                    crate::run::FirstOp::After
                } else {
                    crate::run::FirstOp::Before
                };
                let (anchor, size) = decode_ref(&bytes[pos..], &id_list, &run_elems, &remove_ids)?;
                pos += size;
                let (first_extra_deps, size) =
                    decode_ref_set(&bytes[pos..], &id_list, &run_elems, &remove_ids)?;
                pos += size;
                let (run_str, size) = decode_string(&bytes[pos..])?;
                pos += size;

                let (num_interior, size) = decode_varint(&bytes[pos..])?;
                pos += size;
                let mut interior_extra_deps = BTreeMap::new();
                for _ in 0..num_interior {
                    let (offset, size) = decode_varint(&bytes[pos..])?;
                    pos += size;
                    let (deps, size) =
                        decode_ref_set(&bytes[pos..], &id_list, &run_elems, &remove_ids)?;
                    pos += size;
                    interior_extra_deps.insert(offset, deps);
                }

                let run = Run::from_text(
                    anchor,
                    first_op,
                    first_extra_deps,
                    &run_str,
                    interior_extra_deps,
                )
                .ok_or(DecodeError::EmptyRun)?;
                run_elems.push(run.elements.clone());
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
                let backwards = tag == BLK_REMOVE_BWD;
                let (first_extra_deps, size) =
                    decode_ref_set(&bytes[pos..], &id_list, &run_elems, &remove_ids)?;
                pos += size;
                let (target_run, size) = decode_varint(&bytes[pos..])?;
                pos += size;
                let (start_idx, size) = decode_varint(&bytes[pos..])?;
                pos += size;
                let (end_idx, size) = decode_varint(&bytes[pos..])?;
                pos += size;

                let span: Vec<usize> = if backwards {
                    (end_idx..=start_idx).rev().collect()
                } else {
                    (start_idx..=end_idx).collect()
                };
                let mut exposed = Vec::with_capacity(span.len());
                let mut prev_remove_id: Option<Id> = None;
                for elem_idx in span {
                    let removed_id = run_elems
                        .get(target_run)
                        .and_then(|e| e.get(elem_idx))
                        .copied()
                        .ok_or(DecodeError::InvalidIdIndex(elem_idx))?;
                    let extra_deps = match prev_remove_id {
                        Some(prev_id) => BTreeSet::from_iter([prev_id]),
                        None => first_extra_deps.clone(),
                    };
                    let node = HashNode {
                        extra_dependencies: extra_deps,
                        op: Op::Remove(BTreeSet::from_iter([removed_id])),
                    };
                    let id = node.id();
                    prev_remove_id = Some(id);
                    exposed.push(id);
                    seq.apply_with_id(id, node);
                }
                remove_ids.push(exposed);
            }
            BLK_REMOVE_SINGLE => {
                let (extra_deps, size) =
                    decode_ref_set(&bytes[pos..], &id_list, &run_elems, &remove_ids)?;
                pos += size;
                let (target, size) = decode_ref(&bytes[pos..], &id_list, &run_elems, &remove_ids)?;
                pos += size;
                let node = HashNode {
                    extra_dependencies: extra_deps,
                    op: Op::Remove(std::iter::once(target).collect()),
                };
                let id = node.id();
                remove_ids.push(vec![id]);
                seq.apply_with_id(id, node);
            }
            BLK_REMOVE_OTHER => {
                let (extra_deps, size) =
                    decode_ref_set(&bytes[pos..], &id_list, &run_elems, &remove_ids)?;
                pos += size;
                let (num_targets, size) = decode_varint(&bytes[pos..])?;
                pos += size;
                let mut targets = BTreeSet::new();
                for _ in 0..num_targets {
                    let (id, size) = decode_ref(&bytes[pos..], &id_list, &run_elems, &remove_ids)?;
                    pos += size;
                    targets.insert(id);
                }
                let node = HashNode {
                    extra_dependencies: extra_deps,
                    op: Op::Remove(targets),
                };
                let id = node.id();
                remove_ids.push(vec![id]);
                seq.apply_with_id(id, node);
            }
            other => return Err(DecodeError::InvalidOpTag(other)),
        }
    }

    // Orphans (tagged)
    let (num_orphans, size) = decode_varint(&bytes[pos..])?;
    pos += size;
    for _ in 0..num_orphans {
        if pos >= bytes.len() {
            return Err(DecodeError::UnexpectedEof);
        }
        let tag = bytes[pos];
        pos += 1;
        match tag {
            TAG_INSERT_AFTER => {
                let (extra_deps, size) = decode_ref_set(&bytes[pos..], &id_list, &run_elems, &remove_ids)?;
                pos += size;
                let (id, size) = decode_ref(&bytes[pos..], &id_list, &run_elems, &remove_ids)?;
                pos += size;
                let (ch, size) = decode_utf8_char(&bytes[pos..])?;
                pos += size;
                seq.apply(HashNode {
                    extra_dependencies: extra_deps,
                    op: Op::InsertAfter(id, ch),
                });
            }
            TAG_INSERT_BEFORE => {
                let (extra_deps, size) = decode_ref_set(&bytes[pos..], &id_list, &run_elems, &remove_ids)?;
                pos += size;
                let (id, size) = decode_ref(&bytes[pos..], &id_list, &run_elems, &remove_ids)?;
                pos += size;
                let (ch, size) = decode_utf8_char(&bytes[pos..])?;
                pos += size;
                seq.apply(HashNode {
                    extra_dependencies: extra_deps,
                    op: Op::InsertBefore(id, ch),
                });
            }
            TAG_REMOVE => {
                let (extra_deps, size) = decode_ref_set(&bytes[pos..], &id_list, &run_elems, &remove_ids)?;
                pos += size;
                let (count, size) = decode_varint(&bytes[pos..])?;
                pos += size;
                let mut removed_ids = BTreeSet::new();
                for _ in 0..count {
                    let (id, size) = decode_ref(&bytes[pos..], &id_list, &run_elems, &remove_ids)?;
                    pos += size;
                    removed_ids.insert(id);
                }
                seq.apply(HashNode {
                    extra_dependencies: extra_deps,
                    op: Op::Remove(removed_ids),
                });
            }
            other => return Err(DecodeError::InvalidOpTag(other)),
        }
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

    #[test]
    fn test_origin_anchored_insert_roundtrip() {
        let node = HashNode {
            extra_dependencies: BTreeSet::new(),
            op: Op::InsertAfter(Id::default(), 'a'),
        };

        let mut buf = Vec::new();
        encode_hash_node(&node, &mut buf);

        let (decoded, size) = decode_op(&buf).unwrap();
        assert_eq!(decoded, EncodableOp::Node(node));
        assert_eq!(size, buf.len());
    }

    #[test]
    fn test_insert_before_roundtrip() {
        let node = HashNode {
            extra_dependencies: BTreeSet::new(),
            op: Op::InsertBefore(test_id(5), 'z'),
        };

        let mut buf = Vec::new();
        encode_hash_node(&node, &mut buf);

        let (decoded, size) = decode_op(&buf).unwrap();
        assert_eq!(decoded, EncodableOp::Node(node));
        assert_eq!(size, buf.len());
    }

    #[test]
    fn test_remove_roundtrip() {
        let mut remove_ids = BTreeSet::new();
        remove_ids.insert(test_id(1));
        remove_ids.insert(test_id(2));
        remove_ids.insert(test_id(3));

        let node = HashNode {
            extra_dependencies: BTreeSet::new(),
            op: Op::Remove(remove_ids),
        };

        let mut buf = Vec::new();
        encode_hash_node(&node, &mut buf);

        let (decoded, size) = decode_op(&buf).unwrap();
        assert_eq!(decoded, EncodableOp::Node(node));
        assert_eq!(size, buf.len());
    }

    #[test]
    fn test_batch_roundtrip() {
        let anchor = test_id(0);
        let mut run = Run::new(anchor, BTreeSet::new(), 'h');
        run.extend('e');
        run.extend('l');
        run.extend('l');
        run.extend('o');

        let ops = vec![
            EncodableOp::Node(HashNode {
                extra_dependencies: BTreeSet::new(),
                op: Op::InsertAfter(Id::default(), 'a'),
            }),
            EncodableOp::Run(run),
            EncodableOp::Node(HashNode {
                extra_dependencies: BTreeSet::new(),
                op: Op::InsertBefore(test_id(10), 'x'),
            }),
        ];

        let encoded = encode_batch(&ops);
        let decoded = decode_batch(&encoded).unwrap();

        assert_eq!(decoded, ops);
    }

    #[test]
    fn test_empty_batch() {
        let ops: Vec<EncodableOp> = vec![];
        let encoded = encode_batch(&ops);
        let decoded = decode_batch(&encoded).unwrap();
        assert_eq!(decoded, ops);
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

    #[test]
    fn test_insert_after_roundtrip() {
        let node = HashNode {
            extra_dependencies: BTreeSet::new(),
            op: Op::InsertAfter(test_id(5), 'z'),
        };

        let mut buf = Vec::new();
        encode_hash_node(&node, &mut buf);

        let (decoded, size) = decode_op(&buf).unwrap();
        assert_eq!(decoded, EncodableOp::Node(node));
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
        // Note: re-encoding `decoded` may produce *different bytes* — chain
        // and run storage are arrival-order dependent under concurrency
        // (e.g. two removes claiming the same parent), so byte-canonical
        // encoding is not a property the format has. Logical state must
        // roundtrip exactly, and a second roundtrip must be logically stable.
        let encoded2 = encode_hashseq(&decoded);
        let decoded2 = decode_hashseq(&encoded2).unwrap();
        decoded == seq_a
            && decoded.iter().collect::<String>() == seq_a.iter().collect::<String>()
            && decoded2 == seq_a
            && decoded2.iter().collect::<String>() == seq_a.iter().collect::<String>()
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

    #[quickcheck]
    fn prop_hashseq_roundtrip_preserves_content(ops: Vec<(bool, u8, char)>) -> bool {
        let mut seq = HashSeq::default();

        for (is_insert, idx, ch) in ops {
            let idx = idx as usize;
            if is_insert {
                let insert_idx = if seq.is_empty() {
                    0
                } else {
                    idx % (seq.len() + 1)
                };
                seq.insert(insert_idx, ch);
            } else if !seq.is_empty() {
                let remove_idx = idx % seq.len();
                seq.remove(remove_idx);
            }
        }

        let original_str: String = seq.iter().collect();

        let encoded = encode_hashseq(&seq);
        let decoded = decode_hashseq(&encoded).unwrap();

        original_str == decoded.iter().collect::<String>()
    }

    #[quickcheck]
    fn prop_hashseq_roundtrip_preserves_equality(ops: Vec<(bool, u8, char)>) -> bool {
        let mut seq = HashSeq::default();

        for (is_insert, idx, ch) in ops {
            let idx = idx as usize;
            if is_insert {
                let insert_idx = if seq.is_empty() {
                    0
                } else {
                    idx % (seq.len() + 1)
                };
                seq.insert(insert_idx, ch);
            } else if !seq.is_empty() {
                let remove_idx = idx % seq.len();
                seq.remove(remove_idx);
            }
        }

        let encoded = encode_hashseq(&seq);
        let decoded = decode_hashseq(&encoded).unwrap();

        seq == decoded
    }

    /// Encoding the same logical state twice must produce bit-identical bytes,
    /// even when the underlying `HashMap`s have different randomization seeds.
    /// Decoded seqs use fresh `HashMap`s, so this round-trip is a real cross-seed test.
    #[quickcheck]
    fn prop_encoding_is_deterministic(ops: Vec<(bool, u8, char)>) -> bool {
        let mut seq = HashSeq::default();

        for (is_insert, idx, ch) in ops {
            let idx = idx as usize;
            if is_insert {
                let insert_idx = if seq.is_empty() {
                    0
                } else {
                    idx % (seq.len() + 1)
                };
                seq.insert(insert_idx, ch);
            } else if !seq.is_empty() {
                let remove_idx = idx % seq.len();
                seq.remove(remove_idx);
            }
        }

        let first = encode_hashseq(&seq);
        let decoded = decode_hashseq(&first).unwrap();
        let second = encode_hashseq(&decoded);
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
