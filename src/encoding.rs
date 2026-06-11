use std::collections::{BTreeSet, HashMap};

use rustc_hash::FxHashMap;

use crate::hashseq::{CausalRemove, Loc, RemoveRun};
use crate::{HashNode, HashSeq, Id, NodeIdx, Op, Run, StoredRun};

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

// Operation type tags (used for batch encoding and orphans)
const TAG_RUN: u8 = 0x00;
const TAG_INSERT_ROOT: u8 = 0x01;
const TAG_INSERT_BEFORE: u8 = 0x02;
const TAG_REMOVE: u8 = 0x03;
const TAG_INSERT_AFTER: u8 = 0x04;

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

    let first_op = match first_op_tag {
        RUN_OP_AFTER => crate::run::FirstOp::After,
        RUN_OP_BEFORE => crate::run::FirstOp::Before,
        _ => return Err(DecodeError::InvalidOpTag(first_op_tag)),
    };
    let run = Run::from_text(anchor, first_op, first_extra_deps, &run_str)
        .ok_or(DecodeError::EmptyRun)?;

    Ok((run, pos))
}

// --- HashNode (InsertRoot, InsertBefore, Remove) encoding/decoding ---

pub fn encode_hash_node(node: &HashNode, buf: &mut Vec<u8>) {
    match &node.op {
        Op::InsertRoot(ch) => {
            buf.push(TAG_INSERT_ROOT);
            encode_id_set(&node.extra_dependencies, buf);
            encode_utf8_char(*ch, buf);
        }
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

fn decode_insert_root(bytes: &[u8]) -> Result<(HashNode, usize), DecodeError> {
    let mut pos = 0;

    let (extra_deps, deps_size) = decode_id_set(bytes)?;
    pos += deps_size;

    let (ch, ch_size) = decode_utf8_char(&bytes[pos..])?;
    pos += ch_size;

    Ok((
        HashNode {
            extra_dependencies: extra_deps,
            op: Op::InsertRoot(ch),
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
        TAG_INSERT_ROOT => {
            let (node, size) = decode_insert_root(bytes)?;
            Ok((EncodableOp::Node(node), 1 + size))
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
// Removes are addressed positionally (run_idx, elem_idx) and adjacent
// remove chains are compressed into ranges. Every other ID — run anchors,
// extra_dependencies, orphan refs — goes through a dictionary header so
// each unique ID only takes 32 bytes once and is referenced by varint
// index thereafter.
//
// Format: [id_dict][roots][after-runs][before-runs][removes][orphans]

// Target-reference tags in the "other removes" section.
const RM_TARGET_RUN: u8 = 0x00;
const RM_TARGET_ROOT: u8 = 0x01;
const RM_TARGET_DICT: u8 = 0x02;

/// Encode a HashSeq to a compact byte representation.
///
/// Format:
/// - [num_ids: varint][id_0..id_n: 32 bytes each]
/// - [num_roots][roots...]            roots: { idx_set extra_deps, utf8 ch }
/// - [num_after_runs][runs...]        run:   { idx anchor, idx_set first_extra_deps, string }
/// - [num_before_runs][runs...]       same shape; first char anchors InsertBefore
/// - [num_forward_runs][...]          rmrun: { idx_set first_extra_deps, varint run_idx, varint start, varint end }
/// - [num_backward_runs][...]
/// - [num_single_run][...]            { idx_set extra_deps, varint run_idx, varint elem_idx }
/// - [num_root_removes][...]          { idx_set extra_deps, varint root_idx }
/// - [num_other_removes][...]         { idx_set extra_deps, varint n, n × tagged target }
///   (multi-target removes; targets tagged run/root/dict — identity-preserving)
/// - [num_orphans][orphans...]        tagged HashNodes with idx-encoded IDs
///
/// Remove sections address elements by `run_idx` into the concatenated
/// after-runs ++ before-runs list.
pub fn encode_hashseq(seq: &HashSeq) -> Vec<u8> {
    // root_nodes is a BTreeMap, so its iteration order is already deterministic.
    // runs/remove_nodes are HashMaps with a randomized iteration order; we sort
    // by ID so the encoded bytes are byte-identical across processes (handles
    // are replica-local and never drive ordering).
    let roots: Vec<_> = seq.root_nodes.iter().collect();
    let mut after_runs: Vec<(NodeIdx, &StoredRun)> = Vec::new();
    let mut before_runs: Vec<(NodeIdx, &StoredRun)> = Vec::new();
    for (head, run) in &seq.runs {
        match run.first_op {
            crate::run::FirstOp::After => after_runs.push((*head, run)),
            crate::run::FirstOp::Before => before_runs.push((*head, run)),
        }
    }
    after_runs.sort_by_key(|(h, _)| seq.id_of(*h));
    before_runs.sort_by_key(|(h, _)| seq.id_of(*h));
    // Removes address run elements by index into this concatenated list.
    let runs: Vec<(NodeIdx, &StoredRun)> = after_runs
        .iter()
        .chain(before_runs.iter())
        .copied()
        .collect();

    // Map storage handles to positions in the encoded layout.
    let run_pos: FxHashMap<NodeIdx, usize> =
        runs.iter().enumerate().map(|(i, (h, _))| (*h, i)).collect();
    let root_pos: FxHashMap<NodeIdx, usize> = roots
        .iter()
        .enumerate()
        .map(|(i, (id, _))| (seq.idx_of(id).unwrap(), i))
        .collect();

    // --- Removes ---
    // In-memory `RemoveRun` chains are already what the wire wants; each chain is
    // segmented into maximal spans that are contiguous in the *current* encoded
    // runs (run splits since removal can break a chain across runs). Spans become
    // wire remove-runs; isolated links become standalone singles. Segments after
    // the first synthesize extra_deps = {previous remove id} — exactly the deps
    // those links carry, so decode reconstructs identical nodes.

    let mut orphans: Vec<&HashNode> = seq.orphaned.iter().collect();
    orphans.sort_by_key(|n| n.id());

    /// Where a remove target lives in the encoded layout.
    enum TargetRef {
        Run(usize, usize),
        Root(usize),
        /// Not positionally addressable (e.g. a remove targeting a remove);
        /// referenced through the ID dictionary instead.
        Dict(Id),
    }
    let resolve_target = |idx: NodeIdx| -> TargetRef {
        match seq.loc_of(idx) {
            Loc::Run { run, pos } => TargetRef::Run(run_pos[&run], pos as usize),
            Loc::Root => TargetRef::Root(root_pos[&idx]),
            _ => TargetRef::Dict(seq.id_of(idx)),
        }
    };

    struct WireRemoveRun {
        extra_deps: BTreeSet<Id>,
        run_idx: usize,
        start_idx: usize,
        end_idx: usize,
    }

    let mut forward_runs: Vec<WireRemoveRun> = Vec::new();
    let mut backward_runs: Vec<WireRemoveRun> = Vec::new();
    let mut single_run_removes: Vec<(BTreeSet<Id>, usize, usize)> = Vec::new();
    let mut root_removes: Vec<(BTreeSet<Id>, usize)> = Vec::new();
    // Identity-preserving section for multi-target removes and exotic targets:
    // (extra_deps, target refs) — decode rebuilds the exact Op::Remove set.
    let mut other_removes: Vec<(BTreeSet<Id>, Vec<TargetRef>)> = Vec::new();

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
            match resolve_target(chain.targets[i]) {
                TargetRef::Run(run_idx, elem_idx) => {
                    // Greedy span: stay in the same encoded run, stepping ±1 in a
                    // consistent direction.
                    let mut j = i + 1;
                    let mut backwards = None;
                    let mut last = elem_idx;
                    while j < chain.targets.len() {
                        let TargetRef::Run(r2, e2) = resolve_target(chain.targets[j]) else {
                            break;
                        };
                        if r2 != run_idx {
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
                    if j - i > 1 {
                        let wr = WireRemoveRun {
                            extra_deps: deps,
                            run_idx,
                            start_idx: elem_idx,
                            end_idx: last,
                        };
                        if backwards == Some(true) {
                            backward_runs.push(wr);
                        } else {
                            forward_runs.push(wr);
                        }
                    } else {
                        single_run_removes.push((deps, run_idx, elem_idx));
                    }
                    i = j;
                }
                TargetRef::Root(root_idx) => {
                    root_removes.push((deps, root_idx));
                    i += 1;
                }
                target @ TargetRef::Dict(_) => {
                    other_removes.push((deps, vec![target]));
                    i += 1;
                }
            }
        }
    }

    let mut multi_removes: Vec<(NodeIdx, &CausalRemove)> =
        seq.remove_nodes.iter().map(|(i, r)| (*i, r)).collect();
    multi_removes.sort_by_key(|(idx, _)| seq.id_of(*idx));
    for (_idx, remove) in &multi_removes {
        let targets = remove.nodes.iter().map(|t| resolve_target(*t)).collect();
        other_removes.push((remove.extra_dependencies.clone(), targets));
    }

    // --- Build the ID dictionary ---
    // Includes every ID that will be encoded as a varint index in the body below.
    // Notably excludes: removed-element IDs targeted by RemoveRuns or by the standalone
    // single_run/before/root sections, since those use positional refs.
    let mut id_set: BTreeSet<Id> = BTreeSet::new();

    for (_, run) in &runs {
        id_set.insert(run.anchor);
        for id in &run.first_extra_deps {
            id_set.insert(*id);
        }
    }
    for (_id, root) in &roots {
        for dep in &root.extra_dependencies {
            id_set.insert(*dep);
        }
    }
    for rr in forward_runs.iter().chain(backward_runs.iter()) {
        for dep in &rr.extra_deps {
            id_set.insert(*dep);
        }
    }
    for (extra_deps, _, _) in &single_run_removes {
        for dep in extra_deps {
            id_set.insert(*dep);
        }
    }
    for (extra_deps, _) in &root_removes {
        for dep in extra_deps {
            id_set.insert(*dep);
        }
    }
    for (extra_deps, targets) in &other_removes {
        for dep in extra_deps {
            id_set.insert(*dep);
        }
        for t in targets {
            if let TargetRef::Dict(id) = t {
                id_set.insert(*id);
            }
        }
    }
    for orphan in &orphans {
        for dep in &orphan.extra_dependencies {
            id_set.insert(*dep);
        }
        match &orphan.op {
            Op::InsertRoot(_) => {}
            Op::InsertAfter(id, _) | Op::InsertBefore(id, _) => {
                id_set.insert(*id);
            }
            Op::Remove(ids) => {
                for id in ids {
                    id_set.insert(*id);
                }
            }
        }
    }

    let id_list: Vec<Id> = id_set.into_iter().collect();
    let id_to_idx: HashMap<Id, usize> =
        id_list.iter().enumerate().map(|(i, id)| (*id, i)).collect();

    // --- Emit ---
    let mut buf = Vec::new();

    encode_varint(id_list.len(), &mut buf);
    for id in &id_list {
        encode_id(id, &mut buf);
    }

    let encode_idx = |id: &Id, buf: &mut Vec<u8>| {
        encode_varint(id_to_idx[id], buf);
    };
    let encode_idx_set = |ids: &BTreeSet<Id>, buf: &mut Vec<u8>| {
        encode_varint(ids.len(), buf);
        for id in ids {
            encode_varint(id_to_idx[id], buf);
        }
    };

    // Roots
    encode_varint(roots.len(), &mut buf);
    for (_id, root) in &roots {
        encode_idx_set(&root.extra_dependencies, &mut buf);
        encode_utf8_char(root.ch, &mut buf);
    }

    // Runs: After-anchored, then Before-anchored. The two sections share a shape,
    // so the op kind is implied by the section rather than a per-run tag.
    for runs in [&after_runs, &before_runs] {
        encode_varint(runs.len(), &mut buf);
        for (_, run) in runs.iter() {
            encode_idx(&run.anchor, &mut buf);
            encode_idx_set(&run.first_extra_deps, &mut buf);
            encode_string(&run.text, &mut buf);
        }
    }

    // Remove runs: forward chains, then backward chains (must match decode order).
    for runs in [&forward_runs, &backward_runs] {
        encode_varint(runs.len(), &mut buf);
        for rr in runs.iter() {
            encode_idx_set(&rr.extra_deps, &mut buf);
            encode_varint(rr.run_idx, &mut buf);
            encode_varint(rr.start_idx, &mut buf);
            encode_varint(rr.end_idx, &mut buf);
        }
    }

    // Single-run standalone removes
    encode_varint(single_run_removes.len(), &mut buf);
    for (extra_deps, run_idx, elem_idx) in &single_run_removes {
        encode_idx_set(extra_deps, &mut buf);
        encode_varint(*run_idx, &mut buf);
        encode_varint(*elem_idx, &mut buf);
    }

    // Root-target standalone removes
    encode_varint(root_removes.len(), &mut buf);
    for (extra_deps, root_idx) in &root_removes {
        encode_idx_set(extra_deps, &mut buf);
        encode_varint(*root_idx, &mut buf);
    }

    // Other removes (multi-target / dict-referenced targets), identity-preserving:
    // decode rebuilds the exact Op::Remove target set so the node id survives.
    encode_varint(other_removes.len(), &mut buf);
    for (extra_deps, targets) in &other_removes {
        encode_idx_set(extra_deps, &mut buf);
        encode_varint(targets.len(), &mut buf);
        for t in targets {
            match t {
                TargetRef::Run(run_idx, elem_idx) => {
                    buf.push(RM_TARGET_RUN);
                    encode_varint(*run_idx, &mut buf);
                    encode_varint(*elem_idx, &mut buf);
                }
                TargetRef::Root(root_idx) => {
                    buf.push(RM_TARGET_ROOT);
                    encode_varint(*root_idx, &mut buf);
                }
                TargetRef::Dict(id) => {
                    buf.push(RM_TARGET_DICT);
                    encode_idx(id, &mut buf);
                }
            }
        }
    }

    // Orphans (tagged, with idx-encoded IDs)
    encode_varint(orphans.len(), &mut buf);
    for orphan in &orphans {
        match &orphan.op {
            Op::InsertRoot(ch) => {
                buf.push(TAG_INSERT_ROOT);
                encode_idx_set(&orphan.extra_dependencies, &mut buf);
                encode_utf8_char(*ch, &mut buf);
            }
            Op::InsertAfter(id, ch) => {
                buf.push(TAG_INSERT_AFTER);
                encode_idx_set(&orphan.extra_dependencies, &mut buf);
                encode_idx(id, &mut buf);
                encode_utf8_char(*ch, &mut buf);
            }
            Op::InsertBefore(id, ch) => {
                buf.push(TAG_INSERT_BEFORE);
                encode_idx_set(&orphan.extra_dependencies, &mut buf);
                encode_idx(id, &mut buf);
                encode_utf8_char(*ch, &mut buf);
            }
            Op::Remove(ids) => {
                buf.push(TAG_REMOVE);
                encode_idx_set(&orphan.extra_dependencies, &mut buf);
                encode_varint(ids.len(), &mut buf);
                for id in ids {
                    encode_idx(id, &mut buf);
                }
            }
        }
    }

    buf
}

/// Decode a HashSeq from its byte representation.
pub fn decode_hashseq(bytes: &[u8]) -> Result<HashSeq, DecodeError> {
    let mut pos = 0;

    // Read dictionary
    let (num_ids, size) = decode_varint(bytes)?;
    pos += size;

    let mut id_list: Vec<Id> = Vec::with_capacity(num_ids);
    for _ in 0..num_ids {
        let (id, size) = decode_id(&bytes[pos..])?;
        id_list.push(id);
        pos += size;
    }

    let lookup_id = |idx: usize| -> Result<Id, DecodeError> {
        id_list
            .get(idx)
            .copied()
            .ok_or(DecodeError::InvalidIdIndex(idx))
    };
    let decode_idx_at = |bytes: &[u8]| -> Result<(Id, usize), DecodeError> {
        let (idx, size) = decode_varint(bytes)?;
        Ok((lookup_id(idx)?, size))
    };
    let decode_idx_set_at = |bytes: &[u8]| -> Result<(BTreeSet<Id>, usize), DecodeError> {
        let (count, size) = decode_varint(bytes)?;
        let mut total = size;
        let mut ids = BTreeSet::new();
        for _ in 0..count {
            let (idx, size) = decode_varint(&bytes[total..])?;
            ids.insert(lookup_id(idx)?);
            total += size;
        }
        Ok((ids, total))
    };

    let mut seq = HashSeq::default();
    let mut root_ids: Vec<Id> = Vec::new();
    let mut run_element_ids: Vec<Vec<Id>> = Vec::new();

    // Roots
    let (num_roots, size) = decode_varint(&bytes[pos..])?;
    pos += size;
    for _ in 0..num_roots {
        let (extra_deps, size) = decode_idx_set_at(&bytes[pos..])?;
        pos += size;
        let (ch, size) = decode_utf8_char(&bytes[pos..])?;
        pos += size;
        let node = HashNode {
            extra_dependencies: extra_deps,
            op: Op::InsertRoot(ch),
        };
        root_ids.push(node.id());
        seq.apply(node);
    }

    // Runs: After-anchored section, then Before-anchored section. Both feed
    // run_element_ids, which the remove sections below index positionally.
    for first_op in [crate::run::FirstOp::After, crate::run::FirstOp::Before] {
        let (num_runs, size) = decode_varint(&bytes[pos..])?;
        pos += size;
        for _ in 0..num_runs {
            let (anchor, size) = decode_idx_at(&bytes[pos..])?;
            pos += size;
            let (first_extra_deps, size) = decode_idx_set_at(&bytes[pos..])?;
            pos += size;
            let (run_str, size) = decode_string(&bytes[pos..])?;
            pos += size;

            let run = Run::from_text(anchor, first_op, first_extra_deps, &run_str)
                .ok_or(DecodeError::EmptyRun)?;
            run_element_ids.push(run.elements.clone());
            for node in run.decompress() {
                seq.apply(node);
            }
        }
    }

    // Remove runs: a chain of single-element removes over a contiguous span of a
    // run, each depending on the previous. Encoded as two sections — forward
    // chains (e.g. delete-key bursts), then backward chains (backspace bursts) —
    // identical except for the direction the chain walks the span.
    for backwards in [false, true] {
        let (num_remove_runs, size) = decode_varint(&bytes[pos..])?;
        pos += size;
        for _ in 0..num_remove_runs {
            let (first_extra_deps, size) = decode_idx_set_at(&bytes[pos..])?;
            pos += size;
            let (run_idx, size) = decode_varint(&bytes[pos..])?;
            pos += size;
            let (start_idx, size) = decode_varint(&bytes[pos..])?;
            pos += size;
            let (end_idx, size) = decode_varint(&bytes[pos..])?;
            pos += size;

            let run_elements = run_element_ids
                .get(run_idx)
                .ok_or(DecodeError::InvalidIdIndex(run_idx))?;

            let mut prev_remove_id: Option<Id> = None;
            let mut apply_remove = |elem_idx: usize| -> Result<(), DecodeError> {
                let removed_id = run_elements
                    .get(elem_idx)
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
                prev_remove_id = Some(node.id());
                seq.apply(node);
                Ok(())
            };

            if backwards {
                for elem_idx in (end_idx..=start_idx).rev() {
                    apply_remove(elem_idx)?;
                }
            } else {
                for elem_idx in start_idx..=end_idx {
                    apply_remove(elem_idx)?;
                }
            }
        }
    }

    // Single-run standalone removes
    let (num_single_run, size) = decode_varint(&bytes[pos..])?;
    pos += size;
    for _ in 0..num_single_run {
        let (extra_deps, size) = decode_idx_set_at(&bytes[pos..])?;
        pos += size;
        let (run_idx, size) = decode_varint(&bytes[pos..])?;
        pos += size;
        let (elem_idx, size) = decode_varint(&bytes[pos..])?;
        pos += size;

        let removed_id = run_element_ids
            .get(run_idx)
            .and_then(|e| e.get(elem_idx))
            .copied()
            .ok_or(DecodeError::InvalidIdIndex(elem_idx))?;

        seq.apply(HashNode {
            extra_dependencies: extra_deps,
            op: Op::Remove(std::iter::once(removed_id).collect()),
        });
    }

    // Root-target standalone removes
    let (num_root_removes, size) = decode_varint(&bytes[pos..])?;
    pos += size;
    for _ in 0..num_root_removes {
        let (extra_deps, size) = decode_idx_set_at(&bytes[pos..])?;
        pos += size;
        let (root_idx, size) = decode_varint(&bytes[pos..])?;
        pos += size;

        let removed_id = root_ids
            .get(root_idx)
            .copied()
            .ok_or(DecodeError::InvalidIdIndex(root_idx))?;

        seq.apply(HashNode {
            extra_dependencies: extra_deps,
            op: Op::Remove(std::iter::once(removed_id).collect()),
        });
    }

    // Other removes (multi-target / dict-referenced targets)
    let (num_other_removes, size) = decode_varint(&bytes[pos..])?;
    pos += size;
    for _ in 0..num_other_removes {
        let (extra_deps, size) = decode_idx_set_at(&bytes[pos..])?;
        pos += size;
        let (num_targets, size) = decode_varint(&bytes[pos..])?;
        pos += size;

        let mut targets = BTreeSet::new();
        for _ in 0..num_targets {
            if pos >= bytes.len() {
                return Err(DecodeError::UnexpectedEof);
            }
            let tag = bytes[pos];
            pos += 1;
            let target = match tag {
                RM_TARGET_RUN => {
                    let (run_idx, size) = decode_varint(&bytes[pos..])?;
                    pos += size;
                    let (elem_idx, size) = decode_varint(&bytes[pos..])?;
                    pos += size;
                    run_element_ids
                        .get(run_idx)
                        .and_then(|e| e.get(elem_idx))
                        .copied()
                        .ok_or(DecodeError::InvalidIdIndex(elem_idx))?
                }
                RM_TARGET_ROOT => {
                    let (root_idx, size) = decode_varint(&bytes[pos..])?;
                    pos += size;
                    root_ids
                        .get(root_idx)
                        .copied()
                        .ok_or(DecodeError::InvalidIdIndex(root_idx))?
                }
                RM_TARGET_DICT => {
                    let (id, size) = decode_idx_at(&bytes[pos..])?;
                    pos += size;
                    id
                }
                _ => return Err(DecodeError::InvalidOpTag(tag)),
            };
            targets.insert(target);
        }

        seq.apply(HashNode {
            extra_dependencies: extra_deps,
            op: Op::Remove(targets),
        });
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
            TAG_INSERT_ROOT => {
                let (extra_deps, size) = decode_idx_set_at(&bytes[pos..])?;
                pos += size;
                let (ch, size) = decode_utf8_char(&bytes[pos..])?;
                pos += size;
                seq.apply(HashNode {
                    extra_dependencies: extra_deps,
                    op: Op::InsertRoot(ch),
                });
            }
            TAG_INSERT_AFTER => {
                let (extra_deps, size) = decode_idx_set_at(&bytes[pos..])?;
                pos += size;
                let (id, size) = decode_idx_at(&bytes[pos..])?;
                pos += size;
                let (ch, size) = decode_utf8_char(&bytes[pos..])?;
                pos += size;
                seq.apply(HashNode {
                    extra_dependencies: extra_deps,
                    op: Op::InsertAfter(id, ch),
                });
            }
            TAG_INSERT_BEFORE => {
                let (extra_deps, size) = decode_idx_set_at(&bytes[pos..])?;
                pos += size;
                let (id, size) = decode_idx_at(&bytes[pos..])?;
                pos += size;
                let (ch, size) = decode_utf8_char(&bytes[pos..])?;
                pos += size;
                seq.apply(HashNode {
                    extra_dependencies: extra_deps,
                    op: Op::InsertBefore(id, ch),
                });
            }
            TAG_REMOVE => {
                let (extra_deps, size) = decode_idx_set_at(&bytes[pos..])?;
                pos += size;
                let (count, size) = decode_varint(&bytes[pos..])?;
                pos += size;
                let mut removed_ids = BTreeSet::new();
                for _ in 0..count {
                    let (id, size) = decode_idx_at(&bytes[pos..])?;
                    pos += size;
                    removed_ids.insert(id);
                }
                seq.apply(HashNode {
                    extra_dependencies: extra_deps,
                    op: Op::Remove(removed_ids),
                });
            }
            _ => return Err(DecodeError::InvalidOpTag(tag)),
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
    fn test_insert_root_roundtrip() {
        let node = HashNode {
            extra_dependencies: BTreeSet::new(),
            op: Op::InsertRoot('a'),
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
                op: Op::InsertRoot('a'),
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
