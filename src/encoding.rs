use std::collections::{BTreeSet, HashMap};

use crate::{HashNode, HashSeq, Id, Op, Run};

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

pub fn encode_run(run: &Run, buf: &mut Vec<u8>) {
    encode_id(&run.insert_after, buf);
    encode_id_set(&run.first_extra_deps, buf);
    encode_string(&run.run, buf);
}

pub fn decode_run(bytes: &[u8]) -> Result<(Run, usize), DecodeError> {
    let mut pos = 0;

    let (insert_after, id_size) = decode_id(bytes)?;
    pos += id_size;

    let (first_extra_deps, deps_size) = decode_id_set(&bytes[pos..])?;
    pos += deps_size;

    let (run_str, str_size) = decode_string(&bytes[pos..])?;
    pos += str_size;

    // Reconstruct the Run with computed elements
    let mut chars = run_str.chars();
    let first_char = chars.next().ok_or(DecodeError::EmptyRun)?;

    let mut run = Run::new(insert_after, first_extra_deps, first_char);
    for ch in chars {
        run.extend(ch);
    }

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

// Op reference tags for compact ID encoding
const REF_TAG_RUN: u8 = 0x00;
const REF_TAG_ROOT: u8 = 0x01;
const REF_TAG_BEFORE: u8 = 0x02;

/// A compact reference to an operation's output ID.
/// Instead of storing 32-byte IDs, we store (op_type, op_idx, sub_idx).
#[derive(Debug, Clone, Copy)]
struct OpRef {
    tag: u8,
    op_idx: usize,
    sub_idx: usize,
}


/// Encode an entire HashSeq to bytes for persistence.
///
/// Format: [roots][runs][befores][removes][orphans]
/// Each section: [count: varint][items...]
///
/// Removes use compact OpRef encoding instead of full 32-byte IDs.
pub fn encode_hashseq(seq: &HashSeq) -> Vec<u8> {
    let mut buf = Vec::new();

    // Build ID -> OpRef mapping for compact remove encoding
    let mut id_to_ref: HashMap<Id, OpRef> = HashMap::new();

    // Collect roots, runs, befores in order (we need stable indices)
    let roots: Vec<_> = seq.root_nodes.iter().collect();
    let runs: Vec<_> = seq.runs.values().collect();
    let befores: Vec<_> = seq.before_nodes.iter().collect();

    // Map root IDs (the key is the ID)
    for (op_idx, (id, _root)) in roots.iter().enumerate() {
        id_to_ref.insert(**id, OpRef { tag: REF_TAG_ROOT, op_idx, sub_idx: 0 });
    }

    // Map run element IDs
    for (op_idx, run) in runs.iter().enumerate() {
        for (sub_idx, id) in run.elements.iter().enumerate() {
            id_to_ref.insert(*id, OpRef { tag: REF_TAG_RUN, op_idx, sub_idx });
        }
    }

    // Map before IDs (the key is the ID)
    for (op_idx, (id, _before)) in befores.iter().enumerate() {
        id_to_ref.insert(**id, OpRef { tag: REF_TAG_BEFORE, op_idx, sub_idx: 0 });
    }

    // Encode roots: [count][extra_deps, char]...
    encode_varint(roots.len(), &mut buf);
    for (_id, root) in &roots {
        encode_id_set(&root.extra_dependencies, &mut buf);
        encode_utf8_char(root.ch, &mut buf);
    }

    // Encode runs: [count][insert_after, first_extra_deps, run_string]...
    encode_varint(runs.len(), &mut buf);
    for run in &runs {
        encode_run(run, &mut buf);
    }

    // Encode befores: [count][extra_deps, anchor, char]...
    encode_varint(befores.len(), &mut buf);
    for (_id, before) in &befores {
        encode_id_set(&before.extra_dependencies, &mut buf);
        encode_id(&before.anchor, &mut buf);
        encode_utf8_char(before.ch, &mut buf);
    }

    // Encode removes with run compression for sequential backspace deletions
    // Format: [num_remove_runs][remove_runs...][num_standalone][standalone_removes...]

    // First, analyze removes to find sequential chains
    // A remove chain is: each remove's extra_deps = {prev_remove_id}, removes adjacent elements

    // Collect remove info: (remove_id, extra_deps, removed_ref)
    struct RemoveInfo {
        id: Id,
        extra_deps: BTreeSet<Id>,
        // Only track single-element removes from runs for chaining
        run_ref: Option<(usize, usize)>, // (run_idx, elem_idx)
    }

    let removes: Vec<_> = seq.remove_nodes.iter().collect();
    let mut remove_infos: Vec<RemoveInfo> = Vec::new();

    for (remove_id, remove) in &removes {
        let mut run_ref = None;
        // Check if this is a single-element remove from a run
        if remove.nodes.len() == 1 {
            let removed_id = remove.nodes.iter().next().unwrap();
            if let Some(op_ref) = id_to_ref.get(removed_id) {
                if op_ref.tag == REF_TAG_RUN {
                    run_ref = Some((op_ref.op_idx, op_ref.sub_idx));
                }
            }
        }
        remove_infos.push(RemoveInfo {
            id: **remove_id,
            extra_deps: remove.extra_dependencies.clone(),
            run_ref,
        });
    }

    // Build maps for O(n) chain detection
    // Map from singleton extra_dep -> remove index (for removes with exactly 1 dep)
    let mut dep_to_idx: HashMap<Id, usize> = HashMap::new();
    for (i, info) in remove_infos.iter().enumerate() {
        if info.extra_deps.len() == 1 && info.run_ref.is_some() {
            let dep = *info.extra_deps.iter().next().unwrap();
            dep_to_idx.insert(dep, i);
        }
    }

    // Find chain heads: removes that are not pointed to by any other remove's extra_deps
    // OR removes whose predecessor is not adjacent
    let mut in_chain: Vec<bool> = vec![false; remove_infos.len()];
    let mut chain_next: Vec<Option<usize>> = vec![None; remove_infos.len()];

    // Build forward chain links
    for (i, info) in remove_infos.iter().enumerate() {
        if let Some((run_idx, elem_idx)) = info.run_ref {
            // Check if there's a remove that depends on us and is adjacent
            if let Some(&next_idx) = dep_to_idx.get(&info.id) {
                let next_info = &remove_infos[next_idx];
                if let Some((next_run, next_elem)) = next_info.run_ref {
                    if next_run == run_idx {
                        // Check adjacency (backspace: next_elem = elem_idx - 1, or forward: next_elem = elem_idx + 1)
                        let is_adjacent = (elem_idx > 0 && next_elem == elem_idx - 1)
                            || next_elem == elem_idx + 1;
                        if is_adjacent {
                            chain_next[i] = Some(next_idx);
                        }
                    }
                }
            }
        }
    }

    // Find chain heads (removes with no predecessor in chain)
    let mut has_predecessor: Vec<bool> = vec![false; remove_infos.len()];
    for next in chain_next.iter().flatten() {
        has_predecessor[*next] = true;
    }

    // Build chains from heads
    struct RemoveRun {
        first_extra_deps: BTreeSet<Id>,
        run_idx: usize,
        start_idx: usize,   // First element in chain order
        end_idx: usize,     // Last element in chain order
        backwards: bool,    // true if chain goes from high to low indices
    }

    let mut remove_runs: Vec<RemoveRun> = Vec::new();

    for (i, info) in remove_infos.iter().enumerate() {
        if has_predecessor[i] || in_chain[i] { continue; }
        if info.run_ref.is_none() { continue; }
        if chain_next[i].is_none() { continue; } // Must have at least one successor

        // Follow chain and collect elements in order
        let (run_idx, first_elem) = info.run_ref.unwrap();
        let mut elems_in_order = vec![first_elem];
        let mut chain_len = 1;

        in_chain[i] = true;
        let mut current = i;
        while let Some(next) = chain_next[current] {
            if in_chain[next] { break; }
            in_chain[next] = true;
            if let Some((_, elem)) = remove_infos[next].run_ref {
                elems_in_order.push(elem);
            }
            chain_len += 1;
            current = next;
        }

        // Check if contiguous
        let min_elem = *elems_in_order.iter().min().unwrap();
        let max_elem = *elems_in_order.iter().max().unwrap();
        let expected_len = max_elem - min_elem + 1;
        let is_contiguous = chain_len == expected_len;

        // Determine direction: backwards if first_elem > last_elem
        let last_elem = *elems_in_order.last().unwrap();
        let backwards = first_elem > last_elem;

        // Only use chain if it saves space (chain_len > 1) and is contiguous
        if chain_len > 1 && is_contiguous {
            remove_runs.push(RemoveRun {
                first_extra_deps: info.extra_deps.clone(),
                run_idx,
                start_idx: first_elem,
                end_idx: last_elem,
                backwards,
            });
        } else {
            // Mark as not in chain so it goes to standalone
            // Need to unmark all elements we marked
            in_chain[i] = false;
            let mut cur = i;
            while let Some(nxt) = chain_next[cur] {
                if !in_chain[nxt] { break; }
                in_chain[nxt] = false;
                cur = nxt;
            }
        }
    }

    // Collect standalone removes (not in any chain)
    let standalone_removes: Vec<_> = removes.iter()
        .enumerate()
        .filter(|(i, _)| !in_chain[*i])
        .map(|(_, r)| r)
        .collect();

    // Split remove runs by direction
    let forward_runs: Vec<_> = remove_runs.iter().filter(|rr| !rr.backwards).collect();
    let backward_runs: Vec<_> = remove_runs.iter().filter(|rr| rr.backwards).collect();

    // Encode forward remove runs: [count][first_extra_deps, run_idx, start_idx, end_idx]...
    encode_varint(forward_runs.len(), &mut buf);
    for rr in &forward_runs {
        encode_id_set(&rr.first_extra_deps, &mut buf);
        encode_varint(rr.run_idx, &mut buf);
        encode_varint(rr.start_idx, &mut buf);
        encode_varint(rr.end_idx, &mut buf);
    }

    // Encode backward remove runs: [count][first_extra_deps, run_idx, start_idx, end_idx]...
    encode_varint(backward_runs.len(), &mut buf);
    for rr in &backward_runs {
        encode_id_set(&rr.first_extra_deps, &mut buf);
        encode_varint(rr.run_idx, &mut buf);
        encode_varint(rr.start_idx, &mut buf);
        encode_varint(rr.end_idx, &mut buf);
    }

    // Partition standalone removes by target type
    let mut single_run_removes: Vec<(&BTreeSet<Id>, usize, usize)> = Vec::new(); // (extra_deps, run_idx, elem_idx)
    let mut before_removes: Vec<(&BTreeSet<Id>, usize)> = Vec::new(); // (extra_deps, before_idx)
    let mut root_removes: Vec<(&BTreeSet<Id>, usize)> = Vec::new(); // (extra_deps, root_idx)

    for (_id, remove) in &standalone_removes {
        for id in &remove.nodes {
            if let Some(op_ref) = id_to_ref.get(id) {
                match op_ref.tag {
                    REF_TAG_RUN => {
                        single_run_removes.push((&remove.extra_dependencies, op_ref.op_idx, op_ref.sub_idx));
                    }
                    REF_TAG_BEFORE => {
                        before_removes.push((&remove.extra_dependencies, op_ref.op_idx));
                    }
                    REF_TAG_ROOT => {
                        root_removes.push((&remove.extra_dependencies, op_ref.op_idx));
                    }
                    _ => {}
                }
            }
        }
    }

    // Encode single-run removes: [count][extra_deps, run_idx, elem_idx]...
    encode_varint(single_run_removes.len(), &mut buf);
    for (extra_deps, run_idx, elem_idx) in &single_run_removes {
        encode_id_set(extra_deps, &mut buf);
        encode_varint(*run_idx, &mut buf);
        encode_varint(*elem_idx, &mut buf);
    }

    // Encode before removes: [count][extra_deps, before_idx]...
    encode_varint(before_removes.len(), &mut buf);
    for (extra_deps, before_idx) in &before_removes {
        encode_id_set(extra_deps, &mut buf);
        encode_varint(*before_idx, &mut buf);
    }

    // Encode root removes: [count][extra_deps, root_idx]...
    encode_varint(root_removes.len(), &mut buf);
    for (extra_deps, root_idx) in &root_removes {
        encode_id_set(extra_deps, &mut buf);
        encode_varint(*root_idx, &mut buf);
    }

    // Encode orphans (these need tags since they can be any type)
    encode_varint(seq.orphaned.len(), &mut buf);
    for orphan in &seq.orphaned {
        encode_hash_node(orphan, &mut buf);
    }

    buf
}

/// Decode a HashSeq from bytes.
///
/// Format: [roots][runs][befores][removes][orphans]
pub fn decode_hashseq(bytes: &[u8]) -> Result<HashSeq, DecodeError> {
    let mut pos = 0;
    let mut seq = HashSeq::default();

    // We need to collect IDs as we decode to resolve OpRefs in removes
    let mut root_ids: Vec<Id> = Vec::new();
    let mut run_element_ids: Vec<Vec<Id>> = Vec::new();
    let mut before_ids: Vec<Id> = Vec::new();

    // Decode roots
    let (num_roots, size) = decode_varint(bytes)?;
    pos += size;
    for _ in 0..num_roots {
        let (extra_deps, size) = decode_id_set(&bytes[pos..])?;
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

    // Decode runs
    let (num_runs, size) = decode_varint(&bytes[pos..])?;
    pos += size;
    for _ in 0..num_runs {
        let (run, size) = decode_run(&bytes[pos..])?;
        pos += size;
        run_element_ids.push(run.elements.clone());
        for node in run.decompress() {
            seq.apply(node);
        }
    }

    // Decode befores
    let (num_befores, size) = decode_varint(&bytes[pos..])?;
    pos += size;
    for _ in 0..num_befores {
        let (extra_deps, size) = decode_id_set(&bytes[pos..])?;
        pos += size;
        let (anchor, size) = decode_id(&bytes[pos..])?;
        pos += size;
        let (ch, size) = decode_utf8_char(&bytes[pos..])?;
        pos += size;
        let node = HashNode {
            extra_dependencies: extra_deps,
            op: Op::InsertBefore(anchor, ch),
        };
        before_ids.push(node.id());
        seq.apply(node);
    }

    // Decode forward remove runs: [count][first_extra_deps, run_idx, start_idx, end_idx]...
    let (num_forward_runs, size) = decode_varint(&bytes[pos..])?;
    pos += size;
    for _ in 0..num_forward_runs {
        let (first_extra_deps, size) = decode_id_set(&bytes[pos..])?;
        pos += size;
        let (run_idx, size) = decode_varint(&bytes[pos..])?;
        pos += size;
        let (start_idx, size) = decode_varint(&bytes[pos..])?;
        pos += size;
        let (end_idx, size) = decode_varint(&bytes[pos..])?;
        pos += size;

        // Expand the remove run into individual removes (forward direction)
        let run_elements = run_element_ids.get(run_idx)
            .ok_or(DecodeError::InvalidIdIndex(run_idx))?;

        let mut prev_remove_id: Option<Id> = None;
        for elem_idx in start_idx..=end_idx {
            let removed_id = run_elements.get(elem_idx)
                .copied()
                .ok_or(DecodeError::InvalidIdIndex(elem_idx))?;

            let extra_deps = if let Some(prev_id) = prev_remove_id {
                let mut deps = BTreeSet::new();
                deps.insert(prev_id);
                deps
            } else {
                first_extra_deps.clone()
            };

            let node = HashNode {
                extra_dependencies: extra_deps,
                op: Op::Remove(std::iter::once(removed_id).collect()),
            };
            prev_remove_id = Some(node.id());
            seq.apply(node);
        }
    }

    // Decode backward remove runs: [count][first_extra_deps, run_idx, start_idx, end_idx]...
    let (num_backward_runs, size) = decode_varint(&bytes[pos..])?;
    pos += size;
    for _ in 0..num_backward_runs {
        let (first_extra_deps, size) = decode_id_set(&bytes[pos..])?;
        pos += size;
        let (run_idx, size) = decode_varint(&bytes[pos..])?;
        pos += size;
        let (start_idx, size) = decode_varint(&bytes[pos..])?;
        pos += size;
        let (end_idx, size) = decode_varint(&bytes[pos..])?;
        pos += size;

        // Expand the remove run into individual removes (backward direction: start > end)
        let run_elements = run_element_ids.get(run_idx)
            .ok_or(DecodeError::InvalidIdIndex(run_idx))?;

        let mut prev_remove_id: Option<Id> = None;
        for elem_idx in (end_idx..=start_idx).rev() {
            let removed_id = run_elements.get(elem_idx)
                .copied()
                .ok_or(DecodeError::InvalidIdIndex(elem_idx))?;

            let extra_deps = if let Some(prev_id) = prev_remove_id {
                let mut deps = BTreeSet::new();
                deps.insert(prev_id);
                deps
            } else {
                first_extra_deps.clone()
            };

            let node = HashNode {
                extra_dependencies: extra_deps,
                op: Op::Remove(std::iter::once(removed_id).collect()),
            };
            prev_remove_id = Some(node.id());
            seq.apply(node);
        }
    }

    // Decode single-run removes: [count][extra_deps, run_idx, elem_idx]...
    let (num_single_run, size) = decode_varint(&bytes[pos..])?;
    pos += size;
    for _ in 0..num_single_run {
        let (extra_deps, size) = decode_id_set(&bytes[pos..])?;
        pos += size;
        let (run_idx, size) = decode_varint(&bytes[pos..])?;
        pos += size;
        let (elem_idx, size) = decode_varint(&bytes[pos..])?;
        pos += size;

        let removed_id = run_element_ids.get(run_idx)
            .and_then(|e| e.get(elem_idx))
            .copied()
            .ok_or(DecodeError::InvalidIdIndex(elem_idx))?;

        seq.apply(HashNode {
            extra_dependencies: extra_deps,
            op: Op::Remove(std::iter::once(removed_id).collect()),
        });
    }

    // Decode before removes: [count][extra_deps, before_idx]...
    let (num_before_removes, size) = decode_varint(&bytes[pos..])?;
    pos += size;
    for _ in 0..num_before_removes {
        let (extra_deps, size) = decode_id_set(&bytes[pos..])?;
        pos += size;
        let (before_idx, size) = decode_varint(&bytes[pos..])?;
        pos += size;

        let removed_id = before_ids.get(before_idx)
            .copied()
            .ok_or(DecodeError::InvalidIdIndex(before_idx))?;

        seq.apply(HashNode {
            extra_dependencies: extra_deps,
            op: Op::Remove(std::iter::once(removed_id).collect()),
        });
    }

    // Decode root removes: [count][extra_deps, root_idx]...
    let (num_root_removes, size) = decode_varint(&bytes[pos..])?;
    pos += size;
    for _ in 0..num_root_removes {
        let (extra_deps, size) = decode_id_set(&bytes[pos..])?;
        pos += size;
        let (root_idx, size) = decode_varint(&bytes[pos..])?;
        pos += size;

        let removed_id = root_ids.get(root_idx)
            .copied()
            .ok_or(DecodeError::InvalidIdIndex(root_idx))?;

        seq.apply(HashNode {
            extra_dependencies: extra_deps,
            op: Op::Remove(std::iter::once(removed_id).collect()),
        });
    }

    // Decode orphans (these have tags)
    let (num_orphans, size) = decode_varint(&bytes[pos..])?;
    pos += size;
    for _ in 0..num_orphans {
        let (op, size) = decode_op(&bytes[pos..])?;
        pos += size;
        if let EncodableOp::Node(node) = op {
            seq.apply(node);
        }
    }

    Ok(seq)
}

// --- Dictionary-based HashSeq encoding/decoding ---
// Format: [id_dict][roots][runs][befores][removes][orphans]
// All ID references use varint indices into the dictionary

/// Encode a HashSeq using an ID dictionary for compact representation.
///
/// Format:
/// - [num_ids: varint][id_0..id_n: 32 bytes each]
/// - [num_roots: varint][roots...]
/// - [num_runs: varint][runs...]
/// - [num_befores: varint][befores...]
/// - [num_removes: varint][removes...]
/// - [num_orphans: varint][orphans...]
pub fn encode_hashseq_dict(seq: &HashSeq) -> Vec<u8> {
    let mut buf = Vec::new();

    // Collect all unique IDs that are actually encoded
    // Note: We don't include node IDs (keys in maps) since those are computed on decode
    // We don't include run.elements since those are reconstructed on decode
    let mut id_set: BTreeSet<Id> = BTreeSet::new();

    // From runs: only insert_after and first_extra_deps (not elements)
    for run in seq.runs.values() {
        id_set.insert(run.insert_after);
        for id in &run.first_extra_deps {
            id_set.insert(*id);
        }
    }

    // From roots: only extra_dependencies (not the root's own ID)
    for root in seq.root_nodes.values() {
        for dep in &root.extra_dependencies {
            id_set.insert(*dep);
        }
    }

    // From befores: anchor and extra_dependencies (not the before's own ID)
    for before in seq.before_nodes.values() {
        id_set.insert(before.anchor);
        for dep in &before.extra_dependencies {
            id_set.insert(*dep);
        }
    }

    // From removes: extra_dependencies and removed node IDs (not the remove's own ID)
    for remove in seq.remove_nodes.values() {
        for dep in &remove.extra_dependencies {
            id_set.insert(*dep);
        }
        for removed_id in &remove.nodes {
            id_set.insert(*removed_id);
        }
    }

    // From orphans
    for orphan in &seq.orphaned {
        for dep in &orphan.extra_dependencies {
            id_set.insert(*dep);
        }
        match &orphan.op {
            Op::InsertRoot(_) => {}
            Op::InsertAfter(id, _) => {
                id_set.insert(*id);
            }
            Op::InsertBefore(id, _) => {
                id_set.insert(*id);
            }
            Op::Remove(ids) => {
                for id in ids {
                    id_set.insert(*id);
                }
            }
        }
    }

    // Build ID -> index mapping
    let id_list: Vec<Id> = id_set.into_iter().collect();
    let id_to_idx: HashMap<Id, usize> = id_list.iter().enumerate().map(|(i, id)| (*id, i)).collect();

    // Encode ID dictionary
    encode_varint(id_list.len(), &mut buf);
    for id in &id_list {
        encode_id(id, &mut buf);
    }

    // Helper to encode an ID as an index
    let encode_idx = |id: &Id, buf: &mut Vec<u8>| {
        let idx = id_to_idx[id];
        encode_varint(idx, buf);
    };

    // Helper to encode a set of IDs as indices
    let encode_idx_set = |ids: &BTreeSet<Id>, buf: &mut Vec<u8>| {
        encode_varint(ids.len(), buf);
        for id in ids {
            encode_varint(id_to_idx[id], buf);
        }
    };

    // Encode roots
    encode_varint(seq.root_nodes.len(), &mut buf);
    for root in seq.root_nodes.values() {
        encode_idx_set(&root.extra_dependencies, &mut buf);
        encode_utf8_char(root.ch, &mut buf);
    }

    // Encode runs
    encode_varint(seq.runs.len(), &mut buf);
    for run in seq.runs.values() {
        encode_idx(&run.insert_after, &mut buf);
        encode_idx_set(&run.first_extra_deps, &mut buf);
        encode_string(&run.run, &mut buf);
    }

    // Encode befores
    encode_varint(seq.before_nodes.len(), &mut buf);
    for before in seq.before_nodes.values() {
        encode_idx_set(&before.extra_dependencies, &mut buf);
        encode_idx(&before.anchor, &mut buf);
        encode_utf8_char(before.ch, &mut buf);
    }

    // Encode removes
    encode_varint(seq.remove_nodes.len(), &mut buf);
    for remove in seq.remove_nodes.values() {
        encode_idx_set(&remove.extra_dependencies, &mut buf);
        encode_varint(remove.nodes.len(), &mut buf);
        for id in &remove.nodes {
            encode_idx(id, &mut buf);
        }
    }

    // Encode orphans
    encode_varint(seq.orphaned.len(), &mut buf);
    for orphan in &seq.orphaned {
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

/// Decode a HashSeq from dictionary-encoded bytes.
pub fn decode_hashseq_dict(bytes: &[u8]) -> Result<HashSeq, DecodeError> {
    let mut pos = 0;

    // Decode ID dictionary
    let (num_ids, size) = decode_varint(bytes)?;
    pos += size;

    let mut id_list: Vec<Id> = Vec::with_capacity(num_ids);
    for _ in 0..num_ids {
        let (id, size) = decode_id(&bytes[pos..])?;
        id_list.push(id);
        pos += size;
    }

    // Helper to decode an index to an ID (bytes should be sliced to current pos)
    let decode_idx_at = |bytes: &[u8]| -> Result<(Id, usize), DecodeError> {
        let (idx, size) = decode_varint(bytes)?;
        let id = id_list
            .get(idx)
            .copied()
            .ok_or(DecodeError::InvalidIdIndex(idx))?;
        Ok((id, size))
    };

    // Helper to decode a set of indices to IDs (bytes should be sliced to current pos)
    let decode_idx_set_at = |bytes: &[u8]| -> Result<(BTreeSet<Id>, usize), DecodeError> {
        let (count, size) = decode_varint(bytes)?;
        let mut total_size = size;
        let mut ids = BTreeSet::new();
        for _ in 0..count {
            let (idx, size) = decode_varint(&bytes[total_size..])?;
            let id = id_list
                .get(idx)
                .copied()
                .ok_or(DecodeError::InvalidIdIndex(idx))?;
            ids.insert(id);
            total_size += size;
        }
        Ok((ids, total_size))
    };

    let mut seq = HashSeq::default();

    // Decode roots
    let (num_roots, size) = decode_varint(&bytes[pos..])?;
    pos += size;
    for _ in 0..num_roots {
        let (extra_deps, size) = decode_idx_set_at(&bytes[pos..])?;
        pos += size;
        let (ch, size) = decode_utf8_char(&bytes[pos..])?;
        pos += size;
        seq.apply(HashNode {
            extra_dependencies: extra_deps,
            op: Op::InsertRoot(ch),
        });
    }

    // Decode runs
    let (num_runs, size) = decode_varint(&bytes[pos..])?;
    pos += size;
    for _ in 0..num_runs {
        let (insert_after, size) = decode_idx_at(&bytes[pos..])?;
        pos += size;
        let (first_extra_deps, size) = decode_idx_set_at(&bytes[pos..])?;
        pos += size;
        let (run_str, size) = decode_string(&bytes[pos..])?;
        pos += size;

        // Reconstruct run by applying nodes
        let mut chars = run_str.chars();
        if let Some(first_char) = chars.next() {
            seq.apply(HashNode {
                extra_dependencies: first_extra_deps.clone(),
                op: Op::InsertAfter(insert_after, first_char),
            });

            // For subsequent chars, we need to compute IDs as we go
            let mut run = Run::new(insert_after, first_extra_deps, first_char);
            for ch in chars {
                let prev_id = run.last_id();
                seq.apply(HashNode {
                    extra_dependencies: BTreeSet::new(),
                    op: Op::InsertAfter(prev_id, ch),
                });
                run.extend(ch);
            }
        }
    }

    // Decode befores
    let (num_befores, size) = decode_varint(&bytes[pos..])?;
    pos += size;
    for _ in 0..num_befores {
        let (extra_deps, size) = decode_idx_set_at(&bytes[pos..])?;
        pos += size;
        let (anchor, size) = decode_idx_at(&bytes[pos..])?;
        pos += size;
        let (ch, size) = decode_utf8_char(&bytes[pos..])?;
        pos += size;
        seq.apply(HashNode {
            extra_dependencies: extra_deps,
            op: Op::InsertBefore(anchor, ch),
        });
    }

    // Decode removes
    let (num_removes, size) = decode_varint(&bytes[pos..])?;
    pos += size;
    for _ in 0..num_removes {
        let (extra_deps, size) = decode_idx_set_at(&bytes[pos..])?;
        pos += size;
        let (num_removed, size) = decode_varint(&bytes[pos..])?;
        pos += size;
        let mut removed_ids = BTreeSet::new();
        for _ in 0..num_removed {
            let (id, size) = decode_idx_at(&bytes[pos..])?;
            pos += size;
            removed_ids.insert(id);
        }
        seq.apply(HashNode {
            extra_dependencies: extra_deps,
            op: Op::Remove(removed_ids),
        });
    }

    // Decode orphans
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
                let (num_removed, size) = decode_varint(&bytes[pos..])?;
                pos += size;
                let mut removed_ids = BTreeSet::new();
                for _ in 0..num_removed {
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

    #[test]
    fn test_hashseq_empty_roundtrip() {
        let seq = HashSeq::default();
        let encoded = encode_hashseq(&seq);
        let decoded = decode_hashseq(&encoded).unwrap();

        // Empty seqs should produce empty strings
        assert_eq!(seq.iter().collect::<String>(), decoded.iter().collect::<String>());
    }

    #[test]
    fn test_hashseq_simple_roundtrip() {
        let mut seq = HashSeq::default();
        seq.insert(0, 'h');
        seq.insert(1, 'e');
        seq.insert(2, 'l');
        seq.insert(3, 'l');
        seq.insert(4, 'o');

        let original_str: String = seq.iter().collect();
        assert_eq!(original_str, "hello");

        let encoded = encode_hashseq(&seq);
        let decoded = decode_hashseq(&encoded).unwrap();

        let decoded_str: String = decoded.iter().collect();
        assert_eq!(decoded_str, "hello");
    }

    #[test]
    fn test_hashseq_with_removes_roundtrip() {
        let mut seq = HashSeq::default();
        seq.insert(0, 'a');
        seq.insert(1, 'b');
        seq.insert(2, 'c');
        seq.remove(1); // Remove 'b'

        let original_str: String = seq.iter().collect();
        assert_eq!(original_str, "ac");

        let encoded = encode_hashseq(&seq);
        let decoded = decode_hashseq(&encoded).unwrap();

        let decoded_str: String = decoded.iter().collect();
        assert_eq!(decoded_str, "ac");
    }

    #[test]
    fn test_hashseq_batch_insert_roundtrip() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "hello world".chars());

        let original_str: String = seq.iter().collect();
        assert_eq!(original_str, "hello world");

        let encoded = encode_hashseq(&seq);
        let decoded = decode_hashseq(&encoded).unwrap();

        let decoded_str: String = decoded.iter().collect();
        assert_eq!(decoded_str, "hello world");
    }

    #[test]
    fn test_hashseq_complex_roundtrip() {
        let mut seq = HashSeq::default();

        // Build up a complex sequence with multiple operations
        seq.insert_batch(0, "hello".chars());
        seq.insert(0, 'X'); // Insert at beginning
        seq.insert(6, 'Y'); // Insert at end
        seq.remove(3); // Remove something in the middle

        let original_str: String = seq.iter().collect();

        let encoded = encode_hashseq(&seq);
        let decoded = decode_hashseq(&encoded).unwrap();

        let decoded_str: String = decoded.iter().collect();
        assert_eq!(decoded_str, original_str);
    }

    #[quickcheck]
    fn prop_hashseq_roundtrip_preserves_content(ops: Vec<(bool, u8, char)>) -> bool {
        let mut seq = HashSeq::default();

        // Build up a sequence with random operations
        for (is_insert, idx, ch) in ops {
            let idx = idx as usize;
            if is_insert {
                let insert_idx = if seq.is_empty() { 0 } else { idx % (seq.len() + 1) };
                seq.insert(insert_idx, ch);
            } else if !seq.is_empty() {
                let remove_idx = idx % seq.len();
                seq.remove(remove_idx);
            }
        }

        let original_str: String = seq.iter().collect();

        let encoded = encode_hashseq(&seq);
        let decoded = decode_hashseq(&encoded).unwrap();

        let decoded_str: String = decoded.iter().collect();
        original_str == decoded_str
    }

    #[quickcheck]
    fn prop_hashseq_roundtrip_preserves_equality(ops: Vec<(bool, u8, char)>) -> bool {
        let mut seq = HashSeq::default();

        for (is_insert, idx, ch) in ops {
            let idx = idx as usize;
            if is_insert {
                let insert_idx = if seq.is_empty() { 0 } else { idx % (seq.len() + 1) };
                seq.insert(insert_idx, ch);
            } else if !seq.is_empty() {
                let remove_idx = idx % seq.len();
                seq.remove(remove_idx);
            }
        }

        let encoded = encode_hashseq(&seq);
        let decoded = decode_hashseq(&encoded).unwrap();

        // HashSeq equality is based on tips
        seq == decoded
    }

    #[quickcheck]
    fn prop_hashseq_batch_roundtrip(text: String, remove_indices: Vec<u8>) -> bool {
        let mut seq = HashSeq::default();

        if !text.is_empty() {
            seq.insert_batch(0, text.chars());

            // Apply some removes
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

        let decoded_str: String = decoded.iter().collect();
        original_str == decoded_str && seq == decoded
    }

    #[quickcheck]
    fn prop_run_roundtrip(run: Run) -> bool {
        let mut buf = Vec::new();
        encode_run(&run, &mut buf);

        let (decoded, size) = decode_run(&buf).unwrap();
        size == buf.len() && decoded == run
    }
}
