//! nool delta files: the op-difference between two stores, written to a file
//! that the *first* store can apply to reach the converged (union) state.
//!
//! Container layout:
//!
//!   b"nooldelta1\n" ‖ varint n ‖ n × (varint len ‖ artifact bytes)
//!                   ‖ 0xDE delta message (encoding::encode_delta, to EOF)
//!
//! Ops ride as the standard delta wire frames, addressed by (kind, origin).
//! Value artifacts (registry keys and object-id values) are content-addressed
//! and ride whole — without them a receiver could store a put but never
//! resolve what it says. Apply is idempotent: already-known ops deliver zero.

use std::collections::BTreeSet;

use hashseq::encoding::{decode_id, decode_varint, encode_delta, encode_varint};
use hashseq::value::{KIND_KV, KIND_SEQ};
use hashseq::{HashNode, HashWeb, Id, Op};

pub const MAGIC: &[u8] = b"nooldelta1\n";

/// Ops present in `have` that `base` does not know, grouped per object,
/// plus the value artifacts those ops commit to (resolved from `have`).
pub fn diff(base: &HashWeb, have: &HashWeb) -> (Vec<(u8, Id, Vec<HashNode>)>, Vec<Vec<u8>>) {
    let mut groups = Vec::new();
    let mut artifact_ids: BTreeSet<Id> = BTreeSet::new();
    let mut objs: Vec<Id> = have.objects().copied().collect();
    objs.sort();
    for obj in objs {
        let (kind, origin, nodes) = if let Some(seq) = have.seq(&obj) {
            (KIND_SEQ, seq.origin(), seq.all_nodes())
        } else if let Some(kv) = have.kv(&obj) {
            (KIND_KV, kv.origin(), kv.all_nodes())
        } else {
            continue;
        };
        let missing: Vec<HashNode> = nodes
            .into_iter()
            .filter(|(id, _)| !base.knows(obj, id))
            .map(|(_, node)| node)
            .collect();
        if missing.is_empty() {
            continue;
        }
        for node in &missing {
            // nool authors text (chars, not value payloads), puts, and
            // places — only puts commit to artifacts a receiver may lack.
            if let Op::Put { key, value, .. } = &node.op {
                artifact_ids.insert(*key);
                artifact_ids.insert(*value);
            }
        }
        groups.push((kind, origin, missing));
    }
    let artifacts = artifact_ids
        .iter()
        .filter_map(|vid| artifact_bytes_anywhere(have, vid))
        .collect();
    (groups, artifacts)
}

/// An artifact's canonical bytes from the web-level store or any kv's local
/// store (local puts land in the latter, merged-in ones in the former).
fn artifact_bytes_anywhere(web: &HashWeb, vid: &Id) -> Option<Vec<u8>> {
    if let Some(bytes) = web.artifact_bytes(vid) {
        return Some(bytes.clone());
    }
    for obj in web.objects() {
        if let Some(kv) = web.kv(obj) {
            for (id, bytes) in kv.value_store() {
                if id == vid {
                    return Some(bytes.clone());
                }
            }
        }
    }
    None
}

pub fn encode_file(groups: &[(u8, Id, Vec<HashNode>)], artifacts: &[Vec<u8>]) -> Vec<u8> {
    let mut buf = MAGIC.to_vec();
    encode_varint(artifacts.len(), &mut buf);
    for a in artifacts {
        encode_varint(a.len(), &mut buf);
        buf.extend_from_slice(a);
    }
    buf.extend_from_slice(&encode_delta(groups));
    buf
}

/// Split a delta file into its artifact frames and the 0xDE delta message.
pub fn parse_file(bytes: &[u8]) -> Result<(Vec<Vec<u8>>, &[u8]), String> {
    let rest = bytes
        .strip_prefix(MAGIC)
        .ok_or("not a nool delta file (bad magic)")?;
    let mut pos = 0;
    let (n, used) = decode_varint(&rest[pos..]).map_err(|e| format!("{e:?}"))?;
    pos += used;
    let mut artifacts = Vec::with_capacity(n);
    for _ in 0..n {
        let (len, used) = decode_varint(&rest[pos..]).map_err(|e| format!("{e:?}"))?;
        pos += used;
        let frame = pos
            .checked_add(len)
            .and_then(|end| rest.get(pos..end))
            .ok_or("truncated artifact frame")?;
        pos += len;
        artifacts.push(frame.to_vec());
    }
    Ok((artifacts, &rest[pos..]))
}

/// The (kind, origin, op-count) headers of a delta message, without decoding
/// node bodies — enough to sanity-check what a delta addresses.
pub fn group_heads(msg: &[u8]) -> Result<Vec<(u8, Id, usize)>, String> {
    if msg.first() != Some(&0xDE) {
        return Err("not a delta message".into());
    }
    let mut pos = 1;
    let mut out = Vec::new();
    while pos < msg.len() {
        let kind = msg[pos];
        pos += 1;
        let (origin, used) = decode_id(&msg[pos..]).map_err(|e| format!("{e:?}"))?;
        pos += used;
        let (n, used) = decode_varint(&msg[pos..]).map_err(|e| format!("{e:?}"))?;
        pos += used;
        for _ in 0..n {
            let (len, used) = decode_varint(&msg[pos..]).map_err(|e| format!("{e:?}"))?;
            pos += used;
            if pos.checked_add(len).and_then(|end| msg.get(pos..end)).is_none() {
                return Err("truncated delta node".into());
            }
            pos += len;
        }
        out.push((kind, origin, n));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hashseq::HashSeq;
    use hashseq::encoding::apply_delta;
    use hashseq::value::object_id;

    #[test]
    fn delta_file_roundtrips_and_applies_idempotently() {
        let origin = Id([9u8; 32]);
        let mut a = HashSeq::new(origin);
        a.insert_batch(0, "base\n".chars());
        let mut b = a.clone();
        b.insert_batch(4, " camp".chars());

        // Ops b has that a lacks, shipped as a delta file.
        let missing: Vec<HashNode> = b
            .all_nodes()
            .into_iter()
            .filter(|(id, _)| !a.contains_node(id))
            .map(|(_, n)| n)
            .collect();
        assert!(!missing.is_empty());
        let file = encode_file(&[(KIND_SEQ, origin, missing)], &[]);

        let (artifacts, msg) = parse_file(&file).unwrap();
        assert!(artifacts.is_empty());
        assert_eq!(group_heads(msg).unwrap(), vec![(KIND_SEQ, origin, 5)]);

        // Apply through a temp web wrapping a's state.
        let mut web = HashWeb::new();
        let obj = web.create_seq(origin);
        assert_eq!(obj, object_id(KIND_SEQ, &origin));
        for (id, node) in a.all_nodes() {
            web.apply_to_with_id(obj, id, node);
        }
        let delivered = apply_delta(&mut web, msg).unwrap();
        assert_eq!(delivered, 5);
        assert_eq!(web.seq(&obj).unwrap().iter().collect::<String>(), "base camp\n");
        // Replay: nothing new.
        assert_eq!(apply_delta(&mut web, msg).unwrap(), 0);
    }

    #[test]
    fn huge_varint_lengths_are_rejected_not_overflowed() {
        // One artifact whose claimed length is usize::MAX - 1: `pos + len`
        // would wrap; it must read as truncated instead.
        let mut file = MAGIC.to_vec();
        encode_varint(1, &mut file);
        encode_varint(usize::MAX - 1, &mut file);
        assert_eq!(parse_file(&file).unwrap_err(), "truncated artifact frame");

        let mut msg = vec![0xDE, KIND_SEQ];
        msg.extend_from_slice(&[0u8; 32]);
        encode_varint(1, &mut msg);
        encode_varint(usize::MAX - 1, &mut msg);
        assert_eq!(group_heads(&msg).unwrap_err(), "truncated delta node");
    }
}
