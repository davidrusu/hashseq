//! nool delta files: the op-difference between two stores, written to a file
//! that the *first* store can apply to reach the converged (union) state.
//!
//! Container layout:
//!
//!   b"nooldelta2\n" ‖ varint n ‖ n × (varint len ‖ artifact bytes)
//!                   ‖ 0xDE delta message (encoding::encode_delta, to EOF)
//!
//! Ops ride as the standard delta wire frames, addressed by (kind, origin).
//! Value artifacts too long to be their own id (registry values holding
//! object ids) ride whole — without them a receiver could store a put but
//! never resolve it; short ones (most paths) are carried by their id. Apply
//! is idempotent: already-known ops deliver zero.

use std::collections::BTreeSet;

use hashseq::encoding::{
    EncodableOp, decode_id, decode_op, decode_varint, encode_delta, encode_varint,
};
use hashseq::{HashNode, HashWeb, Id, Op};

pub const MAGIC: &[u8] = b"nooldelta2\n";
/// Deltas from before the id change (see `main::SIDECAR_MAGIC`).
const OLD_MAGIC: &[u8] = b"nooldelta1\n";

/// Ops present in `have` outside `base`'s causal closure (the DAG diff
/// against `base`'s clock), grouped per object, plus the value artifacts
/// those ops commit to that `have` stores.
pub fn diff(base: &HashWeb, have: &HashWeb) -> (Vec<(u8, Id, Vec<HashNode>)>, Vec<Vec<u8>>) {
    let groups = have.deltas_for(&base.clock());
    let mut artifact_ids: BTreeSet<Id> = BTreeSet::new();
    for (_, _, nodes) in &groups {
        for node in nodes {
            // nool authors text (chars, not value payloads), puts, and
            // places — only puts commit to artifacts a receiver may lack.
            if let Op::Put { key, value, .. } = &node.op {
                artifact_ids.insert(*key);
                artifact_ids.insert(*value);
            }
        }
    }
    let artifacts = artifact_ids
        .iter()
        .filter_map(|vid| artifact_bytes_anywhere(have, vid).cloned())
        .collect();
    (groups, artifacts)
}

/// An artifact's bytes from the web-level store or any kv's own store
/// (local puts land in the latter until a save/load round trip).
fn artifact_bytes_anywhere<'a>(web: &'a HashWeb, vid: &Id) -> Option<&'a Vec<u8>> {
    web.artifact_bytes(vid).or_else(|| {
        web.objects()
            .filter_map(|obj| web.kv(obj))
            .find_map(|kv| kv.artifact_bytes(vid))
    })
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
    if bytes.starts_with(OLD_MAGIC) {
        return Err(
            "written by an older nool (before 2026-09-29) — its ops no longer apply".into(),
        );
    }
    let rest = bytes
        .strip_prefix(MAGIC)
        .ok_or("not a nool delta file (bad magic)")?;
    let mut pos = 0;
    let (n, used) = decode_varint(&rest[pos..]).map_err(|e| format!("{e:?}"))?;
    pos += used;
    let mut artifacts = Vec::new(); // `n` is untrusted: no capacity from it
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

/// The groups of a 0xDE delta message: (kind, origin, nodes).
pub fn decode_groups(msg: &[u8]) -> Result<Vec<(u8, Id, Vec<HashNode>)>, String> {
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
        let mut nodes = Vec::new();
        for _ in 0..n {
            let (len, used) = decode_varint(&msg[pos..]).map_err(|e| format!("{e:?}"))?;
            pos += used;
            let frame = pos
                .checked_add(len)
                .and_then(|end| msg.get(pos..end))
                .ok_or("truncated delta node")?;
            pos += len;
            match decode_op(frame).map_err(|e| format!("{e:?}"))? {
                (EncodableOp::Node(node), used) if used == len => nodes.push(node),
                _ => return Err("malformed delta node".into()),
            }
        }
        out.push((kind, origin, nodes));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hashseq::HashSeq;
    use hashseq::encoding::apply_delta;
    use hashseq::value::{KIND_SEQ, object_id};

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
        let groups = decode_groups(msg).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(
            (groups[0].0, groups[0].1, groups[0].2.len()),
            (KIND_SEQ, origin, 5)
        );

        // Apply through a temp web wrapping a's state.
        let mut web = HashWeb::new();
        let obj = web.create_seq(origin);
        assert_eq!(obj, object_id(KIND_SEQ, &origin));
        for (id, node) in a.all_nodes() {
            let _ = web.apply_to_with_id(obj, id, node);
        }
        let delivered = apply_delta(&mut web, msg).unwrap();
        assert_eq!(delivered, 5);
        assert_eq!(
            web.seq(&obj).unwrap().iter().collect::<String>(),
            "base camp\n"
        );
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
        assert_eq!(decode_groups(&msg).unwrap_err(), "truncated delta node");

        // A huge artifact count must not size an allocation.
        let mut file = MAGIC.to_vec();
        encode_varint(1usize << 60, &mut file);
        assert_eq!(parse_file(&file).unwrap_err(), "UnexpectedEof");

        // A delta from before the id change is refused, not applied.
        let mut old = OLD_MAGIC.to_vec();
        encode_varint(0, &mut old);
        assert!(parse_file(&old).unwrap_err().contains("older nool"));
    }
}
