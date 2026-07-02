//! Node identity: the op shape and its self-certifying id
//! (FRAMEWORK.md "The op shape"; GRAMMAR_SPEC.md Part A).
//!
//! A node is `{ refs, op }` — one flat reference set plus a meaning over it.
//! In memory we store the normalized split: `pins = refs ∖ named` (the
//! frontier pins) plus the op whose fields carry the named ids. The preimage
//! is the envelope grammar over the unified, sorted `refs(u)`.

use std::collections::BTreeSet;
use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

use crate::Id;
use crate::value::{NODE_CONTEXT, char_value_id};

/// Op kind tags (GRAMMAR_SPEC.md "Op kinds"). One shared tag space; kinds are
/// tags inside the encoding, never separate contexts.
pub const KIND_INSERT: u8 = 0;
pub const KIND_REMOVE: u8 = 1;
pub const KIND_MOVE: u8 = 2;
pub const KIND_PUT: u8 = 3;
pub const KIND_MARK: u8 = 4;

/// THE glued point — the addressing primitive shared by inserts, moves, and
/// marks (HASHSEQ_SPEC.md). The side is data, not op kind: a gap *is* an
/// `(anchor, side)` pair. There is deliberately no end sentinel — sentinels
/// are user-space objects.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
pub enum Anchor {
    Before(Id),
    After(Id),
}

impl Anchor {
    #[inline]
    pub fn id(&self) -> &Id {
        match self {
            Anchor::Before(id) | Anchor::After(id) => id,
        }
    }

    /// GRAMMAR side bit: 0 = Before, 1 = After.
    #[inline]
    pub fn side_bit(&self) -> usize {
        match self {
            Anchor::Before(_) => 0,
            Anchor::After(_) => 1,
        }
    }
}

/// An insert's payload — semantically always a value commitment (an id); the
/// `Char` variant is the in-memory/run-column form of a char artifact whose
/// `value_id` is derived on demand (cached). Transport inlines it; identity
/// always hashes the id (GRAMMAR_SPEC.md "Value fields").
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
pub enum Payload {
    /// A char value artifact, stored inline (text's hot path).
    Char(char),
    /// Any other value commitment: a value artifact id, an object's origin id
    /// (a link / transclusion), or an op-node id.
    Id(Id),
}

impl Payload {
    /// The committed value id — what the preimage hashes.
    #[inline]
    pub fn value_id(&self) -> Id {
        match self {
            Payload::Char(c) => char_value_id(*c),
            Payload::Id(id) => *id,
        }
    }
}

/// The op kinds. Named ids (anchor / targets / overwrites) live in the op's
/// fields; together with the node's pins they form `refs(u)`.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Op {
    /// Claim a gap: place `payload` at the glued point `at`.
    Insert { at: Anchor, payload: Payload },
    /// Claim liveness: tombstone every target (idempotent union).
    Remove(BTreeSet<Id>),
    /// Claim placement: relocate `target` within its own container
    /// (same-container only — a stable gate) superseding `overwrites`.
    Move {
        target: Id,
        to: Anchor,
        overwrites: BTreeSet<Id>,
    },
    /// Claim a key's register: `key` and `value` are value commitments;
    /// `overwrites` names the per-key heads this put saw and replaces.
    Put {
        key: Id,
        value: Id,
        overwrites: BTreeSet<Id>,
    },
    /// Claim a (element, kind) formatting register over the span
    /// `[start, end]` (MARKS.md): `kind_v`/`value` are value commitments
    /// (`TOMBSTONE` value = unmark); `overwrites` names the same-kind marks
    /// this op saw and supersedes within its range.
    Mark {
        start: Anchor,
        end: Anchor,
        kind_v: Id,
        value: Id,
        overwrites: BTreeSet<Id>,
    },
}

impl Op {
    /// Convenience ctor: insert `ch` after `anchor`.
    pub fn insert_after(anchor: Id, ch: char) -> Op {
        Op::Insert {
            at: Anchor::After(anchor),
            payload: Payload::Char(ch),
        }
    }

    /// Convenience ctor: insert `ch` before `anchor`.
    pub fn insert_before(anchor: Id, ch: char) -> Op {
        Op::Insert {
            at: Anchor::Before(anchor),
            payload: Payload::Char(ch),
        }
    }

    #[inline]
    fn kind(&self) -> u8 {
        match self {
            Op::Insert { .. } => KIND_INSERT,
            Op::Remove(_) => KIND_REMOVE,
            Op::Move { .. } => KIND_MOVE,
            Op::Put { .. } => KIND_PUT,
            Op::Mark { .. } => KIND_MARK,
        }
    }

    /// First named single-id role, if any (no allocation).
    #[inline]
    fn named_primary(&self) -> Option<&Id> {
        match self {
            Op::Insert { at, .. } => Some(at.id()),
            Op::Move { target, .. } => Some(target),
            Op::Mark { start, .. } => Some(start.id()),
            Op::Remove(_) | Op::Put { .. } => None,
        }
    }

    /// Second named single-id role, if any.
    #[inline]
    fn named_secondary(&self) -> Option<&Id> {
        match self {
            Op::Move { to, .. } => Some(to.id()),
            Op::Mark { end, .. } => Some(end.id()),
            _ => None,
        }
    }

    /// Named set-valued role, if any.
    #[inline]
    fn named_set(&self) -> Option<&BTreeSet<Id>> {
        match self {
            Op::Remove(targets) => Some(targets),
            Op::Move { overwrites, .. }
            | Op::Put { overwrites, .. }
            | Op::Mark { overwrites, .. } => Some(overwrites),
            Op::Insert { .. } => None,
        }
    }
}

/// A node: `refs(u) = pins ∪ named(u)`, stored normalized (`pins` holds only
/// the refs no role addresses — the honest author's frontier remainder).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HashNode {
    pub pins: BTreeSet<Id>,
    pub op: Op,
}

static NODE_HASHER: LazyLock<blake3::Hasher> =
    LazyLock::new(|| blake3::Hasher::new_derive_key(NODE_CONTEXT));

pub(crate) fn update_varint(hasher: &mut blake3::Hasher, mut value: usize) {
    // LEB128, identical to `encoding::encode_varint`, minimal form.
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        hasher.update(&[byte]);
        if value == 0 {
            break;
        }
    }
}

#[inline]
pub(crate) fn varint_len(mut value: usize) -> usize {
    let mut n = 1;
    while value >= 0x80 {
        value >>= 7;
        n += 1;
    }
    n
}

impl HashNode {
    /// Every id this node references: `refs(u) = pins ∪ named(u)`.
    /// Delivery buffers on exactly this set; there is no other gating.
    pub fn iter_refs(&self) -> impl Iterator<Item = &Id> {
        self.pins
            .iter()
            .chain(self.op.named_primary())
            .chain(self.op.named_secondary())
            .chain(self.op.named_set().into_iter().flatten())
    }

    /// The sorted, deduplicated `refs(u)` table — the envelope's refs.
    /// `pins` is normalized to exclude named ids, and `pins`/set roles are
    /// `BTreeSet`s (already sorted), so this is a merge.
    fn refs_table(&self) -> Vec<Id> {
        let mut refs: Vec<Id> = self.iter_refs().copied().collect();
        refs.sort_unstable();
        refs.dedup();
        refs
    }

    /// The node's self-certifying id:
    /// `BLAKE3::derive_key(NODE_CONTEXT, envelope ‖ body)` per GRAMMAR_SPEC.md
    /// Part A. Streamed without building a buffer;
    /// `id_preimage_is_the_canonical_encoding` locks this to
    /// `encoding::encode_node_preimage`.
    pub fn id(&self) -> Id {
        debug_assert!(
            {
                let named: Vec<&Id> = self
                    .op
                    .named_primary()
                    .into_iter()
                    .chain(self.op.named_secondary())
                    .chain(self.op.named_set().into_iter().flatten())
                    .collect();
                named.iter().all(|n| !self.pins.contains(n))
            },
            "pins must be normalized: refs ∖ named"
        );

        let mut hasher = NODE_HASHER.clone();

        // Fast path — the typing chain: an insert whose only ref is its
        // anchor. refs = [anchor], anchor ref_idx = 0; every length is a
        // single-byte varint. This is the shape of every run-interior op.
        // The whole 68-byte preimage is assembled on the stack and hashed in
        // one update call (per-update overhead dominates at this size).
        if let Op::Insert { at, payload } = &self.op
            && self.pins.is_empty()
        {
            let mut pre = [0u8; 68];
            pre[0] = KIND_INSERT;
            pre[1] = 1; // ref_count
            pre[2..34].copy_from_slice(&at.id().0);
            pre[34] = 33; // body_len: anchor varint (1) + value id (32)
            pre[35] = at.side_bit() as u8;
            pre[36..68].copy_from_slice(&payload.value_id().0);
            hasher.update(&pre);
            return Id(*hasher.finalize().as_bytes());
        }

        let refs = self.refs_table();
        let ref_idx = |id: &Id| -> usize {
            refs.binary_search(id)
                .expect("named id is in the refs table")
        };

        hasher.update(&[self.op.kind()]);
        update_varint(&mut hasher, refs.len());
        for r in &refs {
            hasher.update(&r.0);
        }

        match &self.op {
            Op::Insert { at, payload } => {
                let packed = (ref_idx(at.id()) << 1) | at.side_bit();
                update_varint(&mut hasher, varint_len(packed) + 32); // body_len
                update_varint(&mut hasher, packed);
                hasher.update(&payload.value_id().0);
            }
            Op::Remove(targets) => {
                // Ascending target indices via a sorted merge walk.
                let idxs = sorted_subset_indices(&refs, targets);
                let body_len = varint_len(idxs.len())
                    + idxs.iter().map(|&i| varint_len(i)).sum::<usize>();
                update_varint(&mut hasher, body_len);
                update_varint(&mut hasher, idxs.len());
                for i in idxs {
                    update_varint(&mut hasher, i);
                }
            }
            Op::Move {
                target,
                to,
                overwrites,
            } => {
                let t = ref_idx(target);
                let packed = (ref_idx(to.id()) << 1) | to.side_bit();
                let idxs = sorted_subset_indices(&refs, overwrites);
                let body_len = varint_len(t)
                    + varint_len(packed)
                    + varint_len(idxs.len())
                    + idxs.iter().map(|&i| varint_len(i)).sum::<usize>();
                update_varint(&mut hasher, body_len);
                update_varint(&mut hasher, t);
                update_varint(&mut hasher, packed);
                update_varint(&mut hasher, idxs.len());
                for i in idxs {
                    update_varint(&mut hasher, i);
                }
            }
            Op::Put {
                key,
                value,
                overwrites,
            } => {
                let idxs = sorted_subset_indices(&refs, overwrites);
                let body_len = 64
                    + varint_len(idxs.len())
                    + idxs.iter().map(|&i| varint_len(i)).sum::<usize>();
                update_varint(&mut hasher, body_len);
                hasher.update(&key.0);
                hasher.update(&value.0);
                update_varint(&mut hasher, idxs.len());
                for i in idxs {
                    update_varint(&mut hasher, i);
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
                let idxs = sorted_subset_indices(&refs, overwrites);
                let body_len = varint_len(sp)
                    + varint_len(ep)
                    + 64
                    + varint_len(idxs.len())
                    + idxs.iter().map(|&i| varint_len(i)).sum::<usize>();
                update_varint(&mut hasher, body_len);
                update_varint(&mut hasher, sp);
                update_varint(&mut hasher, ep);
                hasher.update(&kind_v.0);
                hasher.update(&value.0);
                update_varint(&mut hasher, idxs.len());
                for i in idxs {
                    update_varint(&mut hasher, i);
                }
            }
        }
        Id(*hasher.finalize().as_bytes())
    }
}

/// Indices (ascending) of `subset`'s members within sorted `refs`.
/// Both are sorted, so this is a linear merge walk.
pub(crate) fn sorted_subset_indices(refs: &[Id], subset: &BTreeSet<Id>) -> Vec<usize> {
    let mut idxs = Vec::with_capacity(subset.len());
    let mut i = 0usize;
    for want in subset {
        while refs[i] != *want {
            i += 1;
        }
        idxs.push(i);
        i += 1;
    }
    idxs
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tid(n: u8) -> Id {
        Id([n; 32])
    }

    /// The id preimage must be byte-identical to
    /// `encoding::encode_node_preimage`; if the streaming copy in `id()`
    /// drifts from the reference encoder, this fails.
    #[test]
    fn id_preimage_is_the_canonical_encoding() {
        let a = tid(0xAA);
        let b = tid(0xBB);
        let pins = BTreeSet::from_iter([a, tid(0x07)]);
        let nodes = [
            HashNode {
                pins: BTreeSet::new(),
                op: Op::insert_after(a, 'x'),
            },
            HashNode {
                pins: pins.clone(),
                op: Op::insert_after(b, '🦀'),
            },
            HashNode {
                pins: pins.clone(),
                op: Op::insert_before(b, '\u{0}'),
            },
            HashNode {
                pins: BTreeSet::new(),
                op: Op::Insert {
                    at: Anchor::After(a),
                    payload: Payload::Id(tid(0x33)),
                },
            },
            HashNode {
                pins: BTreeSet::new(),
                op: Op::Remove(BTreeSet::from_iter([a, b])),
            },
            HashNode {
                pins: pins.clone(),
                op: Op::Remove(BTreeSet::from_iter([b])),
            },
            HashNode {
                pins: pins.clone(),
                op: Op::Move {
                    target: tid(0x01),
                    to: Anchor::Before(tid(0x02)),
                    overwrites: BTreeSet::from_iter([tid(0x03)]),
                },
            },
            HashNode {
                pins: pins.clone(),
                op: Op::Put {
                    key: tid(0x11),
                    value: tid(0x22),
                    overwrites: BTreeSet::from_iter([tid(0x0C), tid(0x0D)]),
                },
            },
            HashNode {
                pins,
                op: Op::Mark {
                    start: Anchor::Before(tid(0x01)),
                    end: Anchor::After(tid(0x02)),
                    kind_v: tid(0x30),
                    value: tid(0x31),
                    overwrites: BTreeSet::from_iter([tid(0x03)]),
                },
            },
        ];
        for node in nodes {
            let mut preimage = Vec::new();
            crate::encoding::encode_node_preimage(&node, &mut preimage);
            let mut hasher = blake3::Hasher::new_derive_key(NODE_CONTEXT);
            hasher.update(&preimage);
            assert_eq!(
                node.id().0,
                *hasher.finalize().as_bytes(),
                "id() drifted from the canonical preimage for {node:?}"
            );
        }
    }

    /// The fast path (pins-empty insert) and the general path must agree.
    #[test]
    fn fast_path_matches_general_path() {
        // Same op via the general path by putting a pin that IS the anchor…
        // is not allowed (normalization), so compare against the reference
        // encoder instead — covered by the preimage lock — and additionally
        // check that a pins-empty insert and one with an unrelated pin
        // differ (the pin is committed).
        let with_pin = HashNode {
            pins: BTreeSet::from_iter([tid(9)]),
            op: Op::insert_after(tid(1), 'a'),
        };
        let without = HashNode {
            pins: BTreeSet::new(),
            op: Op::insert_after(tid(1), 'a'),
        };
        assert_ne!(with_pin.id(), without.id());
    }

    /// Payload identity: a char payload and its value id hash identically —
    /// inline vs by-id is transport, never identity.
    #[test]
    fn payload_char_and_id_forms_agree() {
        let by_char = HashNode {
            pins: BTreeSet::new(),
            op: Op::Insert {
                at: Anchor::After(tid(1)),
                payload: Payload::Char('q'),
            },
        };
        let by_id = HashNode {
            pins: BTreeSet::new(),
            op: Op::Insert {
                at: Anchor::After(tid(1)),
                payload: Payload::Id(crate::value::char_value_id('q')),
            },
        };
        assert_eq!(by_char.id(), by_id.id());
    }

    /// Side is data: Before(x) and After(x) are different gaps.
    #[test]
    fn anchor_side_changes_identity() {
        let before = HashNode {
            pins: BTreeSet::new(),
            op: Op::insert_before(tid(1), 'a'),
        };
        let after = HashNode {
            pins: BTreeSet::new(),
            op: Op::insert_after(tid(1), 'a'),
        };
        assert_ne!(before.id(), after.id());
    }

    #[test]
    fn iter_refs_is_pins_union_named() {
        let node = HashNode {
            pins: BTreeSet::from_iter([tid(5)]),
            op: Op::Move {
                target: tid(1),
                to: Anchor::After(tid(2)),
                overwrites: BTreeSet::from_iter([tid(3)]),
            },
        };
        let refs: BTreeSet<Id> = node.iter_refs().copied().collect();
        assert_eq!(refs, BTreeSet::from_iter([tid(1), tid(2), tid(3), tid(5)]));
    }
}
