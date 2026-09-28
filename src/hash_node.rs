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
use crate::value::{NODE_CONTEXT, VALUE_FIELD_MAX, char_value_id, value_field};

/// Op kind tags (GRAMMAR_SPEC.md "Op kinds"). One shared tag space; kinds are
/// tags inside the encoding, never separate contexts.
pub const KIND_INSERT: u8 = 0;
pub const KIND_REMOVE: u8 = 1;
pub const KIND_MOVE: u8 = 2;
pub const KIND_PUT: u8 = 3;
pub const KIND_MARK: u8 = 4;
pub const KIND_PLACE: u8 = 5;

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

    pub fn is_after(&self) -> bool {
        matches!(self, Anchor::After(_))
    }

    pub fn is_before(&self) -> bool {
        matches!(self, Anchor::Before(_))
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

    /// The stored form: a by-id payload whose value this replica can
    /// resolve becomes the resolved value. Identity is unchanged (the
    /// preimage hashes the value id either way); only the rendering is —
    /// the char instead of an opaque atom. A char's value id is its
    /// identity form (`value::char_of_value_id`), so every replica
    /// resolves it the same way without holding anything.
    #[inline]
    pub fn resolved(self) -> Payload {
        match self {
            Payload::Id(id) => match crate::value::char_of_value_id(&id) {
                Some(c) => Payload::Char(c),
                None => self,
            },
            Payload::Char(_) => self,
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
    /// (same-container only — a stable admission rule) superseding `overwrites`.
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
    /// Claim the object's containment register (PLACEMENT_SPEC.md): which
    /// link atom — anywhere in the store — places this object. `placed_at`
    /// is a value commitment, never a ref (an Insert op id in some
    /// container, or `TOMBSTONE` = detach); `overwrites` names the
    /// placement heads this op saw and replaces, in THIS object's own DAG.
    /// Valid in any object kind — the register concerns the object's
    /// placement, not its content projection.
    Place {
        placed_at: Id,
        overwrites: BTreeSet<Id>,
    },
}

impl Op {
    pub fn insert(at: Anchor, payload: Payload) -> Op {
        Op::Insert { at, payload }
    }

    pub fn insert_after(at: Id, ch: char) -> Op {
        Self::insert(Anchor::After(at), Payload::Char(ch))
    }

    pub fn insert_before(at: Id, ch: char) -> Op {
        Self::insert(Anchor::Before(at), Payload::Char(ch))
    }

    /// First named single-id role, if any (no allocation).
    #[inline]
    fn named_primary(&self) -> Option<&Id> {
        match self {
            Op::Insert { at, .. } => Some(at.id()),
            Op::Move { target, .. } => Some(target),
            Op::Mark { start, .. } => Some(start.id()),
            // Place's placed_at is a value commitment, never a named ref.
            Op::Remove(_) | Op::Put { .. } | Op::Place { .. } => None,
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
            | Op::Mark { overwrites, .. }
            | Op::Place { overwrites, .. } => Some(overwrites),
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

/// Pre-hashed node context key (see `value.rs` — output is identical to
/// `Hasher::new_derive_key(NODE_CONTEXT)`, construction is much cheaper).
static NODE_KEY: LazyLock<blake3::hazmat::ContextKey> =
    LazyLock::new(|| blake3::hazmat::hash_derive_key_context(NODE_CONTEXT));

#[inline]
fn node_hasher() -> blake3::Hasher {
    use blake3::hazmat::HasherExt;
    blake3::Hasher::new_from_context_key(&NODE_KEY)
}

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
    /// Delivery buffers on exactly this set; nothing else holds delivery back.
    pub fn iter_refs(&self) -> impl Iterator<Item = &Id> {
        self.pins
            .iter()
            .chain(self.op.named_primary())
            .chain(self.op.named_secondary())
            .chain(self.op.named_set().into_iter().flatten())
    }

    /// The ids a role of `op` already names.
    fn named(&self) -> impl Iterator<Item = &Id> {
        self.op
            .named_primary()
            .into_iter()
            .chain(self.op.named_secondary())
            .chain(self.op.named_set().into_iter().flatten())
    }

    /// `pins ∩ named = ∅` — the stored form every constructor maintains and
    /// the wire decoders reject violations of (`DecodeError::RedundantPin`).
    pub fn is_normalized(&self) -> bool {
        self.named().all(|n| !self.pins.contains(n))
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
            self.is_normalized(),
            "pins must be normalized: refs ∖ named"
        );

        // Fast path — the typing chain: an insert whose only ref is its
        // anchor.
        if let Op::Insert { at, payload } = &self.op
            && self.pins.is_empty()
        {
            return chain_insert_id(at.id(), at.side_bit(), payload);
        }
        // Fast path — a backspace: one target, at most one pin.
        if let Op::Remove(targets) = &self.op
            && targets.len() == 1
            && self.pins.len() <= 1
        {
            let target = targets.first().expect("one target");
            return single_remove_id(target, self.pins.first());
        }

        let refs = self.refs_table();
        let ref_idx = |id: &Id| -> usize {
            refs.binary_search(id)
                .expect("named id is in the refs table")
        };
        let body = match &self.op {
            Op::Insert { at, payload } => ResolvedBody::Insert {
                packed: (ref_idx(at.id()) << 1) | at.side_bit(),
                payload: payload.value_id(),
            },
            Op::Remove(targets) => ResolvedBody::Remove(sorted_subset_indices(&refs, targets)),
            Op::Move {
                target,
                to,
                overwrites,
            } => ResolvedBody::Move {
                target: ref_idx(target),
                packed: (ref_idx(to.id()) << 1) | to.side_bit(),
                overwrites: sorted_subset_indices(&refs, overwrites),
            },
            Op::Put {
                key,
                value,
                overwrites,
            } => ResolvedBody::Put {
                key: *key,
                value: *value,
                overwrites: sorted_subset_indices(&refs, overwrites),
            },
            Op::Mark {
                start,
                end,
                kind_v,
                value,
                overwrites,
            } => ResolvedBody::Mark {
                start: (ref_idx(start.id()) << 1) | start.side_bit(),
                end: (ref_idx(end.id()) << 1) | end.side_bit(),
                kind_v: *kind_v,
                value: *value,
                overwrites: sorted_subset_indices(&refs, overwrites),
            },
            Op::Place {
                placed_at,
                overwrites,
            } => ResolvedBody::Place {
                placed_at: *placed_at,
                overwrites: sorted_subset_indices(&refs, overwrites),
            },
        };
        hash_resolved(refs.iter(), &body)
    }
}

/// The id of the typing-chain shape: an insert whose only ref is its
/// anchor. refs = [anchor], anchor ref_idx = 0; every length is a
/// single-byte varint. This is the shape of every run-interior op. The
/// whole preimage is assembled on the stack and hashed in one update call
/// (per-update overhead dominates at this size). A char payload's value
/// field is its short form, so the preimage is 39–42 bytes: one BLAKE3
/// block, one compression.
#[inline]
pub(crate) fn chain_insert_id(anchor: &Id, side_bit: usize, payload: &Payload) -> Id {
    let mut pre = [0u8; 36 + VALUE_FIELD_MAX];
    pre[0] = KIND_INSERT;
    pre[1] = 1; // ref_count
    pre[2..34].copy_from_slice(&anchor.0);
    let field: &mut [u8; VALUE_FIELD_MAX] =
        (&mut pre[36..]).try_into().expect("sized for one field");
    let n = value_field(&payload.value_id(), field);
    pre[34] = 1 + n as u8; // body_len: anchor varint (1) + value field
    pre[35] = side_bit as u8;
    let mut hasher = node_hasher();
    hasher.update(&pre[..36 + n]);
    Id(*hasher.finalize().as_bytes())
}

/// The id of the backspace shape: a remove of one target with at most one
/// pin (the previous op of a delete chain). Stack-assembled like
/// `chain_insert_id`: `kind ‖ ref_count ‖ refs (sorted) ‖ body_len = 2 ‖
/// count = 1 ‖ target's ref index` — 37 bytes, or 69 with the pin.
#[inline]
pub(crate) fn single_remove_id(target: &Id, pin: Option<&Id>) -> Id {
    let mut pre = [0u8; 2 + 64 + 3];
    pre[0] = KIND_REMOVE;
    let (refs_end, target_idx) = match pin {
        None => {
            pre[1] = 1;
            pre[2..34].copy_from_slice(&target.0);
            (34, 0)
        }
        Some(pin) => {
            pre[1] = 2;
            let (lo, hi, target_idx) = if pin < target {
                (pin, target, 1)
            } else {
                (target, pin, 0)
            };
            pre[2..34].copy_from_slice(&lo.0);
            pre[34..66].copy_from_slice(&hi.0);
            (66, target_idx)
        }
    };
    pre[refs_end..refs_end + 3].copy_from_slice(&[2, 1, target_idx]);
    let mut hasher = node_hasher();
    hasher.update(&pre[..refs_end + 3]);
    Id(*hasher.finalize().as_bytes())
}

/// A node body with every named ref already its index in the refs table
/// (anchors packed as `(index << 1) | side_bit`, sets as ascending
/// indices). Both node forms — `HashNode` in id space, `InternedHashNode`
/// in handle space — reduce to this, so the preimage layout lives once
/// (`hash_resolved`).
pub(crate) enum ResolvedBody {
    Insert {
        packed: usize,
        payload: Id,
    },
    Remove(Vec<usize>),
    Move {
        target: usize,
        packed: usize,
        overwrites: Vec<usize>,
    },
    Put {
        key: Id,
        value: Id,
        overwrites: Vec<usize>,
    },
    Mark {
        start: usize,
        end: usize,
        kind_v: Id,
        value: Id,
        overwrites: Vec<usize>,
    },
    Place {
        placed_at: Id,
        overwrites: Vec<usize>,
    },
}

/// `BLAKE3::derive_key(NODE_CONTEXT, kind ‖ ref_count ‖ refs ‖ body_len ‖
/// body)` (GRAMMAR_SPEC.md Part A) from the sorted refs table and a
/// resolved body. Streamed without building a buffer.
pub(crate) fn hash_resolved<'a>(
    refs: impl ExactSizeIterator<Item = &'a Id>,
    body: &ResolvedBody,
) -> Id {
    let set_len = |idxs: &[usize]| {
        varint_len(idxs.len()) + idxs.iter().map(|&i| varint_len(i)).sum::<usize>()
    };
    let put_set = |hasher: &mut blake3::Hasher, idxs: &[usize]| {
        update_varint(hasher, idxs.len());
        for &i in idxs {
            update_varint(hasher, i);
        }
    };

    let mut hasher = node_hasher();
    let kind = match body {
        ResolvedBody::Insert { .. } => KIND_INSERT,
        ResolvedBody::Remove(_) => KIND_REMOVE,
        ResolvedBody::Move { .. } => KIND_MOVE,
        ResolvedBody::Put { .. } => KIND_PUT,
        ResolvedBody::Mark { .. } => KIND_MARK,
        ResolvedBody::Place { .. } => KIND_PLACE,
    };
    hasher.update(&[kind]);
    update_varint(&mut hasher, refs.len());
    for r in refs {
        hasher.update(&r.0);
    }

    match body {
        ResolvedBody::Insert { packed, payload } => {
            let (field, n) = field_of(payload);
            update_varint(&mut hasher, varint_len(*packed) + n); // body_len
            update_varint(&mut hasher, *packed);
            hasher.update(&field[..n]);
        }
        ResolvedBody::Remove(idxs) => {
            update_varint(&mut hasher, set_len(idxs));
            put_set(&mut hasher, idxs);
        }
        ResolvedBody::Move {
            target,
            packed,
            overwrites,
        } => {
            update_varint(
                &mut hasher,
                varint_len(*target) + varint_len(*packed) + set_len(overwrites),
            );
            update_varint(&mut hasher, *target);
            update_varint(&mut hasher, *packed);
            put_set(&mut hasher, overwrites);
        }
        ResolvedBody::Put {
            key,
            value,
            overwrites,
        } => {
            let ((kf, kn), (vf, vn)) = (field_of(key), field_of(value));
            update_varint(&mut hasher, kn + vn + set_len(overwrites));
            hasher.update(&kf[..kn]);
            hasher.update(&vf[..vn]);
            put_set(&mut hasher, overwrites);
        }
        ResolvedBody::Mark {
            start,
            end,
            kind_v,
            value,
            overwrites,
        } => {
            let ((kf, kn), (vf, vn)) = (field_of(kind_v), field_of(value));
            update_varint(
                &mut hasher,
                varint_len(*start) + varint_len(*end) + kn + vn + set_len(overwrites),
            );
            update_varint(&mut hasher, *start);
            update_varint(&mut hasher, *end);
            hasher.update(&kf[..kn]);
            hasher.update(&vf[..vn]);
            put_set(&mut hasher, overwrites);
        }
        ResolvedBody::Place {
            placed_at,
            overwrites,
        } => {
            let (pf, pn) = field_of(placed_at);
            update_varint(&mut hasher, pn + set_len(overwrites));
            hasher.update(&pf[..pn]);
            put_set(&mut hasher, overwrites);
        }
    }
    Id(*hasher.finalize().as_bytes())
}

/// A value field's preimage form and its length (`value::value_field`).
#[inline]
fn field_of(id: &Id) -> ([u8; VALUE_FIELD_MAX], usize) {
    let mut out = [0u8; VALUE_FIELD_MAX];
    let n = value_field(id, &mut out);
    (out, n)
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
            // Backspace shapes (`single_remove_id`): no pin, a pin sorting
            // before the target, a pin sorting after it.
            HashNode {
                pins: BTreeSet::new(),
                op: Op::Remove(BTreeSet::from_iter([b])),
            },
            HashNode {
                pins: BTreeSet::from_iter([a]),
                op: Op::Remove(BTreeSet::from_iter([b])),
            },
            HashNode {
                pins: BTreeSet::from_iter([tid(0xCC)]),
                op: Op::Remove(BTreeSet::from_iter([b])),
            },
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
                pins: pins.clone(),
                op: Op::Mark {
                    start: Anchor::Before(tid(0x01)),
                    end: Anchor::After(tid(0x02)),
                    kind_v: tid(0x30),
                    value: tid(0x31),
                    overwrites: BTreeSet::from_iter([tid(0x03)]),
                },
            },
            HashNode {
                pins,
                op: Op::Place {
                    placed_at: tid(0x44),
                    overwrites: BTreeSet::from_iter([tid(0x03), tid(0x0C)]),
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
