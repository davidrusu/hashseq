//! The interned node: a `HashNode` with every ref resolved to a `NodeIdx`.
//!
//! `HashNode` is the wire and hashing form: it names its refs by `Id`.
//! Everything past the apply seam works in handle space, so a node is
//! resolved once — when every ref is known to be present — and the interned
//! form is what `interpret` consumes, what the stored forms are built from,
//! and what `node_at` maps back through the id table.
//!
//! Only refs are interned. Payloads, keys, mark kinds and values stay `Id`:
//! they are commitments, not refs, and need not be in the object's id table.
//!
//! Handles are replica-local (see `NodeIdx`), so nothing here may be hashed,
//! compared across replicas, or put on the wire; set-valued roles are
//! `SortedIdVec`s, which keep `Id` order.

use crate::{Anchor, HashNode, Id, InternIndex, Op, Payload};
use crate::{InternedId, SortedIdVec};

/// The glued point in handle space: `Anchor` with its id resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InternedAnchor {
    Before(InternedId),
    After(InternedId),
}

impl InternedAnchor {
    #[inline]
    pub fn idx(&self) -> InternedId {
        match self {
            InternedAnchor::Before(i) | InternedAnchor::After(i) => *i,
        }
    }

    #[inline]
    pub fn is_after(&self) -> bool {
        matches!(self, InternedAnchor::After(_))
    }

    #[inline]
    pub fn is_before(&self) -> bool {
        matches!(self, InternedAnchor::Before(_))
    }

    /// The point as `(node, after-side)` — the shape the glue-point
    /// comparisons take.
    #[inline]
    pub fn point(&self) -> (InternedId, bool) {
        (self.idx(), self.is_after())
    }

    /// Map the handle back through the id table (`interns.id(h)` is `h`'s id).
    #[inline]
    pub fn to_anchor(&self, interns: &InternIndex) -> Anchor {
        match self {
            InternedAnchor::Before(i) => Anchor::Before(*interns.id(*i)),
            InternedAnchor::After(i) => Anchor::After(*interns.id(*i)),
        }
    }

    #[inline]
    fn resolve(a: &Anchor, index: &InternIndex) -> Result<Self, Id> {
        Ok(match a {
            Anchor::Before(id) => InternedAnchor::Before(index.get(id).ok_or(*id)?),
            Anchor::After(id) => InternedAnchor::After(index.get(id).ok_or(*id)?),
        })
    }
}

/// `Op` in handle space: named refs (anchors, targets, overwrites) are
/// handles; value commitments stay ids.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InternedOp {
    Insert {
        at: InternedAnchor,
        payload: Payload,
    },
    /// Targets in `Id` order.
    Remove(SortedIdVec),
    Move {
        target: InternedId,
        to: InternedAnchor,
        overwrites: SortedIdVec,
    },
    Put {
        key: Id,
        value: Id,
        overwrites: SortedIdVec,
    },
    Mark {
        start: InternedAnchor,
        end: InternedAnchor,
        kind_v: Id,
        value: Id,
        overwrites: SortedIdVec,
    },
    Place {
        placed_at: Id,
        overwrites: SortedIdVec,
    },
}

/// `HashNode` in handle space: `refs(u) = pins ∪ named(u)`, every member a
/// handle into the owning object's id table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternedHashNode {
    pub pins: SortedIdVec,
    pub op: InternedOp,
}

impl InternedHashNode {
    /// Resolve every ref of `node` through `to_handle`, once. `Err` is the
    /// first ref in `HashNode::iter_refs` order that is not present — the
    /// key the node orphans on, so the walk order here must stay that order.
    pub fn resolve(node: &HashNode, interns: &InternIndex) -> Result<Self, Id> {
        let pins = SortedIdVec::try_from_id_set(&node.pins, interns)?;
        let op = match &node.op {
            Op::Insert { at, payload } => InternedOp::Insert {
                at: InternedAnchor::resolve(at, interns)?,
                payload: *payload,
            },
            Op::Remove(targets) => {
                InternedOp::Remove(SortedIdVec::try_from_id_set(targets, interns)?)
            }
            Op::Move {
                target,
                to,
                overwrites,
            } => InternedOp::Move {
                target: interns.get(target).ok_or(*target)?,
                to: InternedAnchor::resolve(to, interns)?,
                overwrites: SortedIdVec::try_from_id_set(overwrites, interns)?,
            },
            Op::Put {
                key,
                value,
                overwrites,
            } => InternedOp::Put {
                key: *key,
                value: *value,
                overwrites: SortedIdVec::try_from_id_set(overwrites, interns)?,
            },
            Op::Mark {
                start,
                end,
                kind_v,
                value,
                overwrites,
            } => InternedOp::Mark {
                start: InternedAnchor::resolve(start, interns)?,
                end: InternedAnchor::resolve(end, interns)?,
                kind_v: *kind_v,
                value: *value,
                overwrites: SortedIdVec::try_from_id_set(overwrites, interns)?,
            },
            Op::Place {
                placed_at,
                overwrites,
            } => InternedOp::Place {
                placed_at: *placed_at,
                overwrites: SortedIdVec::try_from_id_set(overwrites, interns)?,
            },
        };
        Ok(InternedHashNode { pins, op })
    }

    /// Every handle this node references, in `HashNode::iter_refs` order.
    pub fn refs(&self) -> impl Iterator<Item = InternedId> + '_ {
        let (primary, secondary, set) = match &self.op {
            InternedOp::Insert { at, .. } => (Some(at.idx()), None, None),
            InternedOp::Remove(targets) => (None, None, Some(targets)),
            InternedOp::Move {
                target,
                to,
                overwrites,
            } => (Some(*target), Some(to.idx()), Some(overwrites)),
            InternedOp::Mark {
                start,
                end,
                overwrites,
                ..
            } => (Some(start.idx()), Some(end.idx()), Some(overwrites)),
            InternedOp::Put { overwrites, .. } | InternedOp::Place { overwrites, .. } => {
                (None, None, Some(overwrites))
            }
        };
        self.pins
            .iter()
            .chain(primary)
            .chain(secondary)
            .chain(set.into_iter().flatten())
    }

    /// The wire form: every handle mapped back through the id table
    /// (`interns.id(h)` is `h`'s id). No rehashing — the node's own id is
    /// `interns.id(its handle)`.
    pub fn to_node(&self, interns: &InternIndex) -> HashNode {
        let op = match &self.op {
            InternedOp::Insert { at, payload } => Op::Insert {
                at: at.to_anchor(interns),
                payload: *payload,
            },
            InternedOp::Remove(targets) => Op::Remove(targets.to_id_set(interns)),
            InternedOp::Move {
                target,
                to,
                overwrites,
            } => Op::Move {
                target: *interns.id(*target),
                to: to.to_anchor(interns),
                overwrites: overwrites.to_id_set(interns),
            },
            InternedOp::Put {
                key,
                value,
                overwrites,
            } => Op::Put {
                key: *key,
                value: *value,
                overwrites: overwrites.to_id_set(interns),
            },
            InternedOp::Mark {
                start,
                end,
                kind_v,
                value,
                overwrites,
            } => Op::Mark {
                start: start.to_anchor(interns),
                end: end.to_anchor(interns),
                kind_v: *kind_v,
                value: *value,
                overwrites: overwrites.to_id_set(interns),
            },
            InternedOp::Place {
                placed_at,
                overwrites,
            } => Op::Place {
                placed_at: *placed_at,
                overwrites: overwrites.to_id_set(interns),
            },
        };
        HashNode {
            pins: self.pins.to_id_set(interns),
            op,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn tid(n: u8) -> Id {
        Id([n; 32])
    }

    fn sample_nodes() -> Vec<HashNode> {
        let pins = BTreeSet::from_iter([tid(7), tid(9)]);
        vec![
            HashNode {
                pins: BTreeSet::new(),
                op: Op::insert_after(tid(1), 'x'),
            },
            HashNode {
                pins: pins.clone(),
                op: Op::Insert {
                    at: Anchor::Before(tid(2)),
                    payload: Payload::Id(tid(0x33)),
                },
            },
            HashNode {
                pins: pins.clone(),
                op: Op::Remove(BTreeSet::from_iter([tid(1), tid(2)])),
            },
            HashNode {
                pins: pins.clone(),
                op: Op::Move {
                    target: tid(1),
                    to: Anchor::Before(tid(2)),
                    overwrites: BTreeSet::from_iter([tid(3)]),
                },
            },
            HashNode {
                pins: pins.clone(),
                op: Op::Put {
                    key: tid(0x11),
                    value: tid(0x22),
                    overwrites: BTreeSet::from_iter([tid(3), tid(4)]),
                },
            },
            HashNode {
                pins: pins.clone(),
                op: Op::Mark {
                    start: Anchor::Before(tid(1)),
                    end: Anchor::After(tid(2)),
                    kind_v: tid(0x30),
                    value: tid(0x31),
                    overwrites: BTreeSet::from_iter([tid(3)]),
                },
            },
            HashNode {
                pins,
                op: Op::Place {
                    placed_at: tid(0x44),
                    overwrites: BTreeSet::from_iter([tid(3), tid(4)]),
                },
            },
        ]
    }

    /// An id table holding `tid(0)..tid(15)` at handle `n`: sorted by id, so
    /// handle order and id order coincide.
    fn table() -> InternIndex {
        table_without(&BTreeSet::new())
    }

    /// `table()` minus the `unknown` ids.
    fn table_without(unknown: &BTreeSet<Id>) -> InternIndex {
        let mut interns = InternIndex::default();
        for id in (0..16).map(tid).filter(|id| !unknown.contains(id)) {
            interns.intern(id);
        }
        interns
    }

    #[test]
    fn resolve_then_to_node_is_identity() {
        let interns = table();
        for node in sample_nodes() {
            let interned = InternedHashNode::resolve(&node, &interns).expect("all refs known");
            assert_eq!(interned.to_node(&interns), node);
        }
    }

    #[test]
    fn refs_follow_iter_refs_order() {
        let interns = table();
        for node in sample_nodes() {
            let interned = InternedHashNode::resolve(&node, &interns).expect("all refs known");
            let via_handles: Vec<Id> = interned.refs().map(|h| *interns.id(h)).collect();
            let via_ids: Vec<Id> = node.iter_refs().copied().collect();
            assert_eq!(via_handles, via_ids);
        }
    }

    /// The orphan key: the first ref in `iter_refs` order that is missing.
    #[test]
    fn resolve_reports_first_missing_ref_in_iter_refs_order() {
        for node in sample_nodes() {
            let refs: Vec<Id> = node.iter_refs().copied().collect();
            for missing_from in 0..refs.len() {
                // Everything from `missing_from` on is unknown.
                let unknown: BTreeSet<Id> = refs[missing_from..].iter().copied().collect();
                let got = InternedHashNode::resolve(&node, &table_without(&unknown));
                let want = refs.iter().find(|r| unknown.contains(r)).copied();
                assert_eq!(got.err(), want);
            }
        }
    }
}
