//! The containment register (PLACEMENT_SPEC.md).
//!
//! One register per object — "which link atom, anywhere in the store,
//! places this object" — claimed by `Place` ops in the object's own DAG.
//! The fourth instance of the supersession pattern: `heads` maintenance is
//! O(1) per applied op, conflicts freeze (never flip to an id winner), and
//! the last-agreed walk reads the retained spine.
//!
//! The register is deliberately Id-based and self-contained so `HashSeq`
//! and `HashKv` embed the same state machine; membership resolution
//! (matching `placed_at` against live link atoms, the legacy presence
//! rule, D4 cycle detachment) is the read layer above — it needs container
//! walks the register cannot see.

use std::collections::{BTreeMap, BTreeSet};

use crate::Id;

/// A register entry: one applied `Place` op's semantic content. The full
/// spine is retained — superseded entries are what the last-agreed walk
/// descends through (PLACEMENT_SPEC.md "Retention").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaceEntry {
    pub placed_at: Id,
    pub overwrites: BTreeSet<Id>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PlacementRegister {
    /// Applied `Place` ops by node id. BTreeMap: deterministic iteration
    /// (registers are gesture-sized, not keystroke-sized).
    entries: BTreeMap<Id, PlaceEntry>,
    /// Live heads, id-sorted (convergence-safe order, never replica-local).
    heads: Vec<Id>,
}

impl PlacementRegister {
    /// No `Place` op ever applied — the read layer's legacy-presence rule
    /// triggers on exactly this.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn heads(&self) -> &[Id] {
        &self.heads
    }

    /// `|heads| > 1` — surfaced, frozen, resolved by the next op naming
    /// both heads. Never decided by id.
    pub fn conflicted(&self) -> bool {
        self.heads.len() > 1
    }

    pub fn entry(&self, id: &Id) -> Option<&PlaceEntry> {
        self.entries.get(id)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// O(1) head-set update: `heads = heads − overwrites ∪ {id}`.
    /// `overwrites` are refs, so every overwritten op applied before its
    /// superseder — a newly applied op is never already superseded.
    /// Entries not naming Place ops in this object simply are not in the
    /// head list — ignored by construction (the definitional filter).
    pub fn apply(&mut self, id: Id, placed_at: Id, overwrites: BTreeSet<Id>) {
        if self.entries.contains_key(&id) {
            return;
        }
        self.heads.retain(|h| !overwrites.contains(h));
        if let Err(pos) = self.heads.binary_search(&id) {
            self.heads.insert(pos, id);
        }
        self.entries.insert(
            id,
            PlaceEntry {
                placed_at,
                overwrites,
            },
        );
    }

    /// Everything `id` transitively overwrites (strict — excludes `id`),
    /// filtered to applied entries. The overwrites DAG is acyclic by
    /// construction (overwrites are refs: an op can only name ids that
    /// already exist).
    fn closure(&self, id: &Id) -> BTreeSet<Id> {
        let mut out = BTreeSet::new();
        let mut stack: Vec<Id> = vec![*id];
        while let Some(next) = stack.pop() {
            if let Some(e) = self.entries.get(&next) {
                for o in &e.overwrites {
                    if self.entries.contains_key(o) && out.insert(*o) {
                        stack.push(*o);
                    }
                }
            }
        }
        out
    }

    /// Members of `set` not transitively overwritten by another member.
    fn maximal(&self, set: &BTreeSet<Id>) -> Vec<Id> {
        set.iter()
            .filter(|m| {
                !set.iter()
                    .any(|n| n != *m && self.closure(n).contains(*m))
            })
            .copied()
            .collect()
    }

    /// The fallback chain (PLACEMENT_SPEC.md "Freeze"): `placed_at` values
    /// in descending order of agreement. First entry = the p₀ candidate
    /// (the single head's value; on a multi-head conflict the walk starts
    /// below the heads, at the last agreed placement). The read layer
    /// tries entries in order, skipping values whose atom is dead or
    /// unresolvable, and treats an exhausted chain as unplaced.
    pub fn chain(&self) -> Vec<Id> {
        let mut out = Vec::new();
        let mut frontier: Vec<Id> = self.heads.clone();
        // The frontier strictly descends the (acyclic) overwrites DAG;
        // the guard is defensive only.
        let mut guard = self.entries.len() + 2;
        while !frontier.is_empty() && guard > 0 {
            guard -= 1;
            if frontier.len() == 1 {
                let h = frontier[0];
                let Some(e) = self.entries.get(&h) else { break };
                out.push(e.placed_at);
                let below: BTreeSet<Id> = e
                    .overwrites
                    .iter()
                    .filter(|o| self.entries.contains_key(o))
                    .copied()
                    .collect();
                frontier = self.maximal(&below);
            } else {
                // Last agreed: the maximal ops every frontier member
                // transitively overwrites.
                let mut iter = frontier.iter();
                let mut common = self.closure(iter.next().expect("non-empty"));
                for h in iter {
                    let c = self.closure(h);
                    common.retain(|x| c.contains(x));
                }
                frontier = self.maximal(&common);
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tid(n: u8) -> Id {
        Id([n; 32])
    }

    #[test]
    fn single_head_chain_descends_history() {
        let mut r = PlacementRegister::default();
        r.apply(tid(1), tid(0xA1), BTreeSet::new());
        r.apply(tid(2), tid(0xA2), BTreeSet::from_iter([tid(1)]));
        r.apply(tid(3), tid(0xA3), BTreeSet::from_iter([tid(2)]));
        assert_eq!(r.heads(), &[tid(3)]);
        assert!(!r.conflicted());
        assert_eq!(r.chain(), vec![tid(0xA3), tid(0xA2), tid(0xA1)]);
    }

    #[test]
    fn concurrent_places_freeze_at_last_agreed() {
        let mut r = PlacementRegister::default();
        r.apply(tid(1), tid(0xA1), BTreeSet::new());
        // Two ops each superseding only tid(1) — concurrent.
        r.apply(tid(2), tid(0xA2), BTreeSet::from_iter([tid(1)]));
        r.apply(tid(3), tid(0xA3), BTreeSet::from_iter([tid(1)]));
        assert!(r.conflicted());
        // Frozen: the chain starts at the last agreed placement (0xA1) —
        // neither contender's destination appears.
        assert_eq!(r.chain(), vec![tid(0xA1)]);
        // A resolving op naming both heads dominates.
        r.apply(tid(4), tid(0xA4), BTreeSet::from_iter([tid(2), tid(3)]));
        assert!(!r.conflicted());
        assert_eq!(r.chain()[0], tid(0xA4));
    }

    #[test]
    fn conflict_with_no_common_history_is_unplaced() {
        let mut r = PlacementRegister::default();
        // Two first-places racing (both overwrites = ∅): no agreement
        // exists — chain is empty, the read layer renders detached.
        r.apply(tid(1), tid(0xA1), BTreeSet::new());
        r.apply(tid(2), tid(0xA2), BTreeSet::new());
        assert!(r.conflicted());
        assert_eq!(r.chain(), Vec::<Id>::new());
    }

    #[test]
    fn apply_is_idempotent_and_order_independent() {
        let ops: Vec<(Id, Id, BTreeSet<Id>)> = vec![
            (tid(1), tid(0xA1), BTreeSet::new()),
            (tid(2), tid(0xA2), BTreeSet::from_iter([tid(1)])),
            (tid(3), tid(0xA3), BTreeSet::from_iter([tid(1)])),
            (tid(4), tid(0xA4), BTreeSet::from_iter([tid(2), tid(3)])),
        ];
        // NOTE: delivery guarantees overwritten-before-superseder; test
        // every order consistent with that partial order.
        let orders: Vec<Vec<usize>> = vec![
            vec![0, 1, 2, 3],
            vec![0, 2, 1, 3],
        ];
        let mut results = Vec::new();
        for ord in orders {
            let mut r = PlacementRegister::default();
            for i in ord {
                let (id, at, ows) = &ops[i];
                r.apply(*id, *at, ows.clone());
            }
            // double-apply is a no-op
            let (id, at, ows) = &ops[3];
            r.apply(*id, *at, ows.clone());
            results.push((r.heads().to_vec(), r.chain()));
        }
        assert!(results.windows(2).all(|w| w[0] == w[1]));
    }
}
