//! The delivery state every projection shares (FRAMEWORK.md): content-
//! addressed ops buffer on their refs and quarantine on their edge-table
//! verdict. Each projection owns its own (small, deliberately duplicated)
//! apply loop — `apply_with_id` + `park_or_dispatch` + `interpret` — and
//! calls into this struct for the bookkeeping, which is where the
//! correctness discipline lives: re-delivery dedup, park/wake pairing, and
//! the wake-only-if-applied quarantine cascade.

use std::collections::HashMap;

use crate::hashseq::{IdMap, IdSet};
use crate::{HashNode, Id};

/// Delivery state: parked orphans plus the quarantine. One per projection.
#[derive(Debug, Clone, Default)]
pub struct Delivery {
    /// Ops waiting on a dependency, keyed by *one* missing dep id (the first
    /// found): applying that id wakes exactly these waiters — no global
    /// retries. A waiter still missing more deps is re-parked on the next
    /// one. Values carry the precomputed node id so wakes don't rehash.
    ///
    /// Keys are adversary-chosen bytes (an op can name any id as a dep), so
    /// this stays a std `HashMap` for SipHash's HashDoS protection — unlike
    /// `orphan_ids`, whose keys we computed ourselves with BLAKE3.
    pub(crate) orphaned: HashMap<Id, Vec<(Id, HashNode)>>,
    /// Ids of all parked orphans: dedups network re-delivery while parked.
    orphan_ids: IdSet,
    /// Permanently quarantined nodes (the apply-time gate, HASHWEB_SPEC.md
    /// "The edge table"): ops this projection does not admit. Gated nodes
    /// never apply and never enter tips, so anything referencing them stays
    /// parked — the quarantine cascades. Kept so merge/encode re-present
    /// them (rules may loosen; the op set is never thinned).
    pub(crate) gated: IdMap<HashNode>,
}

impl Delivery {
    /// Is `id` parked or quarantined? (The re-delivery dedup.)
    pub(crate) fn holds(&self, id: &Id) -> bool {
        (!self.orphan_ids.is_empty() && self.orphan_ids.contains(id))
            || (!self.gated.is_empty() && self.gated.contains_key(id))
    }

    pub(crate) fn park(&mut self, missing: Id, id: Id, node: HashNode) {
        self.orphan_ids.insert(id);
        self.orphaned.entry(missing).or_default().push((id, node));
    }

    pub(crate) fn unpark(&mut self, id: &Id) {
        if !self.orphan_ids.is_empty() {
            self.orphan_ids.remove(id);
        }
    }

    /// Quarantine `node` — the edge-table verdict. The caller must NOT wake
    /// `id`'s waiters afterwards: dependents of a gated op stay parked.
    pub(crate) fn gate(&mut self, id: Id, node: HashNode) {
        self.gated.insert(id, node);
    }

    /// Push the waiters parked on `id` onto the worklist.
    pub(crate) fn wake(&mut self, id: &Id, queue: &mut Vec<(Id, HashNode)>) {
        if !self.orphaned.is_empty()
            && let Some(waiting) = self.orphaned.remove(id)
        {
            queue.extend(waiting);
        }
    }

    /// Parked nodes (gated ones are in `gated`).
    pub fn orphans(&self) -> impl Iterator<Item = &HashNode> {
        self.orphaned.values().flatten().map(|(_, node)| node)
    }

    /// Everything held here, borrowed — parked then gated (snapshot carriage).
    pub(crate) fn held(&self) -> impl Iterator<Item = (&Id, &HashNode)> {
        self.orphaned
            .values()
            .flatten()
            .map(|(id, node)| (id, node))
            .chain(self.gated.iter())
    }

    /// Everything held here, owned — for merge re-presentation.
    pub(crate) fn into_held(self) -> impl Iterator<Item = (Id, HashNode)> {
        self.orphaned.into_values().flatten().chain(self.gated)
    }
}
