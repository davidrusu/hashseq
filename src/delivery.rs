use std::collections::HashMap;

use crate::hashseq::IdMap;
use crate::{HashNode, Id};

#[derive(Debug, Clone, Default)]
pub struct Delivery {
    /// Ops waiting on a dependency, keyed by their canonical missing ref.
    /// Applying that id wakes exactly these waiters. Each bucket is
    /// keyed by node id (computed by us with BLAKE3, so Fx-safe) to ensure idempotency
    ///
    /// Outer keys are adversary-chosen bytes (an op can name any id as a
    /// dep), so the outer map stays a std `HashMap` for SipHash's HashDoS
    /// protection.
    pub(crate) orphaned: HashMap<Id, IdMap<HashNode>>,
}

impl Delivery {
    pub(crate) fn orphan(&mut self, key: Id, id: Id, node: HashNode) -> bool {
        match self.orphaned.entry(key).or_default().entry(id) {
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(node);
                true
            }
            std::collections::hash_map::Entry::Occupied(_) => false,
        }
    }

    pub(crate) fn wake(&mut self, id: &Id, queue: &mut Vec<(Id, HashNode)>) {
        if !self.orphaned.is_empty()
            && let Some(waiting) = self.orphaned.remove(id)
        {
            queue.extend(waiting);
        }
    }

    pub fn orphans(&self) -> impl Iterator<Item = (&Id, &HashNode)> {
        self.orphaned.values().flatten()
    }

    pub(crate) fn into_orphans(self) -> impl Iterator<Item = (Id, HashNode)> {
        self.orphaned.into_values().flatten()
    }
}
