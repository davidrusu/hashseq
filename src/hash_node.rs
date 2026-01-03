use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::Id;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Op {
    InsertRoot(char),
    InsertAfter(Id, char),
    InsertBefore(Id, char),
    Remove(BTreeSet<Id>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HashNode {
    pub extra_dependencies: BTreeSet<Id>,
    pub op: Op,
}

impl Op {
    /// Returns the primary dependency if this op has one (avoids allocation)
    fn primary_dep(&self) -> Option<&Id> {
        match self {
            Op::InsertRoot(_) => None,
            Op::InsertAfter(dep, _) | Op::InsertBefore(dep, _) => Some(dep),
            Op::Remove(_) => None,
        }
    }

    /// Returns iterator over remove dependencies (for Remove ops only)
    fn remove_deps(&self) -> impl Iterator<Item = &Id> {
        match self {
            Op::Remove(deps) => Some(deps.iter()),
            _ => None,
        }
        .into_iter()
        .flatten()
    }

    fn hash_update(&self, hasher: &mut blake3::Hasher) {
        match self {
            Op::InsertRoot(c) => {
                hasher.update(b"root");
                hasher.update(&(*c as u32).to_le_bytes());
            }
            Op::InsertAfter(n, c) => {
                hasher.update(b"after");
                hasher.update(&n.0);
                hasher.update(b"$");
                hasher.update(&(*c as u32).to_le_bytes());
            }
            Op::InsertBefore(n, c) => {
                hasher.update(b"before");
                hasher.update(&n.0);
                hasher.update(b"$");
                hasher.update(&(*c as u32).to_le_bytes());
            }
            Op::Remove(n) => {
                hasher.update(b"remove");
                for node_id in n {
                    hasher.update(&node_id.0);
                }
            }
        }
    }
}

impl HashNode {
    /// Iterate over all dependencies without allocation
    pub fn iter_dependencies(&self) -> impl Iterator<Item = &Id> {
        self.extra_dependencies
            .iter()
            .chain(self.op.primary_dep())
            .chain(self.op.remove_deps())
    }

    pub fn id(&self) -> Id {
        let mut hasher = blake3::Hasher::new();

        hasher.update(b"extra_deps");
        for dep in self.extra_dependencies.iter() {
            hasher.update(b"$");
            hasher.update(&dep.0);
        }

        hasher.update(b"op");
        self.op.hash_update(&mut hasher);
        hasher.update(b"done");

        let hash = hasher.finalize();
        Id(*hash.as_bytes())
    }
}
