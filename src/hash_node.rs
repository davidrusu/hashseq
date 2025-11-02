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
    pub fn dependencies(&self) -> BTreeSet<Id> {
        match &self {
            Op::InsertRoot(_) => BTreeSet::new(),
            Op::InsertAfter(dep, _) | Op::InsertBefore(dep, _) => BTreeSet::from_iter([*dep]),
            Op::Remove(deps) => deps.clone(),
        }
    }

    #[cfg(feature = "sha3-hash")]
    pub fn hash_update(&self, sha: &mut tiny_keccak::Sha3) {
        use tiny_keccak::Hasher;
        match self {
            Op::InsertRoot(c) => {
                sha.update(b"root");
                sha.update(&(*c as u32).to_le_bytes());
            }
            Op::InsertAfter(n, c) => {
                sha.update(b"after");
                sha.update(n);
                sha.update(b"$");
                sha.update(&(*c as u32).to_le_bytes());
            }
            Op::InsertBefore(n, c) => {
                sha.update(b"before");
                sha.update(n);
                sha.update(b"$");
                sha.update(&(*c as u32).to_le_bytes());
            }
            Op::Remove(n) => {
                sha.update(b"remove");
                for node_id in n {
                    sha.update(node_id);
                }
            }
        }
    }

    #[cfg(feature = "blake3-hash")]
    pub fn hash_update(&self, hasher: &mut blake3::Hasher) {
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
    pub fn dependencies(&self) -> impl Iterator<Item = Id> + '_ {
        self.extra_dependencies
            .iter()
            .copied()
            .chain(self.op.dependencies())
    }

    pub fn id(&self) -> Id {
        #[cfg(feature = "sha3-hash")]
        {
            use tiny_keccak::Hasher;
            let mut sha3 = tiny_keccak::Sha3::v256();
            let mut hash = [0u8; 32];

            sha3.update(b"extra_deps");
            for dep in self.extra_dependencies.iter() {
                sha3.update(b"$");
                sha3.update(&dep.0);
            }

            sha3.update(b"op");
            self.op.hash_update(&mut sha3);
            sha3.update(b"done");

            sha3.finalize(&mut hash);
            Id(hash);
        }

        #[cfg(feature = "blake3-hash")]
        {
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

        #[cfg(feature = "fast-hash")]
        {
            use std::hash::Hash;
            use std::hash::Hasher;
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            self.hash(&mut hasher);
            let hash_u64 = hasher.finish();

            // Convert u64 to [u8; 32] by padding with zeros
            let mut id = [0u8; 32];
            id[..8].copy_from_slice(&hash_u64.to_le_bytes());
            Id(id)
        }
    }

    #[cfg(feature = "sha3-hash")]
    pub fn hash_update(&self, sha: &mut tiny_keccak::Sha3) {
        use tiny_keccak::Hasher;

        sha.update(b"extra_deps");
        for dep in self.extra_dependencies.iter() {
            sha.update(b"$");
            sha.update(dep);
        }

        sha.update(b"op");
        self.op.hash_update(sha);
        sha.update(b"done");
    }

    #[cfg(feature = "blake3-hash")]
    pub fn hash_update(&self, hasher: &mut blake3::Hasher) {
        hasher.update(b"extra_deps");
        for dep in self.extra_dependencies.iter() {
            hasher.update(b"$");
            hasher.update(&dep.0);
        }

        hasher.update(b"op");
        self.op.hash_update(hasher);
        hasher.update(b"done");
    }
}
