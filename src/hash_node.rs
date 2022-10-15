use std::collections::BTreeSet;

use crate::Id;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum Op {
    InsertRoot(char),
    InsertAfter(Id, char),
    InsertBefore(Id, char),
    Remove(Id),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HashNode {
    pub extra_dependencies: BTreeSet<Id>,
    pub op: Op,
}

impl Op {
    pub fn dependency(&self) -> Option<Id> {
        match &self {
            Op::InsertRoot(_) => None,
            Op::InsertAfter(dep, _) | Op::InsertBefore(dep, _) | Op::Remove(dep) => Some(*dep),
        }
    }

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
                sha.update(n);
            }
        }
    }
}

impl HashNode {
    pub fn dependencies(&self) -> impl Iterator<Item = Id> + '_ {
        self.extra_dependencies
            .iter()
            .copied()
            .chain(self.op.dependency())
    }

    pub fn id(&self) -> Id {
        use tiny_keccak::Hasher;
        let mut sha3 = tiny_keccak::Sha3::v256();
        let mut hash = [0u8; 32];

        sha3.update(b"extra_deps");
        for dep in self.extra_dependencies.iter() {
            sha3.update(b"$");
            sha3.update(dep);
        }

        sha3.update(b"op");
        self.op.hash_update(&mut sha3);
        sha3.update(b"done");

        sha3.finalize(&mut hash);

        hash
    }
}
