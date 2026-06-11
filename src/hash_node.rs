use std::collections::BTreeSet;
use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

use crate::Id;

/// Inserts anchor at another node's id — or at the document's origin id
/// (`HashSeq::origin`), which is how a document's first characters are
/// expressed; there is no separate root op.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Op {
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
}

/// Domain-separation context for node ids. Bump the version when the
/// canonical node encoding changes.
const ID_CONTEXT: &str = "hashseq v1 node id";

/// Pre-keyed hasher template: `new_derive_key` pays for hashing the context
/// once; per-node hashing just clones the initialized state.
static ID_HASHER: LazyLock<blake3::Hasher> =
    LazyLock::new(|| blake3::Hasher::new_derive_key(ID_CONTEXT));

fn update_varint(hasher: &mut blake3::Hasher, mut value: usize) {
    // LEB128, identical to `encoding::encode_varint`.
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

fn update_id_set(hasher: &mut blake3::Hasher, ids: &BTreeSet<Id>) {
    update_varint(hasher, ids.len());
    for id in ids {
        hasher.update(&id.0);
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

    /// The node's self-certifying id:
    /// `BLAKE3::derive_key(ID_CONTEXT, encode_hash_node(self))`.
    ///
    /// The preimage is exactly the node's canonical wire encoding — injective
    /// because the encoding is decodable, domain-separated and versioned by
    /// the derive-key context. The bytes are streamed here without building a
    /// buffer; `id_preimage_is_the_canonical_wire_encoding` locks this
    /// streaming copy to `encoding::encode_hash_node`.
    pub fn id(&self) -> Id {
        let mut hasher = ID_HASHER.clone();
        match &self.op {
            Op::InsertAfter(anchor, ch) => {
                hasher.update(&[crate::encoding::TAG_INSERT_AFTER]);
                update_id_set(&mut hasher, &self.extra_dependencies);
                hasher.update(&anchor.0);
                let mut tmp = [0u8; 4];
                hasher.update(ch.encode_utf8(&mut tmp).as_bytes());
            }
            Op::InsertBefore(anchor, ch) => {
                hasher.update(&[crate::encoding::TAG_INSERT_BEFORE]);
                update_id_set(&mut hasher, &self.extra_dependencies);
                hasher.update(&anchor.0);
                let mut tmp = [0u8; 4];
                hasher.update(ch.encode_utf8(&mut tmp).as_bytes());
            }
            Op::Remove(targets) => {
                hasher.update(&[crate::encoding::TAG_REMOVE]);
                update_id_set(&mut hasher, &self.extra_dependencies);
                update_varint(&mut hasher, targets.len());
                for id in targets {
                    hasher.update(&id.0);
                }
            }
        }
        Id(*hasher.finalize().as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The id preimage must be byte-identical to the canonical wire encoding;
    /// if `encode_hash_node` and the streaming copy in `id()` ever drift,
    /// this fails.
    #[test]
    fn id_preimage_is_the_canonical_wire_encoding() {
        let id_a = Id([0xAA; 32]);
        let id_b = Id([0xBB; 32]);
        let deps = BTreeSet::from_iter([id_a, Id([0x07; 32])]);
        let nodes = [
            HashNode {
                extra_dependencies: BTreeSet::new(),
                op: Op::InsertAfter(id_a, 'x'),
            },
            HashNode {
                extra_dependencies: deps.clone(),
                op: Op::InsertAfter(id_b, '🦀'),
            },
            HashNode {
                extra_dependencies: deps.clone(),
                op: Op::InsertBefore(id_a, '\u{0}'),
            },
            HashNode {
                extra_dependencies: BTreeSet::new(),
                op: Op::Remove(BTreeSet::from_iter([id_a, id_b])),
            },
            HashNode {
                extra_dependencies: deps,
                op: Op::Remove(BTreeSet::from_iter([id_b])),
            },
        ];
        for node in nodes {
            let mut wire = Vec::new();
            crate::encoding::encode_hash_node(&node, &mut wire);
            let mut hasher = blake3::Hasher::new_derive_key(ID_CONTEXT);
            hasher.update(&wire);
            assert_eq!(
                node.id().0,
                *hasher.finalize().as_bytes(),
                "id() drifted from the canonical encoding for {node:?}"
            );
        }
    }
}
