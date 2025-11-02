pub mod hash_node;
pub mod hashseq;
pub mod run;
pub mod topo_sort;

// pub mod bloom_tree;
// pub mod bloom_tree_balanced;
// pub mod bloom_tree_do;
// pub mod pbt;

pub use self::hash_node::{HashNode, Op};
pub use self::hashseq::{HashSeq, RunPosition};
pub use self::run::Run;

#[derive(
    Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Id(pub [u8; 32]);

impl std::fmt::Debug for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &hex::encode(self.0)[..3])
    }
}
