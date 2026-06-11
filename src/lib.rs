pub mod encoding;
pub mod hash_node;
pub mod hashseq;
pub mod hashseq_iter;
pub mod run;
mod run_index;
pub mod wasm;

pub use self::encoding::{
    DecodeError, EncodableOp, decode_batch, decode_hashseq, encode_batch, encode_hashseq,
};
pub use self::hash_node::{HashNode, Op};
pub use self::hashseq::{Cursor, HashSeq, Loc, NodeIdx, StoredRun};
pub use self::hashseq_iter::HashSeqIter;
pub use self::run::{FirstOp, Run};

#[derive(
    Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Id(pub [u8; 32]);

impl std::fmt::Debug for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &hex::encode(self.0)[..3])
    }
}
