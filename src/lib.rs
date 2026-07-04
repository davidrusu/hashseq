pub mod bitset;
pub mod delivery;
pub mod encoding;
pub mod hash_node;
pub mod hashkv;
pub mod hashseq;
pub mod hashweb;
pub mod placement;
pub mod hashseq_iter;
pub mod run;
mod run_index;
pub mod value;
pub mod wasm;

pub use self::encoding::{
    DecodeError, EncodableOp, decode_hashkv, decode_hashkv_strict, decode_hashseq,
    decode_hashseq_strict, decode_hashweb, decode_hashweb_strict, encode_hashkv, encode_hashseq,
    encode_hashweb,
};
pub use self::hash_node::{Anchor, HashNode, Op, Payload};
pub use self::hashkv::{HashKv, Read};
pub use self::hashseq::{Cursor, HashSeq, Loc, MarkSet, NodeIdx, StoredRun};
pub use self::hashweb::HashWeb;
pub use self::run::{FirstOp, Run};
pub use self::value::{Value, object_id};

#[derive(
    Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Id(pub [u8; 32]);

impl std::fmt::Debug for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &hex::encode(self.0)[..3])
    }
}
