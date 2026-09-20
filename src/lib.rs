pub mod bitset;
pub mod delivery;
pub mod encoding;
pub mod hash_node;
pub mod hashkv;
pub mod hashseq;
pub mod hashseq_iter;
pub mod hashweb;
pub mod interned_hash_node;
pub mod placement;
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
pub use self::hashseq::{Cursor, HashSeq, InternedId, Loc, MarkSet, StoredRun};
pub use self::hashweb::{HashWeb, HashWebClock};
pub use self::run::Run;
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

/// A peer's clock for one object: the tips of the peer's copy as far as
/// this replica knows them. Every ancestor is implied, so the set names
/// the peer's whole causal closure, and a delta (`HashSeq::delta_for`,
/// `HashKv::delta_for`) is exactly what this replica holds outside it.
///
/// A value, never edited: it is what the peer told us (a 0xC1 frontier
/// frame, a snapshot) or what we last sent it (our own clock, taken
/// after a drain). Tips are portable, so both come from the same
/// `HashSeq::clock` / `HashKv::clock`. Ids this replica lacks are
/// simply not walked.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Clock(pub std::collections::BTreeSet<Id>);

/// Why the apply-time rules refused an op (HASHWEB_SPEC.md "The edge
/// table"). Every verdict is a function of hash-committed facts, so it is
/// the same on every replica and never changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refused {
    /// A move's target is not an element of this object.
    NotAnElement,
    /// An insert, move or mark anchor is not a glue point: an element, the
    /// origin, or a move op's splice point.
    NotAGluePoint,
    /// A move whose destination is its own target.
    SelfMove,
    /// A mark whose start point sorts after its end point.
    InvertedSpan,
    /// An op kind this object's projection does not carry: a seq op in a
    /// map, a map op in a seq.
    WrongObjectKind,
}

/// What delivering a node did (`HashSeq::apply`, `HashKv::apply`,
/// `HashWeb::apply_to`) when the rules did not refuse it; a refusal is
/// the `Err(Refused)` of those calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Interned and interpreted; its waiters were woken.
    Applied,
    /// Seen for the first time but waiting on a missing ref (or, at the
    /// store level, on an object not yet opened).
    Orphaned,
    /// Already applied or already orphaned: nothing changed.
    Known,
}

impl Outcome {
    /// Did this delivery change anything — apply or first-time orphan?
    /// The signal a client uses to re-render and a relay uses to forward.
    pub fn is_news(self) -> bool {
        matches!(self, Outcome::Applied | Outcome::Orphaned)
    }
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Refused::NotAnElement => "target is not an element",
            Refused::NotAGluePoint => "anchor is not a glue point",
            Refused::SelfMove => "an element cannot be moved next to itself",
            Refused::InvertedSpan => "the span is inverted",
            Refused::WrongObjectKind => "op kind does not belong in this object",
        })
    }
}
