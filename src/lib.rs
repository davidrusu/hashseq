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

use std::collections::BTreeSet;

use rustc_hash::FxHashMap;

pub use self::encoding::{
    DecodeError, EncodableOp, decode_hashkv, decode_hashkv_strict, decode_hashseq,
    decode_hashseq_strict, decode_hashweb, decode_hashweb_strict, encode_hashkv, encode_hashseq,
    encode_hashweb,
};
pub use self::hash_node::{Anchor, HashNode, Op, Payload};
pub use self::hashkv::{HashKv, Read};
pub use self::hashseq::{Cursor, HashSeq, Loc, MarkSet, StoredRun};
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

/// Compact handle for accepted ids. Handles are allocated densely in local
/// apply order, so `Vec`s indexed by `InternedId` replace `Id`-keyed maps for
/// everything but the single interning map.
///
/// Handles are replica-local: two replicas applying the same ops in different
/// orders assign different handles. They must never participate in anything
/// convergence-relevant — sibling ordering, hashing, and the wire format all
/// operate on `Id`s.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InternedId(pub u32);

/// The `Id -> InternedId` intern map, keyed by the id's u64 prefix instead of the
/// full 32 bytes. A prefix hit is verified against `ids[idx]`, which makes
/// lookups exact, a true prefix collision just fails verification and falls
/// through to the `spill` map of full-key entries (expected to stay empty:
/// ~N^2 / 2^64 chance per pair, and harmless when it does fire).
#[derive(Debug, Default, Clone)]
struct IdIndex {
    prefix: FxHashMap<u64, InternedId>,
    spill: FxHashMap<Id, InternedId>,
}

fn id_prefix(id: &Id) -> u64 {
    u64::from_le_bytes(id.0[..8].try_into().expect("Id has 32 bytes"))
}

impl IdIndex {
    /// `ids` is the `InternedId -> Id` table used to verify prefix hits.
    fn get(&self, id: &Id, ids: &[Id]) -> Option<InternedId> {
        let idx = *self.prefix.get(&id_prefix(id))?;
        if ids[idx.0 as usize] == *id {
            Some(idx)
        } else {
            self.spill.get(id).copied()
        }
    }

    fn insert(&mut self, id: Id, idx: InternedId) {
        match self.prefix.entry(id_prefix(&id)) {
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(idx);
            }
            std::collections::hash_map::Entry::Occupied(e) => {
                if *e.get() != idx {
                    self.spill.insert(id, idx);
                }
            }
        }
    }
}

/// A set of node handles kept sorted by their `Id`, stored as `Vec<InternedId>`
/// (4 bytes/entry) instead of `BTreeSet<Id>` (32 bytes/entry plus a ~400 B
/// B-tree node each).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SortedIdVec(Vec<InternedId>);

impl SortedIdVec {
    /// Index of the handle whose id equals `id` (`Ok`) or where it would be
    /// inserted to stay sorted (`Err`).
    #[inline]
    fn search(&self, id: &Id, ids: &[Id]) -> Result<usize, usize> {
        self.0.binary_search_by(|h| ids[h.0 as usize].cmp(id))
    }

    /// Build from an already-`Id`-sorted set, mapping each id to its handle
    /// (`BTreeSet` iterates in `Id` order, so the handles land sorted).
    /// `Err` is the first member (in `Id` order) with no handle.
    pub(crate) fn try_from_id_set(
        set: &BTreeSet<Id>,
        mut to_handle: impl FnMut(&Id) -> Option<InternedId>,
    ) -> Result<Self, Id> {
        set.iter()
            .map(|id| to_handle(id).ok_or(*id))
            .collect::<Result<Vec<InternedId>, Id>>()
            .map(SortedIdVec)
    }

    /// The one-member set.
    pub(crate) fn single(handle: InternedId) -> Self {
        SortedIdVec(vec![handle])
    }

    /// Rebuild the `Id` set (for the wire format / hashing).
    pub fn to_id_set(&self, ids: &[Id]) -> BTreeSet<Id> {
        self.0.iter().map(|h| ids[h.0 as usize]).collect()
    }

    /// Iterate the member ids in `Id` order.
    pub fn iter_ids<'a>(&'a self, ids: &'a [Id]) -> impl Iterator<Item = Id> + 'a {
        self.0.iter().map(move |h| ids[h.0 as usize])
    }

    /// Insert `handle` keyed by its id; a no-op if an equal id is already
    /// present (set semantics, like the `BTreeSet<Id>` it replaces).
    pub fn insert(&mut self, handle: InternedId, ids: &[Id]) {
        let id = ids[handle.0 as usize];
        if let Err(pos) = self.search(&id, ids) {
            self.0.insert(pos, handle);
        }
    }

    /// Is a handle whose id equals `id` a member?
    pub fn contains(&self, id: &Id, ids: &[Id]) -> bool {
        self.search(id, ids).is_ok()
    }

    /// Remove the handle whose id equals `id`, if present.
    pub fn remove(&mut self, id: &Id, ids: &[Id]) {
        if let Ok(pos) = self.search(id, ids) {
            self.0.remove(pos);
        }
    }

    /// Handle with the smallest id.
    #[inline]
    pub fn first(&self) -> Option<InternedId> {
        self.0.first().copied()
    }

    /// Handle with the largest id.
    #[inline]
    pub fn last(&self) -> Option<InternedId> {
        self.0.last().copied()
    }

    /// Handle with the smallest id `>= id` (the `range(id..).next()` seek).
    pub fn first_ge(&self, id: &Id, ids: &[Id]) -> Option<InternedId> {
        let pos = match self.search(id, ids) {
            Ok(p) | Err(p) => p,
        };
        self.0.get(pos).copied()
    }

    #[inline]
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = InternedId> + '_ {
        self.0.iter().copied()
    }

    /// The handles, in `Id` order.
    #[inline]
    pub fn as_slice(&self) -> &[InternedId] {
        &self.0
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl<'a> IntoIterator for &'a SortedIdVec {
    type Item = InternedId;
    type IntoIter = std::iter::Copied<std::slice::Iter<'a, InternedId>>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter().copied()
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
