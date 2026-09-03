use std::collections::{BTreeMap, BTreeSet};

use rustc_hash::{FxHashMap, FxHashSet};

use crate::bitset::BitSet;
use crate::run_index::{ElemRef, IndexTarget, RunIndex, SweepFrag, SweepPos};
use crate::delivery::Delivery;
use crate::placement::PlacementRegister;
use crate::{Anchor, EncodableOp, FirstOp, HashNode, Id, Op, Payload, Run};

/// HashMap keyed by `Id`. Uses FxHash instead of SipHash: safe because `Id` is
/// already a BLAKE3 hash, so adversaries cannot craft colliding keys without
/// inverting BLAKE3 (HashDoS protection from SipHash is redundant here).
pub type IdMap<V> = FxHashMap<Id, V>;
/// HashSet of `Id`. Same FxHash rationale as `IdMap`.
pub type IdSet = FxHashSet<Id>;

/// The `Id -> NodeIdx` intern map, keyed by the id's u64 prefix instead of the
/// full 32 bytes. `Id` is BLAKE3 output, so the prefix is effectively a
/// perfect hash; a prefix hit is verified against `ids[idx]`, which makes
/// lookups exact — a true prefix collision just fails verification and falls
/// through to the `spill` map of full-key entries (expected to stay empty:
/// ~N²/2⁶⁴ chance per pair, and harmless when it does fire).
#[derive(Debug, Default, Clone)]
struct IdIndex {
    prefix: FxHashMap<u64, NodeIdx>,
    spill: IdMap<NodeIdx>,
}

fn id_prefix(id: &Id) -> u64 {
    u64::from_le_bytes(id.0[..8].try_into().expect("Id has 32 bytes"))
}

impl IdIndex {
    /// `ids` is the `NodeIdx -> Id` table used to verify prefix hits.
    fn get(&self, id: &Id, ids: &[Id]) -> Option<NodeIdx> {
        let idx = *self.prefix.get(&id_prefix(id))?;
        if ids[idx.0 as usize] == *id {
            Some(idx)
        } else {
            self.spill.get(id).copied()
        }
    }

    fn insert(&mut self, id: Id, idx: NodeIdx, ids: &[Id]) {
        match self.prefix.entry(id_prefix(&id)) {
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(idx);
            }
            std::collections::hash_map::Entry::Occupied(mut e) => {
                if ids[e.get().0 as usize] == id {
                    e.insert(idx);
                } else {
                    self.spill.insert(id, idx);
                }
            }
        }
    }
}

/// Compact handle for an applied node. Handles are allocated densely in local
/// apply order, so `Vec`s indexed by `NodeIdx` replace `Id`-keyed maps for
/// everything but the single interning map.
///
/// Handles are replica-local: two replicas applying the same ops in different
/// orders assign different handles. They must never participate in anything
/// convergence-relevant — sibling ordering, hashing, and the wire format all
/// operate on `Id`s.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeIdx(pub u32);

/// The virtual origin's handle — always the first interned node.
pub(crate) const ORIGIN_IDX: NodeIdx = NodeIdx(0);

/// Where an applied node lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Loc {
    /// Element of an insert run: (run head, position within the run).
    Run { run: NodeIdx, pos: u32 },
    /// The document's virtual origin (`NodeIdx(0)`, interned at construction).
    /// Never visible, never yielded; it exists so ops can anchor at the
    /// document itself — a "root" insert is just `InsertAfter(origin, ch)`.
    Origin,
    /// Link in a remove chain: (chain head, position within the chain).
    RemoveChain { chain: NodeIdx, pos: u32 },
    /// Multi-target remove (stored in `remove_nodes`).
    MultiRemove,
    /// A placement-register move op (stored in `move_nodes`).
    MoveOp,
    /// A span-annotation mark op (stored in `mark_nodes`).
    MarkOp,
    /// A containment-register place op (stored in `place_nodes`;
    /// PLACEMENT_SPEC.md).
    PlaceOp,
}

/// `Loc` packed into 8 bytes for the per-node `locs` Vec (the enum is 12).
/// Layout: 3-bit kind | 32-bit handle | 29-bit position. The handle keeps the
/// full `NodeIdx` (u32) range; `pos` is a within-run/-chain offset, so 29 bits
/// (~536M) is far beyond any real run length (the longest in the test corpus
/// is ~69k).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackedLoc(u64);

impl PackedLoc {
    const KIND_RUN: u64 = 0;
    const KIND_ORIGIN: u64 = 1;
    const KIND_REMOVE_CHAIN: u64 = 2;
    const KIND_MULTI_REMOVE: u64 = 3;
    const KIND_MOVE_OP: u64 = 4;
    const KIND_MARK_OP: u64 = 5;
    const KIND_PLACE_OP: u64 = 6;

    #[inline]
    fn pack(handle: NodeIdx, pos: u32, kind: u64) -> Self {
        debug_assert!(pos < (1 << 29), "Loc position {pos} exceeds 29 bits");
        PackedLoc(kind | (handle.0 as u64) << 3 | (pos as u64) << 35)
    }

    #[inline]
    fn unpack(self) -> Loc {
        let handle = NodeIdx((self.0 >> 3) as u32);
        let pos = (self.0 >> 35) as u32;
        match self.0 & 0b111 {
            Self::KIND_RUN => Loc::Run { run: handle, pos },
            Self::KIND_ORIGIN => Loc::Origin,
            Self::KIND_REMOVE_CHAIN => Loc::RemoveChain { chain: handle, pos },
            Self::KIND_MOVE_OP => Loc::MoveOp,
            Self::KIND_MARK_OP => Loc::MarkOp,
            Self::KIND_PLACE_OP => Loc::PlaceOp,
            _ => Loc::MultiRemove,
        }
    }
}

impl From<Loc> for PackedLoc {
    #[inline]
    fn from(loc: Loc) -> Self {
        match loc {
            Loc::Run { run, pos } => PackedLoc::pack(run, pos, PackedLoc::KIND_RUN),
            Loc::Origin => PackedLoc::pack(NodeIdx(0), 0, PackedLoc::KIND_ORIGIN),
            Loc::RemoveChain { chain, pos } => {
                PackedLoc::pack(chain, pos, PackedLoc::KIND_REMOVE_CHAIN)
            }
            Loc::MultiRemove => PackedLoc::pack(NodeIdx(0), 0, PackedLoc::KIND_MULTI_REMOVE),
            Loc::MoveOp => PackedLoc::pack(NodeIdx(0), 0, PackedLoc::KIND_MOVE_OP),
            Loc::MarkOp => PackedLoc::pack(NodeIdx(0), 0, PackedLoc::KIND_MARK_OP),
            Loc::PlaceOp => PackedLoc::pack(NodeIdx(0), 0, PackedLoc::KIND_PLACE_OP),
        }
    }
}

/// A causal anchor for inserting into a HashSeq.
///
/// Captures everything needed to land an insert at the cursor's position
/// deterministically — even when the local neighborhood is involved in concurrent
/// forks: the anchor node, the op kind, and `extra_deps`, a snapshot of the tips
/// visible at cursor-placement time (with the anchor itself removed).
///
/// This is the canonical insertion path: `insert_batch` is built on it, and a
/// `Run` built from a cursor (`into_run`) can be applied later — even after
/// concurrent mutations — and will land in the causally correct position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cursor {
    /// Insert immediately after `anchor`. In an empty sequence the anchor is
    /// the document origin.
    After {
        anchor: Id,
        extra_deps: BTreeSet<Id>,
    },
    /// Insert immediately before `anchor`. Used (per the Fugue rule) when the
    /// left neighbor already has a right child, so a fork at the left neighbor
    /// would otherwise give hash-determined ordering.
    Before {
        anchor: Id,
        extra_deps: BTreeSet<Id>,
    },
}

impl Cursor {
    /// Build the first `HashNode` of an insertion at this cursor.
    /// Subsequent chars of a burst chain `InsertAfter` from this node.
    pub fn first_node(self, ch: char) -> HashNode {
        self.payload_node(Payload::Char(ch))
    }

    /// Build the insert node for any payload — a char, or a value
    /// commitment id (a link or an artifact).
    pub fn payload_node(self, payload: Payload) -> HashNode {
        let (pins, at) = match self {
            Cursor::After { anchor, extra_deps } => (extra_deps, Anchor::After(anchor)),
            Cursor::Before { anchor, extra_deps } => (extra_deps, Anchor::Before(anchor)),
        };
        HashNode {
            pins,
            op: Op::Insert { at, payload },
        }
    }

    /// Build a `Run` starting at this cursor with `first` as its first character.
    pub fn into_run(self, first: char) -> Run {
        match self {
            Cursor::After { anchor, extra_deps } => Run::new(anchor, extra_deps, first),
            Cursor::Before { anchor, extra_deps } => Run::new_before(anchor, extra_deps, first),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CausalInsert {
    pub pins: BTreeSet<Id>,
    pub anchor: Id,
    pub ch: char,
    /// `Some(value id)` for an atom (a non-char payload): `ch` is then the
    /// U+FFFC placeholder and the commitment id lives in the value column.
    pub payload: Option<Id>,
}

/// The placeholder char an atom occupies in run text and `iter()` output —
/// U+FFFC OBJECT REPLACEMENT CHARACTER; renderers substitute the resolved
/// value (the payload id is the identity, `payload_of` reads it).
pub const ATOM_CHAR: char = '\u{FFFC}';

/// Storage form of a multi-target remove (`remove_batch` spanning several
/// chars). Single-target removes live in `RemoveRun` chains instead.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CausalRemove {
    pub pins: SortedIdVec,
    /// Removed element handles, in Id order.
    pub nodes: Box<[NodeIdx]>,
}

/// Storage form of an applied move op (the placement projection,
/// HASHSEQ_SPEC.md `Move`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredMove {
    pub target: NodeIdx,
    /// Destination glued point: side + anchor handle.
    pub to_before: bool,
    pub to_anchor: NodeIdx,
    /// Superseded move ops (the register's overwrites edges), in Id order.
    pub overwrites: SortedIdVec,
    /// Frontier pins, in Id order.
    pub pins: SortedIdVec,
}

/// Storage form of an applied mark op (the span-annotation projection,
/// MARKS.md). Anchors are glue points on elements (or the origin); `kind`
/// and `value` are value commitments, not references.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredMark {
    pub start_after: bool,
    pub start_anchor: NodeIdx,
    pub end_after: bool,
    pub end_anchor: NodeIdx,
    pub kind: Id,
    pub value: Id,
    /// Superseded mark ops (range- and kind-scoped at read), in Id order.
    pub overwrites: SortedIdVec,
    /// Frontier pins (the mark layer's own frontier), in Id order.
    pub pins: SortedIdVec,
}

/// A coalesced chain of single-target removes: remove `i` deletes `targets[i]`
/// and causally depends on remove `i-1`; the first link carries
/// `first_extra_deps`. This is how backspace/delete bursts are stored — the
/// in-memory mirror of the wire format's remove-run sections, and the remove
/// analog of insert runs.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RemoveRun {
    pub first_extra_deps: SortedIdVec,
    /// Element handles removed, in removal order.
    pub targets: Vec<NodeIdx>,
    /// Handles of the remove nodes themselves; `links[i]` removes `targets[i]`.
    pub links: Vec<NodeIdx>,
}

impl RemoveRun {
    pub fn len(&self) -> usize {
        self.links.len()
    }

    pub fn is_empty(&self) -> bool {
        self.links.is_empty()
    }
}

/// Storage form of an insert run. Mirrors the wire-level [`Run`] but holds
/// element handles instead of full ids: the id of element `i` is
/// `seq.id_of(elements[i])`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRun {
    pub anchor: Id,
    pub first_op: FirstOp,
    pub first_extra_deps: SortedIdVec,
    /// Extra deps of interior elements (offset >= 1), sparse — see
    /// [`Run::interior_extra_deps`]. Lets a typing burst extend its run
    /// across a remove instead of starting a new run per burst.
    pub interior_extra_deps: BTreeMap<usize, SortedIdVec>,
    pub text: String,
    pub elements: Vec<NodeIdx>,
}

impl StoredRun {
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    pub fn char_at(&self, pos: usize) -> char {
        if self.text.len() == self.elements.len() {
            // All single-byte chars: byte offset is char offset.
            self.text.as_bytes()[pos] as char
        } else {
            self.text.chars().nth(pos).unwrap()
        }
    }

    pub fn head(&self) -> NodeIdx {
        self.elements[0]
    }

    pub fn last(&self) -> NodeIdx {
        *self.elements.last().unwrap()
    }

    fn extend(&mut self, idx: NodeIdx, ch: char, extra_deps: SortedIdVec) {
        if !extra_deps.is_empty() {
            self.interior_extra_deps
                .insert(self.elements.len(), extra_deps);
        }
        self.text.push(ch);
        self.elements.push(idx);
    }

    /// Split at element index `at` (0 < at < len), returning the right portion.
    /// `right_anchor` is the id of the left portion's last element.
    fn split_at(&mut self, at: usize, right_anchor: Id) -> StoredRun {
        assert!(at > 0 && at < self.len(), "Invalid split position");
        let right_elements = self.elements.split_off(at);
        let byte_pos = self.text.char_indices().nth(at).unwrap().0;
        let right_text = self.text.split_off(byte_pos);
        // Deps at the split point become the right run's first deps (its
        // head keeps its id); later offsets rebase.
        let mut right_interior = self.interior_extra_deps.split_off(&at);
        let right_first_deps = right_interior.remove(&at).unwrap_or_default();
        let right_interior: BTreeMap<usize, SortedIdVec> = right_interior
            .into_iter()
            .map(|(k, v)| (k - at, v))
            .collect();
        StoredRun {
            anchor: right_anchor,
            first_op: FirstOp::After,
            first_extra_deps: right_first_deps,
            interior_extra_deps: right_interior,
            text: right_text,
            elements: right_elements,
        }
    }

    /// Reconstruct the wire-level run. Element ids are copied from the seq's
    /// id table (`ids[elements[i]]`) — no rehashing.
    pub fn to_run(&self, ids: &[Id]) -> Run {
        Run {
            anchor: self.anchor,
            first_op: self.first_op,
            first_extra_deps: self.first_extra_deps.to_id_set(ids),
            interior_extra_deps: self
                .interior_extra_deps
                .iter()
                .map(|(off, deps)| (*off, deps.to_id_set(ids)))
                .collect(),
            run: self.text.clone(),
            elements: self.elements.iter().map(|e| ids[e.0 as usize]).collect(),
        }
    }
}

/// A set of node handles kept sorted by their `Id`, stored as `Vec<NodeIdx>`
/// (4 bytes/entry) instead of `BTreeSet<Id>` (32 bytes/entry plus a ~400 B
/// B-tree node each). Used for the `afters` / `befores_by_anchor` sibling
/// sets, and as the set-once storage for applied deps/pins/overwrites
/// (built with `from_id_set`, rebuilt as `BTreeSet<Id>` for the wire /
/// hashing at encode and decompress time).
///
/// The order is by `Id` — never by handle — because sibling order is a
/// convergence concern and handles are replica-local (see the interning
/// invariant). The handle is only the compact payload; every Id comparison
/// dereferences through the caller-supplied `ids` table, so methods that need
/// ordering take `ids: &[Id]` (where `ids[h.0]` is `h`'s id).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SortedIdVec(Vec<NodeIdx>);

impl SortedIdVec {
    /// Index of the handle whose id equals `id` (`Ok`) or where it would be
    /// inserted to stay sorted (`Err`).
    #[inline]
    fn search(&self, id: &Id, ids: &[Id]) -> Result<usize, usize> {
        self.0.binary_search_by(|h| ids[h.0 as usize].cmp(id))
    }

    /// Build from an already-`Id`-sorted set, mapping each id to its handle
    /// (`BTreeSet` iterates in `Id` order, so the handles land sorted).
    pub(crate) fn from_id_set(set: &BTreeSet<Id>, to_handle: impl FnMut(&Id) -> NodeIdx) -> Self {
        SortedIdVec(set.iter().map(to_handle).collect())
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
    pub fn insert(&mut self, handle: NodeIdx, ids: &[Id]) {
        let id = ids[handle.0 as usize];
        if let Err(pos) = self.search(&id, ids) {
            self.0.insert(pos, handle);
        }
    }

    /// Remove the handle whose id equals `id`, if present.
    pub fn remove(&mut self, id: &Id, ids: &[Id]) {
        if let Ok(pos) = self.search(id, ids) {
            self.0.remove(pos);
        }
    }

    /// Handle with the smallest id.
    #[inline]
    pub fn first(&self) -> Option<NodeIdx> {
        self.0.first().copied()
    }

    /// Handle with the largest id.
    #[inline]
    pub fn last(&self) -> Option<NodeIdx> {
        self.0.last().copied()
    }

    /// Handle with the smallest id `>= id` (the `range(id..).next()` seek).
    pub fn first_ge(&self, id: &Id, ids: &[Id]) -> Option<NodeIdx> {
        let pos = match self.search(id, ids) {
            Ok(p) | Err(p) => p,
        };
        self.0.get(pos).copied()
    }

    #[inline]
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = NodeIdx> + '_ {
        self.0.iter().copied()
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
    type Item = NodeIdx;
    type IntoIter = std::iter::Copied<std::slice::Iter<'a, NodeIdx>>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter().copied()
    }
}

/// One element's placement register: its live move heads (Id order —
/// convergence-safe, never replica-local) and the decider they resolve to
/// under the last-agreed rule. `decider` is derived state, recomputed by
/// the register's only writer (`apply_move`) whenever the head set
/// changes, so reads are O(1): a contested (frozen) register used to be
/// re-resolved — an O(history) walk — on every cursor, iterator and
/// marked-spans read until an honest move collapsed it.
#[derive(Debug, Clone, Default)]
pub struct MoveRegister {
    pub heads: SortedIdVec,
    /// `None` = the creation placement (base slot).
    pub decider: Option<NodeIdx>,
}

#[derive(Debug, Clone)]
pub struct HashSeq {
    /// The document's identity: ops anchor at this id to insert at top level.
    /// All op hashes transitively commit to it, so documents with different
    /// origins can never merge. Interned as `NodeIdx(0)`, tombstoned (it is
    /// virtual and never visible).
    origin: Id,
    // ---- id <-> handle interning: the only Id-keyed lookup structure ----
    id_to_idx: IdIndex,
    /// NodeIdx -> Id (append-only).
    pub ids: Vec<Id>,
    /// NodeIdx -> location (8 bytes each; see [`PackedLoc`]).
    pub locs: Vec<PackedLoc>,
    /// NodeIdx -> tombstone (one bit per handle).
    pub removed: BitSet,

    // All inserts live in runs: sequential typing extends a run, and a lone
    // insert is just a 1-char run. A run is After- or Before-anchored
    // (`StoredRun::first_op`); subsequent elements always chain InsertAfter.
    // Top-level inserts are runs anchored at `origin` like any other.
    pub runs: FxHashMap<NodeIdx, StoredRun>,
    /// Reverse index: anchor -> heads of Before-runs anchored at it. Values are
    /// Id-ordered (a [`SortedIdVec`]): sibling order is a convergence concern,
    /// so it must not use replica-local handles.
    pub befores_by_anchor: FxHashMap<NodeIdx, SortedIdVec>,
    /// Multi-target removes only; single-target removes coalesce into chains.
    pub remove_nodes: FxHashMap<NodeIdx, CausalRemove>,
    /// Applied move ops, by their own handle (the placement-register
    /// history: `overwrites` edges are walked by the last-agreed rule).
    pub move_nodes: FxHashMap<NodeIdx, StoredMove>,
    /// Placement registers: target element -> live move heads + their
    /// resolved decider (see `MoveRegister`).
    pub moves: FxHashMap<NodeIdx, MoveRegister>,
    #[cfg(test)]
    /// How many contested resolutions `resolve_decider` has run (tests
    /// assert reads never re-resolve).
    pub(crate) resolutions: std::cell::Cell<usize>,
    /// Chained single-target removes (backspace/delete bursts), keyed by the
    /// first remove's handle.
    pub remove_runs: FxHashMap<NodeIdx, RemoveRun>,
    /// Fork tracking: anchor -> handles that fork from it (Id-ordered, see
    /// `befores_by_anchor`).
    pub afters: FxHashMap<NodeIdx, SortedIdVec>,

    /// The value column: atom elements' payload commitment ids (HASHSEQ_SPEC
    /// "Payload"). Atoms are single-element runs holding `ATOM_CHAR` as
    /// placeholder text; empty in char-only documents — hot paths guard on
    /// that first.
    pub elem_payloads: FxHashMap<NodeIdx, Id>,
    /// Applied mark ops, by their own handle (MARKS.md).
    pub mark_nodes: FxHashMap<NodeIdx, StoredMark>,
    /// Anchor events for the mark sweep: anchor element (or origin) ->
    /// events crossing at its glue points. Empty in mark-free documents.
    mark_events: FxHashMap<NodeIdx, Vec<MarkEvent>>,
    /// Applied place ops, by their own handle — this object's containment-
    /// register history (PLACEMENT_SPEC.md). Retention: the full spine.
    pub place_nodes: FxHashMap<NodeIdx, StoredPlace>,
    /// The containment register: live heads + the last-agreed walk.
    pub(crate) placement: PlacementRegister,

    pub(crate) tips: BTreeSet<Id>,
    /// The mark layer's own frontier: marks are downstream-only (content
    /// never references marks), so mark ops never enter the text tips.
    pub(crate) mark_tips: BTreeSet<Id>,
    /// Authored-ops outbox (APP_NOTES #8 / delta sync): locally-authored
    /// nodes since the last drain, in apply order. `None` = disabled (the
    /// default — servers and tests never author-and-forget). Remote
    /// application paths never record; only the authoring helpers do.
    pub(crate) outbox: Option<Vec<HashNode>>,
    /// Parked orphans + the gate (see `delivery::Delivery`). Gated here
    /// today: `Move` targets/anchors that fail the placement rows, `Put`
    /// (a map op in a seq), non-char insert payloads (the value column
    /// generalization), inverted mark spans, and mark anchors on move-op
    /// splice points.
    pub(crate) delivery: Delivery,
    index: RunIndex,
}

/// The live mark set at a point, grouped by kind: `(kind, [(mark id,
/// value)])`, both levels id-ordered. Multiple entries under one kind are a
/// surfaced conflict (MVR); a TOMBSTONE value is an unmark.
pub type MarkSet = Vec<(Id, Vec<(Id, Id)>)>;

/// One crossing at an anchor's glue point: a mark op starting or ending.
#[derive(Debug, Clone, Copy)]
struct MarkEvent {
    op: NodeIdx,
    start: bool,
    /// Which of the anchor's two points: `After` (true) or `Before`.
    after: bool,
}

/// An applied `Place` op — the containment register's stored form
/// (PLACEMENT_SPEC.md). `placed_at` is a foreign commitment, kept as a raw
/// id (it is not in this object's id table); `overwrites`/`pins` are refs,
/// interned.
#[derive(Debug, Clone)]
pub struct StoredPlace {
    pub placed_at: Id,
    pub overwrites: SortedIdVec,
    pub pins: SortedIdVec,
}

impl PartialEq for HashSeq {
    fn eq(&self, other: &Self) -> bool {
        self.tips == other.tips && self.mark_tips == other.mark_tips
    }
}

impl Eq for HashSeq {}

/// An anonymous document (all-zero origin). Replicas using `default()`
/// converge with each other, mirroring the pre-origin behavior.
impl Default for HashSeq {
    fn default() -> Self {
        Self::new(Id::default())
    }
}

impl HashSeq {
    /// Create an empty document identified by `doc_id`. The id is the anchor
    /// for top-level inserts and is committed to by every op hash; replicas
    /// must construct with the same `doc_id` to converge.
    pub fn new(doc_id: Id) -> Self {
        let mut seq = Self {
            origin: doc_id,
            id_to_idx: IdIndex::default(),
            ids: Vec::new(),
            locs: Vec::new(),
            removed: BitSet::default(),
            runs: FxHashMap::default(),
            befores_by_anchor: FxHashMap::default(),
            remove_nodes: FxHashMap::default(),
            move_nodes: FxHashMap::default(),
            moves: FxHashMap::default(),
            #[cfg(test)]
            resolutions: std::cell::Cell::new(0),
            remove_runs: FxHashMap::default(),
            afters: FxHashMap::default(),
            elem_payloads: FxHashMap::default(),
            mark_nodes: FxHashMap::default(),
            mark_events: FxHashMap::default(),
            place_nodes: FxHashMap::default(),
            placement: PlacementRegister::default(),
            tips: BTreeSet::new(),
            mark_tips: BTreeSet::new(),
            outbox: None,
            delivery: Delivery::default(),
            index: RunIndex::default(),
        };
        // The origin is an axiom: present (so anchoring at it always
        // satisfies the dependency check) but tombstoned (never visible, and
        // the iterator skips it like any removed node).
        let idx = seq.intern(doc_id, Loc::Origin);
        debug_assert_eq!(idx, ORIGIN_IDX);
        seq.removed.set(ORIGIN_IDX.0 as usize);
        seq.tips.insert(doc_id);
        seq
    }

    /// The document's identity (the anchor of top-level inserts).
    pub fn origin(&self) -> Id {
        self.origin
    }

    // ---- interning ----

    fn next_idx(&self) -> NodeIdx {
        NodeIdx(self.ids.len() as u32)
    }

    fn intern(&mut self, id: Id, loc: Loc) -> NodeIdx {
        let idx = self.next_idx();
        self.ids.push(id);
        self.locs.push(loc.into());
        self.removed.push(false);
        self.id_to_idx.insert(id, idx, &self.ids);
        idx
    }

    /// Record a locally-authored node into the outbox (delta sync). Called
    /// ONLY by authoring helpers — never by apply — so remote and replayed
    /// ops can never echo back onto the wire.
    #[inline]
    pub(crate) fn record_authored(&mut self, node: &HashNode) {
        if let Some(ob) = &mut self.outbox {
            ob.push(node.clone());
        }
    }

    /// Apply a locally-authored node and record it for delta sync only if
    /// it was admitted. `Err` hands the node back: the gate refused it
    /// (it sits in quarantine, `contains_node` is false) and nothing was
    /// recorded, so peers are never sent an op this replica itself
    /// rejected. Local deps are always applied, so a refusal is a gate
    /// verdict, never a parked orphan.
    fn author(&mut self, node: HashNode) -> Result<HashNode, HashNode> {
        let id = node.id();
        self.apply_with_id(id, node.clone());
        if self.contains_node(&id) {
            self.record_authored(&node);
            Ok(node)
        } else {
            Err(node)
        }
    }

    pub fn idx_of(&self, id: &Id) -> Option<NodeIdx> {
        self.id_to_idx.get(id, &self.ids)
    }

    /// `idx_of` for ids that are known to be interned.
    fn idx_of_known(&self, id: &Id) -> NodeIdx {
        self.idx_of(id).expect("id was interned")
    }

    pub fn id_of(&self, idx: NodeIdx) -> Id {
        self.ids[idx.0 as usize]
    }

    pub(crate) fn id_ref(&self, idx: NodeIdx) -> &Id {
        &self.ids[idx.0 as usize]
    }

    pub fn loc_of(&self, idx: NodeIdx) -> Loc {
        self.locs[idx.0 as usize].unpack()
    }

    pub fn is_removed(&self, idx: NodeIdx) -> bool {
        self.removed.get(idx.0 as usize)
    }

    /// Check if a node ID exists (insert, remove, or root) — one map probe.
    pub fn contains_node(&self, id: &Id) -> bool {
        self.idx_of(id).is_some()
    }

    /// Is this element an atom (a non-char payload)?
    pub(crate) fn is_atom(&self, e: NodeIdx) -> bool {
        !self.elem_payloads.is_empty() && self.elem_payloads.contains_key(&e)
    }

    /// The value commitment id of the element, if it is an atom. Chars
    /// answer `None` (their value ids derive from the char; the wire and
    /// preimage layers handle that).
    pub fn payload_of(&self, id: &Id) -> Option<Id> {
        let e = self.idx_of(id)?;
        self.elem_payloads.get(&e).copied()
    }

    /// Reconstruct an atom's `HashNode` (for merge / re-broadcast / wire).
    pub(crate) fn atom_node(&self, e: NodeIdx) -> HashNode {
        let Loc::Run { run, .. } = self.loc_of(e) else {
            unreachable!("atoms are single-element runs")
        };
        let r = &self.runs[&run];
        debug_assert_eq!(r.elements.len(), 1, "atoms never chain");
        let at = match r.first_op {
            FirstOp::After => Anchor::After(r.anchor),
            FirstOp::Before => Anchor::Before(r.anchor),
        };
        HashNode {
            pins: r.first_extra_deps.to_id_set(&self.ids),
            op: Op::Insert {
                at,
                payload: Payload::Id(self.elem_payloads[&e]),
            },
        }
    }

    pub(crate) fn char_at(&self, idx: NodeIdx) -> char {
        match self.loc_of(idx) {
            Loc::Run { run, pos } => self.runs[&run].char_at(pos as usize),
            _ => panic!("char_at on a non-insert node"),
        }
    }

    /// Get the character value for a given node ID
    pub fn get_node_char(&self, id: &Id) -> char {
        self.char_at(self.idx_of(id).expect("unknown id"))
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub fn orphans(&self) -> impl Iterator<Item = &HashNode> {
        self.delivery.orphans()
    }

    // ---- causal adjacency (handle space) ----

    /// Successors of `idx`: explicit forks (Id-ordered) or the run continuation.
    pub(crate) fn afters_of(&self, idx: NodeIdx) -> impl DoubleEndedIterator<Item = NodeIdx> + '_ {
        let explicit = self.afters.get(&idx);
        // Run fallback only fires when there's no explicit afters entry.
        let from_run = if explicit.is_none() {
            match self.loc_of(idx) {
                Loc::Run { run, pos } => self.runs[&run].elements.get(pos as usize + 1).copied(),
                _ => None,
            }
        } else {
            None
        };
        explicit.into_iter().flatten().chain(from_run)
    }

    /// Before-run heads anchored at `idx`, in Id order.
    pub(crate) fn befores_of(&self, idx: NodeIdx) -> impl DoubleEndedIterator<Item = NodeIdx> + '_ {
        self.befores_by_anchor.get(&idx).into_iter().flatten()
    }

    /// Ids that come after `id`: explicit forks (Id-ordered) or the run
    /// continuation. Id-space convenience over [`Self::afters_of`].
    pub fn afters(&self, id: &Id) -> impl Iterator<Item = Id> + '_ {
        self.idx_of(id)
            .into_iter()
            .flat_map(|i| self.afters_of(i))
            .map(|i| self.id_of(i))
    }

    /// Ids of Before-runs anchored at `id`, in Id order.
    pub fn befores(&self, id: &Id) -> impl Iterator<Item = Id> + '_ {
        self.idx_of(id)
            .into_iter()
            .flat_map(|i| self.befores_of(i))
            .map(|i| self.id_of(i))
    }

    /// Whether `id` is a tombstoned insert.
    pub fn is_removed_id(&self, id: &Id) -> bool {
        self.idx_of(id).is_some_and(|i| self.is_removed(i))
    }

    // ---- position index plumbing ----

    /// The index addresses an insert as (run head, element offset).
    fn elem_ref(&self, idx: NodeIdx) -> ElemRef {
        match self.loc_of(idx) {
            Loc::Run { run, pos } => (run, pos),
            _ => panic!("elem_ref on a non-insert node"),
        }
    }

    /// The insert node at visible position `pos`.
    fn element_at(&self, pos: usize) -> Option<NodeIdx> {
        let (head, off) = self.index.get(pos)?;
        Some(self.runs[&head].elements[off as usize])
    }

    /// First element, in document order, of the region rooted at `n`: a node's
    /// before-runs precede it, recursively.
    fn region_first(&self, mut n: NodeIdx) -> NodeIdx {
        while let Some(first) = self.befores_by_anchor.get(&n).and_then(|s| s.first()) {
            n = first;
        }
        n
    }

    /// Last element, in document order, of the subtree hanging off `n`:
    /// follow the run to its tail, then the largest after-fork, repeatedly.
    /// (Befores precede their anchors, so they never contribute the last
    /// element; run interiors never carry explicit afters — forks split.)
    fn subtree_last(&self, mut n: NodeIdx) -> NodeIdx {
        loop {
            if let Loc::Run { run, .. } = self.loc_of(n) {
                n = self.runs[&run].last();
            }
            match self.afters.get(&n).and_then(|s| s.last()) {
                Some(next) => n = next,
                None => return n,
            }
        }
    }

    /// Where a node with `id` anchored `After(anchor)` lands: directly
    /// before the next bigger sibling's region — or, as the biggest sibling,
    /// directly after everything hanging off the anchor. Siblings are
    /// explicit forks, the implicit run continuation, and rendered move-ins;
    /// tombstones don't matter (a removed element's region still occupies
    /// its place in document order).
    fn after_sibling_target(&self, anchor: NodeIdx, id: &Id) -> (NodeIdx, bool) {
        // Explicit-afters case: O(log n) range seek. Run-fallback case: at
        // most one candidate, just check it.
        let next_node = if let Some(siblings) = self.afters.get(&anchor) {
            siblings.first_ge(id, &self.ids)
        } else {
            self.afters_of(anchor)
                .find(|a| self.ids[a.0 as usize] >= *id)
        };
        match next_node {
            Some(next) => (self.region_first(next), true),
            None => (self.subtree_last(anchor), false),
        }
    }

    /// Where a node with `id` anchored `Before(anchor)` lands: before the
    /// next bigger before-sibling's region, or — as the biggest — directly
    /// before the anchor itself. (Before-siblings release in Id order,
    /// directly before their anchor.)
    fn before_sibling_target(&self, anchor: NodeIdx, id: &Id) -> NodeIdx {
        match self
            .befores_by_anchor
            .get(&anchor)
            .and_then(|s| s.first_ge(id, &self.ids))
        {
            Some(next) => self.region_first(next),
            None => anchor,
        }
    }

    fn neighbours(&self, idx: usize) -> (Option<NodeIdx>, Option<NodeIdx>) {
        let left = idx
            .checked_sub(1)
            .and_then(|prev_idx| self.element_at(prev_idx));
        let right = self.element_at(idx);
        (left, right)
    }

    /// Clone of `self.tips` with `anchor` removed.
    ///
    /// Fast path: sequential typing leaves `tips == {anchor}`, in which case the
    /// result is empty and we skip cloning the BTreeSet entirely (which would
    /// allocate a tree node just to drop it).
    fn tips_minus(&self, anchor: &Id) -> BTreeSet<Id> {
        if self.tips.len() == 1 && self.tips.contains(anchor) {
            BTreeSet::new()
        } else {
            let mut deps = self.tips.clone();
            deps.remove(anchor);
            deps
        }
    }

    pub fn insert(&mut self, idx: usize, value: char) {
        self.insert_batch(idx, [value]);
    }

    /// Insert a burst of characters at visible position `idx` (clamped to the end
    /// of the sequence). The first char lands at the cursor for `idx`; the rest
    /// chain `InsertAfter` from it.
    pub fn insert_batch(&mut self, idx: usize, batch: impl IntoIterator<Item = char>) {
        let mut chars = batch.into_iter();
        let Some(first_ch) = chars.next() else {
            return;
        };

        let cursor = self
            .cursor_at(idx.min(self.len()))
            .expect("cursor_at is total for idx <= len");
        let first_node = cursor.first_node(first_ch);

        // Cursor-derived inserts anchor on applied elements/origin and are
        // always admitted, so recording before apply is safe here and saves
        // a clone per char on the hot path (`author` is for gate-able ops).
        let mut prev_id = first_node.id();
        self.record_authored(&first_node);
        self.apply_with_id(prev_id, first_node);

        // After the first apply, tips == {prev_id}, so the chained nodes carry no
        // extra deps.
        for ch in chars {
            let node = HashNode {
                pins: BTreeSet::new(),
                op: Op::insert_after(prev_id, ch),
            };
            prev_id = node.id();
            self.record_authored(&node);
            self.apply_with_id(prev_id, node);
        }
    }

    /// Build (without applying) an insert of a value commitment id at
    /// visible position `idx` — an atom: a link or an artifact id.
    pub fn make_insert_value(&self, idx: usize, payload: Id) -> Option<HashNode> {
        let cursor = self.cursor_at(idx.min(self.len()))?;
        Some(cursor.payload_node(Payload::Id(payload).resolved()))
    }

    /// Insert a value commitment id at visible position `idx`. The atom
    /// renders as `ATOM_CHAR`; `payload_of` reads its id back.
    pub fn insert_value(&mut self, idx: usize, payload: Id) -> HashNode {
        let node = self
            .make_insert_value(idx, payload)
            .expect("cursor_at is total for clamped idx");
        self.author(node).expect("cursor-derived inserts are always admitted")
    }

    pub fn remove(&mut self, idx: usize) {
        self.remove_batch(idx, 1);
    }

    /// Remove `amount` characters starting at visible position `idx`.
    ///
    /// Returns the `HashNode` that was applied — useful when the caller wants to
    /// re-broadcast the op over the wire. Returns `None` if `amount == 0` or if
    /// `idx` is past the end (no characters were actually removed).
    pub fn remove_batch(&mut self, idx: usize, amount: usize) -> Option<HashNode> {
        let node = self.make_remove_batch(idx, amount)?;
        Some(self.author(node).expect("removes of applied elements are always admitted"))
    }

    /// Build (without applying) the removal of `amount` characters starting
    /// at visible position `idx`. `None` if nothing is there to remove.
    pub fn make_remove_batch(&self, idx: usize, amount: usize) -> Option<HashNode> {
        if amount == 0 {
            return None;
        }

        let mut to_remove = BTreeSet::new();
        for pos in idx..idx.saturating_add(amount) {
            if let Some(i) = self.element_at(pos) {
                to_remove.insert(self.id_of(i));
            } else {
                break;
            }
        }

        if to_remove.is_empty() {
            return None;
        }

        let pins = BTreeSet::from_iter(self.tips.difference(&to_remove).cloned());
        Some(HashNode {
            pins,
            op: Op::Remove(to_remove),
        })
    }

    /// Resolve "directly before/after node `el`" into an index target.
    /// `el` may be an element, a rendered move op (resolving to its target's
    /// destination fragment), or the virtual origin, whose "position" is the
    /// start of its afters region (the origin's own befores precede that
    /// point — with no afters, everything precedes it, so it is the end).
    fn index_target(&self, el: NodeIdx, before: bool) -> IndexTarget {
        if el == ORIGIN_IDX {
            if before {
                return match self.afters.get(&ORIGIN_IDX).and_then(|s| s.first()) {
                    Some(first) => self.index_target(self.region_first(first), true),
                    None => IndexTarget::Back,
                };
            }
            return IndexTarget::Back;
        }
        if let Loc::MoveOp = self.loc_of(el) {
            // The op fragment: the rendered element when `el` decides its
            // register, its zero-width splice ghost otherwise.
            let target = self.move_nodes[&el].target;
            if self.decider_of(target) == Some(el) && !self.is_removed(target) {
                let elem = self.elem_ref(target);
                return if before {
                    IndexTarget::BeforeMoved(elem)
                } else {
                    IndexTarget::AfterMoved(elem)
                };
            }
            return if before {
                IndexTarget::BeforeSplice(el)
            } else {
                IndexTarget::AfterSplice(el)
            };
        }
        let at = self.elem_ref(el);
        if before {
            IndexTarget::BeforeElem(at)
        } else {
            IndexTarget::AfterElem(at)
        }
    }

    /// `anchor` is `after.anchor` resolved (a checked dependency, so interned).
    fn insert_after(&mut self, id: Id, anchor: NodeIdx, after: CausalInsert) {

        // A move-op anchor: content anchoring at the splice point — make
        // sure the op holds a physical rank first.
        if let Loc::MoveOp = self.loc_of(anchor) {
            self.ensure_op_fragment(anchor);
        }

        // Fast path: extend the run whose tail is the anchor. Extra deps on
        // the new element (e.g. `{remove_id}` when typing resumes after a
        // delete) don't block extension — they're stored sparsely at the
        // element's offset and participate in its id, so the chain still
        // reconstructs exactly.
        if let Loc::Run { run, pos } = self.loc_of(anchor) {
            // Check for explicit forks first (cheap u32-keyed lookup)
            let has_explicit_afters = self.afters.get(&anchor).is_some_and(|ns| !ns.is_empty());

            let atomic = after.payload.is_some() || self.is_atom(anchor);
            if !has_explicit_afters && !atomic && pos as usize + 1 == self.runs[&run].len() {
                // Run extension - most common case for sequential typing
                let idx = self.intern(id, Loc::Run { run, pos: pos + 1 });
                let deps = SortedIdVec::from_id_set(&after.pins, |d| self.idx_of_known(d));
                self.runs
                    .get_mut(&run)
                    .unwrap()
                    .extend(idx, after.ch, deps);
                self.index.extend_run(run, pos + 1);
                return;
            }
        }

        // Slow path: this insert forks.
        let target = self.after_sibling_target(anchor, &id);

        // We are inserting after a node inside a run (the extension case was
        // handled by the fast path above, so this is a fork). If the anchor isn't
        // the run's tail, split off everything after it first.
        if let Loc::Run { run, pos } = self.loc_of(anchor)
            && (pos as usize) + 1 < self.runs[&run].len()
        {
            self.split_run_at(run, pos as usize + 1);
            debug_assert_eq!(self.runs[&run].last(), anchor);
        }

        // Start a new run anchored at the anchor node.
        let idx = self.next_idx();
        self.intern(id, Loc::Run { run: idx, pos: 0 });
        let first_extra_deps = SortedIdVec::from_id_set(&after.pins, |d| self.idx_of_known(d));
        self.runs.insert(
            idx,
            StoredRun {
                anchor: after.anchor,
                first_op: FirstOp::After,
                first_extra_deps,
                interior_extra_deps: BTreeMap::new(),
                text: after.ch.to_string(),
                elements: vec![idx],
            },
        );

        if let Some(v) = after.payload {
            self.elem_payloads.insert(idx, v);
        }

        // run extension is handled in the fast path above, fork/split updates the afters set
        self.afters.entry(anchor).or_default().insert(idx, &self.ids);

        // Resolve the target node only now: the split above may have
        // relocated it into the right-hand run.
        let (el, before) = target;
        let t = self.index_target(el, before);
        self.index.insert_span_at(t, idx);
    }

    /// Split the run `run` at element index `at` (0 < at < len). The right
    /// portion becomes its own run, re-located and tracked in `afters` of the
    /// left portion's last element. Returns the right run's head.
    fn split_run_at(&mut self, run: NodeIdx, at: usize) -> NodeIdx {
        let r = self.runs.get_mut(&run).unwrap();
        let left_last = r.elements[at - 1];
        let right_anchor = self.ids[left_last.0 as usize];
        let right_run = r.split_at(at, right_anchor);
        let right_head = right_run.head();

        // re-locate the right run's elements
        for (i, e) in right_run.elements.iter().enumerate() {
            self.locs[e.0 as usize] = Loc::Run {
                run: right_head,
                pos: i as u32,
            }
            .into();
        }

        self.runs.insert(right_head, right_run);
        self.index.split_run(run, at as u32, right_head);
        // Track the split in afters so iteration can find the right portion
        self.afters
            .entry(left_last)
            .or_default()
            .insert(right_head, &self.ids);
        right_head
    }

    /// Apply a move: O(1) register bookkeeping plus, when the *rendered*
    /// placement changed, one index relocation (HASHSEQ_SPEC.md "Move"):
    /// excise at the origin — keeping the base slot, the origin ghost — and
    /// insert a singleton fragment at the newly rendered glue point.
    fn apply_move(
        &mut self,
        id: Id,
        pins: BTreeSet<Id>,
        target: Id,
        to: Anchor,
        overwrites: BTreeSet<Id>,
    ) {
        let target_idx = self.idx_of_known(&target);
        let old_decider = self.decider_of(target_idx);
        let idx = self.intern(id, Loc::MoveOp);
        let (to_before, to_id) = match to {
            Anchor::Before(a) => (true, a),
            Anchor::After(a) => (false, a),
        };
        let to_anchor = self.idx_of_known(&to_id);

        // heads(x) = heads(x) − overwrites ∪ {u}. The same-register filter is
        // structural: we only remove ids present in THIS target's head list,
        // so overwrites naming moves of other targets (or non-moves) are
        // ignored, never errors.
        let reg = self.moves.entry(target_idx).or_default();
        for o in &overwrites {
            reg.heads.remove(o, &self.ids);
        }
        reg.heads.insert(idx, &self.ids);

        let stored = StoredMove {
            target: target_idx,
            to_before,
            to_anchor,
            overwrites: SortedIdVec::from_id_set(&overwrites, |d| {
                self.id_to_idx.get(d, &self.ids).expect("ref was interned")
            }),
            pins: SortedIdVec::from_id_set(&pins, |d| {
                self.id_to_idx.get(d, &self.ids).expect("ref was interned")
            }),
        };
        self.move_nodes.insert(idx, stored);

        // The head set changed: resolve once, here, and cache it on the
        // register (the history it depends on — `move_nodes` — is
        // append-only, so nothing else can invalidate it).
        let heads: Vec<NodeIdx> = self.moves[&target_idx].heads.iter().collect();
        let new_decider = self.resolve_decider(heads);
        self.moves.get_mut(&target_idx).expect("just written").decider = new_decider;

        // Remove beats move: a tombstoned element renders nowhere, so a
        // register change must not touch the index.
        if !self.is_removed(target_idx) {
            if old_decider != new_decider {
                self.rerender(target_idx, old_decider, new_decider);
            }
        }
    }

    /// Reconstruct a move op's `HashNode` (for merge / re-broadcast).
    pub fn move_node(&self, idx: NodeIdx, mv: &StoredMove) -> HashNode {
        let _ = idx;
        let to_id = self.id_of(mv.to_anchor);
        HashNode {
            pins: mv.pins.to_id_set(&self.ids),
            op: Op::Move {
                target: self.id_of(mv.target),
                to: if mv.to_before {
                    Anchor::Before(to_id)
                } else {
                    Anchor::After(to_id)
                },
                overwrites: mv.overwrites.to_id_set(&self.ids),
            },
        }
    }

    // ---- placement reads (arbitration happens here, per Law II) ----

    /// The live move heads of `target` (move-op ids, in id order).
    pub fn move_heads(&self, target: &Id) -> Vec<Id> {
        self.idx_of(target)
            .and_then(|t| self.moves.get(&t))
            .map(|reg| reg.heads.iter().map(|i| self.id_of(i)).collect())
            .unwrap_or_default()
    }

    /// Whether `target`'s placement register is contested (`|heads| > 1`) —
    /// the surfaced conflict flag.
    pub fn placement_conflicted(&self, target: &Id) -> bool {
        self.idx_of(target)
            .and_then(|t| self.moves.get(&t))
            .is_some_and(|reg| reg.heads.len() > 1)
    }

    /// The rendered placement of `target`: `None` = the creation placement
    /// (its base slot), or the element is tombstoned (remove beats move —
    /// an absorption rule, the register is moot).
    ///
    /// Freeze, don't flip: a contested register renders at the **last
    /// agreed** placement — recurse on the maximal move ops every head
    /// transitively overwrites; the creation placement is the implicit root,
    /// so the recursion is total. No winner is ever picked by id.
    pub fn placement_of(&self, target: &Id) -> Option<Anchor> {
        let t = self.idx_of(target)?;
        if self.is_removed(t) {
            return None;
        }
        self.decider_of(t).map(|op| self.move_anchor(op))
    }

    /// The move op whose destination `target` renders at (`None` = the
    /// creation placement). Pure register read — removal is the caller's
    /// concern.
    pub(crate) fn decider_of(&self, t: NodeIdx) -> Option<NodeIdx> {
        self.moves.get(&t)?.decider
    }

    fn move_anchor(&self, m: NodeIdx) -> Anchor {
        let mv = &self.move_nodes[&m];
        let a = self.id_of(mv.to_anchor);
        if mv.to_before {
            Anchor::Before(a)
        } else {
            Anchor::After(a)
        }
    }

    /// Transitive overwrites of `m` within the register history (same-target
    /// moves only — the definitional filter). The filter runs before an id
    /// enters the set: `overwrites` is never validated at admission (foreign
    /// ids are ignored, never errors), so a remote op may name an insert or
    /// a move of another target, and those must not surface as candidate
    /// deciders — `resolve_decider` indexes `move_nodes` by what comes back.
    fn move_ancestors(&self, m: NodeIdx, target: NodeIdx) -> FxHashSet<NodeIdx> {
        let mut seen: FxHashSet<NodeIdx> = FxHashSet::default();
        let mut stack: Vec<NodeIdx> = vec![m];
        while let Some(n) = stack.pop() {
            let Some(mv) = self.move_nodes.get(&n) else {
                continue;
            };
            for o in mv.overwrites.0.iter() {
                let same_register = self.move_nodes.get(o).is_some_and(|m| m.target == target);
                if same_register && seen.insert(*o) {
                    stack.push(*o);
                }
            }
        }
        seen
    }

    /// The last-agreed decider of a head set. Called by `apply_move` only;
    /// reads use the cached `MoveRegister::decider`.
    fn resolve_decider(&self, heads: Vec<NodeIdx>) -> Option<NodeIdx> {
        // Ancestor sets are reused across the intersection, the maximal
        // filter (which asks for every pair) and the recursion.
        let mut ancestors: FxHashMap<NodeIdx, FxHashSet<NodeIdx>> = FxHashMap::default();
        self.resolve_decider_with(heads, &mut ancestors)
    }

    /// `move_ancestors`, memoized for one resolution.
    fn ancestors_memo<'m>(
        &self,
        m: NodeIdx,
        target: NodeIdx,
        memo: &'m mut FxHashMap<NodeIdx, FxHashSet<NodeIdx>>,
    ) -> &'m FxHashSet<NodeIdx> {
        memo.entry(m)
            .or_insert_with(|| self.move_ancestors(m, target))
    }

    fn resolve_decider_with(
        &self,
        heads: Vec<NodeIdx>,
        ancestors: &mut FxHashMap<NodeIdx, FxHashSet<NodeIdx>>,
    ) -> Option<NodeIdx> {
        match heads.as_slice() {
            [] => None, // the creation placement — the implicit root
            [one] => Some(*one),
            many => {
                #[cfg(test)]
                self.resolutions.set(self.resolutions.get() + 1);
                let target = self.move_nodes[&many[0]].target;
                // Intersection of every head's transitive overwrites.
                let mut iter = many.iter();
                let mut common = self
                    .ancestors_memo(*iter.next().unwrap(), target, ancestors)
                    .clone();
                for h in iter {
                    let anc = self.ancestors_memo(*h, target, ancestors);
                    common.retain(|c| anc.contains(c));
                    if common.is_empty() {
                        break;
                    }
                }
                if common.is_empty() {
                    return None; // bottoms out at creation
                }
                // Maximal elements: drop anything transitively overwritten by
                // another member of the set.
                let members: Vec<NodeIdx> = common.iter().copied().collect();
                let mut maximal: Vec<NodeIdx> = Vec::new();
                for &c in &members {
                    let dominated = members
                        .iter()
                        .any(|&d| d != c && self.ancestors_memo(d, target, ancestors).contains(&c));
                    if !dominated {
                        maximal.push(c);
                    }
                }
                // Deterministic order (by id) for the recursion.
                maximal.sort_by_key(|i| self.id_of(*i));
                self.resolve_decider_with(maximal, ancestors)
            }
        }
    }

    /// Re-render `target` after its placement register's decider changed:
    /// one index relocation. The old rendering is excised (the base slot
    /// stays — the origin ghost); the new deciding move op joins its
    /// anchor's sibling set like an insert child, keyed by its own id, and
    /// the destination fragment is placed exactly where an insert with that
    /// id would land. Later inserts interleave with it by the same rules.
    fn rerender(&mut self, target: NodeIdx, old: Option<NodeIdx>, new: Option<NodeIdx>) {
        match old {
            Some(op) => {
                // An op with splice children keeps its rank: its destination
                // fragment demotes in place to a zero-width splice ghost.
                if self.op_has_children(op) {
                    self.index.demote_to_splice(self.elem_ref(target), op);
                } else {
                    self.index.remove_moved(self.elem_ref(target));
                    self.unregister_sibling(op);
                }
            }
            None => {
                self.index.remove_element(self.elem_ref(target));
            }
        }
        let Some(op) = new else {
            self.index.restore_base(self.elem_ref(target));
            return;
        };
        if self.index.has_splice(op) {
            // The op already holds its rank (anchored-to while superseded):
            // its splice ghost promotes back into the rendered element.
            self.index.promote_splice(op, self.elem_ref(target));
        } else {
            self.register_op_fragment(op, true);
        }
    }

    /// Does anything anchor at `op`'s splice point — causal children or
    /// mark span endpoints? (Sibling-set entries are dropped when emptied,
    /// so presence means non-empty; `mark_events` is keyed by anchor and
    /// only ever grows, so an entry at `op` means a mark point sits there.)
    /// Anchored ops keep a physical fragment at their rank for life.
    fn op_has_children(&self, op: NodeIdx) -> bool {
        self.afters.contains_key(&op)
            || self.befores_by_anchor.contains_key(&op)
            || (!self.mark_events.is_empty() && self.mark_events.contains_key(&op))
    }

    /// Register `op` in its destination's sibling set (keyed by its own id,
    /// like an insert child) and place its op fragment at that rank: the
    /// rendered target element when `render`, a zero-width splice ghost
    /// otherwise.
    fn register_op_fragment(&mut self, op: NodeIdx, render: bool) {
        let op_id = self.id_of(op);
        let mv = &self.move_nodes[&op];
        let (anchor, to_before, target) = (mv.to_anchor, mv.to_before, mv.target);
        // A destination on another op's splice point: that op needs a
        // physical rank first (terminates — anchors are causal refs, so
        // the recursion strictly descends the DAG).
        if let Loc::MoveOp = self.loc_of(anchor) {
            self.ensure_op_fragment(anchor);
        }
        let (el, before) = if to_before {
            (self.before_sibling_target(anchor, &op_id), true)
        } else {
            let (el, before) = self.after_sibling_target(anchor, &op_id);
            // Mirror the insert fork path: materialize the run fork when the
            // anchor sits mid-run, so the continuation becomes an explicit
            // sibling (afters_of's run fallback is suppressed by the new
            // afters entry). The split can rebase element refs — including
            // the target's — so refs resolve fresh after it.
            if let Loc::Run { run, pos } = self.loc_of(anchor)
                && (pos as usize) + 1 < self.runs[&run].len()
            {
                self.split_run_at(run, pos as usize + 1);
            }
            (el, before)
        };
        let set = if to_before {
            self.befores_by_anchor.entry(anchor).or_default()
        } else {
            self.afters.entry(anchor).or_default()
        };
        set.insert(op, &self.ids);
        let t = self.index_target(el, before);
        if render {
            self.index.place_moved_at(t, self.elem_ref(target));
        } else {
            self.index.place_splice_at(t, op);
        }
    }

    /// Make sure `op` (an insert anchor) has a physical op fragment: content
    /// is about to anchor at its splice point. No-op when the op currently
    /// renders its target (the destination fragment serves) or already has a
    /// splice ghost.
    fn ensure_op_fragment(&mut self, op: NodeIdx) {
        let target = self.move_nodes[&op].target;
        if self.decider_of(target) == Some(op) && !self.is_removed(target) {
            return;
        }
        if self.index.has_splice(op) {
            return;
        }
        self.register_op_fragment(op, false);
    }

    /// Retire a no-longer-deciding move op from its anchor's sibling set.
    /// An emptied set drops its entry: entry presence means "explicit forks
    /// exist" everywhere (`afters_of`'s run fallback, the causal iterator's
    /// run-rest release), so an empty entry would suppress the implicit run
    /// continuation.
    fn unregister_sibling(&mut self, op: NodeIdx) {
        let mv = &self.move_nodes[&op];
        let (anchor, to_before) = (mv.to_anchor, mv.to_before);
        let op_id = self.id_of(op);
        let map = if to_before {
            &mut self.befores_by_anchor
        } else {
            &mut self.afters
        };
        if let Some(set) = map.get_mut(&anchor) {
            set.remove(&op_id, &self.ids);
            if set.is_empty() {
                map.remove(&anchor);
            }
        }
    }

    /// Is `t` rendered away from its base slot (a live element whose
    /// register decides a move)?
    pub(crate) fn rendered_elsewhere(&self, t: NodeIdx) -> bool {
        !self.moves.is_empty() && !self.is_removed(t) && self.decider_of(t).is_some()
    }

    /// Author a move of the element at `target` to the glued point `to`,
    /// superseding the heads this replica sees. Returns the applied node;
    /// `Err` = the gate refused it (target not an element, anchor not a
    /// glue point, or a self-move): nothing applied, nothing queued.
    pub fn move_element(&mut self, target: Id, to: Anchor) -> Result<HashNode, HashNode> {
        let overwrites: BTreeSet<Id> = self.move_heads(&target).into_iter().collect();
        let mut named: BTreeSet<Id> = overwrites.clone();
        named.insert(target);
        named.insert(*to.id());
        let pins: BTreeSet<Id> = self.tips.difference(&named).cloned().collect();
        let node = HashNode {
            pins,
            op: Op::Move {
                target,
                to,
                overwrites,
            },
        };
        self.author(node)
    }

    /// Resolve a mark anchor to a glue point `(node, after-side)`: an
    /// element, the origin, or a move op (its splice point — the bracket
    /// around wherever its target renders). `None` for anything else.
    fn glue_point(&self, a: &Anchor) -> Option<(NodeIdx, bool)> {
        let i = self.idx_of(a.id())?;
        match self.loc_of(i) {
            Loc::Run { .. } | Loc::Origin | Loc::MoveOp => {
                Some((i, matches!(a, Anchor::After(_))))
            }
            _ => None,
        }
    }

    /// Mark admissibility (the Mark gate rows): both anchors resolve to
    /// glue points, and the span is not inverted. Op-anchored points need a
    /// physical fragment to compare — materialized here even when the
    /// verdict is "gate": a zero-width splice slot for an already-applied
    /// move op is derived, convergence-neutral index state, not a trace of
    /// the gated mark.
    fn mark_admissible(&mut self, start: &Anchor, end: &Anchor) -> bool {
        let (Some(s), Some(e)) = (self.glue_point(start), self.glue_point(end)) else {
            return false;
        };
        for (n, _) in [s, e] {
            if let Loc::MoveOp = self.loc_of(n) {
                self.ensure_op_fragment(n);
            }
        }
        self.cmp_points(s, e) != std::cmp::Ordering::Greater
    }

    /// Sweep position of a move op's fragment point (fragment must exist —
    /// deciders own their destination fragment, anchored ops their splice
    /// slot).
    fn op_point_pos(&self, op: NodeIdx, tie: u8) -> SweepPos {
        let target = self.move_nodes[&op].target;
        if self.decider_of(target) == Some(op) && !self.is_removed(target) {
            let slot = self
                .index
                .moved_slot(self.elem_ref(target))
                .expect("deciders render their target");
            return (slot, 0, tie);
        }
        let slot = self.index.splice_slot(op).expect("anchored ops keep a slot");
        (slot, 0, tie)
    }

    /// Base-order comparison of two glue points. Same element: `Before`
    /// precedes `After`. Distinct elements: base element order decides.
    /// Origin points compare below every element point (imprecise only for
    /// content inserted Before(origin) — the sweep's activation guard keeps
    /// that corner inert).
    fn cmp_points(&self, a: (NodeIdx, bool), b: (NodeIdx, bool)) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        if a.0 == b.0 {
            return a.1.cmp(&b.1);
        }
        if a.0 == ORIGIN_IDX {
            return Ordering::Less;
        }
        if b.0 == ORIGIN_IDX {
            return Ordering::Greater;
        }
        let pa = self.point_pos(a.0, a.1).expect("non-origin point");
        let pb = self.point_pos(b.0, b.1).expect("non-origin point");
        self.index.cmp_sweep(pa, pb)
    }

    /// Apply a mark: O(1) bookkeeping (MARKS.md "Apply") — intern, attach
    /// the two anchor events, update the mark layer's tips. Suppression is
    /// computed at read, never at apply.
    #[allow(clippy::too_many_arguments)] // one parameter per op field
    fn apply_mark(
        &mut self,
        id: Id,
        pins: BTreeSet<Id>,
        start: Anchor,
        end: Anchor,
        kind: Id,
        value: Id,
        overwrites: BTreeSet<Id>,
    ) {
        let idx = self.intern(id, Loc::MarkOp);
        let (start_anchor, start_after) = self.glue_point(&start).expect("gated above");
        let (end_anchor, end_after) = self.glue_point(&end).expect("gated above");

        for r in overwrites.iter().chain(pins.iter()) {
            self.mark_tips.remove(r);
        }
        self.mark_tips.insert(id);

        self.mark_events.entry(start_anchor).or_default().push(MarkEvent {
            op: idx,
            start: true,
            after: start_after,
        });
        self.mark_events.entry(end_anchor).or_default().push(MarkEvent {
            op: idx,
            start: false,
            after: end_after,
        });

        let stored = StoredMark {
            start_after,
            start_anchor,
            end_after,
            end_anchor,
            kind,
            value,
            overwrites: SortedIdVec::from_id_set(&overwrites, |d| self.idx_of_known(d)),
            pins: SortedIdVec::from_id_set(&pins, |d| self.idx_of_known(d)),
        };
        self.mark_nodes.insert(idx, stored);
    }

    /// Reconstruct a mark op's `HashNode` (for merge / re-broadcast).
    pub fn mark_node(&self, mk: &StoredMark) -> HashNode {
        let anchor = |after: bool, n: NodeIdx| {
            let id = self.id_of(n);
            if after { Anchor::After(id) } else { Anchor::Before(id) }
        };
        HashNode {
            pins: mk.pins.to_id_set(&self.ids),
            op: Op::Mark {
                start: anchor(mk.start_after, mk.start_anchor),
                end: anchor(mk.end_after, mk.end_anchor),
                kind_v: mk.kind,
                value: mk.value,
                overwrites: mk.overwrites.to_id_set(&self.ids),
            },
        }
    }

    fn apply_place(
        &mut self,
        id: Id,
        pins: BTreeSet<Id>,
        placed_at: Id,
        overwrites: BTreeSet<Id>,
    ) {
        let idx = self.intern(id, Loc::PlaceOp);
        let stored = StoredPlace {
            placed_at,
            overwrites: SortedIdVec::from_id_set(&overwrites, |d| self.idx_of_known(d)),
            pins: SortedIdVec::from_id_set(&pins, |d| self.idx_of_known(d)),
        };
        self.place_nodes.insert(idx, stored);
        self.placement.apply(id, placed_at, overwrites);
    }

    /// Reconstruct a place op's `HashNode` (for merge / re-broadcast).
    pub fn place_node(&self, sp: &StoredPlace) -> HashNode {
        HashNode {
            pins: sp.pins.to_id_set(&self.ids),
            op: Op::Place {
                placed_at: sp.placed_at,
                overwrites: sp.overwrites.to_id_set(&self.ids),
            },
        }
    }

    /// The containment register — where does this object live
    /// (PLACEMENT_SPEC.md read surface).
    pub fn placement(&self) -> &PlacementRegister {
        &self.placement
    }

    /// Author a `Place` claiming `placed_at`, superseding the placement
    /// heads this replica sees. Returns the applied node (re-broadcast).
    pub fn place(&mut self, placed_at: Id) -> HashNode {
        let overwrites: BTreeSet<Id> =
            self.placement.heads().iter().copied().collect();
        let pins: BTreeSet<Id> =
            self.tips.difference(&overwrites).cloned().collect();
        let node = HashNode {
            pins,
            op: Op::Place {
                placed_at,
                overwrites,
            },
        };
        // Place is unconditionally admitted (the gate table), so this
        // cannot fail; `author` still keeps the record-after-apply order.
        self.author(node).expect("Place ops are always admitted")
    }

    // ---- mark reads (arbitration happens here, per Law II) ----

    /// Sweep position of a mark anchor point: fixed at the anchor's base
    /// slot (its ghost — points never relocate, so no move can reshape a
    /// span's region). `None` = an origin point (below every element).
    fn point_pos(&self, anchor: NodeIdx, after: bool) -> Option<SweepPos> {
        if anchor == ORIGIN_IDX {
            return None;
        }
        let tie = if after { 2 } else { 0 };
        if let Loc::MoveOp = self.loc_of(anchor) {
            return Some(self.op_point_pos(anchor, tie));
        }
        Some(self.index.base_pos(self.elem_ref(anchor), tie))
    }

    /// Does mark `mk` cover element `x`? Regional semantics: the points are
    /// fixed at their anchors' base slots, and membership is whether `x`'s
    /// *rendered* crossing falls strictly between them — a moved-out
    /// element sheds the region's marks, a moved-in element acquires them.
    fn mark_covers(&self, mk: &StoredMark, x: NodeIdx) -> bool {
        use std::cmp::Ordering;
        let xp = self.index.rendered_pos(self.elem_ref(x));
        let after_start = match self.point_pos(mk.start_anchor, mk.start_after) {
            None => true, // origin point: below everything
            Some(s) => self.index.cmp_sweep(s, xp) == Ordering::Less,
        };
        let before_end = match self.point_pos(mk.end_anchor, mk.end_after) {
            None => false, // an origin end point precedes every element
            Some(e) => self.index.cmp_sweep(xp, e) == Ordering::Less,
        };
        after_start && before_end
    }

    /// The live mark set at element `id`, grouped by kind: every covering
    /// mark not named in the `overwrites` of a same-kind mark that also
    /// covers `id` (range- and kind-scoped suppression, MARKS.md). MVR: the
    /// whole live set is exposed, unmark tombstones included; no LWW.
    pub fn marks_at(&self, id: &Id) -> MarkSet {
        let Some(x) = self.idx_of(id) else {
            return Vec::new();
        };
        // Only insert elements have a rendered position for marks to cover;
        // the origin and op nodes (remove/move/mark/place ids, all of which
        // `tips()` / `move_heads()` hand out) carry no marks.
        if !matches!(self.loc_of(x), Loc::Run { .. }) || self.mark_nodes.is_empty() {
            return Vec::new();
        }
        let covering: Vec<NodeIdx> = self
            .mark_nodes
            .iter()
            .filter(|(_, mk)| self.mark_covers(mk, x))
            .map(|(&i, _)| i)
            .collect();
        self.live_set(&covering)
    }

    /// Suppression among a set of covering marks: drop those named in the
    /// overwrites of a same-kind member. Returns (kind, [(mark id, value)])
    /// groups, both levels id-ordered.
    fn live_set(&self, covering: &[NodeIdx]) -> MarkSet {
        let mut by_kind: BTreeMap<Id, Vec<NodeIdx>> = BTreeMap::new();
        for &m in covering {
            by_kind.entry(self.mark_nodes[&m].kind).or_default().push(m);
        }
        by_kind
            .into_iter()
            .filter_map(|(kind, members)| {
                let mut live: Vec<(Id, Id)> = members
                    .iter()
                    .filter(|&&m| {
                        !members.iter().any(|&o| {
                            o != m && self.mark_nodes[&o].overwrites.iter().any(|w| w == m)
                        })
                    })
                    .map(|&m| (self.id_of(m), self.mark_nodes[&m].value))
                    .collect();
                live.sort();
                if live.is_empty() { None } else { Some((kind, live)) }
            })
            .collect()
    }

    /// The rendered document as coalesced marked spans: a base-order sweep
    /// toggles the active set at anchor events (activation-guarded — an end
    /// for a never-started mark is inert), then rendered-order emission
    /// coalesces equal formats. Unmark tombstone values suppress but do not
    /// display. O(text + anchor events).
    pub fn marked_spans(&self) -> Vec<(String, MarkSet)> {
        if self.mark_nodes.is_empty() {
            let text: String = self.iter().collect();
            return if text.is_empty() {
                Vec::new()
            } else {
                vec![(text, Vec::new())]
            };
        }

        // Base sweep: per-element live sets, recomputed only when the
        // active set changes.
        let mut active: FxHashSet<NodeIdx> = FxHashSet::default();
        let mut ended: FxHashSet<NodeIdx> = FxHashSet::default();
        let fire = |events: &Vec<MarkEvent>,
                        after: bool,
                        active: &mut FxHashSet<NodeIdx>,
                        ended: &mut FxHashSet<NodeIdx>|
         -> bool {
            let mut changed = false;
            for ev in events.iter().filter(|ev| ev.after == after) {
                if ev.start {
                    if !ended.contains(&ev.op) {
                        changed |= active.insert(ev.op);
                    }
                } else if !active.remove(&ev.op) {
                    ended.insert(ev.op);
                } else {
                    changed = true;
                }
            }
            changed
        };

        // Origin events fire up front (After(origin) = the document start;
        // any Before(origin) content predates every mark point — inert by
        // the activation guard).
        if let Some(events) = self.mark_events.get(&ORIGIN_IDX) {
            fire(events, false, &mut active, &mut ended);
            fire(events, true, &mut active, &mut ended);
        }

        let mut sets: Vec<MarkSet> = vec![Vec::new()];
        let mut current = 0usize; // index into `sets` for the current format
        let mut dirty = false;
        let mut out: Vec<(String, usize)> = Vec::new();
        for (head, start, len, kind) in self.index.sweep_coverage() {
            if kind == SweepFrag::Splice {
                // A zero-width op ghost: op-anchored events cross here.
                if let Some(events) = self.mark_events.get(&head) {
                    dirty |= fire(events, false, &mut active, &mut ended);
                    dirty |= fire(events, true, &mut active, &mut ended);
                }
                continue;
            }
            // One sequential pass over the fragment's text: `char_at` per
            // element would rescan the run from its start each time.
            let mut chars = self.runs[&head].text.chars().skip(start as usize);
            for off in start..start + len {
                let e = self.runs[&head].elements[off as usize];
                let ch = chars.next().expect("fragment lies within its run");
                // Element-anchored events fire at base slots (regional
                // points never move); op-anchored events bracket the
                // element at its rendered (destination) crossing.
                let op_events = if kind == SweepFrag::MovedIn && !self.mark_events.is_empty() {
                    self.decider_of(e)
                        .and_then(|op| self.mark_events.get(&op))
                } else {
                    None
                };
                if let Some(events) = op_events {
                    dirty |= fire(events, false, &mut active, &mut ended);
                }
                if kind == SweepFrag::Base
                    && let Some(events) = self.mark_events.get(&e)
                {
                    dirty |= fire(events, false, &mut active, &mut ended);
                }
                let renders_here = if kind == SweepFrag::MovedIn {
                    true // destination fragments hold live elements only
                } else {
                    !self.is_removed(e) && !self.rendered_elsewhere(e)
                };
                if renders_here {
                    if active.is_empty() {
                        current = 0;
                        dirty = false;
                    } else if dirty || current == 0 {
                        let members: Vec<NodeIdx> = active.iter().copied().collect();
                        let set = self.live_set(&members);
                        // Suppression can leave the live set unchanged across
                        // an event boundary — keep the index so spans coalesce.
                        if set != sets[current] {
                            current = if set.is_empty() {
                                0
                            } else {
                                sets.push(set);
                                sets.len() - 1
                            };
                        }
                        dirty = false;
                    }
                    match out.last_mut() {
                        Some((text, last)) if *last == current => text.push(ch),
                        _ => out.push((ch.to_string(), current)),
                    }
                }
                if kind == SweepFrag::Base
                    && let Some(events) = self.mark_events.get(&e)
                {
                    dirty |= fire(events, true, &mut active, &mut ended);
                }
                if let Some(events) = op_events {
                    dirty |= fire(events, true, &mut active, &mut ended);
                }
            }
        }
        out.into_iter()
            .map(|(text, f)| {
                let mut marks = sets[f].clone();
                // Tombstone values suppress but never display.
                for (_, live) in marks.iter_mut() {
                    live.retain(|(_, v)| *v != *crate::value::TOMBSTONE);
                }
                marks.retain(|(_, live)| !live.is_empty());
                (text, marks)
            })
            .collect()
    }

    // ---- mark authoring ----

    /// Author a mark of `kind`/`value` over `[start, end]`, superseding the
    /// same-kind marks intersecting the range that this replica sees.
    /// Anchor-side choice encodes edge-expansion behavior (MARKS.md).
    /// `Err` = the gate refused it (an inverted span, e.g. anchors on
    /// moved-in elements whose base slots cross, or an anchor that is not
    /// a glue point — an unknown id, a remove/mark/place op id): nothing
    /// applied, nothing parked, nothing queued for peers; the built node
    /// is handed back.
    pub fn mark_range(
        &mut self,
        start: Anchor,
        end: Anchor,
        kind: Id,
        value: Id,
    ) -> Result<HashNode, HashNode> {
        let (Some(s), Some(e)) = (self.glue_point(&start), self.glue_point(&end)) else {
            // Refused before authoring: `author` would park a node whose
            // anchor is unknown as an orphan of our own making.
            return Err(HashNode {
                pins: BTreeSet::new(),
                op: Op::Mark {
                    start,
                    end,
                    kind_v: kind,
                    value,
                    overwrites: BTreeSet::new(),
                },
            });
        };
        // Overwrites hygiene (open problem 1, simple form): name every
        // same-kind mark whose span intersects the new range.
        for (n, _) in [s, e] {
            if let Loc::MoveOp = self.loc_of(n) {
                self.ensure_op_fragment(n);
            }
        }
        let overwrites: BTreeSet<Id> = self
            .mark_nodes
            .iter()
            .filter(|(_, mk)| {
                use std::cmp::Ordering;
                mk.kind == kind
                    && self.cmp_points((mk.end_anchor, mk.end_after), s) == Ordering::Greater
                    && self.cmp_points(e, (mk.start_anchor, mk.start_after)) == Ordering::Greater
            })
            .map(|(&i, _)| self.id_of(i))
            .collect();
        let mut named: BTreeSet<Id> = overwrites.clone();
        named.insert(*start.id());
        named.insert(*end.id());
        let pins: BTreeSet<Id> = self.mark_tips.difference(&named).cloned().collect();
        let node = HashNode {
            pins,
            op: Op::Mark {
                start,
                end,
                kind_v: kind,
                value,
                overwrites,
            },
        };
        self.author(node)
    }

    /// Remove `kind` formatting over `[start, end]`: a mark whose value is
    /// the tombstone artifact (partial unmark is the same op over a
    /// sub-range — the overwritten mark keeps applying outside it).
    /// `Err` as for `mark_range`.
    pub fn unmark_range(&mut self, start: Anchor, end: Anchor, kind: Id) -> Result<HashNode, HashNode> {
        self.mark_range(start, end, kind, *crate::value::TOMBSTONE)
    }

    /// The mark layer's frontier (mark ops not superseded or pinned-over).
    pub fn mark_tips(&self) -> &BTreeSet<Id> {
        &self.mark_tips
    }

    fn apply_remove(&mut self, id: Id, extra_deps: BTreeSet<Id>, target_ids: BTreeSet<Id>) {
        // Targets are checked dependencies of the remove, so they are interned.
        // (A remove targeting a non-insert node is harmless: it's not in the
        // position index, and its tombstone bit is inert.)
        let targets: Vec<NodeIdx> = target_ids.iter().map(|t| self.idx_of_known(t)).collect();
        for t in &targets {
            // Removes targeting non-inserts have no index entry and are inert.
            // Base-rendered elements just clear their bit; a moved element's
            // destination fragment retires with its deciding op — demoting
            // to a splice ghost when content anchored at the op.
            if let Loc::Run { run, pos } = self.loc_of(*t)
                && !self.index.remove_element((run, pos))
                && let Some(op) = self.decider_of(*t)
            {
                if self.op_has_children(op) {
                    self.index.demote_to_splice((run, pos), op);
                } else if self.index.remove_moved((run, pos)) {
                    self.unregister_sibling(op);
                }
            }
            self.removed.set(t.0 as usize);
        }

        // Single-target removes coalesce into RemoveRuns, the delete analog of
        // sequential typing: if our only extra dep is the current tail of an
        // existing chain, extend that chain in place.
        if let [target] = targets[..] {
            if extra_deps.len() == 1 {
                let dep = extra_deps.first().unwrap();
                if let Some(dep) = self.idx_of(dep)
                    && let Loc::RemoveChain { chain, pos } = self.loc_of(dep)
                    && pos as usize + 1 == self.remove_runs[&chain].links.len()
                {
                    let idx = self.intern(
                        id,
                        Loc::RemoveChain {
                            chain,
                            pos: pos + 1,
                        },
                    );
                    let rr = self.remove_runs.get_mut(&chain).unwrap();
                    rr.targets.push(target);
                    rr.links.push(idx);
                    return;
                }
            }
            // Start a new chain (a lone remove is a 1-link chain).
            let idx = self.next_idx();
            self.intern(id, Loc::RemoveChain { chain: idx, pos: 0 });
            let first_extra_deps = SortedIdVec::from_id_set(&extra_deps, |d| self.idx_of_known(d));
            self.remove_runs.insert(
                idx,
                RemoveRun {
                    first_extra_deps,
                    targets: vec![target],
                    links: vec![idx],
                },
            );
            return;
        }

        let idx = self.intern(id, Loc::MultiRemove);
        let pins = SortedIdVec::from_id_set(&extra_deps, |d| self.idx_of_known(d));
        self.remove_nodes.insert(
            idx,
            CausalRemove {
                pins,
                nodes: targets.into(),
            },
        );
    }

    /// `anchor` is `before.anchor` resolved (a checked dependency, so interned).
    fn insert_before(&mut self, id: Id, anchor: NodeIdx, before: CausalInsert) {

        if let Loc::MoveOp = self.loc_of(anchor) {
            self.ensure_op_fragment(anchor);
        }

        let target = self.before_sibling_target(anchor, &id);

        // The anchor may sit mid-run: no split is needed. Iteration visits the
        // befores of every run element individually (see HashSeqIter), and unlike
        // an after-fork there is no sibling ordering to resolve — a Before-run
        // always lands immediately before its anchor.
        let idx = self.next_idx();
        self.intern(id, Loc::Run { run: idx, pos: 0 });
        let first_extra_deps = SortedIdVec::from_id_set(&before.pins, |d| self.idx_of_known(d));
        self.runs.insert(
            idx,
            StoredRun {
                anchor: before.anchor,
                first_op: FirstOp::Before,
                first_extra_deps,
                interior_extra_deps: BTreeMap::new(),
                text: before.ch.to_string(),
                elements: vec![idx],
            },
        );

        if let Some(v) = before.payload {
            self.elem_payloads.insert(idx, v);
        }

        self.befores_by_anchor
            .entry(anchor)
            .or_default()
            .insert(idx, &self.ids);

        let t = self.index_target(target, true);
        self.index.insert_span_at(t, idx);
    }

    pub fn apply(&mut self, node: HashNode) {
        let id = node.id();
        self.apply_with_id(id, node);
    }

    /// Apply a node with a pre-computed ID (avoids double hashing). The id
    /// must be the node's true hash — callers either just computed it
    /// (`apply`) or copied it from a locally-computed cache (`merge`, decode);
    /// the wire never supplies ids directly.
    ///
    /// Iterative worklist, no recursion: applying a node wakes exactly the
    /// orphans parked on its id (which may re-park on their next missing
    /// dep), so out-of-order delivery costs each node one park per missing
    /// dep instead of a global retry per apply.
    pub fn apply_with_id(&mut self, id: Id, node: HashNode) {
        debug_assert_eq!(id, node.id(), "apply_with_id called with a wrong id");
        if self.contains_node(&id) || self.delivery.holds(&id) {
            return;
        }
        // `queue` only allocates when an apply actually wakes parked orphans;
        // the common case (sequential typing, nothing parked) stays
        // allocation-free and dispatches `node` directly.
        let mut queue: Vec<(Id, HashNode)> = Vec::new();
        self.park_or_dispatch(id, node, &mut queue);
        while let Some((id, node)) = queue.pop() {
            self.park_or_dispatch(id, node, &mut queue);
        }
    }

    /// One step of the worklist: park `node` on its first missing dep, or
    /// interpret it and wake its waiters. A gated node wakes nothing — its
    /// dependents stay parked (the quarantine cascade).
    fn park_or_dispatch(&mut self, id: Id, node: HashNode, queue: &mut Vec<(Id, HashNode)>) {
        // An insert's anchor is resolved here once and handed down: the
        // dependency check, the admission gate and the run-extension fast
        // path all need the same handle, and on the typing hot path that
        // lookup was paid three times.
        let (insert_anchor, missing) = match &node.op {
            Op::Insert { at, .. } => match self.idx_of(at.id()) {
                None => (None, Some(*at.id())),
                Some(a) => (
                    Some(a),
                    node.pins.iter().find(|d| !self.contains_node(d)).copied(),
                ),
            },
            _ => (
                None,
                node.iter_refs().find(|d| !self.contains_node(d)).copied(),
            ),
        };
        if let Some(missing) = missing {
            self.delivery.park(missing, id, node);
            return;
        }
        self.delivery.unpark(&id);
        match self.interpret(id, node, insert_anchor) {
            Ok(()) => self.delivery.wake(&id, queue),
            Err(node) => self.delivery.gate(id, node),
        }
    }

    /// Interpret one node whose refs are all applied — this projection's
    /// edge-table rows. `Err` hands the node back for quarantine.
    // The Err carries the node back by value — same move the parameters
    // make; boxing would buy an allocation per gated op for nothing.
    #[allow(clippy::result_large_err)]
    /// `insert_anchor`: the resolved anchor handle when `node` is an
    /// Insert (see `park_or_dispatch`), `None` otherwise.
    fn interpret(
        &mut self,
        id: Id,
        node: HashNode,
        insert_anchor: Option<NodeIdx>,
    ) -> Result<(), HashNode> {
        // The apply-time gate: ops this projection does not admit are
        // quarantined before touching tips or the index. They never intern,
        // so dependents stay parked (the correct edge-table semantics).
        let admitted = match &node.op {
            // The Insert.at row: the anchor must be a glued point — an
            // element, a move op's splice point, or the origin. Anything
            // else (a remove op, a mark op) gates: there is no gap to claim
            // at a node that never renders. Payloads are never checked
            // (opaque commitments — HASHSEQ_SPEC "Payload").
            Op::Insert { .. } => insert_anchor.is_some_and(|a| {
                matches!(self.loc_of(a), Loc::Run { .. } | Loc::Origin | Loc::MoveOp)
            }),
            // Removes admit unconditionally: a target that is not an insert
            // is inert (its tombstone bit references nothing rendered),
            // never an error.
            Op::Remove(_) => true,
            // The Move rows of the edge table (all stable — every input is a
            // hash-committed fact about already-applied referents):
            // target must be an element of THIS seq; the destination must be
            // an element or the origin (a Move whose destination is another
            // move op's splice point is not yet admitted — insert anchors on
            // splice points ARE live); self-moves gate.
            Op::Move { target, to, .. } => {
                let t = self.idx_of(target);
                let target_ok =
                    t.is_some_and(|t| matches!(self.loc_of(t), Loc::Run { .. }));
                // Destination: an element, the origin, or another move op's
                // splice point — including an op that moves this same
                // target ("put x where that op placed it"): the excision of
                // the old rendering precedes placement, and a superseded
                // op's rank is permanent, so the case is well-defined, not
                // special.
                let anchor_ok = self.idx_of(to.id()).is_some_and(|a| {
                    matches!(self.loc_of(a), Loc::Run { .. } | Loc::Origin | Loc::MoveOp)
                });
                target_ok && anchor_ok && to.id() != target
            }
            // The Mark rows (MARKS.md "Validation", all stable): anchors
            // must resolve to glue points — elements, the origin, or move
            // ops (splice-point span endpoints) — and the span must not be
            // inverted: one comparison over permanent positions (base slots
            // and op ranks), so no later op can flip the verdict. Checked in
            // the Mark dispatch below (fragment materialization needs &mut).
            Op::Mark { .. } => true,
            // Place admits unconditionally (PLACEMENT_SPEC.md): placed_at
            // is a value commitment nothing can verify at apply time, and
            // every malformed relationship is inert at read time. No new
            // gate rows.
            Op::Place { .. } => true,
            _ => false,
        };
        if !admitted {
            return Err(node);
        }

        // Marks live in their own layer: they never enter the text tips
        // (downstream-only — content never references marks; LAYERING.md).
        if let Op::Mark { start, end, .. } = &node.op {
            let (start, end) = (*start, *end);
            if !self.mark_admissible(&start, &end) {
                return Err(node);
            }
            let Op::Mark {
                kind_v,
                value,
                overwrites,
                ..
            } = node.op
            else {
                unreachable!("matched above")
            };
            self.apply_mark(id, node.pins, start, end, kind_v, value, overwrites);
            return Ok(());
        }

        // Update tips before consuming node (insert ops don't depend on tips)
        for tip in node.iter_refs() {
            self.tips.remove(tip);
        }
        self.tips.insert(id);

        match node.op {
            Op::Insert { at, payload } => {
                let (ch, payload) = match payload.resolved() {
                    Payload::Char(c) => (c, None),
                    Payload::Id(v) => (ATOM_CHAR, Some(v)),
                };
                let ci = |anchor| CausalInsert {
                    pins: node.pins,
                    anchor,
                    ch,
                    payload,
                };
                let a = insert_anchor.expect("admitted above");
                match at {
                    Anchor::After(anchor) => self.insert_after(id, a, ci(anchor)),
                    Anchor::Before(anchor) => self.insert_before(id, a, ci(anchor)),
                }
            }
            Op::Remove(nodes) => self.apply_remove(id, node.pins, nodes),
            Op::Move {
                target,
                to,
                overwrites,
            } => self.apply_move(id, node.pins, target, to, overwrites),
            Op::Place {
                placed_at,
                overwrites,
            } => self.apply_place(id, node.pins, placed_at, overwrites),
            _ => unreachable!("gated above"),
        }
        Ok(())
    }


    /// Reconstruct a remove chain's `HashNode`s (for merge / re-broadcast).
    pub fn remove_run_nodes(&self, rr: &RemoveRun) -> Vec<HashNode> {
        rr.targets
            .iter()
            .enumerate()
            .map(|(i, target)| HashNode {
                pins: if i == 0 {
                    rr.first_extra_deps.to_id_set(&self.ids)
                } else {
                    BTreeSet::from_iter([self.id_of(rr.links[i - 1])])
                },
                op: Op::Remove(BTreeSet::from_iter([self.id_of(*target)])),
            })
            .collect()
    }

    /// Every applied node as `(id, HashNode)` — runs decompressed, remove
    /// chains reconstructed, moves and multi-removes included. Ids come from
    /// the local table (no rehashing). Parked orphans and gated nodes are
    /// NOT included; iterate `orphans()` / `gated` separately.
    pub fn all_nodes(&self) -> Vec<(Id, HashNode)> {
        let mut out: Vec<(Id, HashNode)> = Vec::new();
        for run in self.runs.values() {
            if self.is_atom(run.head()) {
                continue;
            }
            out.extend(run.to_run(&self.ids).decompress_with_ids());
        }
        for &e in self.elem_payloads.keys() {
            out.push((self.id_of(e), self.atom_node(e)));
        }
        for rr in self.remove_runs.values() {
            for (i, node) in self.remove_run_nodes(rr).into_iter().enumerate() {
                out.push((self.id_of(rr.links[i]), node));
            }
        }
        for (idx, causal_remove) in &self.remove_nodes {
            out.push((
                self.id_of(*idx),
                HashNode {
                    pins: causal_remove.pins.to_id_set(&self.ids),
                    op: Op::Remove(
                        causal_remove.nodes.iter().map(|i| self.id_of(*i)).collect(),
                    ),
                },
            ));
        }
        for (idx, mv) in &self.move_nodes {
            out.push((self.id_of(*idx), self.move_node(*idx, mv)));
        }
        for (idx, mk) in &self.mark_nodes {
            out.push((self.id_of(*idx), self.mark_node(mk)));
        }
        for (idx, sp) in &self.place_nodes {
            out.push((self.id_of(*idx), self.place_node(sp)));
        }
        out
    }

    pub fn merge(&mut self, other: Self) {
        // Simple merge: decompress all nodes from other and apply them
        // The apply function will rebuild runs when possible

        assert_eq!(
            self.origin, other.origin,
            "cannot merge documents with different origins"
        );

        // Every applied node, reconstructed by `all_nodes` (runs decompressed,
        // atoms/removes/moves/places/marks from their side tables) with ids
        // from `other`'s table — no rehashing on the merge path. Ops that
        // reference elements not yet applied are ordered by the orphan
        // machinery, so the emission order does not matter.
        for (id, node) in other.all_nodes() {
            self.apply_with_id(id, node);
        }

        // Apply parked orphans (ids were computed when they were parked)
        // and re-present the other side's quarantined nodes: applying
        // re-gates them here (deterministically), keeping merge lossless.
        for (id, node) in other.delivery.into_held() {
            self.apply_with_id(id, node);
        }
    }

    pub fn iter_ids(&self) -> impl Iterator<Item = &Id> {
        self.iter_idxs().map(|idx| self.id_ref(idx))
    }

    /// Document-order handles via the run index: an in-order fragment walk,
    /// O(visible + fragments) — tombstones skip in bulk and no per-element
    /// hashmap probes. The causal traversal (`iter_idxs_causal`) remains the
    /// semantic reference; `prop_index_matches_iterator` keeps them equal.
    pub(crate) fn iter_idxs(&self) -> impl Iterator<Item = NodeIdx> + '_ {
        self.index.frags_in_order().flat_map(move |fv| {
            let elements = &self.runs[&fv.head()].elements;
            (0..fv.len())
                .filter(move |k| fv.fully_visible() || fv.visible_at(*k))
                .map(move |k| elements[(fv.start() + k) as usize])
        })
    }

    /// Document-order handles by walking the causal structure (origin
    /// release, Id-ordered siblings, run chains). This is the *definition*
    /// of document order; production iteration rides the index instead, and
    /// the props hold the two equal (hence test-only callers).
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn iter_idxs_causal(&self) -> impl Iterator<Item = NodeIdx> + '_ {
        crate::hashseq_iter::HashSeqIdxIter::new(self)
    }

    /// The document text. Walks run text per fragment — linear in fragment
    /// text (no per-element `char_at`).
    pub fn iter(&self) -> impl Iterator<Item = char> + '_ {
        self.index.frags_in_order().flat_map(move |fv| {
            let text = &self.runs[&fv.head()].text;
            text.chars()
                .skip(fv.start() as usize)
                .take(fv.len() as usize)
                .enumerate()
                .filter(move |(k, _)| fv.fully_visible() || fv.visible_at(*k as u32))
                .map(|(_, ch)| ch)
        })
    }

    /// Return the node ID at visible position `idx`, if any.
    pub fn id_at(&self, idx: usize) -> Option<Id> {
        self.element_at(idx).map(|i| self.id_of(i))
    }

    /// The id to anchor on for the element rendered at visible position
    /// `pos` — what `cursor_at` uses for its neighbours: the element
    /// itself, or, for a moved-in element, its deciding move op. Anchors
    /// on an element resolve at that element's BASE slot (its ghost), so
    /// anything built from visible positions (moves, marks) must go
    /// through this to land where the user sees the element (MARKS.md
    /// "op-anchored endpoint brackets wherever the op's target renders").
    pub fn anchor_id_at(&self, pos: usize) -> Option<Id> {
        let e = self.element_at(pos)?;
        Some(self.id_of(self.render_anchor(e)))
    }

    /// Return the current visible position of `id`, if it is present and not removed.
    pub fn position_of(&self, id: &Id) -> Option<usize> {
        let idx = self.idx_of(id)?;
        let at = match self.loc_of(idx) {
            Loc::Run { run, pos } => (run, pos),
            _ => return None, // the origin and remove nodes have no position
        };
        self.index.position_of(at)
    }

    /// The current causal tips (heads of the causal DAG).
    pub fn tips(&self) -> &BTreeSet<Id> {
        &self.tips
    }

    /// Build a `Cursor` for inserting at position `idx`. This is the op-choice
    /// logic for all local inserts (`insert_batch` is built on it).
    ///
    /// Anchor choice follows the Fugue rule: if the left neighbor has no right
    /// child, the insert becomes a right child of left (`After(left)`);
    /// otherwise it becomes a left child of right (`Before(right)`), so the
    /// insert has an explicit ordering constraint and doesn't get hash-ordered
    /// into left's existing fork.
    /// At the start of a non-empty sequence, returns a `Before(id_at(0))` cursor.
    /// In an empty sequence, returns an `After(origin)` cursor. Returns `None`
    /// only when `idx` is out of bounds (> len).
    /// The causal node a rendered neighbor stands for when picking a cursor
    /// anchor: a moved-in element is represented by its deciding move op, so
    /// typing next to moved content anchors at the splice point and renders
    /// where the user sees it — never at the element's base ghost.
    fn render_anchor(&self, idx: NodeIdx) -> NodeIdx {
        if self.rendered_elsewhere(idx) {
            self.decider_of(idx)
                .expect("rendered_elsewhere implies a decider")
        } else {
            idx
        }
    }

    pub fn cursor_at(&self, idx: usize) -> Option<Cursor> {
        if idx > self.len() {
            return None;
        }
        let (left, right) = self.neighbours(idx);
        match (
            left.map(|l| self.render_anchor(l)),
            right.map(|r| self.render_anchor(r)),
        ) {
            (Some(left), Some(_)) => {
                // Fugue rule. The Before anchor is left's traversal successor
                // with tombstones included — not the visible right neighbor —
                // and `region_first` guarantees it has no before-children, so
                // the insert lands directly after left with no Id-ordered
                // sibling race.
                match self.afters_of(left).next() {
                    Some(child) => {
                        let anchor = self.id_of(self.region_first(child));
                        Some(Cursor::Before {
                            extra_deps: self.tips_minus(&anchor),
                            anchor,
                        })
                    }
                    None => {
                        let anchor = self.id_of(left);
                        Some(Cursor::After {
                            extra_deps: self.tips_minus(&anchor),
                            anchor,
                        })
                    }
                }
            }
            (Some(left), None) => {
                let anchor = self.id_of(left);
                Some(Cursor::After {
                    extra_deps: self.tips_minus(&anchor),
                    anchor,
                })
            }
            (None, Some(right)) => {
                let anchor = self.id_of(right);
                Some(Cursor::Before {
                    extra_deps: self.tips_minus(&anchor),
                    anchor,
                })
            }
            (None, None) => Some(Cursor::After {
                extra_deps: self.tips_minus(&self.origin),
                anchor: self.origin,
            }),
        }
    }

    /// Apply an `EncodableOp` to the sequence. `Run` ops are decompressed into their
    /// constituent `HashNode`s and applied one at a time.
    pub fn apply_op(&mut self, op: EncodableOp) {
        match op {
            EncodableOp::Node(node) => self.apply(node),
            EncodableOp::Run(run) => {
                for node in run.decompress() {
                    self.apply(node);
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use quickcheck_macros::quickcheck;

    /// `PackedLoc` must round-trip every `Loc`, including handles and positions
    /// near their bit-field boundaries (full u32 handle, 30-bit position).
    #[quickcheck]
    fn prop_packed_loc_roundtrips(handle: u32, pos: u32) -> bool {
        let pos = pos & ((1 << 29) - 1); // pos is a within-run offset (29 bits)
        let cases = [
            Loc::Run {
                run: NodeIdx(handle),
                pos,
            },
            Loc::RemoveChain {
                chain: NodeIdx(handle),
                pos,
            },
            Loc::Origin,
            Loc::MultiRemove,
            Loc::MoveOp,
        ];
        cases
            .into_iter()
            .all(|loc| PackedLoc::from(loc).unpack() == loc)
    }

    /// Drive a HashSeq with (insert?, idx, char) instructions, clamping idx into
    /// range — the op vocabulary the property tests below are expressed in.
    fn apply_ops(seq: &mut HashSeq, ops: &[(bool, u8, char)]) {
        for &(insert, idx, ch) in ops {
            let idx = idx as usize;
            if insert {
                seq.insert(idx.min(seq.len()), ch);
            } else if !seq.is_empty() {
                seq.remove(idx.min(seq.len() - 1));
            }
        }
    }

    fn seq_from_ops(ops: &[(bool, u8, char)]) -> HashSeq {
        let mut seq = HashSeq::default();
        apply_ops(&mut seq, ops);
        seq
    }

    /// Order-stability harness: after merging two editors' sequences, each
    /// editor's own ordering must survive as a subsequence of the merge. Removes
    /// performed on either side are re-applied to both before comparing, since a
    /// merged remove can hide elements the other side still shows.
    fn check_order_is_stable(a: &[(bool, u8, char)], b: &[(bool, u8, char)]) {
        fn apply_tracking_removed(
            seq: &mut HashSeq,
            ops: &[(bool, u8, char)],
            removed: &mut BTreeSet<Id>,
        ) {
            for &(insert, idx, ch) in ops {
                let idx = idx as usize;
                if insert {
                    seq.insert(idx.min(seq.len()), ch);
                } else if !seq.is_empty() {
                    let idx = idx.min(seq.len() - 1);
                    removed.insert(*seq.iter_ids().nth(idx).unwrap());
                    seq.remove(idx);
                }
            }
        }

        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();
        let mut removed = BTreeSet::new();
        apply_tracking_removed(&mut seq_a, a, &mut removed);
        apply_tracking_removed(&mut seq_b, b, &mut removed);

        let mut merged = seq_a.clone();
        merged.merge(seq_b.clone());

        for r in removed {
            let node = HashNode {
                op: Op::Remove(BTreeSet::from_iter([r])),
                pins: BTreeSet::new(),
            };
            seq_a.apply(node.clone());
            seq_b.apply(node);
        }

        let mut iter_a = seq_a.iter_ids();
        let mut iter_b = seq_b.iter_ids();
        let mut next_a = iter_a.next();
        let mut next_b = iter_b.next();
        for id in merged.iter_ids() {
            if Some(id) == next_a {
                next_a = iter_a.next();
            }
            if Some(id) == next_b {
                next_b = iter_b.next();
            }
        }
        assert_eq!(next_a, None, "seq_a's order not preserved in merge");
        assert_eq!(next_b, None, "seq_b's order not preserved in merge");
    }

    #[test]
    fn test_insert_at_end() {
        let mut seq = HashSeq::default();
        seq.insert(0, 'a');
        seq.insert(1, 'b');
        seq.insert(2, 'c');

        assert_eq!(seq.iter().collect::<String>(), "abc");
    }

    #[test]
    fn test_insert_after_before() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(0, 'b');
        seq.insert(1, 'c');

        assert_eq!(String::from_iter(seq.iter()), "bca");
    }

    #[test]
    fn test_insert_batch() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "abc".chars());
        assert_eq!(&seq.iter().collect::<String>(), "abc");
    }

    #[test]
    fn test_insert_batch_vs_single_inserts() {
        // Test that inserting one character at a time produces the same result
        // as using insert_batch

        let test_string = "hello world";

        // Insert one character at a time
        let mut seq_single = HashSeq::default();
        for (i, ch) in test_string.chars().enumerate() {
            seq_single.insert(i, ch);
        }

        // Insert as a batch
        let mut seq_batch = HashSeq::default();
        seq_batch.insert_batch(0, test_string.chars());

        // Verify they produce the same output
        let result_single: String = seq_single.iter().collect();
        let result_batch: String = seq_batch.iter().collect();

        assert_eq!(result_single, test_string);
        assert_eq!(result_batch, test_string);
        assert_eq!(result_single, result_batch);

        // Test inserting in the middle
        let mut seq_single_mid = HashSeq::default();
        seq_single_mid.insert(0, 'a');
        seq_single_mid.insert(1, 'z');
        seq_single_mid.insert(1, 'b');
        seq_single_mid.insert(2, 'c');
        seq_single_mid.insert(3, 'd');

        let mut seq_batch_mid = HashSeq::default();
        seq_batch_mid.insert(0, 'a');
        seq_batch_mid.insert(1, 'z');
        seq_batch_mid.insert_batch(1, "bcd".chars());

        assert_eq!(seq_single_mid.iter().collect::<String>(), "abcdz");
        assert_eq!(seq_batch_mid.iter().collect::<String>(), "abcdz");
    }

    #[test]
    fn test_split_batch_inserts() {
        // Test that insert_batch("abcd") produces the same internal structure as
        // insert_batch("ab") followed by insert_batch("cd")
        // This verifies that runs are collapsed identically

        // Insert entire string as one batch
        let mut seq_single_batch = HashSeq::default();
        seq_single_batch.insert_batch(0, "abcd".chars());

        // Insert as two separate batches
        let mut seq_split_batch = HashSeq::default();
        seq_split_batch.insert_batch(0, "ab".chars());
        seq_split_batch.insert_batch(2, "cd".chars());

        // Verify internal structure is identical
        assert_eq!(
            seq_single_batch.runs, seq_split_batch.runs,
            "Runs should be identical"
        );
        assert_eq!(
            seq_single_batch.tips, seq_split_batch.tips,
            "Tips should be identical"
        );

        // Verify output is also the same
        assert_eq!(seq_single_batch.iter().collect::<String>(), "abcd");
        assert_eq!(seq_split_batch.iter().collect::<String>(), "abcd");
    }

    #[test]
    fn test_batch_split_null_chars() {
        // Regression test for bug found by prop_batch_split_equivalence
        // Issue: inserting "\0\0\0" as single batch vs split ["\0", "\0\0"]
        // produced different first_extra_deps in the run
        let text = "\0\0\0";

        // seq1: insert entire string as one batch
        let mut seq1 = HashSeq::default();
        seq1.insert_batch(0, text.chars());

        // seq2: split into "\0" at position 0, then "\0\0" at position 1
        let mut seq2 = HashSeq::default();
        seq2.insert_batch(0, "\0".chars());
        seq2.insert_batch(1, "\0\0".chars());

        // Verify internal structures are identical
        assert_eq!(seq1.runs, seq2.runs, "Runs should be identical");
        assert_eq!(seq1.tips, seq2.tips, "Tips should be identical");
    }

    #[test]
    fn test_merge_batch_preserves_structure() {
        // Test that merging a HashSeq with "abcd" into an empty HashSeq
        // results in the same structure: root node 'a' + run "bcd"
        let mut seq_with_abcd = HashSeq::default();
        seq_with_abcd.insert_batch(0, "abcd".chars());

        let mut empty_seq = HashSeq::default();
        empty_seq.merge(seq_with_abcd.clone());

        // Verify internal structures are identical
        assert_eq!(
            seq_with_abcd.runs, empty_seq.runs,
            "Runs should be identical after merge"
        );
        assert_eq!(
            seq_with_abcd.tips, empty_seq.tips,
            "tips should be identical after merge"
        );

        // Verify the structure is as expected: one origin-anchored run
        // containing the whole batch.
        assert_eq!(seq_with_abcd.runs.len(), 1, "Should have 1 run");
        let run = seq_with_abcd.runs.values().next().unwrap();
        assert_eq!(run.text, "abcd", "Run should contain 'abcd'");

        // Verify the text is correct
        assert_eq!(seq_with_abcd.iter().collect::<String>(), "abcd");
        assert_eq!(empty_seq.iter().collect::<String>(), "abcd");
    }

    #[quickcheck]
    fn prop_batch_split_equivalence(text: String, split_points: Vec<usize>) -> bool {
        // Property: inserting a string as a single batch produces the same internal
        // structure as splitting it into multiple batches and inserting sequentially

        if text.is_empty() {
            return true;
        }

        // Convert text to character count for proper indexing
        let chars: Vec<char> = text.chars().collect();
        let char_len = chars.len();

        // Normalize split points to valid positions within character boundaries
        let mut splits: Vec<usize> = split_points
            .iter()
            .filter_map(|&p| {
                if char_len > 0 {
                    Some((p % char_len.max(1)).min(char_len))
                } else {
                    None
                }
            })
            .collect();

        // Sort and deduplicate
        splits.sort_unstable();
        splits.dedup();

        // Ensure boundaries are included
        if splits.is_empty() || splits[0] != 0 {
            splits.insert(0, 0);
        }
        if splits[splits.len() - 1] != char_len {
            splits.push(char_len);
        }

        // Remove consecutive duplicates that might have been created
        splits.dedup();

        // If we only have start and end (no actual splits), treat as single batch
        if splits.len() <= 2 {
            return true; // This is a trivial case
        }

        // Create seq1: insert entire string as one batch
        let mut seq1 = HashSeq::default();
        seq1.insert_batch(0, text.chars());

        // Create seq2: insert string split into batches sequentially
        let mut seq2 = HashSeq::default();
        let mut current_pos = 0;

        for i in 0..splits.len() - 1 {
            let start = splits[i];
            let end = splits[i + 1];

            if start < end {
                let substring: String = chars[start..end].iter().collect();
                seq2.insert_batch(current_pos, substring.chars());
                current_pos += end - start;
            }
        }

        // Verify internal structures are identical
        assert_eq!(seq1.runs, seq2.runs);
        assert_eq!(seq1.befores_by_anchor, seq2.befores_by_anchor);
        assert_eq!(seq1.remove_nodes, seq2.remove_nodes);
        assert_eq!(seq1.tips, seq2.tips);

        true
    }

    #[test]
    fn test_run_creation() {
        let mut seq = HashSeq::default();

        // A single character is a 1-char origin-anchored run
        seq.insert(0, 'x');
        assert_eq!(seq.runs.len(), 1);

        // A batch typed after it extends the same run
        seq.insert_batch(1, "abc".chars());
        assert_eq!(seq.runs.len(), 1);

        // Verify the run contains the right data
        let run = seq.runs.values().next().unwrap();
        assert_eq!(run.text, "xabc");

        // Verify the final string
        assert_eq!(&seq.iter().collect::<String>(), "xabc");
    }

    #[test]
    fn test_run_memory_efficiency() {
        let mut seq = HashSeq::default();

        // Create a long sequence using batch insert
        let long_string = "The quick brown fox jumps over the lazy dog. ".repeat(10);
        seq.insert_batch(0, long_string.chars());

        // Should create one run holding the entire string
        assert_eq!(seq.runs.len(), 1);

        let run = seq.runs.values().next().unwrap();
        assert_eq!(run.len(), long_string.len());

        // Verify content
        assert_eq!(seq.iter().collect::<String>(), long_string);
    }

    #[test]
    fn test_concurrent_inserts() {
        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        seq_a.insert_batch(0, "we wrote".chars());
        seq_b.insert_batch(0, "this together ".chars());

        let mut merged_ab = seq_a.clone();
        merged_ab.merge(seq_b.clone());
        let mut merged_ba = seq_b.clone();
        merged_ba.merge(seq_a);

        // Concurrent top-level runs land whole (no interleaving), in
        // id-determined order — the same on every replica.
        let text: String = merged_ab.iter().collect();
        assert!(
            text == "we wrotethis together " || text == "this together we wrote",
            "concurrent runs must not interleave: {text:?}"
        );
        assert_eq!(merged_ab, merged_ba);
        assert_eq!(text, merged_ba.iter().collect::<String>());
    }

    #[test]
    fn test_common_prefix_is_deduplicated() {
        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        seq_a.insert_batch(0, "hello my name is david".chars());
        seq_b.insert_batch(0, "hello my name is zameena".chars());

        seq_a.merge(seq_b);

        let merged = seq_a.iter().collect::<String>();
        assert_eq!(merged, "hello my name is zameenadavid");
    }

    #[test]
    fn test_common_prefix_is_deduplicated_simple() {
        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        seq_a.insert_batch(0, "aba".chars());
        assert_eq!(&seq_a.iter().collect::<String>(), "aba");

        seq_b.insert_batch(0, "aza".chars());
        assert_eq!(&seq_b.iter().collect::<String>(), "aza");

        seq_a.merge(seq_b);
        assert_eq!(&seq_a.iter().collect::<String>(), "azaba");
    }

    #[test]
    fn test_common_prefix_is_deduplicated_simple_2() {
        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        seq_a.insert_batch(0, "aaab".chars());
        seq_b.insert_batch(0, "aaac".chars());

        seq_a.merge(seq_b);

        let merged = seq_a.iter().collect::<String>();
        // 'b' and 'c' are concurrent siblings in one gap: their mutual order
        // is id-determined (either is a legal outcome; the concrete value
        // locks determinism under the current preimage grammar).
        assert_eq!(merged, "aaacb");
    }

    #[test]
    fn test_insert_different_chars_at_front() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(0, 'b');

        assert_eq!(&String::from_iter(seq.iter()), "ba");
    }

    #[test]
    fn test_insert_same_char_at_front() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(0, 'a');

        assert_eq!(&String::from_iter(seq.iter()), "aa");
    }

    #[test]
    fn test_insert_delete_then_reinsert() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.remove(0);
        seq.insert(0, 'a');

        assert_eq!(&String::from_iter(seq.iter()), "a");
    }

    #[test]
    fn test_add_twice_then_remove_both() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(0, 'a');
        seq.remove(0);
        seq.remove(0);

        assert_eq!(&String::from_iter(seq.iter()), "");
        assert_eq!(seq.len(), 0);
    }

    #[test]
    fn test_inserts_refering_to_out_of_order_inserts_are_cached() {
        let mut seq = HashSeq::default();

        let insert = HashNode {
            op: Op::insert_after(seq.origin(), 'b'),
            pins: BTreeSet::default(),
        };

        seq.apply(HashNode {
            op: Op::insert_after(insert.id(), 'a'),
            pins: BTreeSet::default(),
        });

        assert_eq!(seq.orphans().count(), 1);
        assert_eq!(seq.len(), 0);

        seq.apply(HashNode {
            op: Op::insert_before(insert.id(), 'a'),
            pins: BTreeSet::default(),
        });

        assert_eq!(seq.orphans().count(), 2);
        assert_eq!(seq.len(), 0);

        seq.apply(insert);

        assert_eq!(seq.orphans().count(), 0);
        assert_eq!(seq.len(), 3);

        assert_eq!(&String::from_iter(seq.iter()), "aba");
    }

    #[test]
    fn test_out_of_order_remove_is_cached() {
        let mut seq = HashSeq::default();

        // Attempting to remove insert that doesn't yet exist.
        // We expect the remove operation to be cached and applied
        // once we see the insert.

        let insert = HashNode {
            op: Op::insert_after(seq.origin(), 'a'),
            pins: BTreeSet::new(),
        };

        seq.apply(HashNode {
            op: Op::Remove(BTreeSet::from_iter([insert.id()])),
            pins: BTreeSet::new(),
        });

        assert_eq!(seq.orphans().count(), 1);
        seq.apply(insert);
        assert_eq!(seq.orphans().count(), 0);
        assert_eq!(&String::from_iter(seq.iter()), "");
    }

    /// merge(a, b) == merge(b, a) for one concrete pair of editors.
    fn check_merge_commutes(a: &[(bool, u8, char)], b: &[(bool, u8, char)]) {
        let seq_a = seq_from_ops(a);
        let seq_b = seq_from_ops(b);
        let mut ab = seq_a.clone();
        ab.merge(seq_b.clone());
        let mut ba = seq_b;
        ba.merge(seq_a);
        assert_eq!(ab, ba);
    }

    /// Minimized quickcheck failures, kept as concrete regressions.
    #[test]
    fn test_merge_commutes_regressions() {
        let i = |idx, ch| (true, idx, ch);
        let r = |idx| (false, idx, '\0');
        check_merge_commutes(&[i(0, 'a'), i(0, 'a')], &[i(0, 'b')]);
        check_merge_commutes(&[i(0, 'a'), r(0)], &[i(0, 'a'), i(0, 'b')]);
        check_merge_commutes(&[], &[i(0, '\0'), r(0)]);
        check_merge_commutes(&[i(0, '\0'), i(1, '\0')], &[]);
        check_merge_commutes(&[], &[i(0, '\0'), i(1, '\0'), i(1, '\0'), i(2, '\0')]);
        check_merge_commutes(&[], &[i(0, '\0'), i(1, '\0'), r(0)]);
    }

    /// merge(a, a) == a for one concrete editor.
    fn check_merge_reflexive(ops: &[(bool, u8, char)]) {
        let seq = seq_from_ops(ops);
        let mut merged = seq.clone();
        merged.merge(seq.clone());
        assert_eq!(merged, seq);
    }

    #[quickcheck]
    fn prop_reflexive(ops: Vec<(bool, u8, char)>) {
        check_merge_reflexive(&ops);
    }

    /// Minimized quickcheck failures, kept as concrete regressions.
    #[test]
    fn test_merge_reflexive_regressions() {
        let i = |idx, ch| (true, idx, ch);
        let r = |idx| (false, idx, '\0');
        check_merge_reflexive(&[i(0, '\0'), i(1, '\u{80}'), i(2, '\0'), r(0), i(1, '\0')]);
        check_merge_reflexive(&[i(0, 'a'), i(1, 'b'), r(0), i(1, 'c')]);
    }

    /// The position index must agree with the causal iterator — the causal
    /// traversal is the semantic definition of document order, and everything
    /// index-derived (id_at, position_of, and the production fragment-walk
    /// iterators) must match it. Checked on a merged seq (merging is what
    /// creates sibling forks) plus more local edits on top.
    fn check_index_matches_iter(seq: &HashSeq) {
        let iter_ids: Vec<Id> = seq.iter_idxs_causal().map(|i| seq.id_of(i)).collect();
        assert_eq!(seq.len(), iter_ids.len());
        let index_ids: Vec<Id> = seq.iter_ids().copied().collect();
        assert_eq!(
            index_ids, iter_ids,
            "fragment-walk iteration disagrees with the causal iterator"
        );
        let causal_text: String = seq.iter_idxs_causal().map(|i| seq.char_at(i)).collect();
        let index_text: String = seq.iter().collect();
        assert_eq!(index_text, causal_text);
        for (pos, id) in iter_ids.iter().enumerate() {
            assert_eq!(
                seq.id_at(pos),
                Some(*id),
                "id_at({pos}) disagrees with iterator"
            );
            assert_eq!(
                seq.position_of(id),
                Some(pos),
                "position_of disagrees with iterator"
            );
        }
        assert_eq!(seq.id_at(seq.len()), None);
    }

    #[quickcheck]
    fn prop_index_matches_iterator(
        a: Vec<(bool, u8, char)>,
        b: Vec<(bool, u8, char)>,
        after: Vec<(bool, u8, char)>,
    ) {
        let mut seq = seq_from_ops(&a);
        seq.merge(seq_from_ops(&b));
        check_index_matches_iter(&seq);
        apply_ops(&mut seq, &after);
        check_index_matches_iter(&seq);
    }

    /// Typing across a delete must extend the run, not start a new one: the
    /// first char after the remove carries `extra_deps = {remove_id}`, which
    /// is now stored as interior deps instead of forcing a fork.
    #[test]
    fn burst_after_delete_extends_run() {
        // Delete-ahead shape: type a burst, delete the char just after it,
        // resume typing. The resumed char anchors at the burst's tail with
        // extra_deps = {remove_id} — extendable since interior deps landed.
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "XY".chars());
        seq.insert_batch(0, "ab".chars()); // before-run "ab" at the front
        let runs_before = seq.runs.len();
        seq.remove(2); // delete 'X' (just after the burst)
        seq.insert_batch(2, "cd".chars()); // resume typing where we were
        assert_eq!(String::from_iter(seq.iter()), "abcdY");
        assert_eq!(
            seq.runs.len(),
            runs_before,
            "burst after a delete must extend its run, not fork a new one"
        );
        // the deps landed as interior deps on some run
        assert!(
            seq.runs.values().any(|r| !r.interior_extra_deps.is_empty()),
            "remove dep should be stored as interior extra-deps"
        );
        // and the encoding roundtrips identically
        let encoded = crate::encoding::encode_hashseq(&seq);
        let decoded = crate::encoding::decode_hashseq(&encoded).unwrap();
        assert_eq!(decoded, seq);
        assert_eq!(String::from_iter(decoded.iter()), "abcdY");
        assert_eq!(crate::encoding::encode_hashseq(&decoded), encoded);
    }

    /// Orphan buffering is keyed by missing dep with an iterative worklist:
    /// a long causal chain delivered in reverse must apply without recursion
    /// (the old retry-everything drain recursed once per chain link) and in
    /// roughly linear work (it retried every orphan on every apply).
    #[test]
    fn reverse_delivered_chain_applies_iteratively() {
        let n = 10_000;
        let mut nodes = Vec::with_capacity(n);
        let mut prev = Id::default();
        for _ in 0..n {
            let node = HashNode {
                pins: BTreeSet::new(),
                op: Op::insert_after(prev, 'x'),
            };
            prev = node.id();
            nodes.push(node);
        }

        let mut seq = HashSeq::default();
        for node in nodes.into_iter().rev() {
            seq.apply(node);
        }
        assert_eq!(seq.orphans().count(), 0);
        assert_eq!(seq.len(), n);
    }

    /// An orphan missing several deps re-parks on the next missing dep as
    /// they arrive — in either arrival order.
    #[test]
    fn orphan_reparks_until_all_deps_arrive() {
        let a = HashNode {
            pins: BTreeSet::new(),
            op: Op::insert_after(Id::default(), 'a'),
        };
        let b = HashNode {
            pins: BTreeSet::new(),
            op: Op::insert_after(Id::default(), 'b'),
        };
        // depends on both: anchored at a, extra dep on b
        let c = HashNode {
            pins: BTreeSet::from_iter([b.id()]),
            op: Op::insert_after(a.id(), 'c'),
        };

        for (first, second) in [(a.clone(), b.clone()), (b, a)] {
            let mut seq = HashSeq::default();
            seq.apply(c.clone());
            assert_eq!(seq.orphans().count(), 1);
            seq.apply(first);
            assert_eq!(seq.orphans().count(), 1, "still missing one dep");
            seq.apply(second);
            assert_eq!(seq.orphans().count(), 0);
            assert_eq!(seq.len(), 3);
            assert!(seq.iter().collect::<String>().contains('c'));
        }
    }

    /// Regression: a bigger-id sibling applied after a smaller visible sibling
    /// used to land at `find(anchor)+1` in the index, before the smaller
    /// sibling — while the iterator orders siblings ascending by id.
    #[test]
    fn index_orders_concurrent_siblings_like_the_iterator() {
        let root = HashNode {
            pins: BTreeSet::new(),
            op: Op::insert_after(Id::default(), 'a'),
        };
        let a = root.id();
        for (c1, c2) in [('b', 'c'), ('x', 'y')] {
            let n1 = HashNode {
                pins: BTreeSet::new(),
                op: Op::insert_after(a, c1),
            };
            let n2 = HashNode {
                pins: BTreeSet::new(),
                op: Op::insert_after(a, c2),
            };
            let (small, big) = if n1.id() < n2.id() {
                (n1, n2)
            } else {
                (n2, n1)
            };
            let mut seq = HashSeq::default();
            seq.apply(root.clone());
            seq.apply(small);
            seq.apply(big);
            check_index_matches_iter(&seq);
        }
    }

    #[quickcheck]
    fn prop_commutative(a: Vec<(bool, u8, char)>, b: Vec<(bool, u8, char)>) {
        let seq_a = seq_from_ops(&a);
        let seq_b = seq_from_ops(&b);

        // merge(a, b) == merge(b, a)

        let mut merge_a_b = seq_a.clone();
        merge_a_b.merge(seq_b.clone());

        let mut merge_b_a = seq_b.clone();
        merge_b_a.merge(seq_a.clone());

        assert_eq!(merge_a_b, merge_b_a);
    }

    #[quickcheck]
    fn prop_associative(
        a: Vec<(bool, u8, char)>,
        b: Vec<(bool, u8, char)>,
        c: Vec<(bool, u8, char)>,
    ) {
        let seq_a = seq_from_ops(&a);
        let seq_b = seq_from_ops(&b);
        let seq_c = seq_from_ops(&c);

        // merge(merge(a, b), c) == merge(a, merge(b, c))

        let mut ab_then_c = seq_a.clone();
        ab_then_c.merge(seq_b.clone());
        ab_then_c.merge(seq_c.clone());

        let mut bc_then_a = seq_b.clone();
        bc_then_a.merge(seq_c.clone());
        bc_then_a.merge(seq_a.clone());

        assert_eq!(ab_then_c, bc_then_a);

        // TODO: once insert returns an Op, check that we are op associative as well.
    }

    #[test]
    fn test_prop_vec_model_qc1() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'c');
        seq.insert(0, 'b');
        seq.insert(1, 'a');

        assert_eq!(String::from_iter(seq.iter()), "bac");
    }

    #[test]
    fn test_prop_vec_model_qc2() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(0, 'b');
        seq.insert(1, 'c');
        seq.insert(2, 'd');

        assert_eq!(String::from_iter(seq.iter()), "bcda");
    }

    #[test]
    fn test_prop_vec_model_qc3() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'c'); // "c"
        seq.insert(1, 'c'); // "cc"
        seq.insert(2, 'c'); // "ccc"
        seq.remove(1); // "cc"
        seq.insert(1, 'b'); // "cbc"

        assert_eq!(seq.iter().collect::<String>(), "cbc");
    }

    #[test]
    fn test_prop_vec_model_qc4() {
        let mut seq = HashSeq::default();

        for (idx, elem) in [(0, 'a'), (1, 'a'), (2, 'a'), (3, 'a'), (3, 'a'), (3, 'd')] {
            seq.insert(idx, elem);
        }

        assert_eq!(seq.iter().collect::<String>(), "aaadaa");
    }

    #[test]
    fn test_prop_vec_model_qc5() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(0, 'a');
        seq.remove(0);
        seq.insert(1, 'b');

        assert_eq!(String::from_iter(seq.iter()), "ab");
    }

    #[test]
    fn test_prop_vec_model_qc6() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(1, 'b');
        seq.insert(0, 'c');
        seq.remove(2);

        assert_eq!(String::from_iter(seq.iter()), "ca");
    }

    #[test]
    fn test_prop_vec_model_qc7() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(0, 'b');
        seq.remove(1);

        assert_eq!(String::from_iter(seq.iter()), "b");
    }

    #[test]
    fn test_prop_vec_model_qc8() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(0, 'b');
        seq.insert(1, 'c');
        seq.remove(0);
        seq.insert(2, 'd');

        assert_eq!(String::from_iter(seq.iter()), "cad");
    }

    #[test]
    fn test_prop_vec_model_qc9() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(1, 'a');
        seq.insert(1, 'b');
        seq.insert(1, 'a');
        seq.remove(2);

        assert_eq!(String::from_iter(seq.iter()), "aaa");
    }

    #[test]
    fn test_prop_vec_model_qc10() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(0, 'b');
        seq.insert(1, 'c');
        seq.remove(2);

        assert_eq!(String::from_iter(seq.iter()), "bc");
    }

    #[test]
    fn test_prop_vec_model_qc11() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(0, 'b');
        seq.insert(0, 'c');
        seq.insert(0, 'd');
        seq.remove(3);

        assert_eq!(String::from_iter(seq.iter()), "dcb");
    }

    #[test]
    fn test_prop_vec_model_qc12() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(0, 'a');
        seq.remove(0);
        seq.remove(0);
        seq.insert(0, 'a');
        seq.remove(0);

        assert_eq!(String::from_iter(seq.iter()), "");
    }

    #[test]
    fn test_prop_vec_model_qc13() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(1, 'a');
        seq.insert(1, 'a');
        seq.insert(1, 'b');

        assert_eq!(String::from_iter(seq.iter()), "abaa");
    }

    #[test]
    fn test_prop_vec_model_qc14_missing_char() {
        let mut seq = HashSeq::default();

        // Regression test for bug where multi-byte UTF-8 characters were not handled correctly
        // in runs. The bug was that Run::len() returned byte length instead of character count,
        // causing position calculation errors for characters like '\u{80}' (2 bytes in UTF-8).
        seq.insert(0, '\0');
        seq.insert(1, '\0');
        seq.insert(2, '\0');
        seq.insert(3, '\u{80}');

        let result: Vec<char> = seq.iter().collect();
        assert_eq!(result, vec!['\0', '\0', '\0', '\u{80}']);
    }

    #[test]
    fn test_insert_remove_and_reinsert() {
        let mut seq = HashSeq::default();
        seq.insert(0, 'b');
        seq.remove(0);
        seq.insert(0, 'b');
        assert_eq!(String::from_iter(seq.iter()), "b");
    }

    #[test]
    fn test_removing_an_element_twice() {
        let mut seq = HashSeq::default();
        seq.insert(0, 'a');
        seq.insert(0, 'b');
        let removed = seq.iter_ids().nth(1).copied().unwrap();
        seq.remove(1);

        seq.apply(HashNode {
            op: Op::Remove(BTreeSet::from_iter([removed])),
            pins: BTreeSet::new(),
        });

        assert_eq!(String::from_iter(seq.iter()), "b");
    }

    #[quickcheck]
    fn prop_vec_model(instructions: Vec<(bool, u8, char)>) {
        let mut model = Vec::new();
        let mut seq = HashSeq::default();

        for (insert_or_remove, idx, elem) in instructions {
            let idx = idx as usize;
            match insert_or_remove {
                true => {
                    // insert
                    model.insert(idx.min(model.len()), elem);
                    seq.insert(idx.min(seq.len()), elem);
                }
                false => {
                    // remove
                    assert_eq!(seq.is_empty(), model.is_empty());
                    if !seq.is_empty() {
                        model.remove(idx.min(model.len() - 1));
                        seq.remove(idx.min(seq.len() - 1));
                    }
                }
            }
        }

        assert_eq!(seq.iter().collect::<Vec<_>>(), model);
        assert_eq!(seq.len(), model.len());
        assert_eq!(seq.is_empty(), model.is_empty());
    }

    #[quickcheck]
    fn prop_order_is_stable(a: Vec<(bool, u8, char)>, b: Vec<(bool, u8, char)>) {
        check_order_is_stable(&a, &b);
    }

    #[test]
    fn test_order_is_stable_minimal() {
        // Failing case from quickcheck
        check_order_is_stable(&[], &[(true, 0, '\0'), (true, 0, '\0'), (true, 2, '\0')]);
    }

    #[test]
    fn test_order_is_stable_4_inserts() {
        // Failing case from quickcheck
        check_order_is_stable(
            &[],
            &[
                (true, 0, '\0'),
                (true, 1, '\0'),
                (true, 1, '\0'),
                (true, 2, '\0'),
            ],
        );
    }

    #[test]
    fn test_order_is_stable_remove_then_insert() {
        // Failing case from quickcheck
        check_order_is_stable(
            &[],
            &[
                (true, 0, '\0'),
                (true, 1, '\0'),
                (true, 2, '\0'),
                (false, 2, '\0'),
                (true, 2, '\u{97}'),
            ],
        );
    }

    #[test]
    fn test_order_is_stable_with_removes() {
        // Failing case from quickcheck
        check_order_is_stable(
            &[],
            &[
                (true, 0, '\0'),
                (true, 1, '\0'),
                (true, 1, '\0'),
                (false, 0, '\0'),
                (false, 1, '\0'),
            ],
        );
    }

    #[test]
    fn test_prop_commutative_failing_case() {
        // Failing case from quickcheck: ([(true, 0, '\0'), (true, 0, '\0'), (false, 1, '\0')], [(true, 0, '@')])
        // Seq A: insert at 0, insert at 0, remove at 1
        // Seq B: insert at 0 ('@')

        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        // Build seq_a: insert at 0, insert at 0, remove at 1
        seq_a.insert(0, '\0');
        seq_a.insert(0, '\0');
        seq_a.remove(1);

        // Build seq_b: insert at 0 ('@')
        seq_b.insert(0, '@');

        // Test commutativity
        let mut merge_a_b = seq_a.clone();
        merge_a_b.merge(seq_b.clone());

        let mut merge_b_a = seq_b.clone();
        merge_b_a.merge(seq_a.clone());

        assert_eq!(merge_a_b, merge_b_a);
    }

    #[test]
    fn test_prop_commutative_run_vs_individual() {
        // Failing case: ([], [(true, 0, '\0'), (true, 0, '\0'), (true, 1, '\0'), (true, 2, '\0')])
        // Seq A: empty
        // Seq B: insert at 0, insert at 0, insert at 1, insert at 2

        let seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        // seq_a is empty

        // Build seq_b with 4 inserts
        seq_b.insert(0, '\0');
        seq_b.insert(0, '\0');
        seq_b.insert(1, '\0');
        seq_b.insert(2, '\0');

        // Test commutativity
        let mut merge_a_b = seq_a.clone();
        merge_a_b.merge(seq_b.clone());

        let mut merge_b_a = seq_b.clone();
        merge_b_a.merge(seq_a.clone());

        // Compare content and IDs
        let merge_a_b_content: Vec<char> = merge_a_b.iter().collect();
        let merge_b_a_content: Vec<char> = merge_b_a.iter().collect();
        let merge_a_b_ids: Vec<&Id> = merge_a_b.iter_ids().collect();
        let merge_b_a_ids: Vec<&Id> = merge_b_a.iter_ids().collect();
        assert_eq!(merge_a_b_content, merge_b_a_content);
        assert_eq!(merge_a_b_ids, merge_b_a_ids);
        assert_eq!(merge_a_b, merge_b_a);
    }

    #[test]
    fn test_prop_commutative_remove_with_run() {
        // Failing case: ([(true, 0, '\0'), (true, 0, '\0'), (false, 1, '\0'), (true, 1, '\0'), (true, 2, '\0')], [])
        // Seq A: insert at 0, insert at 0, remove at 1, insert at 1, insert at 2
        // Seq B: empty

        let mut seq_a = HashSeq::default();
        let seq_b = HashSeq::default();

        // Build seq_a with the operations
        seq_a.insert(0, '\0'); // Insert at 0
        seq_a.insert(0, '\0'); // Insert at 0
        seq_a.remove(1); // Remove at 1
        seq_a.insert(1, '\0'); // Insert at 1
        seq_a.insert(2, '\0'); // Insert at 2

        // Test commutativity
        let mut merge_a_b = seq_a.clone();
        merge_a_b.merge(seq_b.clone());

        let mut merge_b_a = seq_b.clone();
        merge_b_a.merge(seq_a.clone());

        assert_eq!(merge_a_b, merge_b_a);
    }

    // Tests for runs (spans have been removed and runs are now the source of truth)
    #[test]
    fn test_runs_basic() {
        let mut seq = HashSeq::default();
        seq.insert(0, 'a'); // This starts an origin-anchored run
        seq.insert(1, 'b'); // This extends the run
        seq.insert(2, 'c'); // This extends the run

        // All three characters land in a single run
        assert_eq!(seq.runs.len(), 1);
        let run = seq.runs.values().next().unwrap();
        assert_eq!(run.len(), 3);
        assert_eq!(run.text, "abc");
        assert_eq!(String::from_iter(seq.iter()), "abc");
    }

    #[test]
    fn test_runs_with_fork() {
        let mut seq = HashSeq::default();
        seq.insert(0, 'a'); // a (root)
        seq.insert(0, 'b'); // ba (insert before 'a')

        // 'b' is an InsertBefore, which creates a before_node
        assert_eq!(String::from_iter(seq.iter()), "ba");
    }

    #[test]
    fn test_id_at_and_position_of_roundtrip() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "hello".chars());

        for i in 0..seq.len() {
            let id = seq.id_at(i).expect("id_at within bounds");
            assert_eq!(seq.position_of(&id), Some(i));
        }
        assert_eq!(seq.id_at(seq.len()), None);
    }

    #[test]
    fn test_position_of_returns_none_for_removed_node() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "abc".chars());
        let id_b = seq.id_at(1).unwrap();
        seq.remove(1);
        assert_eq!(seq.position_of(&id_b), None);
        assert_eq!(String::from_iter(seq.iter()), "ac");
    }

    #[test]
    fn test_cursor_at_edges() {
        let seq = HashSeq::default();
        assert!(
            matches!(seq.cursor_at(0), Some(Cursor::After { anchor, .. }) if anchor == seq.origin()),
            "empty seq at 0 yields an After(origin) cursor"
        );

        let mut seq = HashSeq::default();
        seq.insert_batch(0, "hi".chars());

        // idx 0 in a non-empty seq → Before(id_at(0)) so leading inserts get an
        // explicit ordering constraint relative to the first visible char.
        match seq.cursor_at(0).expect("cursor at idx 0") {
            Cursor::Before { anchor, extra_deps } => {
                assert_eq!(Some(anchor), seq.id_at(0));
                assert!(!extra_deps.contains(&anchor));
            }
            other => panic!("expected Before cursor at idx 0, got {other:?}"),
        }

        // idx 1 sits between 'h' and 'i'; 'h' already has a right child (the
        // run chain), so the Fugue rule picks Before(right) and the insert
        // lands deterministically between them.
        match seq.cursor_at(1).expect("cursor at idx 1") {
            Cursor::Before { anchor, extra_deps } => {
                assert_eq!(Some(anchor), seq.id_at(1));
                assert!(!extra_deps.contains(&anchor));
            }
            other => panic!(
                "expected Before cursor at idx 1 (left neighbor has a right child), got {other:?}"
            ),
        }

        // idx == len: no right neighbor → After(last).
        match seq.cursor_at(seq.len()).expect("cursor at end") {
            Cursor::After { anchor, extra_deps } => {
                assert_eq!(Some(anchor), seq.id_at(seq.len() - 1));
                assert!(!extra_deps.contains(&anchor));
            }
            other => panic!("expected After cursor at end, got {other:?}"),
        }

        assert!(seq.cursor_at(seq.len() + 1).is_none(), "out of bounds");
    }

    #[test]
    fn test_apply_op_matches_apply_for_node() {
        let mut seq_a = HashSeq::default();
        seq_a.insert(0, 'x');
        let mut seq_b = seq_a.clone();

        let node = HashNode {
            pins: BTreeSet::new(),
            op: Op::insert_after(seq_a.id_at(0).unwrap(), 'y'),
        };

        seq_a.apply(node.clone());
        seq_b.apply_op(EncodableOp::Node(node));

        assert_eq!(
            String::from_iter(seq_a.iter()),
            String::from_iter(seq_b.iter()),
        );
        assert_eq!(String::from_iter(seq_a.iter()), "xy");
    }

    #[test]
    fn test_apply_op_run_matches_decompress_and_apply() {
        let mut seq_a = HashSeq::default();
        seq_a.insert(0, 'x');
        let mut seq_b = seq_a.clone();

        let anchor = seq_a.id_at(0).unwrap();
        let mut run = Run::new(anchor, BTreeSet::new(), 'a');
        run.extend('b');
        run.extend('c');

        for node in run.decompress() {
            seq_a.apply(node);
        }
        seq_b.apply_op(EncodableOp::Run(run));

        assert_eq!(
            String::from_iter(seq_a.iter()),
            String::from_iter(seq_b.iter()),
        );
        assert_eq!(String::from_iter(seq_a.iter()), "xabc");
    }

    /// The key behavioral guarantee for the cursor model: a Run built from a Cursor
    /// can be applied later, after concurrent mutation of the HashSeq, and still lands
    /// at the cursor's anchor.
    #[test]
    fn test_run_from_cursor_survives_concurrent_mutation() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "hello world".chars());

        // Place cursor between "hello" and " world". 'o' already has a right
        // child (the run chain), so cursor_at picks Before(' ').
        let cursor = seq.cursor_at(5).expect("cursor after 'hello'");
        let space_id = seq.id_at(5).unwrap();
        assert!(
            matches!(&cursor, Cursor::Before { anchor, .. } if *anchor == space_id),
            "expected Before cursor anchored at ' ', got {cursor:?}",
        );

        // Concurrent mutation: another edit lands at the start of the buffer.
        seq.insert_batch(0, "X ".chars());
        assert_eq!(String::from_iter(seq.iter()), "X hello world");

        // Build a Before-run from the cursor — anchored to the original ' '.
        let mut run = cursor.into_run(',');
        run.extend('!');

        seq.apply_op(EncodableOp::Run(run));

        // The run lands immediately before the ' ' it was anchored to. The "X "
        // prepend doesn't disturb the relative ordering.
        assert_eq!(String::from_iter(seq.iter()), "X hello,! world");
    }

    /// Regression: inserting mid-run via the cursor model must use
    /// InsertBefore — not InsertAfter — to avoid hash-determined fork
    /// ordering (the left neighbor's right child is the run continuation).
    /// Without this, A's run could end up after the existing run
    /// continuation instead of before it.
    #[test]
    fn test_cursor_between_run_chain_uses_insert_before() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "hello world".chars());

        let cursor = seq.cursor_at(5).unwrap();
        assert!(matches!(cursor, Cursor::Before { .. }));

        let mut run = cursor.into_run(' ');
        for ch in "mighty".chars() {
            run.extend(ch);
        }
        seq.apply_op(EncodableOp::Run(run));

        // The crucial assertion: the new run lands between "hello" and " world",
        // deterministically — not after " world" via hash ordering.
        assert_eq!(String::from_iter(seq.iter()), "hello mighty world");
    }
    // ---- placement registers (the Move projection) ----

    #[test]
    fn move_single_head_places_at_destination() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "abc".chars());
        let a = seq.id_at(0).unwrap();
        let c = seq.id_at(2).unwrap();

        seq.move_element(a, Anchor::After(c)).unwrap();
        assert_eq!(seq.placement_of(&a), Some(Anchor::After(c)));
        assert!(!seq.placement_conflicted(&a));
        // the rendered order relocates `a` to the glued point after `c`
        assert_eq!(seq.iter().collect::<String>(), "bca");
        assert_eq!(seq.position_of(&a), Some(2));
        assert_eq!(seq.id_at(2), Some(a));
    }

    #[test]
    fn sequential_moves_supersede() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "abc".chars());
        let a = seq.id_at(0).unwrap();
        let b = seq.id_at(1).unwrap();
        let c = seq.id_at(2).unwrap();

        seq.move_element(a, Anchor::After(b)).unwrap();
        seq.move_element(a, Anchor::After(c)).unwrap();
        assert_eq!(seq.placement_of(&a), Some(Anchor::After(c)));
        assert_eq!(seq.move_heads(&a).len(), 1);
    }

    #[test]
    fn concurrent_moves_freeze_to_last_agreed() {
        let mut base = HashSeq::default();
        base.insert_batch(0, "abc".chars());
        let a = base.id_at(0).unwrap();
        let b = base.id_at(1).unwrap();
        let c = base.id_at(2).unwrap();

        // Both replicas agree on an initial placement, then race.
        base.move_element(a, Anchor::After(b)).unwrap();
        let agreed = Anchor::After(b);

        let mut r1 = base.clone();
        let mut r2 = base.clone();
        r1.move_element(a, Anchor::After(c)).unwrap();
        r2.move_element(a, Anchor::Before(b)).unwrap();

        let mut merged = r1.clone();
        merged.merge(r2.clone());
        // Contested: two heads, surfaced; placement freezes at the last
        // agreed value — never a max-id winner.
        assert!(merged.placement_conflicted(&a));
        assert_eq!(merged.placement_of(&a), Some(agreed));

        // Symmetric merge agrees (commutativity of the read).
        let mut merged2 = r2.clone();
        merged2.merge(r1.clone());
        assert_eq!(merged2.placement_of(&a), merged.placement_of(&a));
        assert_eq!(
            merged2.move_heads(&a),
            merged.move_heads(&a),
            "head sets converge"
        );

        // The next move naming both heads dominates and resolves.
        merged.move_element(a, Anchor::After(c)).unwrap();
        assert!(!merged.placement_conflicted(&a));
        assert_eq!(merged.placement_of(&a), Some(Anchor::After(c)));
    }

    #[test]
    fn concurrent_moves_with_no_agreement_freeze_to_creation() {
        let mut base = HashSeq::default();
        base.insert_batch(0, "abc".chars());
        let a = base.id_at(0).unwrap();
        let b = base.id_at(1).unwrap();
        let c = base.id_at(2).unwrap();

        let mut r1 = base.clone();
        let mut r2 = base.clone();
        r1.move_element(a, Anchor::After(c)).unwrap();
        r2.move_element(a, Anchor::Before(b)).unwrap();

        let mut merged = r1;
        merged.merge(r2);
        assert!(merged.placement_conflicted(&a));
        // No common overwritten ancestor: bottoms out at the creation
        // placement (None = render at the base slot).
        assert_eq!(merged.placement_of(&a), None);
    }

    #[test]
    fn remove_beats_move() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "abc".chars());
        let a = seq.id_at(0).unwrap();
        let c = seq.id_at(2).unwrap();

        seq.move_element(a, Anchor::After(c)).unwrap();
        let rendered = seq.position_of(&a).unwrap();
        assert_eq!(rendered, 2, "moved to the end");
        seq.remove(rendered); // tombstone a at its rendered position
        assert_eq!(seq.placement_of(&a), None, "register is moot once dead");
        assert_eq!(seq.iter().collect::<String>(), "bc");
        assert_eq!(seq.position_of(&a), None);
    }

    #[test]
    fn move_before_renders_at_glued_point() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "abc".chars());
        let a = seq.id_at(0).unwrap();
        let c = seq.id_at(2).unwrap();

        seq.move_element(c, Anchor::Before(a)).unwrap();
        assert_eq!(seq.iter().collect::<String>(), "cab");
        assert_eq!(seq.position_of(&c), Some(0));
        check_index_matches_iter(&seq);
    }

    #[test]
    fn move_to_origin_joins_top_level_siblings() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "abc".chars());
        let origin = seq.origin();
        let a = seq.id_at(0).unwrap();
        let b = seq.id_at(1).unwrap();
        let c = seq.id_at(2).unwrap();

        // After(origin): the move op joins the top-level sibling set,
        // ordered against the run head by id — like any insert would be.
        let m = seq.move_element(c, Anchor::After(origin)).unwrap();
        let expect = if m.id() < a { "cab" } else { "abc" };
        assert_eq!(seq.iter().collect::<String>(), expect);
        check_index_matches_iter(&seq);

        // Before(origin): a before-child of the origin — releases before
        // everything (the origin's befores precede all top-level content).
        seq.move_element(b, Anchor::Before(origin)).unwrap();
        let text: String = seq.iter().collect();
        assert_eq!(text.len(), 3);
        assert!(text.starts_with('b'), "Before(origin) renders first: {text}");
        check_index_matches_iter(&seq);
    }

    #[test]
    fn co_glued_moves_order_by_move_id() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "abcd".chars());
        let a = seq.id_at(0).unwrap();
        let b = seq.id_at(1).unwrap();
        let d = seq.id_at(3).unwrap();

        // Two elements glued at the same point: order among them is the
        // move ops' id order, whatever the application order was.
        let m1 = seq.move_element(a, Anchor::After(d)).unwrap();
        let m2 = seq.move_element(b, Anchor::After(d)).unwrap();
        let expect = if m1.id() < m2.id() { "cdab" } else { "cdba" };
        assert_eq!(seq.iter().collect::<String>(), expect);
        check_index_matches_iter(&seq);
    }

    /// A rendered move-in is an ordinary sibling: later inserts at the same
    /// anchor interleave with it by id, exactly as concurrent inserts do
    /// among themselves.
    #[test]
    fn later_inserts_interleave_with_moved_ins_by_id() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "abc".chars());
        let a = seq.id_at(0).unwrap();
        let b = seq.id_at(1).unwrap();
        let c = seq.id_at(2).unwrap();

        // Base [a b c]; a moves After(b) — the fork materializes, so b's
        // siblings are the c-continuation and the move op, id-ordered.
        let m = seq.move_element(a, Anchor::After(b)).unwrap();
        let mut sibs = [(m.id(), 'a'), (c, 'c')];
        sibs.sort();
        let expect: String = std::iter::once('b').chain(sibs.iter().map(|s| s.1)).collect();
        assert_eq!(seq.iter().collect::<String>(), expect);

        // A later insert anchored After(b) joins the same sibling order.
        let x = HashNode {
            pins: BTreeSet::new(),
            op: Op::insert_after(b, 'x'),
        };
        let mut sibs = [(m.id(), 'a'), (c, 'c'), (x.id(), 'x')];
        sibs.sort();
        let expect: String = std::iter::once('b').chain(sibs.iter().map(|s| s.1)).collect();
        seq.apply(x);
        assert_eq!(seq.iter().collect::<String>(), expect);
        check_index_matches_iter(&seq);
    }

    /// A move-in at a run tail makes the anchor an explicit fork point:
    /// continued typing forks (no silent extension through the sibling) and
    /// orders against the move op by id.
    #[test]
    fn typing_at_a_moved_in_tail_forks_and_orders_by_id() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "ab".chars());
        let a = seq.id_at(0).unwrap();
        let b = seq.id_at(1).unwrap();

        let m = seq.move_element(a, Anchor::After(b)).unwrap();
        assert_eq!(seq.iter().collect::<String>(), "ba");

        let cnode = HashNode {
            pins: BTreeSet::new(),
            op: Op::insert_after(b, 'c'),
        };
        let expect = if m.id() < cnode.id() { "bac" } else { "bca" };
        seq.apply(cnode);
        assert_eq!(seq.iter().collect::<String>(), expect);
        check_index_matches_iter(&seq);
    }

    #[test]
    fn re_move_relocates_and_frees_the_old_destination() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "abcd".chars());
        let a = seq.id_at(0).unwrap();
        let c = seq.id_at(2).unwrap();
        let d = seq.id_at(3).unwrap();

        seq.move_element(a, Anchor::After(d)).unwrap();
        assert_eq!(seq.iter().collect::<String>(), "bcda");
        seq.move_element(a, Anchor::Before(c)).unwrap();
        assert_eq!(seq.iter().collect::<String>(), "bacd");
        assert!(seq.move_element(a, Anchor::After(a)).is_err()); // self-move gates
        assert_eq!(seq.iter().collect::<String>(), "bacd");
        check_index_matches_iter(&seq);
    }

    #[test]
    fn rendered_moves_survive_merge_and_roundtrip() {
        let mut base = HashSeq::default();
        base.insert_batch(0, "abcd".chars());
        let a = base.id_at(0).unwrap();
        let d = base.id_at(3).unwrap();

        let mut r1 = base.clone();
        let mut r2 = base.clone();
        r1.move_element(a, Anchor::After(d)).unwrap();
        r2.insert(2, 'x');

        let mut m1 = r1.clone();
        m1.merge(r2.clone());
        let mut m2 = r2.clone();
        m2.merge(r1.clone());
        assert_eq!(m1, m2);
        assert_eq!(
            m1.iter().collect::<String>(),
            m2.iter().collect::<String>(),
            "rendered order converges"
        );
        check_index_matches_iter(&m1);
        check_index_matches_iter(&m2);

        // Wire roundtrip re-applies the move and reconstructs the rendering.
        let decoded = crate::encoding::decode_hashseq(&crate::encoding::encode_hashseq(&m1))
            .expect("roundtrip");
        assert_eq!(decoded, m1);
        assert_eq!(
            decoded.iter().collect::<String>(),
            m1.iter().collect::<String>()
        );
        check_index_matches_iter(&decoded);
    }

    #[test]
    fn frozen_conflict_renders_at_last_agreed() {
        let mut base = HashSeq::default();
        base.insert_batch(0, "abc".chars());
        let a = base.id_at(0).unwrap();
        let b = base.id_at(1).unwrap();
        let c = base.id_at(2).unwrap();

        let m0 = base.move_element(a, Anchor::After(b)).unwrap();
        // a joins b's sibling set (against the c-continuation), by id.
        let mut sibs = [(m0.id(), 'a'), (c, 'c')];
        sibs.sort();
        let agreed: String = std::iter::once('b').chain(sibs.iter().map(|s| s.1)).collect();
        assert_eq!(base.iter().collect::<String>(), agreed);

        let mut r1 = base.clone();
        let mut r2 = base.clone();
        r1.move_element(a, Anchor::After(c)).unwrap();
        r2.move_element(a, Anchor::Before(b)).unwrap();

        let mut merged = r1.clone();
        merged.merge(r2.clone());
        assert!(merged.placement_conflicted(&a));
        // Frozen at the last agreed placement — rendered there too.
        assert_eq!(merged.iter().collect::<String>(), agreed);
        let mut merged2 = r2;
        merged2.merge(r1);
        assert_eq!(merged2.iter().collect::<String>(), agreed);
        check_index_matches_iter(&merged);
        check_index_matches_iter(&merged2);
    }

    /// Random inserts/removes/moves on one replica: the index must stay
    /// pinned to the definitional causal iterator (Law II — the treap is a
    /// cache of the rendered order).
    #[quickcheck]
    fn prop_index_matches_iterator_with_moves(ops: Vec<(u8, u8, u8)>) -> bool {
        let mut seq = HashSeq::default();
        seq_from_move_ops(&mut seq, &ops);
        let iter_ids: Vec<Id> = seq.iter_idxs_causal().map(|i| seq.id_of(i)).collect();
        let index_ids: Vec<Id> = seq.iter_ids().copied().collect();
        iter_ids == index_ids && seq.len() == iter_ids.len()
    }

    /// Rendered order converges across merge orders when moves are in play.
    #[quickcheck]
    fn prop_moves_merge_commutative(a: Vec<(u8, u8, u8)>, b: Vec<(u8, u8, u8)>) -> bool {
        let mut base = HashSeq::default();
        base.insert_batch(0, "seed".chars());
        let mut seq_a = base.clone();
        let mut seq_b = base;
        seq_from_move_ops(&mut seq_a, &a);
        seq_from_move_ops(&mut seq_b, &b);

        let mut ab = seq_a.clone();
        ab.merge(seq_b.clone());
        let mut ba = seq_b;
        ba.merge(seq_a);

        let ab_ids: Vec<Id> = ab.iter_ids().copied().collect();
        let ba_ids: Vec<Id> = ba.iter_ids().copied().collect();
        let causal: Vec<Id> = ab.iter_idxs_causal().map(|i| ab.id_of(i)).collect();
        ab == ba
            && ab_ids == ba_ids
            && ab_ids == causal
            && ab.marked_spans() == ba.marked_spans()
            && crate::encoding::encode_hashseq(&ab) == crate::encoding::encode_hashseq(&ba)
    }

    /// Fuzz driver mixing inserts, removes, moves, and splice-anchored
    /// inserts (targets/anchors picked by rendered position; self-moves,
    /// moves of removed elements, and inserts on superseded move heads are
    /// generated and must be harmless).
    fn seq_from_move_ops(seq: &mut HashSeq, ops: &[(u8, u8, u8)]) {
        for &(kind, x, y) in ops {
            match kind % 7 {
                0 | 3 => {
                    let at = if seq.is_empty() {
                        0
                    } else {
                        x as usize % (seq.len() + 1)
                    };
                    seq.insert(at, (b'a' + (y % 26)) as char);
                }
                1 => {
                    if !seq.is_empty() {
                        seq.remove(x as usize % seq.len());
                    }
                }
                2 => {
                    if seq.is_empty() {
                        continue;
                    }
                    let target = seq.id_at(x as usize % seq.len()).unwrap();
                    let mut anchor = if y as usize % (seq.len() + 1) == seq.len() {
                        seq.origin()
                    } else {
                        seq.id_at(y as usize % seq.len()).unwrap()
                    };
                    // Sometimes aim at a splice point instead (self-splice
                    // combinations gate harmlessly).
                    if y & 4 != 0
                        && let Some(m) = seq.move_heads(&anchor).first().copied()
                    {
                        anchor = m;
                    }
                    let to = if y & 1 == 0 {
                        Anchor::After(anchor)
                    } else {
                        Anchor::Before(anchor)
                    };
                    let _ = seq.move_element(target, to); // self-moves gate
                }
                4 => {
                    // Insert anchored at a move op's splice point (any head
                    // of some element's register — deciding or not).
                    if seq.is_empty() {
                        continue;
                    }
                    let el = seq.id_at(x as usize % seq.len()).unwrap();
                    if let Some(m) = seq.move_heads(&el).first().copied() {
                        let op = if y & 1 == 0 {
                            Op::insert_after(m, 's')
                        } else {
                            Op::insert_before(m, 's')
                        };
                        seq.apply(HashNode {
                            pins: BTreeSet::new(),
                            op,
                        });
                    }
                }
                5 => {
                    // Insert an atom (a value commitment id) at a position.
                    let at = if seq.is_empty() {
                        0
                    } else {
                        x as usize % (seq.len() + 1)
                    };
                    let v = crate::value::Value::Int(y as i64).value_id();
                    seq.insert_value(at, v);
                }
                _ => {
                    // Mark or unmark a range (possibly inverted — gates
                    // harmlessly; possibly over tombstones — spec-valid).
                    if seq.is_empty() {
                        continue;
                    }
                    let mut p1 = seq.id_at(x as usize % seq.len()).unwrap();
                    let mut p2 = seq.id_at(y as usize % seq.len()).unwrap();
                    // Sometimes anchor a span endpoint at a splice point.
                    if x & 4 != 0
                        && let Some(m) = seq.move_heads(&p1).first().copied()
                    {
                        p1 = m;
                    }
                    if y & 8 != 0
                        && let Some(m) = seq.move_heads(&p2).first().copied()
                    {
                        p2 = m;
                    }
                    let start = if x & 1 == 0 {
                        Anchor::Before(p1)
                    } else {
                        Anchor::After(p1)
                    };
                    let end = if y & 1 == 0 {
                        Anchor::After(p2)
                    } else {
                        Anchor::Before(p2)
                    };
                    let kind = crate::value::Value::String("b".into()).value_id();
                    let value = if (x ^ y) & 2 == 0 {
                        crate::value::Value::Bool(true).value_id()
                    } else {
                        *crate::value::TOMBSTONE
                    };
                    let mut named: BTreeSet<Id> = BTreeSet::new();
                    named.insert(*start.id());
                    named.insert(*end.id());
                    seq.apply(HashNode {
                        pins: BTreeSet::new(),
                        op: Op::Mark {
                            start,
                            end,
                            kind_v: kind,
                            value,
                            overwrites: BTreeSet::new(),
                        },
                    });
                    let _ = named;
                }
            }
        }
    }

    /// The design-review scenario: b = Move(x, After(a)); c = Insert(After(b))
    /// ∥ d = Move(x, After(q)). x freezes at base (surfaced conflict); c
    /// renders at b's splice point on both merge orders.
    #[test]
    fn splice_children_survive_placement_conflicts() {
        let mut base = HashSeq::default();
        base.insert_batch(0, "aq".chars());
        let a = base.id_at(0).unwrap();
        let q = base.id_at(1).unwrap();
        base.insert(1, 'x'); // "axq": x is a before-child of q
        let x = base.id_at(1).unwrap();

        let mut r1 = base.clone();
        let mut r2 = base.clone();
        let b = r1.move_element(x, Anchor::After(a)).unwrap();
        // c: type y right after the moved-in x — the cursor anchors at b's
        // splice point (After(b)), not at x's ghost.
        let pos = r1.position_of(&x).unwrap();
        r1.insert(pos + 1, 'y');

        r2.move_element(x, Anchor::After(q)).unwrap();

        let mut m1 = r1.clone();
        m1.merge(r2.clone());
        let mut m2 = r2.clone();
        m2.merge(r1.clone());

        assert_eq!(m1, m2);
        assert_eq!(
            m1.iter().collect::<String>(),
            m2.iter().collect::<String>()
        );
        assert!(m1.placement_conflicted(&x));
        // x froze at its base slot (creation placement).
        assert_eq!(m1.placement_of(&x), None);
        // y renders at b's rank among a's siblings — a pure function of ids,
        // independent of x's contested register.
        let expect = if b.id() < q { "ayxq" } else { "axqy" };
        assert_eq!(m1.iter().collect::<String>(), expect);
        check_index_matches_iter(&m1);
        check_index_matches_iter(&m2);

        // A resolving move naming both heads re-renders x; y keeps its spot.
        m1.move_element(x, Anchor::After(a)).unwrap();
        assert!(!m1.placement_conflicted(&x));
        assert_eq!(m1.placement_of(&x), Some(Anchor::After(a)));
        check_index_matches_iter(&m1);
    }

    /// A superseded op with splice children keeps its rank (demote), and a
    /// conflict resolving back to it re-renders the element there (promote).
    #[test]
    fn splice_point_survives_demote_and_promote() {
        let mut base = HashSeq::default();
        base.insert_batch(0, "ma".chars());
        let m = base.id_at(0).unwrap();
        let a = base.id_at(1).unwrap();

        let b = base.move_element(m, Anchor::After(a)).unwrap();
        assert_eq!(base.iter().collect::<String>(), "am");
        // Type y after the moved-in m: anchors After(b).
        base.insert(2, 'y');
        assert_eq!(base.iter().collect::<String>(), "amy");
        // And w between a and m: anchors Before(b).
        base.insert(1, 'w');
        assert_eq!(base.iter().collect::<String>(), "awmy");
        check_index_matches_iter(&base);
        let _ = b;

        // Two concurrent re-moves, both overwriting b: the register
        // conflicts and freezes at the last agreed op — b — whose splice
        // slot promotes back to rendering m. Children stay adjacent.
        let mut r1 = base.clone();
        let mut r2 = base.clone();
        r1.move_element(m, Anchor::Before(a)).unwrap();
        assert_eq!(r1.iter().collect::<String>(), "mawy", "demoted: children keep b's rank");
        check_index_matches_iter(&r1);
        r2.move_element(m, Anchor::After(a)).unwrap();

        let mut m1 = r1.clone();
        m1.merge(r2.clone());
        let mut m2 = r2;
        m2.merge(r1);
        assert_eq!(m1, m2);
        assert!(m1.placement_conflicted(&m));
        assert_eq!(m1.placement_of(&m), Some(Anchor::After(a)));
        assert_eq!(m1.iter().collect::<String>(), "awmy");
        assert_eq!(m2.iter().collect::<String>(), "awmy");
        check_index_matches_iter(&m1);
        check_index_matches_iter(&m2);
    }

    /// Removing a moved element keeps its op's splice children rendering.
    #[test]
    fn removing_a_moved_element_keeps_splice_children() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "ma".chars());
        let m = seq.id_at(0).unwrap();
        let a = seq.id_at(1).unwrap();

        seq.move_element(m, Anchor::After(a)).unwrap();
        seq.insert(2, 'y'); // After(op)
        assert_eq!(seq.iter().collect::<String>(), "amy");

        seq.remove(1); // tombstone m at its rendered position
        assert_eq!(seq.iter().collect::<String>(), "ay");
        assert_eq!(seq.position_of(&m), None);
        check_index_matches_iter(&seq);
    }

    /// Splice-anchored runs survive the wire (their anchor is a move-op id,
    /// carried via the dictionary).
    #[test]
    fn splice_children_roundtrip() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "ma".chars());
        let m = seq.id_at(0).unwrap();
        seq.move_element(m, Anchor::After(seq.id_at(1).unwrap())).unwrap();
        seq.insert(2, 'y');
        seq.insert(3, '!');
        assert_eq!(seq.iter().collect::<String>(), "amy!");

        let decoded = crate::encoding::decode_hashseq(&crate::encoding::encode_hashseq(&seq))
            .expect("roundtrip");
        assert_eq!(decoded, seq);
        assert_eq!(decoded.iter().collect::<String>(), "amy!");
        check_index_matches_iter(&decoded);
    }

    // ---- atoms (the value column: non-char payloads) ----

    #[test]
    fn atom_inserts_render_and_read_back() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "ab".chars());
        let link = crate::value::Value::String("target-doc".into()).value_id();
        let node = seq.insert_value(1, link);

        assert_eq!(seq.len(), 3);
        assert_eq!(seq.iter().collect::<String>(), format!("a{ATOM_CHAR}b"));
        let atom_id = node.id();
        assert_eq!(seq.payload_of(&atom_id), Some(link));
        assert_eq!(seq.id_at(1), Some(atom_id));
        assert_eq!(seq.payload_of(&seq.id_at(0).unwrap()), None, "chars have no column entry");
        check_index_matches_iter(&seq);
    }

    /// Typing adjacent to an atom never extends through it — the atom's
    /// placeholder text is not identity input, so chains must not absorb it.
    #[test]
    fn typing_never_chains_through_atoms() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "a".chars());
        let v = crate::value::Value::Int(7).value_id();
        seq.insert_value(1, v);
        seq.insert_batch(2, "bc".chars());

        assert_eq!(seq.iter().collect::<String>(), format!("a{ATOM_CHAR}bc"));
        // the atom is its own single-element run; "bc" is a separate run
        let atom = seq.idx_of(&seq.id_at(1).unwrap()).unwrap();
        let Loc::Run { run, .. } = seq.loc_of(atom) else {
            panic!()
        };
        assert_eq!(seq.runs[&run].elements.len(), 1);
        check_index_matches_iter(&seq);
    }

    #[test]
    fn atoms_roundtrip_and_merge_commute() {
        let mut base = HashSeq::default();
        base.insert_batch(0, "hello".chars());
        let v = crate::value::Value::Bytes(vec![1, 2, 3]).value_id();
        let mut r1 = base.clone();
        let mut r2 = base.clone();
        r1.insert_value(2, v);
        r2.insert(5, '!');

        let mut m1 = r1.clone();
        m1.merge(r2.clone());
        let mut m2 = r2;
        m2.merge(r1);
        assert_eq!(m1, m2);
        assert_eq!(
            m1.iter().collect::<String>(),
            m2.iter().collect::<String>()
        );
        let e1 = crate::encoding::encode_hashseq(&m1);
        assert_eq!(e1, crate::encoding::encode_hashseq(&m2), "byte-canonical with atoms");
        let decoded = crate::encoding::decode_hashseq_strict(&e1).expect("strict");
        assert_eq!(decoded, m1);
        let atom_pos = decoded
            .iter()
            .position(|c| c == ATOM_CHAR)
            .expect("atom rendered");
        assert_eq!(
            decoded.payload_of(&decoded.id_at(atom_pos).unwrap()),
            Some(v),
            "value column survives the wire"
        );
        check_index_matches_iter(&decoded);
    }

    /// Atoms are ordinary elements to the other projections: movable,
    /// markable, removable.
    #[test]
    fn atoms_move_mark_and_remove() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "ab".chars());
        let v = crate::value::Value::Int(42).value_id();
        let node = seq.insert_value(1, v);
        let atom = node.id();
        assert_eq!(seq.iter().collect::<String>(), format!("a{ATOM_CHAR}b"));

        // move the atom to the end
        let b = seq.id_at(2).unwrap();
        seq.move_element(atom, Anchor::After(b)).unwrap();
        assert_eq!(seq.iter().collect::<String>(), format!("ab{ATOM_CHAR}"));
        assert_eq!(seq.payload_of(&atom), Some(v));
        check_index_matches_iter(&seq);

        // mark a range containing its rendered position
        let a = seq.id_at(0).unwrap();
        seq.mark_range(Anchor::Before(a), Anchor::After(b), bold(), yes()).unwrap();
        // regional membership: the atom moved beyond the end point — not marked
        assert!(kinds_at(&seq, 2).is_empty());
        assert_eq!(kinds_at(&seq, 0), vec![bold()]);

        // remove it
        let pos = seq.position_of(&atom).unwrap();
        seq.remove(pos);
        assert_eq!(seq.iter().collect::<String>(), "ab");
        assert_eq!(seq.payload_of(&atom), Some(v), "column persists for tombstones");
    }

    /// The Insert.at gate row: anchors must be glued points (elements,
    /// move ops, the origin) — an insert anchored at a remove or mark op
    /// quarantines instead of creating unrenderable content (or worse).
    #[test]
    fn insert_at_non_glued_anchor_gates() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "ab".chars());
        let rm = seq.remove_batch(0, 1).unwrap();
        let a = seq.id_at(0).unwrap();
        let mk = seq.mark_range(
            Anchor::Before(a),
            Anchor::After(a),
            crate::value::Value::String("b".into()).value_id(),
            crate::value::Value::Bool(true).value_id(),
        ).unwrap();

        for anchor in [rm.id(), mk.id()] {
            seq.apply(HashNode {
                pins: BTreeSet::new(),
                op: Op::insert_after(anchor, 'X'),
            });
        }
        assert_eq!(seq.delivery.gated.len(), 2);
        assert_eq!(seq.iter().collect::<String>(), "b");
        check_index_matches_iter(&seq);
        // ...and the doc still roundtrips with the quarantined pair aboard.
        let decoded = crate::encoding::decode_hashseq_strict(&crate::encoding::encode_hashseq(
            &seq,
        ))
        .expect("strict");
        assert_eq!(decoded.delivery.gated.len(), 2);
    }

    // ---- marks (the span-annotation projection, MARKS.md) ----

    fn bold() -> Id {
        crate::value::Value::String("bold".into()).value_id()
    }

    fn yes() -> Id {
        crate::value::Value::Bool(true).value_id()
    }

    /// Kinds present at an element, tombstone-suppressed but conflict-honest.
    fn kinds_at(seq: &HashSeq, pos: usize) -> Vec<Id> {
        let id = seq.id_at(pos).unwrap();
        seq.marks_at(&id)
            .into_iter()
            .filter(|(_, live)| live.iter().any(|(_, v)| *v != *crate::value::TOMBSTONE))
            .map(|(k, _)| k)
            .collect()
    }

    #[test]
    fn contested_register_reads_never_re_resolve() {
        let mut base = HashSeq::default();
        base.insert_batch(0, "abcd".chars());
        let a = base.id_at(0).unwrap();
        let b = base.id_at(1).unwrap();
        let c = base.id_at(2).unwrap();
        let d = base.id_at(3).unwrap();
        // A chain of agreed moves, then two concurrent forks: contested.
        base.move_element(a, Anchor::After(b)).unwrap();
        base.move_element(a, Anchor::After(c)).unwrap();
        let mut r1 = base.clone();
        let mut r2 = base.clone();
        r1.move_element(a, Anchor::After(d)).unwrap();
        r2.move_element(a, Anchor::Before(b)).unwrap();
        let mut seq = r1;
        seq.merge(r2);
        assert!(seq.placement_conflicted(&a));
        assert_eq!(seq.placement_of(&a), Some(Anchor::After(c)), "last agreed");

        let resolved = seq.resolutions.get();
        assert!(resolved > 0, "the merge resolved the contested register");
        for _ in 0..50 {
            let _ = seq.placement_of(&a);
            let _ = seq.cursor_at(2);
            let _ = seq.iter().count();
            let _ = seq.marked_spans();
            let _ = seq.iter_idxs_causal().count();
        }
        assert_eq!(seq.resolutions.get(), resolved, "reads are cache hits");

        // A move naming both heads collapses the register: one more resolve
        // is fine, but a single head resolves without a walk.
        seq.move_element(a, Anchor::After(d)).unwrap();
        assert!(!seq.placement_conflicted(&a));
        assert_eq!(seq.placement_of(&a), Some(Anchor::After(d)));
        assert_eq!(seq.resolutions.get(), resolved);
    }

    #[test]
    fn anchor_id_at_maps_moved_in_elements_to_their_move_op() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "abcd".chars());
        let a = seq.id_at(0).unwrap();
        let d = seq.id_at(3).unwrap();
        assert_eq!(seq.anchor_id_at(0), Some(a), "in place: the element itself");
        let mv = seq.move_element(a, Anchor::After(d)).unwrap();
        assert_eq!(seq.iter().collect::<String>(), "bcda");
        assert_eq!(seq.id_at(3), Some(a));
        assert_eq!(seq.anchor_id_at(3), Some(mv.id()), "moved in: its decider");
        assert_eq!(seq.anchor_id_at(4), None);

        // Marks built from visible positions the way the wasm layer does:
        // [1,3) = "cd" is Before(c)..Before(anchor at 3), which must bracket
        // the moved-in `a`'s rendered position, not its base ghost.
        let s = Anchor::Before(seq.anchor_id_at(1).unwrap());
        let e = Anchor::Before(seq.anchor_id_at(3).unwrap());
        seq.mark_range(s, e, bold(), yes()).unwrap();
        let spans: Vec<(String, bool)> = seq
            .marked_spans()
            .into_iter()
            .map(|(t, m)| (t, !m.is_empty()))
            .collect();
        assert_eq!(
            spans,
            vec![("b".into(), false), ("cd".into(), true), ("a".into(), false)]
        );
        // And a closed range over the moved-in element itself covers it.
        let s = Anchor::Before(seq.anchor_id_at(3).unwrap());
        let e = Anchor::After(seq.anchor_id_at(3).unwrap());
        seq.mark_range(s, e, bold(), yes()).unwrap();
        assert!(!seq.marks_at(&a).is_empty());
    }

    #[test]
    fn gated_authoring_is_reported_and_never_queued_for_peers() {
        let mut seq = HashSeq::default();
        seq.outbox = Some(Vec::new());
        seq.insert_batch(0, "abcd".chars());
        let a = seq.id_at(0).unwrap();
        let c = seq.id_at(2).unwrap();
        let d = seq.id_at(3).unwrap();
        seq.move_element(a, Anchor::After(d)).unwrap();
        assert_eq!(seq.iter().collect::<String>(), "bcda");
        let queued = seq.outbox.as_ref().unwrap().len();

        // Anchors on the visible order, but `a`'s point sits at its base
        // slot (the front): an inverted span, which the gate refuses.
        let err = seq
            .mark_range(Anchor::Before(c), Anchor::Before(a), bold(), yes())
            .unwrap_err();
        assert!(!seq.contains_node(&err.id()));
        assert_eq!(seq.delivery.gated.len(), 1);
        assert_eq!(seq.outbox.as_ref().unwrap().len(), queued, "nothing shipped");

        // A self-move is refused the same way.
        let err = seq.move_element(c, Anchor::After(c)).unwrap_err();
        assert!(!seq.contains_node(&err.id()));
        assert_eq!(seq.outbox.as_ref().unwrap().len(), queued);
    }

    #[test]
    fn marks_at_non_element_ids_is_empty_not_a_panic() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "abc".chars());
        let a = seq.id_at(0).unwrap();
        let c = seq.id_at(2).unwrap();
        let mark = seq.mark_range(Anchor::Before(a), Anchor::After(c), bold(), yes()).unwrap();
        assert!(!seq.marks_at(&a).is_empty(), "sanity: the element is covered");

        assert!(seq.marks_at(&seq.origin()).is_empty());
        assert!(seq.marks_at(&mark.id()).is_empty());
        let remove = seq.remove_batch(1, 1).unwrap();
        assert!(seq.marks_at(&remove.id()).is_empty());
        let mv = seq.move_element(a, Anchor::After(c)).unwrap();
        assert!(seq.marks_at(&mv.id()).is_empty());
        for tip in seq.tips().clone() {
            let _ = seq.marks_at(&tip);
        }
        for head in seq.move_heads(&a) {
            let _ = seq.marks_at(&head);
        }
    }

    fn span_texts(seq: &HashSeq) -> Vec<(String, bool)> {
        seq.marked_spans()
            .into_iter()
            .map(|(text, marks)| (text, !marks.is_empty()))
            .collect()
    }

    #[test]
    fn mark_renders_coalesced_spans() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "hello world".chars());
        let h = seq.id_at(0).unwrap();
        let space = seq.id_at(5).unwrap();

        // Bold "hello": Before(h) .. Before(space) — the bold expansion
        // choice from the MARKS.md anchor table.
        seq.mark_range(Anchor::Before(h), Anchor::Before(space), bold(), yes()).unwrap();

        assert_eq!(
            span_texts(&seq),
            vec![("hello".into(), true), (" world".into(), false)]
        );
        assert_eq!(kinds_at(&seq, 0), vec![bold()]);
        assert_eq!(kinds_at(&seq, 4), vec![bold()]);
        assert!(kinds_at(&seq, 5).is_empty());
    }

    /// A mark anchored on a non-glue id (unknown, or an op that is not an
    /// element) is refused, not a panic — and leaves no trace: no orphan,
    /// no tip change, nothing in the outbox.
    #[test]
    fn mark_range_on_non_glue_anchor_is_refused() {
        let mut seq = HashSeq::default();
        seq.outbox = Some(Vec::new());
        seq.insert_batch(0, "ab".chars());
        let a = seq.id_at(0).unwrap();
        let b = seq.id_at(1).unwrap();
        let rm = seq.remove_batch(1, 1).unwrap().id();
        let tips = seq.tips().clone();
        let mark_tips = seq.mark_tips().clone();
        seq.outbox.as_mut().unwrap().clear();

        let unknown = Id([0xEE; 32]);
        assert!(seq.mark_range(Anchor::Before(a), Anchor::After(unknown), bold(), yes()).is_err());
        assert!(seq.mark_range(Anchor::Before(unknown), Anchor::After(a), bold(), yes()).is_err());
        // A remove op id is not a glue point either.
        assert!(seq.mark_range(Anchor::Before(a), Anchor::After(rm), bold(), yes()).is_err());
        assert!(seq.unmark_range(Anchor::Before(rm), Anchor::After(a), bold()).is_err());

        assert_eq!(seq.orphans().count(), 0);
        assert_eq!(seq.tips(), &tips);
        assert_eq!(seq.mark_tips(), &mark_tips);
        assert!(seq.outbox.as_ref().unwrap().is_empty());
        // Still authors normally.
        seq.mark_range(Anchor::Before(a), Anchor::After(b), bold(), yes()).unwrap();
        // remove_batch with a huge amount clamps instead of overflowing.
        assert!(seq.remove_batch(0, usize::MAX).is_some());
        assert_eq!(seq.iter().count(), 0);
    }

    /// Grow-at-edges is anchor choice, not a flag: `Before(next)` ends
    /// expand with edge inserts, `After(last)` ends do not.
    #[test]
    fn edge_expansion_is_anchor_choice() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "ab".chars());
        let a = seq.id_at(0).unwrap();
        let b = seq.id_at(1).unwrap();

        let link = crate::value::Value::String("link".into()).value_id();
        // bold: Before(a)..Before(b) — expanding end.
        seq.mark_range(Anchor::Before(a), Anchor::Before(b), bold(), yes()).unwrap();
        // link: Before(a)..After(a) — non-expanding end.
        seq.mark_range(Anchor::Before(a), Anchor::After(a), link, yes()).unwrap();

        // Type between a and b (a before-child of b: inside Before(b),
        // outside After(a)).
        seq.apply(HashNode {
            pins: BTreeSet::new(),
            op: Op::insert_before(b, 'x'),
        });
        let x_pos = seq.position_of(&seq.id_at(1).unwrap()).unwrap();
        assert_eq!(seq.iter().collect::<String>(), "axb");
        assert_eq!(kinds_at(&seq, x_pos), vec![bold()], "bold expands, link does not");
    }

    /// Partial unmark: the overwritten bold keeps applying outside the
    /// unmarked sub-range.
    #[test]
    fn partial_unmark_splits_the_span() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "abcde".chars());
        let ids: Vec<Id> = (0..5).map(|i| seq.id_at(i).unwrap()).collect();

        seq.mark_range(Anchor::Before(ids[0]), Anchor::After(ids[4]), bold(), yes()).unwrap();
        assert_eq!(span_texts(&seq), vec![("abcde".into(), true)]);

        seq.unmark_range(Anchor::Before(ids[2]), Anchor::After(ids[2]), bold()).unwrap();
        assert_eq!(
            span_texts(&seq),
            vec![
                ("ab".into(), true),
                ("c".into(), false),
                ("de".into(), true)
            ]
        );
    }

    /// Peritext case 1: concurrent bold vs overlapping unbold — the unbold
    /// kills only what it names; the concurrent bold survives (add wins).
    #[test]
    fn concurrent_bold_survives_unbold_it_never_saw() {
        let mut base = HashSeq::default();
        base.insert_batch(0, "abcd".chars());
        let ids: Vec<Id> = (0..4).map(|i| base.id_at(i).unwrap()).collect();
        base.mark_range(Anchor::Before(ids[0]), Anchor::After(ids[3]), bold(), yes()).unwrap();

        let mut r1 = base.clone();
        let mut r2 = base.clone();
        // r1 unbolds bc; r2 concurrently re-bolds cd.
        r1.unmark_range(Anchor::Before(ids[1]), Anchor::After(ids[2]), bold()).unwrap();
        r2.mark_range(Anchor::Before(ids[2]), Anchor::After(ids[3]), bold(), yes()).unwrap();

        let mut m1 = r1.clone();
        m1.merge(r2.clone());
        let mut m2 = r2;
        m2.merge(r1);
        assert_eq!(m1, m2);
        assert_eq!(m1.marked_spans(), m2.marked_spans());

        // a bold; b unbolded; c: unbold vs unseen concurrent bold — the
        // bold survives; d bold.
        assert_eq!(kinds_at(&m1, 0), vec![bold()]);
        assert!(kinds_at(&m1, 1).is_empty());
        assert_eq!(kinds_at(&m1, 2), vec![bold()]);
        assert_eq!(kinds_at(&m1, 3), vec![bold()]);
    }

    /// Peritext case 2: an insert into a gap inside an unmarked sub-span is
    /// covered by the unmark — not bold.
    #[test]
    fn insert_into_unmarked_gap_is_not_marked() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "abcd".chars());
        let ids: Vec<Id> = (0..4).map(|i| seq.id_at(i).unwrap()).collect();
        seq.mark_range(Anchor::Before(ids[0]), Anchor::After(ids[3]), bold(), yes()).unwrap();
        seq.unmark_range(Anchor::Before(ids[1]), Anchor::After(ids[2]), bold()).unwrap();

        seq.apply(HashNode {
            pins: BTreeSet::new(),
            op: Op::insert_after(ids[1], 'x'),
        });
        assert_eq!(seq.iter().collect::<String>(), "abxcd");
        assert!(kinds_at(&seq, 2).is_empty(), "x falls inside the unmark");
    }

    /// Peritext case 3: text deleted, new text inserted between the
    /// tombstones inherits the span (documented, correct).
    #[test]
    fn insert_between_tombstones_inherits_the_mark() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "abcd".chars());
        let ids: Vec<Id> = (0..4).map(|i| seq.id_at(i).unwrap()).collect();
        seq.mark_range(Anchor::Before(ids[1]), Anchor::After(ids[2]), bold(), yes()).unwrap();
        seq.remove_batch(1, 2); // tombstone b, c

        seq.apply(HashNode {
            pins: BTreeSet::new(),
            op: Op::insert_after(ids[1], 'x'),
        });
        assert_eq!(seq.iter().collect::<String>(), "axd");
        assert_eq!(kinds_at(&seq, 1), vec![bold()], "between the tombstones");
    }

    /// Peritext case 4: an inverted span (end before start in base order)
    /// gates permanently.
    #[test]
    fn inverted_mark_span_gates() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "ab".chars());
        let a = seq.id_at(0).unwrap();
        let b = seq.id_at(1).unwrap();

        seq.apply(HashNode {
            pins: BTreeSet::new(),
            op: Op::Mark {
                start: Anchor::Before(b),
                end: Anchor::After(a),
                kind_v: bold(),
                value: yes(),
                overwrites: BTreeSet::new(),
            },
        });
        assert_eq!(seq.delivery.gated.len(), 1);
        assert!(seq.mark_nodes.is_empty());
        assert!(seq.mark_tips().is_empty());
    }

    /// Regional semantics: a mark's points are fixed at their anchors'
    /// base slots; membership is the element's *rendered* crossing between
    /// them. Moving an element out of the region sheds its marks; moving
    /// one in acquires them.
    #[test]
    fn marks_are_regional_under_moves() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "abcd".chars());
        let ids: Vec<Id> = (0..4).map(|i| seq.id_at(i).unwrap()).collect();
        seq.mark_range(Anchor::Before(ids[0]), Anchor::After(ids[1]), bold(), yes()).unwrap();
        assert_eq!(
            span_texts(&seq),
            vec![("ab".into(), true), ("cd".into(), false)]
        );

        // Move bold `a` out to the end: it leaves the region and sheds.
        seq.move_element(ids[0], Anchor::After(ids[3])).unwrap();
        assert_eq!(seq.iter().collect::<String>(), "bcda");
        assert_eq!(
            span_texts(&seq),
            vec![("b".into(), true), ("cda".into(), false)]
        );
        assert!(kinds_at(&seq, 3).is_empty(), "moved out: shed");

        // Move plain `d` into the region: it acquires the bold.
        seq.move_element(ids[3], Anchor::Before(ids[1])).unwrap();
        assert_eq!(seq.iter().collect::<String>(), "dbca");
        assert_eq!(
            span_texts(&seq),
            vec![("db".into(), true), ("ca".into(), false)]
        );
    }

    /// The review example: "hello bob", bold "bob", move the middle 'o' out
    /// front — the o is not bold.
    #[test]
    fn moved_out_o_is_not_bold() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "hello bob".chars());
        let b1 = seq.id_at(6).unwrap();
        let o = seq.id_at(7).unwrap();
        let b2 = seq.id_at(8).unwrap();
        seq.mark_range(Anchor::Before(b1), Anchor::After(b2), bold(), yes()).unwrap();

        let h = seq.id_at(0).unwrap();
        seq.move_element(o, Anchor::Before(h)).unwrap();
        assert_eq!(seq.iter().collect::<String>(), "ohello bb");
        assert_eq!(
            span_texts(&seq),
            vec![("ohello ".into(), false), ("bb".into(), true)]
        );
        assert!(kinds_at(&seq, 0).is_empty());
    }

    /// A span endpoint at a move op's splice point covers the moved-in
    /// element at its rendered position — the thing element-anchored
    /// (base-slot) points cannot express.
    #[test]
    fn splice_point_span_covers_moved_in_content() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "ma".chars());
        let m = seq.id_at(0).unwrap();
        let a = seq.id_at(1).unwrap();
        let op = seq.move_element(m, Anchor::After(a)).unwrap();
        assert_eq!(seq.iter().collect::<String>(), "am");

        // Element end point: the moved-in m renders beyond After(a) — not
        // covered (regional membership).
        let el = seq.mark_range(Anchor::Before(a), Anchor::After(a), bold(), yes()).unwrap();
        assert_eq!(
            span_texts(&seq),
            vec![("a".into(), true), ("m".into(), false)]
        );
        seq.unmark_range(Anchor::Before(a), Anchor::After(a), bold()).unwrap();
        let _ = el;

        // Op end point: brackets wherever the target renders — covered.
        seq.mark_range(Anchor::Before(a), Anchor::After(op.id()), bold(), yes()).unwrap();
        assert_eq!(span_texts(&seq), vec![("am".into(), true)]);
        let m_pos = seq.position_of(&m).unwrap();
        assert_eq!(kinds_at(&seq, m_pos), vec![bold()]);
        check_index_matches_iter(&seq);
    }

    /// A superseded op keeps its rank for mark endpoints: the span's shape
    /// is stable while the element moves away.
    #[test]
    fn splice_point_marks_survive_supersession() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "mab".chars());
        let m = seq.id_at(0).unwrap();
        let a = seq.id_at(1).unwrap();
        let b = seq.id_at(2).unwrap();
        let op = seq.move_element(m, Anchor::After(b)).unwrap(); // b is the run tail
        assert_eq!(seq.iter().collect::<String>(), "abm");
        seq.mark_range(Anchor::Before(b), Anchor::After(op.id()), bold(), yes()).unwrap();
        assert_eq!(
            span_texts(&seq),
            vec![("a".into(), false), ("bm".into(), true)]
        );

        // Re-move m to the front: it leaves the span (whose end point stays
        // at the superseded op's rank); b remains covered.
        seq.move_element(m, Anchor::Before(a)).unwrap();
        assert_eq!(seq.iter().collect::<String>(), "mab");
        assert_eq!(
            span_texts(&seq),
            vec![("ma".into(), false), ("b".into(), true)]
        );
        check_index_matches_iter(&seq);

        // Roundtrip carries op-anchored marks (they park until the op
        // applies, then re-anchor identically).
        let decoded = crate::encoding::decode_hashseq_strict(&crate::encoding::encode_hashseq(
            &seq,
        ))
        .expect("strict");
        assert_eq!(decoded, seq);
        assert_eq!(decoded.marked_spans(), seq.marked_spans());
    }

    /// Inverted spans gate for op points too (permanent: op ranks never
    /// move).
    #[test]
    fn inverted_splice_point_span_gates() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "ma".chars());
        let m = seq.id_at(0).unwrap();
        let a = seq.id_at(1).unwrap();
        let op = seq.move_element(m, Anchor::After(a)).unwrap(); // renders after a

        seq.apply(HashNode {
            pins: BTreeSet::new(),
            op: Op::Mark {
                start: Anchor::After(op.id()),
                end: Anchor::Before(a),
                kind_v: bold(),
                value: yes(),
                overwrites: BTreeSet::new(),
            },
        });
        assert_eq!(seq.delivery.gated.len(), 1);
        assert!(seq.mark_tips().is_empty());
    }

    /// Move destinations on splice points: y lands adjacent to wherever the
    /// moved x renders — the drag-next-to-moved-content gesture.
    #[test]
    fn move_to_splice_point_renders_adjacent() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "xya".chars());
        let x = seq.id_at(0).unwrap();
        let y = seq.id_at(1).unwrap();
        let a = seq.id_at(2).unwrap();

        let op1 = seq.move_element(x, Anchor::After(a)).unwrap();
        assert_eq!(seq.iter().collect::<String>(), "yax");

        seq.move_element(y, Anchor::After(op1.id())).unwrap();
        assert_eq!(seq.iter().collect::<String>(), "axy");
        assert_eq!(seq.placement_of(&y), Some(Anchor::After(op1.id())));
        check_index_matches_iter(&seq);

        // Roundtrip + merge-order determinism.
        let decoded = crate::encoding::decode_hashseq_strict(&crate::encoding::encode_hashseq(
            &seq,
        ))
        .expect("strict");
        assert_eq!(decoded, seq);
        assert_eq!(decoded.iter().collect::<String>(), "axy");
    }

    /// Self-splice is well-defined, not special: moving an element to its
    /// own (superseded) placement's splice point renders it at that op's
    /// permanent rank — "put x back where that op placed it".
    #[test]
    fn self_splice_move_renders_at_the_ops_rank() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "xab".chars());
        let x = seq.id_at(0).unwrap();
        let a = seq.id_at(1).unwrap();
        let b = seq.id_at(2).unwrap();
        let op1 = seq.move_element(x, Anchor::After(b)).unwrap();
        assert_eq!(seq.iter().collect::<String>(), "abx");
        seq.move_element(x, Anchor::Before(a)).unwrap();
        assert_eq!(seq.iter().collect::<String>(), "xab");

        // Move x to op1's splice point: back adjacent to op1's rank.
        seq.move_element(x, Anchor::After(op1.id())).unwrap();
        assert_eq!(seq.iter().collect::<String>(), "abx");
        assert_eq!(seq.placement_of(&x), Some(Anchor::After(op1.id())));
        check_index_matches_iter(&seq);

        // The reaffirmation shape: anchor at the CURRENT decider works too.
        let op3 = seq.move_heads(&x)[0];
        seq.move_element(x, Anchor::After(op3)).unwrap();
        assert_eq!(seq.iter().collect::<String>(), "abx");
        check_index_matches_iter(&seq);

        let decoded = crate::encoding::decode_hashseq_strict(&crate::encoding::encode_hashseq(
            &seq,
        ))
        .expect("strict");
        assert_eq!(decoded, seq);
        assert_eq!(decoded.iter().collect::<String>(), "abx");
    }

    /// A mark delivered before its text parks and applies on arrival.
    #[test]
    fn mark_before_its_text_parks() {
        let mut source = HashSeq::default();
        source.insert_batch(0, "ab".chars());
        let a = source.id_at(0).unwrap();
        let b = source.id_at(1).unwrap();
        let mark = source.mark_range(Anchor::Before(a), Anchor::After(b), bold(), yes()).unwrap();

        let mut fresh = HashSeq::default();
        fresh.apply(mark);
        assert_eq!(fresh.orphans().count(), 1);
        assert!(fresh.mark_nodes.is_empty());
        for (id, node) in source.all_nodes() {
            if matches!(node.op, Op::Insert { .. }) {
                fresh.apply_with_id(id, node);
            }
        }
        assert_eq!(fresh.orphans().count(), 0);
        assert_eq!(span_texts(&fresh), vec![("ab".into(), true)]);
    }

    /// Marks survive the wire.
    #[test]
    fn marks_roundtrip() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "hello".chars());
        let h = seq.id_at(0).unwrap();
        let o = seq.id_at(4).unwrap();
        seq.mark_range(Anchor::Before(h), Anchor::After(o), bold(), yes()).unwrap();
        seq.unmark_range(Anchor::Before(seq.id_at(2).unwrap()), Anchor::After(seq.id_at(2).unwrap()), bold()).unwrap();

        let decoded = crate::encoding::decode_hashseq(&crate::encoding::encode_hashseq(&seq))
            .expect("roundtrip");
        assert_eq!(decoded, seq);
        assert_eq!(decoded.marked_spans(), seq.marked_spans());
        assert_eq!(decoded.mark_tips(), seq.mark_tips());
    }

    #[test]
    fn insert_value_of_a_char_id_is_the_char() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "ab".chars());
        let node = seq.insert_value(1, crate::value::char_value_id('z'));
        assert!(matches!(node.op, Op::Insert { payload: Payload::Char('z'), .. }));
        assert_eq!(seq.iter().collect::<String>(), "azb");
        let z = seq.id_at(1).unwrap();
        assert_eq!(seq.payload_of(&z), None, "not an atom");

        // The same node applied in its by-id form renders identically.
        let mut other = HashSeq::default();
        other.insert_batch(0, "ab".chars());
        let by_id = HashNode {
            pins: node.pins.clone(),
            op: match &node.op {
                Op::Insert { at, payload } => Op::Insert {
                    at: *at,
                    payload: Payload::Id(payload.value_id()),
                },
                _ => unreachable!(),
            },
        };
        assert_eq!(by_id.id(), node.id());
        other.apply(by_id);
        assert_eq!(other.iter().collect::<String>(), "azb");
        assert_eq!(other.payload_of(&z), None);
    }

    #[test]
    fn self_move_gates() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "ab".chars());
        let a = seq.id_at(0).unwrap();

        let node = HashNode {
            pins: BTreeSet::new(),
            op: Op::Move {
                target: a,
                to: Anchor::After(a),
                overwrites: BTreeSet::new(),
            },
        };
        seq.apply(node);
        assert_eq!(seq.delivery.gated.len(), 1);
        assert_eq!(seq.placement_of(&a), None);
    }

    #[test]
    fn move_of_non_element_gates() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "ab".chars());
        let a = seq.id_at(0).unwrap();
        let b = seq.id_at(1).unwrap();
        // Remove b, then craft a move whose target is the REMOVE op (not an
        // element) — an edge-table violation.
        let remove = seq.remove_batch(1, 1).unwrap();
        let node = HashNode {
            pins: BTreeSet::new(),
            op: Op::Move {
                target: remove.id(),
                to: Anchor::After(a),
                overwrites: BTreeSet::new(),
            },
        };
        seq.apply(node);
        assert_eq!(seq.delivery.gated.len(), 1);
        let _ = b;
    }

    fn raw_move(target: Id, to: Anchor, overwrites: &[Id]) -> HashNode {
        HashNode {
            pins: BTreeSet::new(),
            op: Op::Move {
                target,
                to,
                overwrites: overwrites.iter().copied().collect(),
            },
        }
    }

    #[test]
    fn foreign_overwrites_never_become_the_decider() {
        // Two concurrent remote moves of `a` both "overwrite" `b`, an insert
        // element. `b` is not a move op of `a`'s register, so it must not
        // survive into the common-ancestor set — otherwise resolve_decider
        // hands it to rerender, which indexes move_nodes by it and panics.
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "abc".chars());
        let a = seq.id_at(0).unwrap();
        let b = seq.id_at(1).unwrap();
        let c = seq.id_at(2).unwrap();

        seq.apply(raw_move(a, Anchor::After(c), &[b]));
        seq.apply(raw_move(a, Anchor::Before(b), &[b]));
        assert!(seq.placement_conflicted(&a));
        // No genuine common ancestor: bottoms out at creation.
        assert_eq!(seq.placement_of(&a), None);
        assert_eq!(seq.iter().collect::<String>(), "abc");
    }

    #[test]
    fn other_register_overwrites_never_become_the_decider() {
        // Honest move of `b`, then two concurrent remote moves of `a` that
        // both name `b`'s move op as overwritten. That op belongs to another
        // register; treating it as `a`'s decider would re-place `b` on top
        // of its existing destination fragment and corrupt the index.
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "abc".chars());
        let a = seq.id_at(0).unwrap();
        let b = seq.id_at(1).unwrap();
        let c = seq.id_at(2).unwrap();

        let q = seq.move_element(b, Anchor::After(c)).unwrap().id();
        assert_eq!(seq.iter().collect::<String>(), "acb");

        seq.apply(raw_move(a, Anchor::After(c), &[q]));
        seq.apply(raw_move(a, Anchor::Before(c), &[q]));
        assert!(seq.placement_conflicted(&a));
        assert_eq!(seq.placement_of(&a), None);
        assert_eq!(seq.placement_of(&b), Some(Anchor::After(c)));
        assert_eq!(seq.iter().collect::<String>(), "acb");
    }

    #[test]
    fn moves_survive_doc_encode_decode() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "abc".chars());
        let a = seq.id_at(0).unwrap();
        let c = seq.id_at(2).unwrap();
        seq.move_element(a, Anchor::After(c)).unwrap();

        let bytes = crate::encode_hashseq(&seq);
        let decoded = crate::decode_hashseq(&bytes).expect("decodes");
        assert_eq!(decoded.placement_of(&a), Some(Anchor::After(c)));
        assert_eq!(decoded.tips(), seq.tips());
    }

    #[test]
    fn moves_merge_through_orphan_buffering() {
        // Deliver the move before its target exists: it parks, then applies.
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "abc".chars());
        let a = seq.id_at(0).unwrap();
        let c = seq.id_at(2).unwrap();
        let mut other = seq.clone();
        let mv = other.move_element(a, Anchor::After(c)).unwrap();

        let mut fresh = HashSeq::default();
        fresh.apply(mv); // parks: target unknown
        assert_eq!(fresh.orphans().count(), 1);
        fresh.merge(seq);
        assert_eq!(fresh.placement_of(&a), Some(Anchor::After(c)));
    }

}
