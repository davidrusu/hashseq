use std::collections::{BTreeMap, BTreeSet, HashMap};

use rustc_hash::{FxHashMap, FxHashSet};

use crate::bitset::BitSet;
use crate::run_index::{ElemRef, RunIndex};
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
}

/// `Loc` packed into 8 bytes for the per-node `locs` Vec (the enum is 12).
/// Layout: 2-bit kind | 32-bit handle | 30-bit position. The handle keeps the
/// full `NodeIdx` (u32) range; `pos` is a within-run/-chain offset, so 30 bits
/// (~1.07B) is far beyond any real run length (the longest in the test corpus
/// is ~69k).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackedLoc(u64);

impl PackedLoc {
    const KIND_RUN: u64 = 0;
    const KIND_ORIGIN: u64 = 1;
    const KIND_REMOVE_CHAIN: u64 = 2;
    const KIND_MULTI_REMOVE: u64 = 3;

    #[inline]
    fn pack(handle: NodeIdx, pos: u32, kind: u64) -> Self {
        debug_assert!(pos < (1 << 30), "Loc position {pos} exceeds 30 bits");
        PackedLoc(kind | (handle.0 as u64) << 2 | (pos as u64) << 34)
    }

    #[inline]
    fn unpack(self) -> Loc {
        let handle = NodeIdx((self.0 >> 2) as u32);
        let pos = (self.0 >> 34) as u32;
        match self.0 & 0b11 {
            Self::KIND_RUN => Loc::Run { run: handle, pos },
            Self::KIND_ORIGIN => Loc::Origin,
            Self::KIND_REMOVE_CHAIN => Loc::RemoveChain { chain: handle, pos },
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
    /// Insert immediately before `anchor`. Used when the cursor sits between two
    /// causally-related neighbors and a fork at the left neighbor would otherwise
    /// give hash-determined ordering.
    Before {
        anchor: Id,
        extra_deps: BTreeSet<Id>,
    },
}

impl Cursor {
    /// Build the first `HashNode` of an insertion at this cursor.
    /// Subsequent chars of a burst chain `InsertAfter` from this node.
    pub fn first_node(self, ch: char) -> HashNode {
        let (pins, op) = match self {
            Cursor::After { anchor, extra_deps } => (extra_deps, Op::insert_after(anchor, ch)),
            Cursor::Before { anchor, extra_deps } => (extra_deps, Op::insert_before(anchor, ch)),
        };
        HashNode { pins, op }
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
}

/// A set-once extra-dependency set, stored as `Box<[NodeIdx]>` in `Id` order
/// (4 B/entry, 16 B inline — like `BTreeSet<Id>` — but a fraction of the heap:
/// a 1-element `BTreeSet<Id>` allocates a ~400 B B-tree node for one 32 B id,
/// while these sets are almost always 1–3 tips). Used for the deps of applied
/// runs and removes. Order is by `Id` (convergence-safe); the handle is the
/// compact payload, and the `BTreeSet<Id>` is rebuilt for the wire / hashing at
/// encode and decompress time.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct IdxSet(Box<[NodeIdx]>);

impl IdxSet {
    /// Build from an already-`Id`-sorted set, mapping each id to its handle.
    /// `BTreeSet` iterates in `Id` order, so the handles land `Id`-sorted.
    pub(crate) fn from_id_set(set: &BTreeSet<Id>, to_handle: impl FnMut(&Id) -> NodeIdx) -> Self {
        IdxSet(set.iter().map(to_handle).collect())
    }

    /// Rebuild the `Id` set (for the wire format / hashing).
    pub fn to_id_set(&self, ids: &[Id]) -> BTreeSet<Id> {
        self.0.iter().map(|h| ids[h.0 as usize]).collect()
    }

    /// Iterate the member ids in `Id` order.
    pub fn iter_ids<'a>(&'a self, ids: &'a [Id]) -> impl Iterator<Item = Id> + 'a {
        self.0.iter().map(move |h| ids[h.0 as usize])
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

/// Storage form of a multi-target remove (`remove_batch` spanning several
/// chars). Single-target removes live in `RemoveRun` chains instead.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CausalRemove {
    pub pins: IdxSet,
    /// Removed element handles, in Id order.
    pub nodes: Box<[NodeIdx]>,
}

/// A coalesced chain of single-target removes: remove `i` deletes `targets[i]`
/// and causally depends on remove `i-1`; the first link carries
/// `first_extra_deps`. This is how backspace/delete bursts are stored — the
/// in-memory mirror of the wire format's remove-run sections, and the remove
/// analog of insert runs.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RemoveRun {
    pub first_extra_deps: IdxSet,
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
    pub first_extra_deps: IdxSet,
    /// Extra deps of interior elements (offset >= 1), sparse — see
    /// [`Run::interior_extra_deps`]. Lets a typing burst extend its run
    /// across a remove instead of starting a new run per burst.
    pub interior_extra_deps: BTreeMap<usize, IdxSet>,
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
        self.text.chars().nth(pos).unwrap()
    }

    pub fn head(&self) -> NodeIdx {
        self.elements[0]
    }

    pub fn last(&self) -> NodeIdx {
        *self.elements.last().unwrap()
    }

    fn extend(&mut self, idx: NodeIdx, ch: char, extra_deps: IdxSet) {
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
        let right_interior: BTreeMap<usize, IdxSet> = right_interior
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
/// (4 bytes/entry) instead of `BTreeSet<Id>` (32 bytes/entry plus a tree node
/// each). Used for the `afters` / `befores_by_anchor` sibling sets.
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

    /// Insert `handle` keyed by its id; a no-op if an equal id is already
    /// present (set semantics, like the `BTreeSet<Id>` it replaces).
    pub fn insert(&mut self, handle: NodeIdx, ids: &[Id]) {
        let id = ids[handle.0 as usize];
        if let Err(pos) = self.search(&id, ids) {
            self.0.insert(pos, handle);
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
    /// Chained single-target removes (backspace/delete bursts), keyed by the
    /// first remove's handle.
    pub remove_runs: FxHashMap<NodeIdx, RemoveRun>,
    /// Fork tracking: anchor -> handles that fork from it (Id-ordered, see
    /// `befores_by_anchor`).
    pub afters: FxHashMap<NodeIdx, SortedIdVec>,

    pub(crate) tips: BTreeSet<Id>,
    /// Ops waiting on a dependency, keyed by *one* missing dep id (the first
    /// found): applying that id wakes exactly these waiters — no global
    /// retries. A waiter still missing more deps is re-parked on the next
    /// one. Values carry the precomputed node id so wakes don't rehash.
    ///
    /// Keys are adversary-chosen bytes (an op can name any id as a dep), so
    /// this stays a std `HashMap` for SipHash's HashDoS protection — unlike
    /// `orphan_ids`, whose keys we computed ourselves with BLAKE3.
    pub(crate) orphaned: HashMap<Id, Vec<(Id, HashNode)>>,
    /// Ids of all parked orphans: dedups network re-delivery while parked.
    orphan_ids: IdSet,
    /// Permanently quarantined nodes (the apply-time gate, HASHWEB_SPEC.md
    /// "The edge table"): ops this projection does not admit — today `Move`
    /// (placement registers land with the move projection), `Put` (a map op
    /// in a seq), and non-char insert payloads (the value column
    /// generalization). Gated nodes never intern and never enter tips, so
    /// anything referencing them stays parked — exactly the spec's
    /// quarantine semantics. Kept so merge/encode re-present them.
    pub(crate) gated: IdMap<HashNode>,
    index: RunIndex,
}

impl PartialEq for HashSeq {
    fn eq(&self, other: &Self) -> bool {
        self.tips == other.tips
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
            remove_runs: FxHashMap::default(),
            afters: FxHashMap::default(),
            tips: BTreeSet::new(),
            orphaned: HashMap::new(),
            orphan_ids: IdSet::default(),
            gated: IdMap::default(),
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
        self.orphaned.values().flatten().map(|(_, node)| node)
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

    /// Check if node `a` is causally before node `b`.
    ///
    /// Walks handle space: the common case (run-chain neighbors) follows
    /// `elements` directly with no hashing at all.
    fn is_causally_before(&self, a: NodeIdx, b: NodeIdx) -> bool {
        let mut seen: FxHashSet<NodeIdx> = FxHashSet::default();
        let mut boundary: Vec<NodeIdx> = self.afters_of(a).collect();
        while let Some(n) = boundary.pop() {
            if n == b {
                return true;
            }

            seen.insert(n);
            boundary.extend(self.afters_of(n).filter(|x| !seen.contains(x)));
            if n != a {
                boundary.extend(self.befores_of(n).filter(|x| !seen.contains(x)));
            }
        }

        false
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

        let mut prev_id = first_node.id();
        self.apply_with_id(prev_id, first_node);

        // After the first apply, tips == {prev_id}, so the chained nodes carry no
        // extra deps.
        for ch in chars {
            let node = HashNode {
                pins: BTreeSet::new(),
                op: Op::insert_after(prev_id, ch),
            };
            prev_id = node.id();
            self.apply_with_id(prev_id, node);
        }
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
        if amount == 0 {
            return None;
        }

        let mut to_remove = BTreeSet::new();
        for pos in idx..(idx + amount) {
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
        let node = HashNode {
            pins,
            op: Op::Remove(to_remove),
        };

        let node_for_return = node.clone();
        self.apply(node);
        Some(node_for_return)
    }

    /// Insert `idx`'s fresh span directly before node `el` in the index.
    /// `el` may be the virtual origin, whose "position" is the start of its
    /// afters region (the origin's own befores precede that point).
    fn index_insert_before_node(&mut self, el: NodeIdx, idx: NodeIdx) {
        if el != ORIGIN_IDX {
            let at = self.elem_ref(el);
            self.index.insert_span_before(at, idx);
            return;
        }
        match self.afters.get(&ORIGIN_IDX).and_then(|s| s.first()) {
            Some(first) => {
                let first = self.region_first(first);
                let at = self.elem_ref(first);
                self.index.insert_span_before(at, idx);
            }
            // No top-level inserts yet: everything in the document (befores
            // of the origin, if any) precedes the origin, so "directly
            // before the origin" is the very end.
            None => self.index.push_span_back(idx),
        }
    }

    fn insert_after(&mut self, id: Id, after: CausalInsert) {
        // The anchor is a checked dependency, so it is interned.
        let anchor = self.idx_of_known(&after.anchor);

        // Fast path: extend the run whose tail is the anchor. Extra deps on
        // the new element (e.g. `{remove_id}` when typing resumes after a
        // delete) don't block extension — they're stored sparsely at the
        // element's offset and participate in its id, so the chain still
        // reconstructs exactly.
        if let Loc::Run { run, pos } = self.loc_of(anchor) {
            // Check for explicit forks first (cheap u32-keyed lookup)
            let has_explicit_afters = self.afters.get(&anchor).is_some_and(|ns| !ns.is_empty());

            if !has_explicit_afters && pos as usize + 1 == self.runs[&run].len() {
                // Run extension - most common case for sequential typing
                let idx = self.intern(id, Loc::Run { run, pos: pos + 1 });
                let deps = IdxSet::from_id_set(&after.pins, |d| self.idx_of_known(d));
                self.runs
                    .get_mut(&run)
                    .unwrap()
                    .extend(idx, after.ch, deps);
                self.index.extend_run(run, pos + 1);
                return;
            }
        }

        // Slow path: this insert forks. Find the smallest afters node >= id.
        // Explicit-afters case: O(log n) range seek into the BTreeSet.
        // Run-fallback case: at most one candidate, just check it.
        let next_node = if let Some(siblings) = self.afters.get(&anchor) {
            siblings.first_ge(&id, &self.ids)
        } else {
            self.afters_of(anchor)
                .find(|a| self.ids[a.0 as usize] >= id)
        };
        // The iterator releases siblings in Id order, each preceded by its
        // before-runs and trailed by its subtree. So the new node lands
        // directly before the next bigger sibling's region — or, when it is
        // the biggest sibling, directly after everything hanging off the
        // anchor. Tombstones don't matter here: a removed element's region
        // still occupies its place in document order.
        let target = match next_node {
            Some(next) => (self.region_first(next), true),
            None => (self.subtree_last(anchor), false),
        };

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
        let first_extra_deps = IdxSet::from_id_set(&after.pins, |d| self.idx_of_known(d));
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

        // run extension is handled in the fast path above, fork/split updates the afters set
        self.afters.entry(anchor).or_default().insert(idx, &self.ids);

        // Resolve the target element only now: the split above may have
        // relocated it into the right-hand run.
        let (el, before) = target;
        if before {
            self.index_insert_before_node(el, idx);
        } else if el == ORIGIN_IDX {
            // subtree_last(origin) returned the origin itself: no top-level
            // inserts exist yet, so the new span is the last (and first)
            // visible content after any origin-befores.
            self.index.push_span_back(idx);
        } else {
            let at = self.elem_ref(el);
            self.index.insert_span_after(at, idx);
        }
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

    fn apply_remove(&mut self, id: Id, extra_deps: BTreeSet<Id>, target_ids: BTreeSet<Id>) {
        // Targets are checked dependencies of the remove, so they are interned.
        // (A remove targeting a non-insert node is harmless: it's not in the
        // position index, and its tombstone bit is inert.)
        let targets: Vec<NodeIdx> = target_ids.iter().map(|t| self.idx_of_known(t)).collect();
        for t in &targets {
            // Removes targeting non-inserts have no index entry and are inert.
            if let Loc::Run { run, pos } = self.loc_of(*t) {
                self.index.remove_element((run, pos));
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
            let first_extra_deps = IdxSet::from_id_set(&extra_deps, |d| self.idx_of_known(d));
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
        let pins = IdxSet::from_id_set(&extra_deps, |d| self.idx_of_known(d));
        self.remove_nodes.insert(
            idx,
            CausalRemove {
                pins,
                nodes: targets.into(),
            },
        );
    }

    fn insert_before(&mut self, id: Id, before: CausalInsert) {
        // The anchor is a checked dependency, so it is interned.
        let anchor = self.idx_of_known(&before.anchor);

        // Before-siblings are released in Id order, directly before their
        // anchor: the new node lands before the next bigger sibling's region,
        // or — as the biggest — directly before the anchor element itself.
        // (O(log n) Id-binary-search seek, no tombstone filtering: removed
        // elements still anchor their regions.)
        let target = match self
            .befores_by_anchor
            .get(&anchor)
            .and_then(|s| s.first_ge(&id, &self.ids))
        {
            Some(next) => self.region_first(next),
            None => anchor,
        };

        // The anchor may sit mid-run: no split is needed. Iteration visits the
        // befores of every run element individually (see HashSeqIter), and unlike
        // an after-fork there is no sibling ordering to resolve — a Before-run
        // always lands immediately before its anchor.
        let idx = self.next_idx();
        self.intern(id, Loc::Run { run: idx, pos: 0 });
        let first_extra_deps = IdxSet::from_id_set(&before.pins, |d| self.idx_of_known(d));
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

        self.befores_by_anchor
            .entry(anchor)
            .or_default()
            .insert(idx, &self.ids);

        self.index_insert_before_node(target, idx);
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
    pub(crate) fn apply_with_id(&mut self, id: Id, node: HashNode) {
        debug_assert_eq!(id, node.id(), "apply_with_id called with a wrong id");
        if self.contains_node(&id) {
            return; // Already processed this node
        }
        if !self.orphan_ids.is_empty() && self.orphan_ids.contains(&id) {
            return; // Already parked, waiting on a dependency
        }
        if !self.gated.is_empty() && self.gated.contains_key(&id) {
            return; // Permanently quarantined
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

    /// One step of the apply worklist: park `node` on its first missing dep,
    /// or apply it and push any orphans waiting on it onto `queue`.
    fn park_or_dispatch(&mut self, id: Id, node: HashNode, queue: &mut Vec<(Id, HashNode)>) {
        let missing = node
            .iter_refs()
            .find(|d| !self.contains_node(d))
            .copied();
        if let Some(missing) = missing {
            self.orphan_ids.insert(id);
            self.orphaned.entry(missing).or_default().push((id, node));
            return;
        }
        if !self.orphan_ids.is_empty() {
            self.orphan_ids.remove(&id);
        }

        // The apply-time gate: ops this projection does not admit are
        // quarantined before touching tips or the index. They never intern,
        // so dependents stay parked (the correct edge-table semantics).
        let admitted = matches!(
            &node.op,
            Op::Insert {
                payload: Payload::Char(_),
                ..
            } | Op::Remove(_)
        );
        if !admitted {
            self.gated.insert(id, node);
            return;
        }

        // Update tips before consuming node (insert ops don't depend on tips)
        for tip in node.iter_refs() {
            self.tips.remove(tip);
        }
        self.tips.insert(id);

        match node.op {
            Op::Insert {
                at: Anchor::After(anchor),
                payload: Payload::Char(ch),
            } => self.insert_after(
                id,
                CausalInsert {
                    pins: node.pins,
                    anchor,
                    ch,
                },
            ),
            Op::Insert {
                at: Anchor::Before(anchor),
                payload: Payload::Char(ch),
            } => self.insert_before(
                id,
                CausalInsert {
                    pins: node.pins,
                    anchor,
                    ch,
                },
            ),
            Op::Remove(nodes) => self.apply_remove(id, node.pins, nodes),
            _ => unreachable!("gated above"),
        }

        // Wake the orphans waiting on this id.
        if !self.orphaned.is_empty()
            && let Some(waiting) = self.orphaned.remove(&id)
        {
            queue.extend(waiting);
        }
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

    pub fn merge(&mut self, other: Self) {
        // Simple merge: decompress all nodes from other and apply them
        // The apply function will rebuild runs when possible

        assert_eq!(
            self.origin, other.origin,
            "cannot merge documents with different origins"
        );

        // Covers both After- and Before-anchored runs: decompress reconstructs
        // the anchoring first node for either kind. Ids come from `other`'s
        // id table — no rehashing on the merge path.
        for run in other.runs.values() {
            for (id, node) in run.to_run(&other.ids).decompress_with_ids() {
                self.apply_with_id(id, node);
            }
        }

        for remove_run in other.remove_runs.values() {
            for (i, node) in other.remove_run_nodes(remove_run).into_iter().enumerate() {
                self.apply_with_id(other.id_of(remove_run.links[i]), node);
            }
        }

        for (idx, causal_remove) in &other.remove_nodes {
            let node = HashNode {
                pins: causal_remove.pins.to_id_set(&other.ids),
                op: Op::Remove(
                    causal_remove
                        .nodes
                        .iter()
                        .map(|i| other.id_of(*i))
                        .collect(),
                ),
            };
            self.apply_with_id(other.id_of(*idx), node)
        }

        // Apply all orphaned nodes (ids were computed when they were parked)
        for (id, orphan) in other.orphaned.into_values().flatten() {
            self.apply_with_id(id, orphan);
        }

        // Re-present the other side's quarantined nodes: applying re-gates
        // them here (deterministically), keeping merge lossless.
        for (id, node) in other.gated {
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
    /// If the cursor sits between two causally-related neighbors, it uses
    /// `InsertBefore(right)` so the insert has an explicit ordering constraint and
    /// doesn't get hash-ordered into a fork. Otherwise it uses `InsertAfter(left)`.
    /// At the start of a non-empty sequence, returns a `Before(id_at(0))` cursor.
    /// In an empty sequence, returns an `After(origin)` cursor. Returns `None`
    /// only when `idx` is out of bounds (> len).
    pub fn cursor_at(&self, idx: usize) -> Option<Cursor> {
        if idx > self.len() {
            return None;
        }
        match self.neighbours(idx) {
            (Some(left), Some(right)) => {
                if self.is_causally_before(left, right) {
                    let anchor = self.id_of(right);
                    Some(Cursor::Before {
                        extra_deps: self.tips_minus(&anchor),
                        anchor,
                    })
                } else {
                    let anchor = self.id_of(left);
                    Some(Cursor::After {
                        extra_deps: self.tips_minus(&anchor),
                        anchor,
                    })
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
        let pos = pos & ((1 << 30) - 1); // pos is a within-run offset (30 bits)
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

    #[test]
    fn test_prop_associative_qc1() {
        // ([(true, 0, '\u{0}'), (true, 0, '\u{0}')], [], [(true, 0, '\u{3}')])

        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        seq_a.insert(0, 'a');
        seq_a.insert(0, 'a');

        seq_b.insert(0, 'b');

        let mut ab = seq_a.clone();
        ab.merge(seq_b.clone());

        let mut ba = seq_b.clone();
        ba.merge(seq_a.clone());

        assert_eq!(ab, ba);
    }

    #[test]
    fn test_prop_commutative_qc1() {
        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        seq_a.insert(0, 'a');
        seq_a.remove(0);
        assert_eq!(String::from_iter(seq_a.iter()), "");

        seq_b.insert(0, 'a');
        seq_b.insert(0, 'b');
        assert_eq!(String::from_iter(seq_b.iter()), "ba");

        // merge(a, b) == merge(b, a)

        let mut merge_a_b = seq_a.clone();
        merge_a_b.merge(seq_b.clone());
        let mut merge_b_a = seq_b.clone();
        merge_b_a.merge(seq_a.clone());

        assert_eq!(merge_a_b, merge_b_a);
    }

    #[test]
    fn test_prop_commutative_insert_remove() {
        // Failing case: a = [], b = [(true, 0, '\0'), (false, 0, '\0')]
        let seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        // seq_a is empty

        // seq_b: insert then remove
        seq_b.insert(0, '\0');
        seq_b.remove(0);

        // merge(a, b) == merge(b, a)
        let mut merge_a_b = seq_a.clone();
        merge_a_b.merge(seq_b.clone());
        let mut merge_b_a = seq_b.clone();
        merge_b_a.merge(seq_a.clone());

        assert_eq!(merge_a_b, merge_b_a);
    }

    #[test]
    fn test_prop_commutative_two_inserts() {
        // Failing case: a = [(true, 0, '\0'), (true, 1, '\0')], b = []
        let mut seq_a = HashSeq::default();
        let seq_b = HashSeq::default();

        // seq_a: two inserts
        seq_a.insert(0, '\0');
        seq_a.insert(1, '\0');

        // seq_b is empty

        // merge(a, b) == merge(b, a)
        let mut merge_a_b = seq_a.clone();
        merge_a_b.merge(seq_b.clone());

        let mut merge_b_a = seq_b.clone();
        merge_b_a.merge(seq_a.clone());

        assert_eq!(merge_a_b, merge_b_a);
    }

    #[test]
    fn test_prop_commutative_four_inserts() {
        // Failing case: a = [], b = [(true, 0, '\0'), (true, 1, '\0'), (true, 1, '\0'), (true, 2, '\0')]
        let seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        // seq_b: four inserts
        seq_b.insert(0, '\0');
        seq_b.insert(1, '\0');
        seq_b.insert(1, '\0');
        seq_b.insert(2, '\0');

        // seq_a is empty

        // merge(a, b) == merge(b, a)
        let mut merge_a_b = seq_a.clone();
        merge_a_b.merge(seq_b.clone());

        let mut merge_b_a = seq_b.clone();
        merge_b_a.merge(seq_a.clone());

        assert_eq!(merge_a_b, merge_b_a);
    }

    #[test]
    fn test_prop_commutative_insert_insert_remove() {
        // Failing case: a = [], b = [(true, 0, '\0'), (true, 1, '\0'), (false, 0, '\0')]
        let seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        // seq_b: insert at 0, insert at 1, remove at 0
        seq_b.insert(0, '\0');
        seq_b.insert(1, '\0');
        seq_b.remove(0);

        // seq_a is empty

        // merge(a, b) == merge(b, a)
        let mut merge_a_b = seq_a.clone();
        merge_a_b.merge(seq_b.clone());

        let mut merge_b_a = seq_b.clone();
        merge_b_a.merge(seq_a.clone());

        assert_eq!(merge_a_b, merge_b_a);
    }

    #[quickcheck]
    fn prop_reflexive(ops: Vec<(bool, u8, char)>) {
        let seq = seq_from_ops(&ops);

        // merge(a, a) == a
        let mut merge_self = seq.clone();
        merge_self.merge(seq.clone());

        assert_eq!(merge_self, seq);
    }

    #[test]
    fn test_reflexive_merge_with_remove() {
        // Failing case: [(true, 0, '\0'), (true, 1, '\u{80}'), (true, 2, '\0'), (false, 0, '\0'), (true, 1, '\0')]
        let mut seq = HashSeq::default();

        seq.insert(0, '\0');
        seq.insert(1, '\u{80}');
        seq.insert(2, '\0');
        seq.remove(0);
        seq.insert(1, '\0');

        // merge(a, a) == a
        let mut merge_self = seq.clone();
        merge_self.merge(seq.clone());

        assert_eq!(merge_self, seq);
    }

    #[test]
    fn test_reflexive_regression() {
        // Regression test from quickcheck failure:
        // [(true, 0, '\0'), (true, 1, '\0'), (false, 0, '\0'), (true, 1, '\0')]
        let mut seq = HashSeq::default();

        seq.insert(0, 'a'); // op 1: idx=0, len=0 -> insert at 0
        seq.insert(1, 'b'); // op 2: idx=1, len=1 -> insert at 1
        seq.remove(0); // op 3: idx=0, len=2 -> remove at 0
        seq.insert(1, 'c'); // op 4: idx=1, len=1 -> insert at 1

        // merge(a, a) == a
        let mut merge_self = seq.clone();
        merge_self.merge(seq.clone());

        assert_eq!(merge_self, seq);
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

        // idx 1 sits between 'h' and 'i', which are causally related (run chain).
        // Should be Before(right) so the insert lands deterministically between them.
        match seq.cursor_at(1).expect("cursor at idx 1") {
            Cursor::Before { anchor, extra_deps } => {
                assert_eq!(Some(anchor), seq.id_at(1));
                assert!(!extra_deps.contains(&anchor));
            }
            other => panic!(
                "expected Before cursor at idx 1 (causally related neighbors), got {other:?}"
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

        // Place cursor between "hello" and " world". The neighbors ('o' and ' ')
        // are causally related, so cursor_at picks Before(' ').
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

    /// Regression: inserting between causally-related neighbors via the cursor
    /// model must use InsertBefore — not InsertAfter — to avoid hash-determined
    /// fork ordering. Without this, A's run could end up after the existing run
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
}
