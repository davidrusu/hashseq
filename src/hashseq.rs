use std::collections::{BTreeMap, BTreeSet, HashSet};

use rustc_hash::{FxHashMap, FxHashSet};

use crate::run_index::{ElemRef, RunIndex};
use crate::{EncodableOp, FirstOp, HashNode, HashSeqIter, Id, Op, Run};

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

/// Where an applied node lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Loc {
    /// Element of an insert run: (run head, position within the run).
    Run { run: NodeIdx, pos: u32 },
    /// Root insert (stored in `root_nodes`).
    Root,
    /// Link in a remove chain: (chain head, position within the chain).
    RemoveChain { chain: NodeIdx, pos: u32 },
    /// Multi-target remove (stored in `remove_nodes`).
    MultiRemove,
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
    /// Insert into an empty sequence (the first char becomes an `InsertRoot`).
    Root { extra_deps: BTreeSet<Id> },
    /// Insert immediately after `anchor`.
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
        let (extra_dependencies, op) = match self {
            Cursor::Root { extra_deps } => (extra_deps, Op::InsertRoot(ch)),
            Cursor::After { anchor, extra_deps } => (extra_deps, Op::InsertAfter(anchor, ch)),
            Cursor::Before { anchor, extra_deps } => (extra_deps, Op::InsertBefore(anchor, ch)),
        };
        HashNode {
            extra_dependencies,
            op,
        }
    }

    /// Build a `Run` starting at this cursor with `first` as its first character.
    ///
    /// Returns `None` for a `Root` cursor: runs are anchored ops, while the first
    /// char of an empty sequence is a standalone `InsertRoot` (apply it via
    /// `first_node` instead).
    pub fn into_run(self, first: char) -> Option<Run> {
        match self {
            Cursor::Root { .. } => None,
            Cursor::After { anchor, extra_deps } => Some(Run::new(anchor, extra_deps, first)),
            Cursor::Before { anchor, extra_deps } => {
                Some(Run::new_before(anchor, extra_deps, first))
            }
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CausalInsert {
    pub extra_dependencies: BTreeSet<Id>,
    pub anchor: Id,
    pub ch: char,
}

/// Storage form of a multi-target remove (`remove_batch` spanning several
/// chars). Single-target removes live in `RemoveRun` chains instead.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CausalRemove {
    pub extra_dependencies: BTreeSet<Id>,
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
    pub first_extra_deps: BTreeSet<Id>,
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
    pub first_extra_deps: BTreeSet<Id>,
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

    fn extend(&mut self, idx: NodeIdx, ch: char) {
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
        StoredRun {
            anchor: right_anchor,
            first_op: FirstOp::After,
            first_extra_deps: BTreeSet::new(),
            text: right_text,
            elements: right_elements,
        }
    }

    /// Reconstruct the wire-level run (recomputes element ids by hashing).
    pub fn to_run(&self) -> Run {
        Run::from_text(
            self.anchor,
            self.first_op,
            self.first_extra_deps.clone(),
            &self.text,
        )
        .expect("stored runs are never empty")
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CausalRoot {
    pub extra_dependencies: BTreeSet<Id>,
    pub ch: char,
}

#[derive(Debug, Default, Clone)]
pub struct HashSeq {
    // ---- id <-> handle interning: the only Id-keyed lookup structure ----
    id_to_idx: IdIndex,
    /// NodeIdx -> Id (append-only).
    pub ids: Vec<Id>,
    /// NodeIdx -> location.
    pub locs: Vec<Loc>,
    /// NodeIdx -> tombstone.
    pub removed: Vec<bool>,

    // All inserts except roots live in runs: sequential typing extends a run,
    // and a lone insert is just a 1-char run. A run is After- or Before-anchored
    // (`StoredRun::first_op`); subsequent elements always chain InsertAfter.
    pub runs: FxHashMap<NodeIdx, StoredRun>,
    /// Root inserts, Id-ordered (root order is a convergence concern).
    pub root_nodes: BTreeMap<Id, CausalRoot>,
    /// Reverse index: anchor -> heads of Before-runs anchored at it. Values are
    /// Id-ordered: sibling order is a convergence concern, so it must not use
    /// replica-local handles.
    pub befores_by_anchor: FxHashMap<NodeIdx, BTreeSet<Id>>,
    /// Multi-target removes only; single-target removes coalesce into chains.
    pub remove_nodes: FxHashMap<NodeIdx, CausalRemove>,
    /// Chained single-target removes (backspace/delete bursts), keyed by the
    /// first remove's handle.
    pub remove_runs: FxHashMap<NodeIdx, RemoveRun>,
    /// Fork tracking: anchor -> ids that fork from it (Id-ordered, see
    /// `befores_by_anchor`).
    pub afters: FxHashMap<NodeIdx, BTreeSet<Id>>,

    pub(crate) tips: BTreeSet<Id>,
    // orphaned uses HashNode as key (not Id), so keep std HashSet — the input is
    // adversary-controllable and benefits from SipHash's HashDoS protection.
    pub(crate) orphaned: HashSet<HashNode>,
    index: RunIndex,
}

impl PartialEq for HashSeq {
    fn eq(&self, other: &Self) -> bool {
        self.tips == other.tips
    }
}

impl Eq for HashSeq {}

impl HashSeq {
    // ---- interning ----

    fn next_idx(&self) -> NodeIdx {
        NodeIdx(self.ids.len() as u32)
    }

    fn intern(&mut self, id: Id, loc: Loc) -> NodeIdx {
        let idx = self.next_idx();
        self.ids.push(id);
        self.locs.push(loc);
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
        self.locs[idx.0 as usize]
    }

    pub fn is_removed(&self, idx: NodeIdx) -> bool {
        self.removed[idx.0 as usize]
    }

    /// Check if a node ID exists (insert, remove, or root) — one map probe.
    pub fn contains_node(&self, id: &Id) -> bool {
        self.idx_of(id).is_some()
    }

    pub(crate) fn char_at(&self, idx: NodeIdx) -> char {
        match self.loc_of(idx) {
            Loc::Run { run, pos } => self.runs[&run].char_at(pos as usize),
            Loc::Root => self.root_nodes[self.id_ref(idx)].ch,
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

    pub fn orphans(&self) -> &HashSet<HashNode> {
        &self.orphaned
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
        explicit
            .into_iter()
            .flatten()
            .map(|id| self.idx_of_known(id))
            .chain(from_run)
    }

    /// Before-run heads anchored at `idx`, in Id order.
    pub(crate) fn befores_of(&self, idx: NodeIdx) -> impl DoubleEndedIterator<Item = NodeIdx> + '_ {
        self.befores_by_anchor
            .get(&idx)
            .into_iter()
            .flatten()
            .map(|id| self.idx_of_known(id))
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

    /// The index addresses an insert as (span head, element offset): a run
    /// element is addressed through its run, a root through itself.
    fn elem_ref(&self, idx: NodeIdx) -> ElemRef {
        match self.loc_of(idx) {
            Loc::Run { run, pos } => (run, pos),
            Loc::Root => (idx, 0),
            _ => panic!("elem_ref on a non-insert node"),
        }
    }

    /// The insert node at visible position `pos`.
    fn element_at(&self, pos: usize) -> Option<NodeIdx> {
        let (head, off) = self.index.get(pos)?;
        Some(match self.loc_of(head) {
            Loc::Root => head,
            _ => self.runs[&head].elements[off as usize],
        })
    }

    /// First element, in document order, of the region rooted at `n`: a node's
    /// before-runs precede it, recursively.
    fn region_first(&self, mut n: NodeIdx) -> NodeIdx {
        while let Some(first) = self.befores_by_anchor.get(&n).and_then(|s| s.first()) {
            n = self.idx_of_known(first);
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
                Some(id) => n = self.idx_of_known(id),
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
                extra_dependencies: BTreeSet::new(),
                op: Op::InsertAfter(prev_id, ch),
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

        let extra_dependencies = BTreeSet::from_iter(self.tips.difference(&to_remove).cloned());
        let op = Op::Remove(to_remove);

        let node = HashNode {
            extra_dependencies,
            op,
        };

        let node_for_return = node.clone();
        self.apply(node);
        Some(node_for_return)
    }

    fn any_missing_dependencies<'a>(&self, deps: impl IntoIterator<Item = &'a Id>) -> bool {
        for dep in deps {
            if !self.contains_node(dep) {
                return true;
            }
        }

        false
    }

    fn insert_root(&mut self, root_id: Id, root: CausalRoot) {
        let idx = self.intern(root_id, Loc::Root);
        // Roots sit in Id order at the top level, each followed by its
        // subtree: the new root's region starts directly before the next
        // biggest root's region (removed or not — its region still occupies
        // positions). With no bigger root, it goes at the very end.
        match self.root_nodes.range(root_id..).next() {
            Some((next_root, _)) => {
                let first = self.region_first(self.idx_of_known(next_root));
                let at = self.elem_ref(first);
                self.index.insert_span_before(at, idx);
            }
            None => self.index.push_span_back(idx),
        }
        self.root_nodes.insert(root_id, root);
    }

    fn insert_after(&mut self, id: Id, after: CausalInsert) {
        // The anchor is a checked dependency, so it is interned.
        let anchor = self.idx_of_known(&after.anchor);

        // Fast path: extend the run whose tail is the anchor.
        if after.extra_dependencies.is_empty()
            && let Loc::Run { run, pos } = self.loc_of(anchor)
        {
            // Check for explicit forks first (cheap u32-keyed lookup)
            let has_explicit_afters = self.afters.get(&anchor).is_some_and(|ns| !ns.is_empty());

            if !has_explicit_afters && pos as usize + 1 == self.runs[&run].len() {
                // Run extension - most common case for sequential typing
                let idx = self.intern(id, Loc::Run { run, pos: pos + 1 });
                self.runs.get_mut(&run).unwrap().extend(idx, after.ch);
                self.index.extend_run(run, pos + 1);
                return;
            }
        }

        // Slow path: this insert forks. Find the smallest afters node >= id.
        // Explicit-afters case: O(log n) range seek into the BTreeSet.
        // Run-fallback case: at most one candidate, just check it.
        let next_node = if let Some(siblings) = self.afters.get(&anchor) {
            siblings
                .range(&id..)
                .next()
                .map(|aid| self.idx_of_known(aid))
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
        self.runs.insert(
            idx,
            StoredRun {
                anchor: after.anchor,
                first_op: FirstOp::After,
                first_extra_deps: after.extra_dependencies,
                text: after.ch.to_string(),
                elements: vec![idx],
            },
        );

        // run extension is handled in the fast path above, fork/split updates the afters set
        self.afters.entry(anchor).or_default().insert(id);

        // Resolve the target element only now: the split above may have
        // relocated it into the right-hand run.
        let (el, before) = target;
        let at = self.elem_ref(el);
        if before {
            self.index.insert_span_before(at, idx);
        } else {
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
            };
        }

        let right_head_id = self.ids[right_head.0 as usize];
        self.runs.insert(right_head, right_run);
        self.index.split_run(run, at as u32, right_head);
        // Track the split in afters so iteration can find the right portion
        self.afters
            .entry(left_last)
            .or_default()
            .insert(right_head_id);
        right_head
    }

    fn apply_remove(&mut self, id: Id, extra_deps: BTreeSet<Id>, target_ids: BTreeSet<Id>) {
        // Targets are checked dependencies of the remove, so they are interned.
        // (A remove targeting a non-insert node is harmless: it's not in the
        // position index, and its tombstone bit is inert.)
        let targets: Vec<NodeIdx> = target_ids.iter().map(|t| self.idx_of_known(t)).collect();
        for t in &targets {
            match self.loc_of(*t) {
                Loc::Run { run, pos } => {
                    self.index.remove_element((run, pos));
                }
                Loc::Root => {
                    self.index.remove_element((*t, 0));
                }
                _ => {} // removes targeting non-inserts are inert
            }
            self.removed[t.0 as usize] = true;
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
            self.remove_runs.insert(
                idx,
                RemoveRun {
                    first_extra_deps: extra_deps,
                    targets: vec![target],
                    links: vec![idx],
                },
            );
            return;
        }

        let idx = self.intern(id, Loc::MultiRemove);
        self.remove_nodes.insert(
            idx,
            CausalRemove {
                extra_dependencies: extra_deps,
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
        // (O(log n) range seek into the BTreeSet, no tombstone filtering:
        // removed elements still anchor their regions.)
        let target = match self
            .befores_by_anchor
            .get(&anchor)
            .and_then(|s| s.range(id..).next())
        {
            Some(next) => self.region_first(self.idx_of_known(next)),
            None => anchor,
        };

        // The anchor may sit mid-run: no split is needed. Iteration visits the
        // befores of every run element individually (see HashSeqIter), and unlike
        // an after-fork there is no sibling ordering to resolve — a Before-run
        // always lands immediately before its anchor.
        let idx = self.next_idx();
        self.intern(id, Loc::Run { run: idx, pos: 0 });
        self.runs.insert(
            idx,
            StoredRun {
                anchor: before.anchor,
                first_op: FirstOp::Before,
                first_extra_deps: before.extra_dependencies,
                text: before.ch.to_string(),
                elements: vec![idx],
            },
        );

        self.befores_by_anchor.entry(anchor).or_default().insert(id);

        let at = self.elem_ref(target);
        self.index.insert_span_before(at, idx);
    }

    pub fn apply(&mut self, node: HashNode) {
        let id = node.id();
        self.apply_with_id(id, node);
    }

    /// Apply a node with a pre-computed ID (avoids double hashing)
    fn apply_with_id(&mut self, id: Id, node: HashNode) {
        if self.contains_node(&id) {
            return; // Already processed this node
        }

        if self.any_missing_dependencies(node.iter_dependencies()) {
            self.orphaned.insert(node);
            return;
        }

        // Update tips before consuming node (insert ops don't depend on tips)
        for tip in node.iter_dependencies() {
            self.tips.remove(tip);
        }
        self.tips.insert(id);

        match node.op {
            Op::InsertRoot(ch) => self.insert_root(
                id,
                CausalRoot {
                    extra_dependencies: node.extra_dependencies,
                    ch,
                },
            ),
            Op::InsertAfter(anchor, ch) => self.insert_after(
                id,
                CausalInsert {
                    extra_dependencies: node.extra_dependencies,
                    anchor,
                    ch,
                },
            ),
            Op::InsertBefore(anchor, ch) => self.insert_before(
                id,
                CausalInsert {
                    extra_dependencies: node.extra_dependencies,
                    anchor,
                    ch,
                },
            ),
            Op::Remove(nodes) => self.apply_remove(id, node.extra_dependencies, nodes),
        }

        for orphan in std::mem::take(&mut self.orphaned) {
            self.apply(orphan);
        }
    }

    /// Reconstruct a remove chain's `HashNode`s (for merge / re-broadcast).
    pub fn remove_run_nodes(&self, rr: &RemoveRun) -> Vec<HashNode> {
        rr.targets
            .iter()
            .enumerate()
            .map(|(i, target)| HashNode {
                extra_dependencies: if i == 0 {
                    rr.first_extra_deps.clone()
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

        for (id, root) in &other.root_nodes {
            let node = HashNode {
                extra_dependencies: root.extra_dependencies.clone(),
                op: Op::InsertRoot(root.ch),
            };
            debug_assert_eq!(*id, node.id());
            self.apply(node)
        }

        // Covers both After- and Before-anchored runs: decompress reconstructs
        // the anchoring first node for either kind.
        for run in other.runs.values() {
            for node in run.to_run().decompress() {
                self.apply(node);
            }
        }

        for remove_run in other.remove_runs.values() {
            for (i, node) in other.remove_run_nodes(remove_run).into_iter().enumerate() {
                debug_assert_eq!(node.id(), other.id_of(remove_run.links[i]));
                self.apply(node);
            }
        }

        for (idx, causal_remove) in &other.remove_nodes {
            let node = HashNode {
                extra_dependencies: causal_remove.extra_dependencies.clone(),
                op: Op::Remove(
                    causal_remove
                        .nodes
                        .iter()
                        .map(|i| other.id_of(*i))
                        .collect(),
                ),
            };
            debug_assert_eq!(other.id_of(*idx), node.id());
            self.apply(node)
        }

        // Apply all orphaned nodes
        for orphan in other.orphaned {
            self.apply(orphan);
        }
    }

    pub fn iter_ids(&self) -> HashSeqIter<'_> {
        HashSeqIter::new(self)
    }

    pub(crate) fn iter_idxs(&self) -> impl Iterator<Item = NodeIdx> + '_ {
        crate::hashseq_iter::HashSeqIdxIter::new(self)
    }

    pub fn iter(&self) -> impl Iterator<Item = char> + '_ {
        self.iter_idxs().map(|idx| self.char_at(idx))
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
            Loc::Root => (idx, 0),
            _ => return None, // remove nodes have no position
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
    /// In an empty sequence, returns a `Root` cursor. Returns `None` only when
    /// `idx` is out of bounds (> len).
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
            (None, None) => Some(Cursor::Root {
                extra_deps: self.tips.clone(),
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
                extra_dependencies: BTreeSet::new(),
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

        // Verify the structure is as expected:
        // - Should have 1 root node for 'a'
        assert_eq!(
            seq_with_abcd.root_nodes.len(),
            1,
            "Should have 1 individual node (root 'a')"
        );

        // - Should have 1 run containing "bcd"
        assert_eq!(seq_with_abcd.runs.len(), 1, "Should have 1 run");
        let run = seq_with_abcd.runs.values().next().unwrap();
        assert_eq!(run.text, "bcd", "Run should contain 'bcd'");

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
        assert_eq!(seq1.root_nodes, seq2.root_nodes);
        assert_eq!(seq1.befores_by_anchor, seq2.befores_by_anchor);
        assert_eq!(seq1.remove_nodes, seq2.remove_nodes);
        assert_eq!(seq1.tips, seq2.tips);

        true
    }

    #[test]
    fn test_run_creation() {
        let mut seq = HashSeq::default();

        // Single characters should create individual nodes
        seq.insert(0, 'x');
        assert_eq!(seq.runs.len(), 0);
        assert_eq!(seq.root_nodes.len(), 1);

        // Multi-character batch should create a run
        seq.insert_batch(1, "abc".chars());
        assert_eq!(seq.runs.len(), 1);
        assert_eq!(seq.root_nodes.len(), 1);

        // Verify the run contains the right data
        let run = seq.runs.values().next().unwrap();
        assert_eq!(run.text, "abc");

        // Verify the final string
        assert_eq!(&seq.iter().collect::<String>(), "xabc");
    }

    #[test]
    fn test_run_memory_efficiency() {
        let mut seq = HashSeq::default();

        // Create a long sequence using batch insert
        let long_string = "The quick brown fox jumps over the lazy dog. ".repeat(10);
        seq.insert_batch(0, long_string.chars());

        // Should create one run
        assert_eq!(seq.runs.len(), 1);
        assert_eq!(seq.root_nodes.len(), 1);

        let run = seq.runs.values().next().unwrap();
        assert_eq!(run.len(), long_string.len() - 1); // First char becomes a root (not included in run)

        // Verify content
        assert_eq!(seq.iter().collect::<String>(), long_string);
    }

    #[test]
    fn test_concurrent_inserts() {
        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        seq_a.insert_batch(0, "we wrote".chars());
        seq_b.insert_batch(0, "this together ".chars());

        seq_a.merge(seq_b);

        assert_eq!(&seq_a.iter().collect::<String>(), "this together we wrote");
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
        assert_eq!(merged, "aaabc");
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
            op: Op::InsertRoot('b'),
            extra_dependencies: BTreeSet::default(),
        };

        seq.apply(HashNode {
            op: Op::InsertAfter(insert.id(), 'a'),
            extra_dependencies: BTreeSet::default(),
        });

        assert_eq!(seq.orphans().len(), 1);
        assert_eq!(seq.len(), 0);

        seq.apply(HashNode {
            op: Op::InsertBefore(insert.id(), 'a'),
            extra_dependencies: BTreeSet::default(),
        });

        assert_eq!(seq.orphans().len(), 2);
        assert_eq!(seq.len(), 0);

        seq.apply(insert);

        assert_eq!(seq.orphans().len(), 0);
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
            op: Op::InsertRoot('a'),
            extra_dependencies: BTreeSet::new(),
        };

        seq.apply(HashNode {
            op: Op::Remove(BTreeSet::from_iter([insert.id()])),
            extra_dependencies: BTreeSet::new(),
        });

        assert_eq!(seq.orphans().len(), 1);
        seq.apply(insert);
        assert_eq!(seq.orphans().len(), 0);
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

    /// The position index must agree with the document iterator — the
    /// iterator is the source of truth for order. Checked on a merged seq
    /// (merging is what creates sibling forks) plus more local edits on top.
    fn check_index_matches_iter(seq: &HashSeq) {
        let iter_ids: Vec<Id> = seq.iter_ids().copied().collect();
        assert_eq!(seq.len(), iter_ids.len());
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

    /// Regression: a bigger-id sibling applied after a smaller visible sibling
    /// used to land at `find(anchor)+1` in the index, before the smaller
    /// sibling — while the iterator orders siblings ascending by id.
    #[test]
    fn index_orders_concurrent_siblings_like_the_iterator() {
        let root = HashNode {
            extra_dependencies: BTreeSet::new(),
            op: Op::InsertRoot('a'),
        };
        let a = root.id();
        for (c1, c2) in [('b', 'c'), ('x', 'y')] {
            let n1 = HashNode {
                extra_dependencies: BTreeSet::new(),
                op: Op::InsertAfter(a, c1),
            };
            let n2 = HashNode {
                extra_dependencies: BTreeSet::new(),
                op: Op::InsertAfter(a, c2),
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
            extra_dependencies: BTreeSet::new(),
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
        seq.insert(0, 'a'); // This is a root, not in runs
        seq.insert(1, 'b'); // This starts a run
        seq.insert(2, 'c'); // This extends the run

        // First character is a root, remaining two should be in a single run
        assert_eq!(seq.root_nodes.len(), 1);
        assert_eq!(seq.runs.len(), 1);
        let run = seq.runs.values().next().unwrap();
        assert_eq!(run.len(), 2);
        assert_eq!(run.text, "bc");
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
            matches!(seq.cursor_at(0), Some(Cursor::Root { .. })),
            "empty seq at 0 yields a Root cursor"
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
            extra_dependencies: BTreeSet::new(),
            op: Op::InsertAfter(seq_a.id_at(0).unwrap(), 'y'),
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
        let mut run = cursor.into_run(',').unwrap();
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

        let mut run = cursor.into_run(' ').unwrap();
        for ch in "mighty".chars() {
            run.extend(ch);
        }
        seq.apply_op(EncodableOp::Run(run));

        // The crucial assertion: the new run lands between "hello" and " world",
        // deterministically — not after " world" via hash ordering.
        assert_eq!(String::from_iter(seq.iter()), "hello mighty world");
    }
}
