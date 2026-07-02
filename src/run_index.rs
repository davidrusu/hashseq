//! Run-granular positional index: an order-statistics treap whose nodes are
//! *fragments* of insert runs (or single root elements), replacing the
//! per-visible-char `AssociativePositionalList`.
//!
//! A run enters the index as one fragment and is fragmented only when another
//! span lands strictly inside it (e.g. a Before-run anchored mid-run) — the
//! `StoredRun` itself is never split by the index. Fragment count is therefore
//! O(runs), not O(chars).
//!
//! Each fragment carries a visibility bitmap (1 bit per element; set =
//! visible) giving rank/select for visible-offset ↔ element-offset mapping,
//! and every treap node caches its subtree's visible count, so:
//!
//! - `get(pos)`: descend by counts, then bitmap-select — O(log F + frag words)
//! - `position_of`: bitmap-rank, then prefix-sum up the tree — O(log F)
//! - inserts/removes: bit flip + count updates along one root path — O(log F)
//!
//! Everything here is replica-local bookkeeping (handle space); nothing in
//! this module participates in convergence.

use rustc_hash::FxHashMap;

use crate::hashseq::NodeIdx;

const NIL: u32 = u32::MAX;

/// Visibility bitmap of one fragment. Median runs are a handful of chars, so
/// fragments up to 64 elements stay inline.
#[derive(Debug, Clone)]
enum Bits {
    Small(u64),
    Large(Vec<u64>),
}

#[derive(Debug, Clone)]
struct Frag {
    left: u32,
    right: u32,
    parent: u32,
    /// Treap heap priority (max-heap).
    prio: u32,
    /// Owning span: run head handle, or the root element's own handle.
    head: NodeIdx,
    /// Element offset within the run where this fragment starts.
    start: u32,
    /// Element count covered by this fragment.
    len: u32,
    /// Visible (bit-set) element count in this fragment.
    visible: u32,
    /// Visible count of the whole subtree rooted here (incl. self).
    subtree: usize,
    bits: Bits,
}

impl Frag {
    fn bit(&self, k: u32) -> bool {
        match &self.bits {
            Bits::Small(w) => w >> k & 1 == 1,
            Bits::Large(ws) => ws[k as usize / 64] >> (k % 64) & 1 == 1,
        }
    }

    fn clear_bit(&mut self, k: u32) {
        match &mut self.bits {
            Bits::Small(w) => *w &= !(1 << k),
            Bits::Large(ws) => ws[k as usize / 64] &= !(1u64 << (k % 64)),
        }
    }

    fn set_bit(&mut self, k: u32) {
        match &mut self.bits {
            Bits::Small(w) => *w |= 1 << k,
            Bits::Large(ws) => ws[k as usize / 64] |= 1u64 << (k % 64),
        }
    }

    /// Append one visible element at the end of the fragment.
    fn push_visible(&mut self) {
        let k = self.len;
        if let Bits::Small(w) = self.bits
            && k == 64
        {
            self.bits = Bits::Large(vec![w]);
        }
        match &mut self.bits {
            Bits::Small(w) => *w |= 1 << k,
            Bits::Large(ws) => {
                if k as usize / 64 == ws.len() {
                    ws.push(0);
                }
                ws[k as usize / 64] |= 1 << (k % 64);
            }
        }
        self.len += 1;
        self.visible += 1;
    }

    /// Count of visible elements strictly before element offset `k`.
    fn rank(&self, k: u32) -> u32 {
        match &self.bits {
            Bits::Small(w) => (w & low_mask(k)).count_ones(),
            Bits::Large(ws) => {
                let full = k as usize / 64;
                let mut r: u32 = ws[..full].iter().map(|w| w.count_ones()).sum();
                r += (ws[full] & low_mask(k % 64)).count_ones();
                r
            }
        }
    }

    /// Element offset of the `r`-th (0-based) visible element. `r < visible`.
    fn select(&self, r: u32) -> u32 {
        match &self.bits {
            Bits::Small(w) => select_in_word(*w, r),
            Bits::Large(ws) => {
                let mut r = r;
                for (i, w) in ws.iter().enumerate() {
                    let c = w.count_ones();
                    if r < c {
                        return i as u32 * 64 + select_in_word(*w, r);
                    }
                    r -= c;
                }
                unreachable!("select past visible count")
            }
        }
    }

    /// Split off elements `[k..len)` into a returned bitmap; keeps `[0..k)`.
    fn split_bits(&mut self, k: u32) -> (Bits, u32, u32) {
        let right_len = self.len - k;
        let right_bits = match &mut self.bits {
            Bits::Small(w) => {
                let right = *w >> k;
                *w &= low_mask(k);
                Bits::Small(right)
            }
            Bits::Large(ws) => {
                let mut right = Vec::with_capacity(right_len as usize / 64 + 1);
                for i in 0..right_len {
                    let src = k + i;
                    if i % 64 == 0 {
                        right.push(0u64);
                    }
                    if ws[src as usize / 64] >> (src % 64) & 1 == 1 {
                        *right.last_mut().unwrap() |= 1 << (i % 64);
                    }
                }
                for j in k..self.len {
                    ws[j as usize / 64] &= !(1u64 << (j % 64));
                }
                ws.truncate(k as usize / 64 + 1);
                Bits::Large(right)
            }
        };
        let right_visible = self.visible - self.rank(k);
        self.len = k;
        self.visible -= right_visible;
        (right_bits, right_len, right_visible)
    }
}

fn low_mask(k: u32) -> u64 {
    if k >= 64 { u64::MAX } else { (1u64 << k) - 1 }
}

fn pack(v: Vec<u32>) -> FragsOf {
    if let [one] = v[..] {
        FragsOf::One(one)
    } else {
        FragsOf::Many(v)
    }
}

fn select_in_word(mut w: u64, r: u32) -> u32 {
    let mut r = r;
    let mut off = 0;
    loop {
        let tz = w.trailing_zeros();
        if r == 0 {
            return off + tz;
        }
        r -= 1;
        w >>= tz + 1;
        off += tz + 1;
    }
}

/// Where a run's fragments live in the treap arena, sorted by `start`. Almost
/// every run is a single fragment.
#[derive(Debug, Clone)]
enum FragsOf {
    One(u32),
    Many(Vec<u32>),
}

/// Where a fresh span or a moved element's destination fragment lands.
/// HashSeq resolves causal placement targets — which may be rendered move
/// ops — into one of these.
#[derive(Debug, Clone, Copy)]
pub(crate) enum IndexTarget {
    /// Directly before this element's base slot.
    BeforeElem(ElemRef),
    /// Directly after this element's base slot.
    AfterElem(ElemRef),
    /// Directly before this moved element's destination fragment.
    BeforeMoved(ElemRef),
    /// Directly after this moved element's destination fragment.
    AfterMoved(ElemRef),
    /// At the very end of the document.
    Back,
}

#[derive(Debug, Clone)]
pub(crate) struct RunIndex {
    frags: Vec<Frag>,
    root: u32,
    frags_of: FxHashMap<NodeIdx, FragsOf>,
    rng: u64,
    /// Rendered relocation (moves): element base ref -> its destination
    /// fragment. The base slot keeps its (cleared) bit for life — the origin
    /// ghost; the destination is a 1-element fragment placed like an insert
    /// sibling. Empty in move-free documents — hot-path checks guard on that.
    moved: FxHashMap<ElemRef, u32>,
    /// Arena slots freed by deleted destination fragments.
    free: Vec<u32>,
}

impl Default for RunIndex {
    fn default() -> Self {
        Self {
            frags: Vec::new(),
            root: NIL,
            frags_of: FxHashMap::default(),
            rng: 0x9E3779B97F4A7C15,
            moved: FxHashMap::default(),
            free: Vec::new(),
        }
    }
}

/// An element reference: (span head, element offset within the run). Root
/// elements are (their own handle, 0).
pub(crate) type ElemRef = (NodeIdx, u32);

/// A fragment as seen by document-order iteration: a contiguous element
/// range of one run plus its visibility bits.
#[derive(Clone, Copy)]
pub(crate) struct FragView<'a> {
    frag: &'a Frag,
}

impl FragView<'_> {
    pub(crate) fn head(&self) -> NodeIdx {
        self.frag.head
    }

    pub(crate) fn start(&self) -> u32 {
        self.frag.start
    }

    pub(crate) fn len(&self) -> u32 {
        self.frag.len
    }

    /// Visibility of the element at fragment-local offset `k`.
    pub(crate) fn visible_at(&self, k: u32) -> bool {
        self.frag.bit(k)
    }

    /// All elements in this fragment are visible (no per-element bit tests
    /// needed by the caller).
    pub(crate) fn fully_visible(&self) -> bool {
        self.frag.visible == self.frag.len
    }
}

/// In-order treap walk — fragments in document order, skipping fragments
/// with no visible elements. No allocation; parent pointers do the climbing.
pub(crate) struct FragsInOrder<'a> {
    index: &'a RunIndex,
    next: u32,
}

impl<'a> Iterator for FragsInOrder<'a> {
    type Item = FragView<'a>;

    fn next(&mut self) -> Option<FragView<'a>> {
        while self.next != NIL {
            let cur = self.next;
            self.next = self.index.successor(cur);
            let frag = &self.index.frags[cur as usize];
            if frag.visible > 0 {
                return Some(FragView { frag });
            }
        }
        None
    }
}

impl RunIndex {
    pub(crate) fn len(&self) -> usize {
        self.subtree(self.root)
    }

    /// Fragments in document order (the in-order traversal of the treap —
    /// verified equal to the causal iterator by `prop_index_matches_iterator`).
    pub(crate) fn frags_in_order(&self) -> FragsInOrder<'_> {
        FragsInOrder {
            index: self,
            next: self.leftmost(self.root),
        }
    }

    fn leftmost(&self, mut n: u32) -> u32 {
        if n == NIL {
            return NIL;
        }
        while self.frags[n as usize].left != NIL {
            n = self.frags[n as usize].left;
        }
        n
    }

    fn successor(&self, mut n: u32) -> u32 {
        let right = self.frags[n as usize].right;
        if right != NIL {
            return self.leftmost(right);
        }
        loop {
            let p = self.frags[n as usize].parent;
            if p == NIL {
                return NIL;
            }
            if self.frags[p as usize].left == n {
                return p;
            }
            n = p;
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Span head + element offset of the visible element at position `pos`.
    pub(crate) fn get(&self, pos: usize) -> Option<ElemRef> {
        if pos >= self.len() {
            return None;
        }
        let mut pos = pos;
        let mut n = self.root;
        loop {
            let f = &self.frags[n as usize];
            let left_count = self.subtree(f.left);
            if pos < left_count {
                n = f.left;
            } else if pos < left_count + f.visible as usize {
                let r = (pos - left_count) as u32;
                return Some((f.head, f.start + f.select(r)));
            } else {
                pos -= left_count + f.visible as usize;
                n = f.right;
            }
        }
    }

    /// Visible position of the element, or `None` if it is removed. A moved
    /// element reports its rendered (destination) position.
    pub(crate) fn position_of(&self, (head, off): ElemRef) -> Option<usize> {
        if !self.moved.is_empty()
            && let Some(&slot) = self.moved.get(&(head, off))
        {
            return Some(self.position_of_slot(slot, 0));
        }
        let slot = self.frag_containing(head, off)?;
        let f = &self.frags[slot as usize];
        let k = off - f.start;
        if !f.bit(k) {
            return None;
        }
        Some(self.position_of_slot(slot, f.rank(k) as usize))
    }

    /// Position of the `local`-th visible element of fragment `slot`.
    fn position_of_slot(&self, slot: u32, local: usize) -> usize {
        let mut pos = self.subtree(self.frags[slot as usize].left) + local;
        let mut cur = slot;
        while self.frags[cur as usize].parent != NIL {
            let p = self.frags[cur as usize].parent;
            let pf = &self.frags[p as usize];
            if cur == pf.right {
                pos += self.subtree(pf.left) + pf.visible as usize;
            }
            cur = p;
        }
        pos
    }

    /// Append one visible element at the run's tail. `off` is the new
    /// element's offset (the run's previous length), for coverage checking.
    pub(crate) fn extend_run(&mut self, head: NodeIdx, off: u32) {
        let slot = match self.frags_of.get(&head) {
            Some(FragsOf::One(s)) => *s,
            Some(FragsOf::Many(v)) => *v.last().unwrap(),
            None => panic!("extend_run on unindexed run"),
        };
        let f = &mut self.frags[slot as usize];
        debug_assert_eq!(
            f.start + f.len,
            off,
            "tail fragment must cover the run tail"
        );
        f.push_visible();
        self.update_to_root(slot);
    }

    /// Insert a fresh 1-element span for `head` at `target`.
    pub(crate) fn insert_span_at(&mut self, target: IndexTarget, head: NodeIdx) {
        let n = self.new_span(head);
        self.attach_at(target, n);
        self.settle(n);
    }

    /// Clear the element's visibility bit. Returns false if already clear
    /// (idempotent, mirroring duplicate concurrent removes).
    pub(crate) fn remove_element(&mut self, (head, off): ElemRef) -> bool {
        let Some(slot) = self.frag_containing(head, off) else {
            return false;
        };
        let f = &mut self.frags[slot as usize];
        let k = off - f.start;
        if !f.bit(k) {
            return false;
        }
        f.clear_bit(k);
        f.visible -= 1;
        self.update_to_root(slot);
        true
    }

    // ---- rendered relocation (moves) ----

    /// Restore the element's visibility bit at its base slot (a placement
    /// returning to the creation placement).
    pub(crate) fn restore_base(&mut self, (head, off): ElemRef) {
        let slot = self
            .frag_containing(head, off)
            .expect("element is indexed");
        let f = &mut self.frags[slot as usize];
        let k = off - f.start;
        debug_assert!(!f.bit(k), "restore_base on a visible element");
        f.set_bit(k);
        f.visible += 1;
        self.update_to_root(slot);
    }

    /// Delete the element's destination fragment (a re-move or a remove of a
    /// moved element). Returns false if the element is not moved-rendered.
    pub(crate) fn remove_moved(&mut self, elem: ElemRef) -> bool {
        if self.moved.is_empty() {
            return false;
        }
        let Some(slot) = self.moved.remove(&elem) else {
            return false;
        };
        self.detach(slot);
        true
    }

    /// Render `elem` (a moved element) at `target` as a 1-element destination
    /// fragment. The caller clears the old rendering first.
    pub(crate) fn place_moved_at(&mut self, target: IndexTarget, elem: ElemRef) {
        let n = self.new_frag(elem.0, elem.1, 1, 1, Bits::Small(1));
        let prev = self.moved.insert(elem, n);
        debug_assert!(prev.is_none(), "old rendering must be cleared first");
        self.attach_at(target, n);
        self.settle(n);
    }

    /// Leaf-attach `n` at `target`.
    fn attach_at(&mut self, target: IndexTarget, n: u32) {
        match target {
            IndexTarget::BeforeElem(at) => {
                let slot = self
                    .frag_containing(at.0, at.1)
                    .expect("anchor element must be indexed");
                let k = at.1 - self.frags[slot as usize].start;
                if k == 0 {
                    self.attach_pred(slot, n);
                } else {
                    self.split_frag(slot, k);
                    self.attach_succ(slot, n);
                }
            }
            IndexTarget::AfterElem(at) => {
                let slot = self
                    .frag_containing(at.0, at.1)
                    .expect("anchor element must be indexed");
                let f = &self.frags[slot as usize];
                let k = at.1 - f.start;
                if k + 1 < f.len {
                    self.split_frag(slot, k + 1);
                }
                self.attach_succ(slot, n);
            }
            IndexTarget::BeforeMoved(other) => {
                let slot = *self.moved.get(&other).expect("target is moved-rendered");
                self.attach_pred(slot, n);
            }
            IndexTarget::AfterMoved(other) => {
                let slot = *self.moved.get(&other).expect("target is moved-rendered");
                self.attach_succ(slot, n);
            }
            IndexTarget::Back => {
                if self.root == NIL {
                    self.root = n;
                    return;
                }
                let mut cur = self.root;
                while self.frags[cur as usize].right != NIL {
                    cur = self.frags[cur as usize].right;
                }
                self.frags[cur as usize].right = n;
                self.frags[n as usize].parent = cur;
            }
        }
    }

    /// Mirror `StoredRun::split_at`: fragments covering `[at..)` now belong to
    /// `right_head`, with offsets rebased. Visible counts are untouched.
    pub(crate) fn split_run(&mut self, head: NodeIdx, at: u32, right_head: NodeIdx) {
        // Make the split point a fragment boundary first.
        if let Some(slot) = self.frag_containing(head, at)
            && self.frags[slot as usize].start < at
        {
            let k = at - self.frags[slot as usize].start;
            self.split_frag(slot, k);
        }
        let mut v = match self.frags_of.remove(&head).expect("run is indexed") {
            FragsOf::One(_) => unreachable!("split point is covered, so >= 2 fragments"),
            FragsOf::Many(v) => v,
        };
        let cut = v.partition_point(|&s| self.frags[s as usize].start < at);
        let moved = v.split_off(cut);
        debug_assert!(!v.is_empty(), "left portion of a split is never empty");
        debug_assert!(!moved.is_empty(), "right portion of a split is never empty");
        self.frags_of.insert(head, pack(v));
        for &s in &moved {
            let f = &mut self.frags[s as usize];
            f.head = right_head;
            f.start -= at;
        }
        let prev = self.frags_of.insert(right_head, pack(moved));
        debug_assert!(prev.is_none(), "right head must be a fresh span");

        // Destination fragments reference elements by (run, offset) too —
        // rebase the ones the split moved.
        if !self.moved.is_empty() {
            let rekeys: Vec<(ElemRef, u32)> = self
                .moved
                .iter()
                .filter(|((h, o), _)| *h == head && *o >= at)
                .map(|(&k, &v)| (k, v))
                .collect();
            for ((h, o), slot) in rekeys {
                self.moved.remove(&(h, o));
                self.moved.insert((right_head, o - at), slot);
                let f = &mut self.frags[slot as usize];
                f.head = right_head;
                f.start = o - at;
            }
        }
    }

    // ---- internals ----

    /// Remove `n` from the treap (rotate to a leaf, unlink) and free its
    /// arena slot. Only destination fragments are ever detached.
    fn detach(&mut self, n: u32) {
        loop {
            let (l, r) = (self.frags[n as usize].left, self.frags[n as usize].right);
            let up = match (l, r) {
                (NIL, NIL) => break,
                (l, NIL) => l,
                (NIL, r) => r,
                (l, r) => {
                    if self.frags[l as usize].prio >= self.frags[r as usize].prio {
                        l
                    } else {
                        r
                    }
                }
            };
            self.rotate_up(up);
        }
        let p = self.frags[n as usize].parent;
        if p == NIL {
            self.root = NIL;
        } else {
            if self.frags[p as usize].left == n {
                self.frags[p as usize].left = NIL;
            } else {
                self.frags[p as usize].right = NIL;
            }
            self.update_to_root(p);
        }
        self.free.push(n);
    }

    fn subtree(&self, n: u32) -> usize {
        if n == NIL {
            0
        } else {
            self.frags[n as usize].subtree
        }
    }

    /// Fragment of `head` covering element offset `off`.
    fn frag_containing(&self, head: NodeIdx, off: u32) -> Option<u32> {
        let slot = match self.frags_of.get(&head)? {
            FragsOf::One(s) => *s,
            FragsOf::Many(v) => {
                let i = v.partition_point(|&s| self.frags[s as usize].start <= off);
                v[i - 1]
            }
        };
        let f = &self.frags[slot as usize];
        debug_assert!(
            f.start <= off && off < f.start + f.len,
            "fragments must cover the run"
        );
        Some(slot)
    }

    fn next_prio(&mut self) -> u32 {
        self.rng = self
            .rng
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.rng >> 33) as u32
    }

    fn new_frag(&mut self, head: NodeIdx, start: u32, len: u32, visible: u32, bits: Bits) -> u32 {
        let prio = self.next_prio();
        let frag = Frag {
            left: NIL,
            right: NIL,
            parent: NIL,
            prio,
            head,
            start,
            len,
            visible,
            subtree: visible as usize,
            bits,
        };
        if let Some(slot) = self.free.pop() {
            self.frags[slot as usize] = frag;
            slot
        } else {
            self.frags.push(frag);
            (self.frags.len() - 1) as u32
        }
    }

    fn new_span(&mut self, head: NodeIdx) -> u32 {
        let slot = self.new_frag(head, 0, 1, 1, Bits::Small(1));
        let prev = self.frags_of.insert(head, FragsOf::One(slot));
        debug_assert!(prev.is_none(), "span heads are unique");
        slot
    }

    /// Split fragment `slot` at local element offset `k` (0 < k < len); the
    /// right piece becomes a new treap node placed as `slot`'s in-order
    /// successor, keeping document order and visible counts intact.
    /// Returns the right piece's slot.
    fn split_frag(&mut self, slot: u32, k: u32) -> u32 {
        let f = &mut self.frags[slot as usize];
        debug_assert!(0 < k && k < f.len);
        let head = f.head;
        let right_start = f.start + k;
        let (bits, len, visible) = f.split_bits(k);
        let n = self.new_frag(head, right_start, len, visible, bits);
        // Register in the run's fragment list, sorted by start.
        let frags = &self.frags;
        let entry = self.frags_of.get_mut(&head).expect("run is indexed");
        match entry {
            FragsOf::One(s) => {
                debug_assert_eq!(*s, slot);
                *entry = FragsOf::Many(vec![slot, n]);
            }
            FragsOf::Many(v) => {
                let i = v.partition_point(|&s| frags[s as usize].start < right_start);
                v.insert(i, n);
            }
        }
        self.attach_succ(slot, n);
        self.settle(n);
        n
    }

    /// Link `n` as the in-order predecessor of `x` (leaf attach).
    fn attach_pred(&mut self, x: u32, n: u32) {
        if self.frags[x as usize].left == NIL {
            self.frags[x as usize].left = n;
            self.frags[n as usize].parent = x;
        } else {
            let mut cur = self.frags[x as usize].left;
            while self.frags[cur as usize].right != NIL {
                cur = self.frags[cur as usize].right;
            }
            self.frags[cur as usize].right = n;
            self.frags[n as usize].parent = cur;
        }
    }

    /// Link `n` as the in-order successor of `x` (leaf attach).
    fn attach_succ(&mut self, x: u32, n: u32) {
        if self.frags[x as usize].right == NIL {
            self.frags[x as usize].right = n;
            self.frags[n as usize].parent = x;
        } else {
            let mut cur = self.frags[x as usize].right;
            while self.frags[cur as usize].left != NIL {
                cur = self.frags[cur as usize].left;
            }
            self.frags[cur as usize].left = n;
            self.frags[n as usize].parent = cur;
        }
    }

    /// Restore invariants after a leaf attach of `n`: refresh subtree counts
    /// up the tree, then rotate `n` up to its priority position.
    fn settle(&mut self, n: u32) {
        self.update_to_root(n);
        while self.frags[n as usize].parent != NIL
            && self.frags[n as usize].prio > self.frags[self.frags[n as usize].parent as usize].prio
        {
            self.rotate_up(n);
        }
    }

    fn update(&mut self, n: u32) {
        let f = &self.frags[n as usize];
        let s = self.subtree(f.left) + f.visible as usize + self.subtree(f.right);
        self.frags[n as usize].subtree = s;
    }

    fn update_to_root(&mut self, mut n: u32) {
        while n != NIL {
            self.update(n);
            n = self.frags[n as usize].parent;
        }
    }

    /// Rotate `n` above its parent, preserving in-order traversal.
    fn rotate_up(&mut self, n: u32) {
        let p = self.frags[n as usize].parent;
        let g = self.frags[p as usize].parent;
        if self.frags[p as usize].left == n {
            // right rotation
            let b = self.frags[n as usize].right;
            self.frags[p as usize].left = b;
            if b != NIL {
                self.frags[b as usize].parent = p;
            }
            self.frags[n as usize].right = p;
        } else {
            // left rotation
            let b = self.frags[n as usize].left;
            self.frags[p as usize].right = b;
            if b != NIL {
                self.frags[b as usize].parent = p;
            }
            self.frags[n as usize].left = p;
        }
        self.frags[p as usize].parent = n;
        self.frags[n as usize].parent = g;
        if g == NIL {
            self.root = n;
        } else if self.frags[g as usize].left == p {
            self.frags[g as usize].left = n;
        } else {
            self.frags[g as usize].right = n;
        }
        self.update(p);
        self.update(n);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(i: u32) -> NodeIdx {
        NodeIdx(i)
    }

    #[test]
    fn push_back_get_position() {
        let mut ix = RunIndex::default();
        ix.insert_span_at(IndexTarget::Back, n(0));
        ix.insert_span_at(IndexTarget::Back, n(1));
        ix.insert_span_at(IndexTarget::Back, n(2));
        assert_eq!(ix.len(), 3);
        assert_eq!(ix.get(0), Some((n(0), 0)));
        assert_eq!(ix.get(1), Some((n(1), 0)));
        assert_eq!(ix.get(2), Some((n(2), 0)));
        assert_eq!(ix.get(3), None);
        assert_eq!(ix.position_of((n(1), 0)), Some(1));
    }

    #[test]
    fn extend_and_remove() {
        let mut ix = RunIndex::default();
        ix.insert_span_at(IndexTarget::Back, n(0));
        for off in 1..100 {
            ix.extend_run(n(0), off);
        }
        assert_eq!(ix.len(), 100);
        assert_eq!(ix.get(70), Some((n(0), 70)));
        assert!(ix.remove_element((n(0), 70)));
        assert!(!ix.remove_element((n(0), 70)));
        assert_eq!(ix.len(), 99);
        assert_eq!(ix.get(70), Some((n(0), 71)));
        assert_eq!(ix.position_of((n(0), 70)), None);
        assert_eq!(ix.position_of((n(0), 71)), Some(70));
    }

    #[test]
    fn insert_before_mid_fragment_splits() {
        let mut ix = RunIndex::default();
        ix.insert_span_at(IndexTarget::Back, n(0));
        for off in 1..10 {
            ix.extend_run(n(0), off);
        }
        // span lands before element 5: [0..5) [span] [5..10)
        ix.insert_span_at(IndexTarget::BeforeElem((n(0), 5)), n(100));
        assert_eq!(ix.len(), 11);
        assert_eq!(ix.get(4), Some((n(0), 4)));
        assert_eq!(ix.get(5), Some((n(100), 0)));
        assert_eq!(ix.get(6), Some((n(0), 5)));
        assert_eq!(ix.position_of((n(0), 5)), Some(6));
        // extending the run still appends at the very end
        ix.extend_run(n(0), 10);
        assert_eq!(ix.get(11), Some((n(0), 10)));
    }

    #[test]
    fn insert_after_and_split_run() {
        let mut ix = RunIndex::default();
        ix.insert_span_at(IndexTarget::Back, n(0));
        for off in 1..6 {
            ix.extend_run(n(0), off);
        }
        ix.insert_span_at(IndexTarget::AfterElem((n(0), 2)), n(50));
        assert_eq!(ix.get(2), Some((n(0), 2)));
        assert_eq!(ix.get(3), Some((n(50), 0)));
        assert_eq!(ix.get(4), Some((n(0), 3)));

        // split the run at element 3: right portion becomes head n(60)
        ix.split_run(n(0), 3, n(60));
        assert_eq!(ix.get(4), Some((n(60), 0)));
        assert_eq!(ix.get(5), Some((n(60), 1)));
        assert_eq!(ix.position_of((n(60), 2)), Some(6));
        assert_eq!(ix.position_of((n(0), 2)), Some(2));
        assert_eq!(ix.len(), 7);
        // and the right run can still be addressed after removal
        assert!(ix.remove_element((n(60), 0)));
        assert_eq!(ix.get(4), Some((n(60), 1)));
    }

    #[test]
    fn removed_elements_still_anchor_inserts() {
        let mut ix = RunIndex::default();
        ix.insert_span_at(IndexTarget::Back, n(0));
        for off in 1..4 {
            ix.extend_run(n(0), off);
        }
        assert!(ix.remove_element((n(0), 1)));
        assert!(ix.remove_element((n(0), 2)));
        // insert after a removed element: lands between visible 0 and 3
        ix.insert_span_at(IndexTarget::AfterElem((n(0), 1)), n(10));
        assert_eq!(ix.len(), 3);
        assert_eq!(ix.get(0), Some((n(0), 0)));
        assert_eq!(ix.get(1), Some((n(10), 0)));
        assert_eq!(ix.get(2), Some((n(0), 3)));
        // insert before a removed element
        ix.insert_span_at(IndexTarget::BeforeElem((n(0), 2)), n(11));
        assert_eq!(ix.get(2), Some((n(11), 0)));
    }

    #[test]
    fn large_fragment_bitmap() {
        let mut ix = RunIndex::default();
        ix.insert_span_at(IndexTarget::Back, n(0));
        for off in 1..300 {
            ix.extend_run(n(0), off);
        }
        for off in (0..300).step_by(3) {
            assert!(ix.remove_element((n(0), off)));
        }
        assert_eq!(ix.len(), 200);
        assert_eq!(ix.get(0), Some((n(0), 1)));
        assert_eq!(ix.position_of((n(0), 1)), Some(0));
        assert_eq!(ix.position_of((n(0), 299)), Some(199));
        // split a Large bitmap mid-word
        ix.insert_span_at(IndexTarget::BeforeElem((n(0), 150)), n(1));
        assert_eq!(ix.len(), 201);
        let p = ix.position_of((n(1), 0)).unwrap();
        assert_eq!(ix.get(p), Some((n(1), 0)));
        assert_eq!(ix.get(p + 1), Some((n(0), 151))); // 150 is removed (multiple of 3)
    }

    /// Randomized model check: drive the index and a naive Vec model with the
    /// same operations and compare all queries.
    #[test]
    fn randomized_against_model() {
        // simple deterministic LCG so the test needs no rng dependency
        let mut state = 42u64;
        let mut rand = move |m: u64| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            ((state >> 33) % m) as usize
        };

        // model: document as Vec of (head, offset, visible)
        let mut doc: Vec<(NodeIdx, u32, bool)> = Vec::new();
        let mut ix = RunIndex::default();
        let mut next_head = 0u32;
        let mut run_len: FxHashMap<NodeIdx, u32> = FxHashMap::default();

        for step in 0..2000 {
            match rand(5) {
                // new span at a random position (relative to an element or back)
                0 => {
                    let head = n(next_head);
                    next_head += 1;
                    run_len.insert(head, 1);
                    if doc.is_empty() || rand(8) == 0 {
                        ix.insert_span_at(IndexTarget::Back, head);
                        doc.push((head, 0, true));
                    } else {
                        let i = rand(doc.len() as u64);
                        let (h, off, _) = doc[i];
                        if rand(2) == 0 {
                            ix.insert_span_at(IndexTarget::BeforeElem((h, off)), head);
                            doc.insert(i, (head, 0, true));
                        } else {
                            ix.insert_span_at(IndexTarget::AfterElem((h, off)), head);
                            doc.insert(i + 1, (head, 0, true));
                        }
                    }
                }
                // extend a random run (must be the run tail in doc order? no —
                // extend appends at the tail fragment, which mirrors HashSeq
                // always extending with the run's last element)
                1 => {
                    let heads: Vec<NodeIdx> = run_len.keys().copied().collect();
                    if heads.is_empty() {
                        continue;
                    }
                    let h = heads[rand(heads.len() as u64)];
                    let l = run_len[&h];
                    ix.extend_run(h, l);
                    // model: insert right after the last element of run h
                    let tail_pos = doc
                        .iter()
                        .rposition(|&(dh, doff, _)| dh == h && doff == l - 1)
                        .expect("tail element is in the model");
                    doc.insert(tail_pos + 1, (h, l, true));
                    run_len.insert(h, l + 1);
                }
                // remove a random element
                2 => {
                    if !doc.is_empty() {
                        let i = rand(doc.len() as u64);
                        let (h, off, vis) = doc[i];
                        assert_eq!(ix.remove_element((h, off)), vis, "step {step}");
                        doc[i].2 = false;
                    }
                }
                // split a random run with len >= 2
                3 => {
                    let candidates: Vec<(NodeIdx, u32)> = run_len
                        .iter()
                        .filter(|&(_, &l)| l >= 2)
                        .map(|(&h, &l)| (h, l))
                        .collect();
                    if !candidates.is_empty() {
                        let (h, l) = candidates[rand(candidates.len() as u64)];
                        let at = 1 + rand((l - 1) as u64) as u32;
                        let right = n(next_head);
                        next_head += 1;
                        ix.split_run(h, at, right);
                        for e in doc.iter_mut() {
                            if e.0 == h && e.1 >= at {
                                e.0 = right;
                                e.1 -= at;
                            }
                        }
                        run_len.insert(h, at);
                        run_len.insert(right, l - at);
                    }
                }
                // verify everything
                _ => {
                    let visible: Vec<ElemRef> = doc
                        .iter()
                        .filter(|e| e.2)
                        .map(|&(h, off, _)| (h, off))
                        .collect();
                    assert_eq!(ix.len(), visible.len(), "step {step}");
                    for (p, &e) in visible.iter().enumerate() {
                        assert_eq!(ix.get(p), Some(e), "get({p}) at step {step}");
                        assert_eq!(ix.position_of(e), Some(p), "position_of at step {step}");
                    }
                    for &(h, off, vis) in &doc {
                        if !vis {
                            assert_eq!(ix.position_of((h, off)), None, "step {step}");
                        }
                    }
                }
            }
        }
    }
}
