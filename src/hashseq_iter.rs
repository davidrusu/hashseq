use std::collections::VecDeque;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::hashseq::{HashSeq, Loc, NodeIdx};

/// Core in-order traversal, in handle space: run interiors walk `elements`
/// directly (no hashing); explicit forks and befores resolve their Id-ordered
/// sibling sets through the interning map.
///
/// Moves render here definitionally (HASHSEQ_SPEC.md "Move"): a moved-out
/// element is skipped at its base slot (its descendants stay — run chaining
/// is a causality artifact, not user intent), and moved-in elements splice
/// at their glue point — after the anchor's before-children / before its
/// after-children — id-ordered among themselves. The glue state is computed
/// up front as a pure function of the placement registers.
///
/// This is the *semantic definition* of document order. Production iteration
/// rides the run index's fragment walk instead (`HashSeq::iter_idxs`);
/// `prop_index_matches_iterator` keeps the two equal, which is why this stays
/// compiled even though only tests call it.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct HashSeqIdxIter<'a> {
    seq: &'a HashSeq,
    waiting_stack: Vec<(NodeIdx, Vec<NodeIdx>)>,
    /// Elements whose rendered placement is a move (skipped at base).
    moved_out: FxHashSet<NodeIdx>,
    /// Moved-in elements by glue point `(anchor, before-side)`, id-ordered.
    glued: FxHashMap<(NodeIdx, bool), Vec<NodeIdx>>,
    /// Spliced output waiting to be yielded.
    pending: VecDeque<NodeIdx>,
}

#[allow(dead_code)]
impl<'a> HashSeqIdxIter<'a> {
    pub(crate) fn new(seq: &'a HashSeq) -> Self {
        let (moved_out, glued) = seq.rendered_moves();
        let mut iter = Self {
            seq,
            waiting_stack: Vec::new(),
            moved_out,
            glued,
            pending: VecDeque::new(),
        };

        // The traversal is simply the origin's release: its befores come
        // first, the origin itself is tombstoned (skipped), and its afters
        // are the top-level runs in Id order.
        iter.push_waiting(crate::hashseq::ORIGIN_IDX);

        iter
    }

    fn push_waiting(&mut self, n: NodeIdx) {
        // befores_of yields Id-sorted; reverse so .pop() returns ascending order.
        let deps: Vec<NodeIdx> = self.seq.befores_of(n).rev().collect();
        self.waiting_stack.push((n, deps));
    }

    fn splice_glued(&mut self, n: NodeIdx, before: bool) {
        if self.glued.is_empty() {
            return;
        }
        if let Some(elems) = self.glued.get(&(n, before)) {
            self.pending.extend(elems.iter().copied());
        }
    }
}

impl<'a> Iterator for HashSeqIdxIter<'a> {
    type Item = NodeIdx;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(x) = self.pending.pop_front() {
                return Some(x);
            }
            let (_, deps) = self.waiting_stack.last_mut()?;

            if let Some(dep) = deps.pop() {
                // This node has dependencies that need to be
                // released ahead of itself.
                self.push_waiting(dep);
            } else {
                let (n, _) = self.waiting_stack.pop().expect("Failed to pop");
                // n's slot: [glued Before(n)] n [glued After(n)] — glue
                // points survive tombstones and moved-out anchors, so the
                // splices happen whether or not n itself is emitted.
                self.splice_glued(n, true);
                if !self.seq.is_removed(n) && !self.moved_out.contains(&n) {
                    self.pending.push_back(n);
                }
                // Queue up any nodes who come after this one (they yield
                // after the pending splice drains).
                if let Some(afters) = self.seq.afters.get(&n) {
                    // Iterates in Id order; reverse for stack push.
                    let afters: Vec<NodeIdx> = afters.iter().rev().collect();
                    for s in afters {
                        self.push_waiting(s);
                    }
                } else if let Loc::Run { run, pos } = self.seq.loc_of(n)
                    && pos == 0
                {
                    // n is the head of a run: push the remaining elements
                    // (skip the head itself, which is n). Use push_waiting to
                    // properly handle each element's befores.
                    let rest: Vec<NodeIdx> = self.seq.runs[&run]
                        .elements
                        .iter()
                        .skip(1)
                        .rev()
                        .copied()
                        .collect();
                    for e in rest {
                        self.push_waiting(e);
                    }
                }
                self.splice_glued(n, false);
            }
        }
    }
}
