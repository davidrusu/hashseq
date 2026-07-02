use crate::hashseq::{HashSeq, Loc, NodeIdx};

/// Core in-order traversal, in handle space: run interiors walk `elements`
/// directly (no hashing); explicit forks and befores resolve their Id-ordered
/// sibling sets through the interning map.
///
/// Moves render structurally: a deciding move op sits in its anchor's
/// sibling set like an insert child (keyed by its own id), and releases as
/// its target element — a leaf; the target's own descendants stay at its
/// base slot, where the element itself is skipped (the origin ghost).
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
}

#[allow(dead_code)]
impl<'a> HashSeqIdxIter<'a> {
    pub(crate) fn new(seq: &'a HashSeq) -> Self {
        let mut iter = Self {
            seq,
            waiting_stack: Vec::new(),
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
}

impl<'a> Iterator for HashSeqIdxIter<'a> {
    type Item = NodeIdx;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (_, deps) = self.waiting_stack.last_mut()?;

            if let Some(dep) = deps.pop() {
                // This node has dependencies that need to be
                // released ahead of itself.
                self.push_waiting(dep);
            } else {
                let (n, _) = self.waiting_stack.pop().expect("Failed to pop");
                // This node is free to be released, but first
                // queue up any nodes who come after this one
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
                if let Loc::MoveOp = self.seq.loc_of(n) {
                    // A deciding move op releases as its target — a leaf.
                    let t = self.seq.move_nodes[&n].target;
                    if !self.seq.is_removed(t) {
                        return Some(t);
                    }
                } else if !self.seq.is_removed(n) && !self.seq.rendered_elsewhere(n) {
                    return Some(n);
                }
            }
        }
    }
}
