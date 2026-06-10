use crate::{
    Id,
    hashseq::{HashSeq, Loc, NodeIdx},
};

/// Core in-order traversal, in handle space: run interiors walk `elements`
/// directly (no hashing); explicit forks and befores resolve their Id-ordered
/// sibling sets through the interning map.
#[derive(Debug, Clone)]
pub(crate) struct HashSeqIdxIter<'a> {
    seq: &'a HashSeq,
    waiting_stack: Vec<(NodeIdx, Vec<NodeIdx>)>,
}

impl<'a> HashSeqIdxIter<'a> {
    pub(crate) fn new(seq: &'a HashSeq) -> Self {
        let mut iter = Self {
            seq,
            waiting_stack: Vec::new(),
        };

        // root_nodes is Id-ordered; reverse so .pop() releases ascending.
        let roots: Vec<NodeIdx> = seq
            .root_nodes
            .keys()
            .map(|id| seq.idx_of(id).unwrap())
            .collect();
        for root in roots.into_iter().rev() {
            iter.push_waiting(root);
        }

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
                    // BTreeSet iterates in Id order; reverse for stack push.
                    let afters: Vec<NodeIdx> = afters
                        .iter()
                        .rev()
                        .map(|id| self.seq.idx_of(id).unwrap())
                        .collect();
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
                if !self.seq.is_removed(n) {
                    return Some(n);
                }
            }
        }
    }
}

/// Public iterator over node ids in document order.
#[derive(Debug, Clone)]
pub struct HashSeqIter<'a> {
    seq: &'a HashSeq,
    inner: HashSeqIdxIter<'a>,
}

impl<'a> HashSeqIter<'a> {
    pub(crate) fn new(seq: &'a HashSeq) -> Self {
        Self {
            seq,
            inner: HashSeqIdxIter::new(seq),
        }
    }
}

impl<'a> Iterator for HashSeqIter<'a> {
    type Item = &'a Id;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|idx| self.seq.id_ref(idx))
    }
}
