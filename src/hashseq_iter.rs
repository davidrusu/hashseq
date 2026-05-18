use crate::{hashseq::HashSeq, Id};

#[derive(Debug, Clone)]
pub struct HashSeqIter<'a> {
    seq: &'a HashSeq,
    waiting_stack: Vec<(Id, Vec<Id>)>,
}

impl<'a> HashSeqIter<'a> {
    pub(crate) fn new(seq: &'a HashSeq) -> Self {
        let mut iter = Self {
            seq,
            waiting_stack: Vec::new(),
        };

        let mut roots_vec: Vec<Id> = seq.root_nodes.keys().copied().collect();
        roots_vec.sort();
        for root in roots_vec.into_iter().rev() {
            iter.push_waiting(root);
        }

        iter
    }

    fn push_waiting(&mut self, n: Id) {
        // befores() yields sorted; reverse so .pop() returns ascending order.
        let deps: Vec<Id> = self.seq.befores(&n).rev().copied().collect();
        self.waiting_stack.push((n, deps));
    }
}

impl<'a> Iterator for HashSeqIter<'a> {
    type Item = &'a Id;

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
                    // BTreeSet iterates in sorted order; reverse for stack push.
                    for s in afters.iter().rev() {
                        self.push_waiting(*s);
                    }
                } else if let Some(run_pos) = self.seq.run_index.get(&n) {
                    // Check if n is the first element of this run
                    if run_pos.position == 0 {
                        // Push remaining run elements (skip first which is n)
                        if let Some(run) = self.seq.runs.get(&run_pos.run_id) {
                            for id in run.elements.iter().skip(1).rev() {
                                // Use push_waiting to properly handle befores
                                self.push_waiting(*id);
                            }
                        }
                    }
                }
                // Return reference from existing data structures
                if !self.seq.removed_inserts.contains(&n)
                    && let Some(id_ref) = self.seq.get_id_ref(&n)
                {
                    return Some(id_ref);
                }
            }
        }
    }
}
