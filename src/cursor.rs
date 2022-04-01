use std::collections::BTreeSet;

use crate::{HashSeq, Id, hashseq::{Op, HashNode}};

pub struct Cursor {
    hashseq: HashSeq,
    position: usize,
    left: Option<Id>,
    right: Option<Id>,
}

impl From<HashSeq> for Cursor {
    fn from(hashseq: HashSeq) -> Self {
	let first = hashseq.iter_ids().next();
        Self {
	    hashseq,
	    position: 0,
	    left: None,
	    right: first,
	}
    }
}

impl From<Cursor> for HashSeq {
    fn from(cursor: Cursor) -> HashSeq {
	cursor.hashseq
    }
}

impl Cursor {
    pub fn seq(&self) -> &HashSeq {
	&self.hashseq
    }
    
    pub fn seek(&mut self, idx: usize) {
	if idx > self.hashseq.len() {
	    return;
	    // TODO: return err
	};
	
        let mut order = self.hashseq.iter_ids();
	
        self.left = if let Some(prev_idx) = idx.checked_sub(1) {
            for _ in 0..prev_idx {
                order.next();
            }
            order.next()
        } else {
            None
        };

        self.right = order.next();
	self.position = idx;
    }

    fn do_insert(&mut self, value: char) -> Id {
	let op = match (self.left, self.right) {
            (Some(l), Some(r)) => {
                if self.hashseq.topo.is_causally_before(l, r) {
                    Op::InsertBefore(r, value)
                } else {
                    Op::InsertAfter(l, value)
                }
            }
            (Some(l), None) => Op::InsertAfter(l, value),
            (None, Some(r)) => Op::InsertBefore(r, value),
            (None, None) => Op::InsertRoot(value),
        };

	let mut extra_dependencies = self.hashseq.roots.clone();

        if let Some(dep) = op.dependency() {
            extra_dependencies.remove(&dep); // the op dependency will already be seen, no need to duplicated it in the extra dependencie.
        }

        let node = HashNode {
            extra_dependencies,
            op,
        };

	let node_id = node.id();
	self.hashseq.apply(node).unwrap();
	node_id
    }

    /// Inserts the element at the current cursor position, cursor moves to after the inserted element.
    pub fn insert(&mut self, value: char) {
	let insert_id = self.do_insert(value);

	self.left = Some(insert_id);
	self.right = None;
	self.position += 1;
    }

    pub fn insert_batch(&mut self, batch: impl IntoIterator<Item = char>) {
	for v in batch {
            self.insert(v)
        }
    }

    pub fn insert_ahead(&mut self, value: char) {
	let insert_id = self.do_insert(value);
	self.right = Some(insert_id);
	self.left = None;
    }

    /// Remove the element to the immediate left (if it exists)
    /// No-op if we are at the beginning of the list
    pub fn remove(&mut self) {
	if let Some(left) = self.left {
	    let mut extra_dependencies = self.hashseq.roots.clone();
            extra_dependencies.remove(&left); // insert will already be seen as a dependency;

            let node = HashNode {
                extra_dependencies,
                op: Op::Remove(left),
            };

            self.hashseq.apply(node).unwrap();
	    match self.hashseq.nodes.get(&left).unwrap().op {
		Op::InsertAfter(prev, _) if self.hashseq.topo.after(prev) == BTreeSet::from_iter([left]) => {
		    self.left = Some(prev);
		    self.position -= 1;
		},
		_ => {
		    assert!(self.position > 0); // since we had a left, we can't be at pos 0
		    self.seek(self.position - 1);
		}
	    };
	}
    }
}
