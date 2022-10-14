use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::topo_sort::Topo;
// use crate::topo_sort_strong_weak::Tree;
use crate::{Cursor, Id};

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum Op {
    InsertRoot(char),
    InsertAfter(Id, char),
    InsertBefore(Id, char),
    Remove(Id),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HashNode {
    pub extra_dependencies: BTreeSet<Id>,
    pub op: Op,
}

impl Op {
    pub fn dependency(&self) -> Option<Id> {
        match &self {
            Op::InsertRoot(_) => None,
            Op::InsertAfter(dep, _) | Op::InsertBefore(dep, _) | Op::Remove(dep) => Some(*dep),
        }
    }
}

impl HashNode {
    pub fn dependencies(&self) -> impl Iterator<Item = Id> + '_ {
        self.extra_dependencies
            .iter()
            .copied()
            .chain(self.op.dependency())
    }

    pub fn id(&self) -> Id {
        use std::hash::Hash;
        use std::hash::Hasher;
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct HashSeq {
    pub(crate) topo: Topo,
    pub(crate) nodes: BTreeMap<Id, HashNode>,
    pub(crate) removed_inserts: BTreeSet<Id>,
    pub(crate) roots: BTreeSet<Id>,
    pub(crate) orphaned: HashSet<HashNode>,
}

impl HashSeq {
    pub fn len(&self) -> usize {
        // nodes contains both insert and remove Ops, so nodes.len() counts both the remove ops and the removed inserts.
        // We subtract out 2 * removed_inserts.len() to account for both the remove op and the insert op that was removed
        self.nodes.len() - self.removed_inserts.len() * 2
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn orphans(&self) -> &HashSet<HashNode> {
        &self.orphaned
    }

    pub fn cursor(self) -> Cursor {
        Cursor::from(self)
    }

    pub fn insert(&mut self, idx: usize, value: char) {
        let op = {
            let mut order = self.iter_ids();

            let left = if let Some(prev_idx) = idx.checked_sub(1) {
                for _ in 0..prev_idx {
                    order.next();
                }
                order.next()
            } else {
                None
            };

            let right = order.next();

            match (left, right) {
                (Some(l), Some(r)) => {
                    if self.topo.is_causally_before(l, r) {
                        Op::InsertBefore(r, value)
                    } else {
                        Op::InsertAfter(l, value)
                    }
                }
                (Some(l), None) => Op::InsertAfter(l, value),
                (None, Some(r)) => Op::InsertBefore(r, value),
                (None, None) => Op::InsertRoot(value),
            }
        };

        let mut extra_dependencies = self.roots.clone();

        if let Some(dep) = op.dependency() {
            extra_dependencies.remove(&dep); // the op dependency will already be seen, no need to duplicated it in the extra dependencie.
        }

        let node = HashNode {
            extra_dependencies,
            op,
        };

        self.apply(node);
    }

    pub fn insert_batch(&mut self, idx: usize, batch: impl IntoIterator<Item = char>) {
        for (i, e) in batch.into_iter().enumerate() {
            self.insert(idx + i, e)
        }
    }

    pub fn remove(&mut self, idx: usize) {
        let mut order = self
            .topo
            .iter()
            .filter(|id| !self.removed_inserts.contains(id));

        for _ in 0..idx {
            order.next();
        }

        if let Some(insert) = order.next() {
            let mut extra_dependencies = self.roots.clone();
            extra_dependencies.remove(&insert); // insert will already be seen as a dependency;

            let node = HashNode {
                extra_dependencies,
                op: Op::Remove(insert),
            };

            self.apply(node);
        }
    }

    fn any_missing_dependencies(&self, deps: &BTreeSet<Id>) -> bool {
        for dep in deps.iter() {
            if !self.nodes.contains_key(dep) {
                return true;
            }
        }

        false
    }

    pub fn apply(&mut self, node: HashNode) {
        let id = node.id();
        // println!("apply({:?} = {:?})", node, id);

        if self.nodes.contains_key(&id) {
            return; // Already processed this node
        }

        let dependencies = BTreeSet::from_iter(node.dependencies());
        if self.any_missing_dependencies(&dependencies) {
            self.orphaned.insert(node);
            return;
        }

        match &node.op {
            Op::InsertRoot(_) => self.topo.add_root(id),
            Op::InsertAfter(node, _) => self.topo.add_after(*node, id),
            Op::InsertBefore(node, _) => self.topo.add_before(*node, id),
            Op::Remove(node) => {
                // TODO: if self.nodes.get(node) is not an insert op, then drop this remove
                self.removed_inserts.insert(*node);
            }
        }

        self.nodes.insert(id, node);

        let superseded_roots = Vec::from_iter(
            self.roots
                .iter()
                .filter(|r| dependencies.contains(r))
                .copied(),
        );

        for r in superseded_roots {
            self.roots.remove(&r);
        }

        self.roots.insert(id);

        let orphans = std::mem::take(&mut self.orphaned);

        for orphan in orphans {
            self.apply(orphan);
        }
    }

    pub fn merge(&mut self, other: Self) {
        for node in other.nodes.into_values() {
            self.apply(node);
        }

        for orphan in other.orphaned {
            self.apply(orphan);
        }
    }

    pub fn iter_ids(&self) -> impl Iterator<Item = Id> + '_ {
        self.topo
            .iter()
            .filter(|id| !self.removed_inserts.contains(id))
    }

    pub fn iter(&self) -> impl Iterator<Item = char> + '_ {
        self.iter_ids()
            .filter_map(|id| self.nodes.get(&id))
            .filter_map(|node| match &node.op {
                Op::InsertRoot(v) | Op::InsertAfter(_, v) | Op::InsertBefore(_, v) => Some(*v),
                Op::Remove(_) => None,
            })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use quickcheck_macros::quickcheck;

    #[test]
    fn test_insert_at_end() {
        let mut seq = HashSeq::default();
        seq.insert(0, 'a');
        seq.insert(1, 'b');
        seq.insert(2, 'c');
        assert_eq!(&seq.iter().collect::<String>(), "abc");
    }

    #[test]
    fn test_insert_batch() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "abc".chars());
        assert_eq!(&seq.iter().collect::<String>(), "abc");
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
    fn test_common_prefix_isnt_duplicated() {
        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        seq_a.insert_batch(0, "hello my name is david".chars());
        seq_b.insert_batch(0, "hello my name is zameena".chars());

        seq_a.merge(seq_b);

        assert_eq!(
            &seq_a.iter().collect::<String>(),
            "hello my name is zameenadavid"
        );
    }

    #[test]
    fn test_common_prefix_isnt_duplicated_simple() {
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
            op: Op::Remove(insert.id()),
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

        dbg!(&ab);
        dbg!(&ba);

        assert_eq!(ab, ba);
    }

    #[quickcheck]
    fn prop_reflexive(ops: Vec<(bool, u8, char)>) {
        let mut seq = HashSeq::default();

        for (insert_or_remove, idx, elem) in ops {
            let idx = idx as usize;
            match insert_or_remove {
                true => {
                    // insert
                    seq.insert(idx.min(seq.len()), elem);
                }
                false => {
                    // remove
                    if !seq.is_empty() {
                        seq.remove(idx.min(seq.len() - 1));
                    }
                }
            }
        }

        // merge(a, a) == a

        let mut merge_self = seq.clone();
        merge_self.merge(seq.clone());

        assert_eq!(merge_self, seq);
    }

    #[quickcheck]
    fn prop_commutative(a: Vec<(bool, u8, char)>, b: Vec<(bool, u8, char)>) {
        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        for (insert_or_remove, idx, elem) in a {
            let idx = idx as usize;
            match insert_or_remove {
                true => {
                    // insert
                    seq_a.insert(idx.min(seq_a.len()), elem);
                }
                false => {
                    // remove
                    if !seq_a.is_empty() {
                        seq_a.remove(idx.min(seq_a.len() - 1));
                    }
                }
            }
        }

        for (insert_or_remove, idx, elem) in b {
            let idx = idx as usize;
            match insert_or_remove {
                true => {
                    // insert
                    seq_b.insert(idx.min(seq_b.len()), elem);
                }
                false => {
                    // remove
                    if !seq_b.is_empty() {
                        seq_b.remove(idx.min(seq_b.len() - 1));
                    }
                }
            }
        }

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
        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();
        let mut seq_c = HashSeq::default();

        for (insert_or_remove, idx, elem) in a {
            let idx = idx as usize;
            match insert_or_remove {
                true => {
                    // insert
                    seq_a.insert(idx.min(seq_a.len()), elem);
                }
                false => {
                    // remove
                    if !seq_a.is_empty() {
                        seq_a.remove(idx.min(seq_a.len() - 1));
                    }
                }
            }
        }

        for (insert_or_remove, idx, elem) in b {
            let idx = idx as usize;
            match insert_or_remove {
                true => {
                    // insert
                    seq_b.insert(idx.min(seq_b.len()), elem);
                }
                false => {
                    // remove
                    if !seq_b.is_empty() {
                        seq_b.remove(idx.min(seq_b.len() - 1));
                    }
                }
            }
        }

        for (insert_or_remove, idx, elem) in c {
            let idx = idx as usize;
            match insert_or_remove {
                true => {
                    // insert
                    seq_c.insert(idx.min(seq_c.len()), elem);
                }
                false => {
                    // remove
                    if !seq_c.is_empty() {
                        seq_c.remove(idx.min(seq_c.len() - 1));
                    }
                }
            }
        }

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
        let mut model = Vec::new();
        let mut seq = HashSeq::default();

        for (insert_or_remove, idx, elem) in [(true, 0, 'b'), (true, 0, 'b'), (true, 1, 'a')] {
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

    #[test]
    fn test_prop_vec_model_qc2() {
        let mut model = Vec::new();
        let mut seq = HashSeq::default();

        for (insert_or_remove, idx, elem) in [
            (true, 0, 'b'),
            (true, 0, 'b'),
            (true, 1, 'b'),
            (true, 2, 'a'),
        ] {
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

    #[test]
    fn test_prop_vec_model_qc3() {
        let mut model = Vec::new();
        let mut seq = HashSeq::default();

        for (insert_or_remove, idx, elem) in [
            (true, 0, 'c'),
            (true, 1, 'c'),
            (true, 2, 'c'),
            (false, 1, 'c'),
            (true, 1, 'b'),
        ] {
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

    #[test]
    fn test_prop_vec_model_qc4() {
        let mut seq = HashSeq::default();

        for (idx, elem) in [(0, 'a'), (1, 'a'), (2, 'a'), (3, 'a'), (3, 'a'), (3, 'd')] {
            seq.insert(idx, elem);
        }

        assert_eq!(seq.iter().collect::<String>(), "aaadaa");
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
        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();
        let mut removed = BTreeSet::new();

        for (insert_or_remove, idx, elem) in a {
            let idx = idx as usize;
            match insert_or_remove {
                true => {
                    // insert
                    seq_a.insert(idx.min(seq_a.len()), elem);
                }
                false => {
                    // remove
                    if !seq_a.is_empty() {
                        let idx = idx.min(seq_a.len() - 1);
                        removed.insert(seq_a.iter_ids().skip(idx).next().unwrap());
                        seq_a.remove(idx);
                    }
                }
            }
        }

        for (insert_or_remove, idx, elem) in b {
            let idx = idx as usize;
            match insert_or_remove {
                true => {
                    // insert
                    seq_b.insert(idx.min(seq_b.len()), elem);
                }
                false => {
                    // remove
                    if !seq_b.is_empty() {
                        let idx = idx.min(seq_b.len() - 1);
                        removed.insert(seq_b.iter_ids().skip(idx).next().unwrap());
                        seq_b.remove(idx);
                    }
                }
            }
        }

        let mut merged = seq_a.clone();
        merged.merge(seq_b.clone());

        for r in removed {
            seq_a.apply(HashNode {
                op: Op::Remove(r),
                extra_dependencies: BTreeSet::new(),
            });
            seq_b.apply(HashNode {
                op: Op::Remove(r),
                extra_dependencies: BTreeSet::new(),
            });
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
        assert_eq!(next_a, None);
        assert_eq!(next_b, None);
    }
}
