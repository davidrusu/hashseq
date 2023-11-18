use std::collections::{BTreeMap, BTreeSet, HashSet};

use associative_positional_list::AssociativePositionalList;

use crate::topo_sort::{Topo, TopoIter};
use crate::{HashNode, Id, Op};

#[derive(Debug, Default, Clone)]
pub struct HashSeq {
    pub topo: Topo,
    pub nodes: BTreeMap<Id, HashNode>,
    pub removed_inserts: HashSet<Id>,
    pub(crate) roots: BTreeSet<Id>,
    pub(crate) orphaned: HashSet<HashNode>,
    index: AssociativePositionalList<Id>,
}

impl PartialEq for HashSeq {
    fn eq(&self, other: &Self) -> bool {
        (&self.roots, &self.orphaned) == (&other.roots, &other.orphaned)
    }
}

impl Eq for HashSeq {}

impl HashSeq {
    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub fn orphans(&self) -> &HashSet<HashNode> {
        &self.orphaned
    }

    fn neighbours(&mut self, idx: usize) -> (Option<Id>, Option<Id>) {
        let left = idx
            .checked_sub(1)
            .and_then(|prev_idx| self.index.get(prev_idx).copied());

        let right = self.index.get(idx).copied();

        (left, right)
    }

    pub fn insert(&mut self, idx: usize, value: char) {
        self.insert_batch(idx, [value]);
    }

    pub fn insert_batch(&mut self, idx: usize, batch: impl IntoIterator<Item = char>) {
        let mut batch = batch.into_iter().enumerate();

        let first_elem = if let Some((_, value)) = batch.next() {
            value
        } else {
            return;
        };

        let (left, right) = self.neighbours(idx);
        let op = match (left, right) {
            (Some(l), Some(r)) => {
                if self.topo.is_causally_before(&l, &r) {
                    Op::InsertBefore(r, first_elem)
                } else {
                    Op::InsertAfter(l, first_elem)
                }
            }
            (Some(l), None) => Op::InsertAfter(l, first_elem),
            (None, Some(r)) => Op::InsertBefore(r, first_elem),
            (None, None) => Op::InsertRoot(first_elem),
        };

        let extra_dependencies =
            BTreeSet::from_iter(self.roots.difference(&op.dependencies()).cloned());

        let node = HashNode {
            extra_dependencies,
            op,
        };

        let first_node_id = node.id();
        self.apply_with_known_position(node, idx);

        // All remaining elements will be chained after the first node
        let mut last_id = first_node_id;
        for (i, e) in batch {
            let node = HashNode {
                extra_dependencies: BTreeSet::new(),
                op: Op::InsertAfter(last_id, e),
            };

            last_id = node.id();

            self.apply_with_known_position(node, idx + i);
        }
    }

    pub fn remove(&mut self, idx: usize) {
        self.remove_batch(idx, 1);
    }

    pub fn remove_batch(&mut self, idx: usize, amount: usize) {
        if amount == 0 {
            // Nothing to remove
            return;
        }

        let mut to_remove = BTreeSet::new();
        for pos in idx..(idx + amount) {
            if let Some(id) = self.index.get(pos) {
                to_remove.insert(*id);
            } else {
                break;
            }
        }
        let op = Op::Remove(to_remove);

        let extra_dependencies =
            BTreeSet::from_iter(self.roots.difference(&op.dependencies()).cloned());

        let node = HashNode {
            extra_dependencies,
            op,
        };

        self.apply(node);
    }

    fn any_missing_dependencies(&self, deps: &BTreeSet<Id>) -> bool {
        for dep in deps.iter() {
            if !self.nodes.contains_key(dep) {
                return true;
            }
        }

        false
    }

    fn insert_root(&mut self, root_id: Id) {
        let position = if let Some(next_root) = self
            .topo
            .roots()
            .range(root_id..)
            .find(|id| !self.removed_inserts.contains(id))
        {
            // new root is inserted just before the next biggest root
            self.index.find(next_root).unwrap()
        } else {
            // otherwise if there is no bigger root, the new root is
            // inserted at end of list
            self.len()
        };
        self.insert_root_with_known_position(root_id, position);
    }

    fn insert_root_with_known_position(&mut self, id: Id, position: usize) {
        self.index.insert(position, id);
        self.topo.add_root(id);
    }

    fn insert_after_anchor(&mut self, id: Id, anchor: Id) {
        let position = if let Some(next_node) = self
            .topo
            .after(anchor)
            .range(id..)
            .find(|id| !self.removed_inserts.contains(id))
        {
            // new node is inserted just before the other node after our anchor node that is
            // bigger than the new node
            Some(self.index.find(next_node).unwrap())
        } else {
            // otherwise the new node is inserted after our anchor node (unless it has been removed)
            self.index.find(&anchor).map(|p| p + 1)
        };

        self.topo.add_after(anchor, id);

        let position = position.unwrap_or_else(|| {
            // fall back to iterating over the entire sequence if the anchor node has been removed
            let (position, _) = self.iter_ids().enumerate().find(|(_, n)| n == &id).unwrap();
            position
        });
        self.insert_after_anchor_with_known_position(id, anchor, position);
    }

    fn insert_after_anchor_with_known_position(&mut self, id: Id, anchor: Id, position: usize) {
        self.topo.add_after(anchor, id);
        self.index.insert(position, id);
    }

    fn remove_nodes(&mut self, nodes: &BTreeSet<Id>) {
        // TODO: if self.nodes.get(node) is not an insert op, then drop this remove.
        //       Are you sure? looks like we would mark this op as an orphan if we hadn't
        //       seen a node yet.
        for n in nodes {
            if let Some(p) = self.index.find(n) {
                self.index.remove(p);
            }
        }
        self.removed_inserts.extend(nodes);
    }

    fn insert_before_anchor(&mut self, id: Id, anchor: Id) {
        let position = if let Some(next_node) = self
            .topo
            .before(anchor)
            .range(id..)
            .find(|id| !self.removed_inserts.contains(id))
        {
            // new node is inserted just before the other node before our anchor node that is
            // bigger than the new node
            Some(self.index.find(next_node).unwrap())
        } else {
            // otherwise the new node is inserted before our anchor node
            self.index.find(&anchor)
        };

        self.topo.add_before(anchor, id);

        let position = position.unwrap_or_else(|| {
            // fall back to iterating over the entire sequence if the anchor node has been removed
            let (position, _) = self.iter_ids().enumerate().find(|(_, n)| n == &id).unwrap();
            position
        });
        self.index.insert(position, id);
    }

    fn insert_before_anchor_with_known_position(&mut self, id: Id, anchor: Id, position: usize) {
        self.topo.add_before(anchor, id);
        self.index.insert(position, id);
    }

    pub fn apply_with_known_position(&mut self, node: HashNode, position: usize) {
        let id = node.id();

        if self.nodes.contains_key(&id) {
            return; // Already processed this node
        }

        let dependencies = BTreeSet::from_iter(node.dependencies());
        if self.any_missing_dependencies(&dependencies) {
            self.orphaned.insert(node);
            return;
        }

        match &node.op {
            Op::InsertRoot(_) => self.insert_root_with_known_position(id, position),
            Op::InsertAfter(anchor, _) => {
                self.insert_after_anchor_with_known_position(id, *anchor, position)
            }
            Op::InsertBefore(anchor, _) => {
                self.insert_before_anchor_with_known_position(id, *anchor, position)
            }
            Op::Remove(nodes) => self.remove_nodes(nodes),
        }

        self.nodes.insert(id, node);

        let superseded_roots = Vec::from_iter(
            self.roots
                .iter()
                .filter(|r| dependencies.contains(*r))
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

    pub fn apply(&mut self, node: HashNode) {
        let id = node.id();

        if self.nodes.contains_key(&id) {
            return; // Already processed this node
        }

        let dependencies = BTreeSet::from_iter(node.dependencies());
        if self.any_missing_dependencies(&dependencies) {
            self.orphaned.insert(node);
            return;
        }

        match &node.op {
            Op::InsertRoot(_) => self.insert_root(id),
            Op::InsertAfter(anchor, _) => self.insert_after_anchor(id, *anchor),
            Op::InsertBefore(anchor, _) => self.insert_before_anchor(id, *anchor),
            Op::Remove(nodes) => self.remove_nodes(nodes),
        }

        self.nodes.insert(id, node);

        let superseded_roots = Vec::from_iter(
            self.roots
                .iter()
                .filter(|r| dependencies.contains(*r))
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

    pub fn iter_ids(&self) -> TopoIter<'_, '_> {
        self.topo.iter(&self.removed_inserts)
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

        assert_eq!(seq.iter().collect::<String>(), "abc");
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

        assert_eq!(String::from_iter(dbg!(&seq).iter()), "bcda");
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
        let removed = seq.iter_ids().nth(1).unwrap();
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
                        removed.insert(seq_a.iter_ids().nth(idx).unwrap());
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
                        removed.insert(seq_b.iter_ids().nth(idx).unwrap());
                        seq_b.remove(idx);
                    }
                }
            }
        }

        let mut merged = seq_a.clone();
        merged.merge(seq_b.clone());

        for r in removed {
            seq_a.apply(HashNode {
                op: Op::Remove(BTreeSet::from_iter([r])),
                extra_dependencies: BTreeSet::new(),
            });
            seq_b.apply(HashNode {
                op: Op::Remove(BTreeSet::from_iter([r])),
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
