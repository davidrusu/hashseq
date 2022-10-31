use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::topo_sort::{Marker, Topo, TopoIter};
// use crate::topo_sort_strong_weak::Tree;
use crate::{Cursor, HashNode, Id, Op};

#[derive(Debug, Default, Clone)]
pub struct HashSeq {
    pub topo: Topo,
    pub nodes: BTreeMap<Id, HashNode>,
    pub removed_inserts: HashSet<Id>,
    pub(crate) roots: BTreeSet<Id>,
    pub(crate) orphaned: HashSet<HashNode>,
    pub markers: BTreeMap<usize, Marker>,
    pub cache_hit: u64,
    pub cache_miss: u64,
}

impl PartialEq for HashSeq {
    fn eq(&self, other: &Self) -> bool {
        (
            &self.topo,
            &self.nodes,
            &self.removed_inserts,
            &self.roots,
            &self.orphaned,
        ) == (
            &other.topo,
            &other.nodes,
            &other.removed_inserts,
            &other.roots,
            &other.orphaned,
        )
    }
}

impl Eq for HashSeq {}

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

    fn iter_from(&mut self, idx: usize) -> TopoIter<'_, '_> {
        let (mut start_idx, mut order) =
            if let Some((start_idx, marker)) = self.markers.range(..=idx).rev().next() {
                let order = self.topo.iter_from(&self.removed_inserts, marker);
                (*start_idx, order)
            } else {
                let order = self.topo.iter(&self.removed_inserts);
                (0, order)
            };

        let diff = idx - start_idx;
        if diff > self.marker_spacing() {
            // we'll insert a marker at the midpoint between the index and the start_idx

            let next_marker_idx = start_idx + diff / 2;
            for _ in start_idx..next_marker_idx {
                order.next();
            }

            let (_, marker) = order.marker().expect("We should have a marker here");
            self.markers.insert(next_marker_idx, marker);
            start_idx = next_marker_idx + 1;

            self.cache_miss += 1;
        } else {
            self.cache_hit += 1;
        }

        for _ in start_idx..idx {
            order.next();
        }

        order
    }

    fn neighbours(&mut self, idx: usize) -> (Option<Id>, Option<Id>, Option<Marker>) {
        let (left, mut order) = if let Some(prev_idx) = idx.checked_sub(1) {
            let mut order = self.iter_from(prev_idx);
            (order.next(), order)
        } else {
            (None, self.iter_from(idx))
        };

        let (right, marker) = match order.marker() {
            Some((id, m)) => (Some(id), Some(m)),
            None => (None, None),
        };

        (left, right, marker)
    }

    fn invalidate_markers_after(&mut self, idx: usize) {
        self.markers.split_off(&(idx + 1));
    }

    pub fn insert(&mut self, idx: usize, value: char) {
        self.insert_batch(idx, [value]);
    }

    pub fn insert_batch(&mut self, idx: usize, batch: impl IntoIterator<Item = char>) {
        let mut batch = batch.into_iter();

        let first_elem = if let Some(value) = batch.next() {
            value
        } else {
            return;
        };

        let (left, right, marker) = self.neighbours(idx);
        let op = match &(left, right) {
            (Some(l), Some(r)) => {
                if self.topo.is_causally_before(l, r) {
                    Op::InsertBefore(*r, first_elem)
                } else {
                    Op::InsertAfter(*l, first_elem)
                }
            }
            (Some(l), None) => Op::InsertAfter(*l, first_elem),
            (None, Some(r)) => Op::InsertBefore(*r, first_elem),
            (None, None) => Op::InsertRoot(first_elem),
        };

        let mut extra_dependencies = self.roots.clone();

        if let Some(dep) = op.dependency() {
            // the op dependency will already be seen, no need to duplicated it in the extra dependencie.
            extra_dependencies.remove(&dep);
        }

        let node = HashNode {
            extra_dependencies,
            op,
        };

        let was_insert_before = matches!(node.op, Op::InsertBefore(_, _));
        let first_node_id = node.id();
        self.apply_without_invalidate(node);
        self.invalidate_markers_after(idx);

        if let Some(mut marker) = marker {
            if was_insert_before {
                let right = right.expect("Right should be defined if we are inserting before");
                marker.insert_dependency(&right, first_node_id);
            } else {
                marker.push_next(first_node_id);
            }

            self.markers.insert(idx, marker);
        }

        // All remaining elements will be chained after the first node
        let mut last_id = first_node_id;
        for e in batch {
            let node = HashNode {
                extra_dependencies: BTreeSet::new(),
                op: Op::InsertAfter(last_id, e),
            };

            last_id = node.id();

            self.apply_without_invalidate(node);
        }
    }

    fn log_len(&self) -> usize {
        let l = self.len();
        if l < 2 {
            1
        } else {
            self.len().ilog2() as usize
        }
    }

    fn marker_spacing(&self) -> usize {
        self.log_len()
        // dbg!(self.len() / self.log_len())
    }

    pub fn remove(&mut self, idx: usize) {
        if let Some((id, marker)) = self.iter_from(idx).marker() {
            let mut extra_dependencies = self.roots.clone();
            extra_dependencies.remove(&id); // insert will already be seen as a dependency;

            let node = HashNode {
                extra_dependencies,
                op: Op::Remove(id),
            };

            self.apply_without_invalidate(node);
            self.invalidate_markers_after(idx);
            let replacement_marker = self.topo.iter_from(&self.removed_inserts, &marker).marker();
            if let Some((_, marker)) = replacement_marker {
                self.markers.insert(idx, marker);
            } else {
                self.markers.remove(&idx);
            }
        }
    }

    pub fn remove_batch(&mut self, idx: usize, amount: usize) {
        for _ in 0..amount {
            self.remove(idx);
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
        self.apply_without_invalidate(node);
        self.markers.clear();
    }

    pub fn apply_without_invalidate(&mut self, node: HashNode) {
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
            "hello my name is davidzameena"
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
        assert_eq!(&seq_a.iter().collect::<String>(), "abaza");
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
