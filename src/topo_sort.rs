use std::collections::{BTreeMap, BTreeSet};

use crate::Id;

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct Topo {
    before: BTreeMap<Id, BTreeSet<Id>>,
    after: BTreeMap<Id, BTreeSet<Id>>,
}

impl Topo {
    pub fn add(&mut self, left: Option<Id>, node: Id, right: Option<Id>) {
        self.insert(node);
        if let Some(left) = left {
            self.add_constraint(left, node);
        }
        if let Some(right) = right {
            self.add_constraint(node, right);
        }
    }

    fn insert(&mut self, n: Id) {
        self.before.entry(n).or_default();
        self.after.entry(n).or_default();
    }

    fn add_constraint(&mut self, before: Id, after: Id) {
        self.before.entry(after).or_default().insert(before);
        self.after.entry(before).or_default().insert(after);
    }

    fn roots(&self) -> impl Iterator<Item = Id> + '_ {
        self.before
            .iter()
            .filter(|(_, befores)| befores.is_empty())
            .map(|(n, _)| *n)
    }

    pub fn after(&self, id: Id) -> BTreeSet<Id> {
        self.after.get(&id).cloned().unwrap_or_default()
    }

    pub fn before(&self, id: Id) -> BTreeSet<Id> {
        self.before.get(&id).cloned().unwrap_or_default()
    }

    pub fn iter<'a>(&'a self) -> TopoIter<'a> {
        TopoIter::new(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopoIter<'a> {
    topo: &'a Topo,
    used: BTreeSet<Id>,
    free_stack: Vec<Id>,
}

impl<'a> TopoIter<'a> {
    fn new(topo: &'a Topo) -> Self {
        let used = BTreeSet::new();
        let mut free_stack: Vec<Id> = topo.roots().collect();
        free_stack.sort();
        free_stack.reverse();

        Self {
            topo,
            used,
            free_stack,
        }
    }

    pub fn next_candidates(&self) -> impl Iterator<Item = Id> + '_ {
        self.free_stack.iter().copied()
    }
}

impl<'a> Iterator for TopoIter<'a> {
    type Item = Id;

    fn next(&mut self) -> Option<Id> {
        if let Some(n) = self.free_stack.pop() {
            self.used.insert(n);

            if let Some(afters) = self.topo.after.get(&n) {
                for after in afters.iter().rev() {
                    if self.topo.before[after].is_subset(&self.used) {
                        // its safe to push directly onto the free-stack since the afters are stored sorted (in a BTreeSet)
                        self.free_stack.push(*after);
                    }
                }
            }

            Some(n)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use quickcheck_macros::quickcheck;

    use super::*;

    #[test]
    fn test_single() {
        let mut topo = Topo::default();

        topo.add(None, 0, None);

        assert_eq!(Vec::from_iter(topo.iter()), vec![0]);
    }

    #[test]
    fn test_double() {
        let mut topo = Topo::default();

        topo.add(None, 0, None);
        topo.add(Some(0), 1, None);

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 1]);

        let mut topo = Topo::default();

        topo.add(None, 1, None);
        topo.add(Some(1), 0, None);

        assert_eq!(Vec::from_iter(topo.iter()), vec![1, 0]);
    }

    #[test]
    fn test_fork() {
        let mut topo = Topo::default();

        topo.add(None, 0, None);
        topo.add(Some(0), 1, None);
        topo.add(Some(0), 2, None);

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 1, 2]);
    }

    #[test]
    fn test_insert() {
        let mut topo = Topo::default();

        topo.add(None, 0, None);
        topo.add(Some(0), 1, None);
        topo.add(Some(0), 2, Some(1));

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 2, 1]);
    }

    #[test]
    fn test_runs_remain_uninterrupted() {
        //   1 - 4
        //  /
        // 0
        //  \
        //   2 - 3

        // linearizes to 01423

        let mut topo = Topo::default();

        topo.add(None, 0, None);
        topo.add(Some(0), 1, None);
        topo.add(Some(1), 4, None);
        topo.add(Some(0), 2, None);
        topo.add(Some(2), 3, None);

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 1, 4, 2, 3]);
    }

    #[test]
    fn test_forks_remain_in_order_despite_prepend() {
        //   2
        //  /
        // 0 - 3
        //  \ /
        //   1
        //
        // linearizes to 0213

        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(Some(0), 2, None);
        topo.add(Some(0), 3, None);

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 2, 3]);

        topo.add(Some(0), 1, Some(3));

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 2, 1, 3]);
    }

    #[test]
    fn test_forks_remain_in_order_despite_prepend_case_2() {
        //   3
        //  /
        // 0 - 2
        //  \ /
        //   1
        //
        // linearizes to 0213

        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(Some(0), 2, None);
        topo.add(Some(0), 3, None);

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 2, 3]);

        topo.add(Some(0), 1, Some(2));

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_adding_larger_vertex_at_fork() {
        // a == b
        //  \ <---- weak
        //   c

        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(Some(0), 1, None);
        topo.add(Some(0), 2, None);

        assert_eq!(topo.after(0), BTreeSet::from_iter([1, 2]));
        assert_eq!(topo.before(1), BTreeSet::from_iter([0]));
        assert_eq!(topo.before(2), BTreeSet::from_iter([0]));

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 1, 2]);
    }

    #[test]
    fn test_adding_smaller_vertex_at_fork() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(Some(0), 2, None);
        topo.add(Some(0), 1, None);

        assert_eq!(topo.after(0), BTreeSet::from_iter([1, 2]));

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 1, 2]);
    }

    #[test]
    fn test_adding_smaller_vertex_at_full_fork() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(Some(0), 2, None);
        topo.add(Some(0), 3, None);
        topo.add(Some(0), 1, None);

        assert_eq!(topo.after(0), BTreeSet::from_iter([1, 2, 3]));
        assert_eq!(topo.after(1), BTreeSet::from_iter([]));
        assert_eq!(topo.after(2), BTreeSet::from_iter([]));
        assert_eq!(topo.after(3), BTreeSet::from_iter([]));

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_adding_concurrent_middle_vertex() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(Some(0), 1, None);
        topo.add(Some(0), 3, None);

        assert_eq!(topo.after(0), BTreeSet::from_iter([1, 3]));

        topo.add(Some(0), 2, None);

        assert_eq!(topo.after(0), BTreeSet::from_iter([1, 2, 3]));

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_adding_concurrent_bigger_vertex() {
        let mut tree = Topo::default();
        tree.add(None, 0, None);
        tree.add(Some(0), 1, None);
        tree.add(Some(0), 2, None);

        assert_eq!(tree.after(0), BTreeSet::from_iter([1, 2]));

        tree.add(Some(0), 3, None);

        assert_eq!(tree.after(0), BTreeSet::from_iter([1, 2, 3]));

        assert_eq!(Vec::from_iter(tree.iter()), vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_insert_before_root() {
        let mut tree = Topo::default();
        tree.add(None, 0, None);
        tree.add(None, 1, Some(0));

        assert_eq!(tree.after(0), BTreeSet::from_iter([]));
        assert_eq!(tree.after(1), BTreeSet::from_iter([0]));
        assert_eq!(tree.before(0), BTreeSet::from_iter([1]));
        assert_eq!(tree.before(1), BTreeSet::from_iter([]));

        assert_eq!(Vec::from_iter(tree.iter()), vec![1, 0]);
    }

    #[test]
    fn test_insert_before_root_twice() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(None, 1, Some(0));
        topo.add(None, 2, Some(0));

        assert_eq!(topo.after(0), BTreeSet::from_iter([]));
        assert_eq!(topo.before(0), BTreeSet::from_iter([1, 2]));
        assert_eq!(topo.after(1), BTreeSet::from_iter([0]));
        assert_eq!(topo.after(2), BTreeSet::from_iter([0]));
        assert_eq!(topo.before(1), BTreeSet::from_iter([]));
        assert_eq!(topo.before(2), BTreeSet::from_iter([]));

        assert_eq!(Vec::from_iter(topo.iter()), vec![1, 2, 0]);
    }

    #[test]
    fn test_insert_before_root_out_of_order() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(None, 2, Some(0));
        topo.add(None, 3, Some(0));
        topo.add(None, 1, Some(0));

        assert_eq!(topo.after(0), BTreeSet::from_iter([]));
        assert_eq!(topo.before(0), BTreeSet::from_iter([1, 2, 3]));
        assert_eq!(topo.after(1), BTreeSet::from_iter([0]));
        assert_eq!(topo.after(2), BTreeSet::from_iter([0]));
        assert_eq!(topo.after(3), BTreeSet::from_iter([0]));

        assert_eq!(Vec::from_iter(topo.iter()), vec![1, 2, 3, 0]);
    }

    #[test]
    fn test_insert_between_root_and_element() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(Some(0), 1, None);

        assert_eq!(topo.after(0), BTreeSet::from_iter([1]));
        assert_eq!(topo.before(1), BTreeSet::from_iter([0]));

        topo.add(Some(0), 2, Some(1));

        assert_eq!(topo.after(0), BTreeSet::from_iter([1, 2]));
        assert_eq!(topo.before(1), BTreeSet::from_iter([0, 2]));
        assert_eq!(topo.before(2), BTreeSet::from_iter([0]));

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 2, 1]);
    }

    #[test]
    fn test_insert_between_root_and_element_twice() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(Some(0), 1, None);

        assert_eq!(topo.after(0), BTreeSet::from_iter([1]));
        assert_eq!(topo.before(1), BTreeSet::from_iter([0]));

        topo.add(Some(0), 2, Some(1));

        assert_eq!(topo.after(0), BTreeSet::from_iter([1, 2]));
        assert_eq!(topo.before(1), BTreeSet::from_iter([0, 2]));
        assert_eq!(topo.before(2), BTreeSet::from_iter([0]));

        topo.add(Some(0), 3, Some(1));

        assert_eq!(topo.after(0), BTreeSet::from_iter([1, 2, 3]));
        assert_eq!(topo.before(1), BTreeSet::from_iter([0, 2, 3]));
        assert_eq!(topo.before(2), BTreeSet::from_iter([0]));
        assert_eq!(topo.before(3), BTreeSet::from_iter([0]));

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 2, 3, 1]);
    }

    #[test]
    fn test_concurrent_inserts() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(None, 1, None);

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 1]);
    }

    #[test]
    fn test_concurrent_inserts_with_run() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(Some(0), 1, None);
        topo.add(None, 2, None);

        assert_eq!(topo.after(0), BTreeSet::from_iter([1]));

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 1, 2]);

        let mut tree_different_order = Topo::default();
        tree_different_order.add(None, 2, None);
        tree_different_order.add(None, 0, None);
        tree_different_order.add(Some(0), 1, None);

        assert_eq!(topo, tree_different_order);
    }

    #[test]
    fn test_triple_concurrent_roots() {
        let mut tree = Topo::default();
        tree.add(None, 0, None);
        tree.add(None, 1, None);
        tree.add(None, 2, None);

        assert_eq!(Vec::from_iter(tree.iter()), vec![0, 1, 2]);
    }

    #[test]
    fn test_insert_at_weak_link() {
        let mut tree = Topo::default();
        tree.add(None, 0, None);
        tree.add(None, 1, None);
        tree.add(None, 2, None);

        tree.add(Some(0), 3, Some(2));
        assert_eq!(Vec::from_iter(tree.iter()), vec![0, 1, 3, 2]);
    }

    #[test]
    fn test_new_root_after_a_run() {
        let mut tree = Topo::default();
        tree.add(None, 0, None);
        tree.add(None, 2, Some(0));
        dbg!(&tree);
        tree.add(None, 1, None);

        dbg!(&tree);
        assert_eq!(Vec::from_iter(tree.iter()), vec![2, 0, 1]);

        assert_eq!(tree.after(2), BTreeSet::from_iter([0]));
        assert_eq!(tree.after(1), BTreeSet::from_iter([]));
        assert_eq!(tree.after(0), BTreeSet::from_iter([]));
        assert_eq!(tree.before(0), BTreeSet::from_iter([2]));

        let mut tree_different_order = Topo::default();
        tree_different_order.add(None, 1, None);
        tree_different_order.add(None, 0, None);
        tree_different_order.add(None, 2, Some(0));

        assert_eq!(tree, tree_different_order);
    }

    #[test]
    fn test_concurrent_prepend_and_append_seperated_by_a_node() {
        let mut tree = Topo::default();
        tree.add(None, 0, None);
        tree.add(Some(0), 1, None);
        tree.add(None, 2, Some(1));
        tree.add(Some(0), 3, None);

        assert_eq!(Vec::from_iter(tree.iter()), vec![0, 2, 1, 3]);

        let mut tree_reverse_order = Topo::default();
        tree_reverse_order.add(None, 0, None);
        tree_reverse_order.add(Some(0), 3, None);
        tree_reverse_order.add(Some(0), 1, None);
        tree_reverse_order.add(None, 2, Some(1));

        assert_eq!(tree, tree_reverse_order);
    }

    #[ignore]
    #[quickcheck]
    fn prop_order_preservation_across_forks() {
        // for nodes a, b
        // if there exists sequence s \in S, a,b \in s with a < b in s
        // then forall q \in S where a,b \in q, a < b in q

        // that is, if node `a` comes before `b` in some sequence, `a` comes before `b` in all sequences.
    }
}
