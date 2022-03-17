use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::Id;

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct Topo {
    roots: BTreeSet<Id>,
    before: BTreeMap<Id, BTreeSet<Id>>,
    after: BTreeMap<Id, BTreeSet<Id>>,
}

impl Topo {
    pub fn is_causally_before(&self, a: Id, b: Id) -> bool {
        let mut seen = BTreeSet::new();
        let mut boundary = VecDeque::from_iter(self.after(a));
        while let Some(n) = boundary.pop_front() {
            if n == b {
                return true;
            }
            seen.insert(n);
            boundary.extend(self.after(n).into_iter().filter(|a| !seen.contains(a)));
            if n != a {
                boundary.extend(self.before(n).into_iter().filter(|a| !seen.contains(a)));
            }
        }

        false
    }

    pub fn add_root(&mut self, node: Id) {
        self.insert(node);
        self.roots.insert(node);
    }

    pub fn add_after(&mut self, left: Id, node: Id) {
        self.insert(node); // is this necessary?
        self.after.entry(left).or_default().insert(node);
    }

    pub fn add_before(&mut self, right: Id, node: Id) {
        self.insert(node); // is this necessary?
        self.before.entry(right).or_default().insert(node);
    }

    pub fn add(&mut self, left: Option<Id>, node: Id, right: Option<Id>) {
        if left.is_none() && right.is_none() {
            self.add_root(node);
        }

        assert!(!(left.is_some() && right.is_some()));

        if let Some(left) = left {
            self.add_after(left, node);
        }

        if let Some(right) = right {
            self.add_before(right, node);
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

    fn roots(&self) -> &BTreeSet<Id> {
        &self.roots
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
    waiting_stack: Vec<(Id, BTreeSet<Id>)>,
}

impl<'a> TopoIter<'a> {
    fn new(topo: &'a Topo) -> Self {
        let used = BTreeSet::new();
        let free_stack = Vec::new();
        let waiting_stack = topo
            .roots()
            .into_iter()
            .map(|r| (*r, topo.before(*r)))
            .rev()
            .collect();

        Self {
            topo,
            used,
            free_stack,
            waiting_stack,
        }
    }
}

impl<'a> Iterator for TopoIter<'a> {
    type Item = Id;

    fn next(&mut self) -> Option<Id> {
        if let Some(n) = self.free_stack.pop() {
            for after in self.topo.after(n).into_iter().rev() {
                self.waiting_stack.push((after, self.topo.before(after)));
            }

            for (_, deps) in self.waiting_stack.iter_mut() {
                deps.remove(&n);
            }

            Some(n)
        } else {
            if let Some((ready, deps)) = self.waiting_stack.pop() {
                if deps.is_empty() {
                    self.free_stack.push(ready);
                } else {
                    self.waiting_stack.push((ready, deps.clone()));
                    for dep in deps.into_iter().rev() {
                        self.waiting_stack.push((dep, self.topo.before(dep)));
                    }
                }
                self.next()
            } else {
                None
            }
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
        topo.add(None, 2, Some(1));

        let mut iter = topo.iter();
        assert_eq!(dbg!(&mut iter).next(), Some(0));
        assert_eq!(dbg!(&mut iter).next(), Some(2));
        assert_eq!(dbg!(&mut iter).next(), Some(1));

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
        //    /
        //   1
        //
        // linearizes to 0213

        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(Some(0), 2, None);
        topo.add(Some(0), 3, None);
        dbg!(&topo);

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 2, 3]);

        topo.add(None, 1, Some(3));
        dbg!(&topo);

        let mut iter = topo.iter();
        assert_eq!(iter.next(), Some(0));
        // dbg!(&iter);
        //assert_eq!(iter.next(), Some(2));
        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 2, 1, 3]);
    }

    #[test]
    fn test_forks_remain_in_order_despite_prepend_case_2() {
        //   3
        //  /
        // 0 - 2
        //    /
        //   1
        //
        // linearizes to 0213

        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(Some(0), 2, None);
        topo.add(Some(0), 3, None);

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 2, 3]);

        topo.add(None, 1, Some(2));

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 1, 2, 3]);
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
        assert_eq!(tree.before(0), BTreeSet::from_iter([1]));

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

        assert_eq!(Vec::from_iter(topo.iter()), vec![1, 2, 3, 0]);
    }

    #[test]
    fn test_insert_between_root_and_element() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(Some(0), 1, None);
        topo.add(None, 2, Some(1));

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 2, 1]);
    }

    #[test]
    fn test_insert_between_root_and_element_twice() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(Some(0), 1, None);

        topo.add(None, 2, Some(1));

        topo.add(None, 3, Some(1));

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
    fn test_prepend_to_larger_branch() {
        let mut tree = Topo::default();
        tree.add(None, 0, None);
        tree.add(None, 1, None);
        tree.add(None, 2, None);

        tree.add(None, 3, Some(2));
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
