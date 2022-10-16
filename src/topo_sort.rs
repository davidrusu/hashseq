use std::collections::{BTreeSet, HashMap, VecDeque};

use crate::Id;

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct Topo {
    roots: BTreeSet<Id>,
    before: HashMap<Id, BTreeSet<Id>>,
    after: HashMap<Id, BTreeSet<Id>>,
}

impl Topo {
    pub fn is_causally_before(&self, a: &Id, b: &Id) -> bool {
        let mut seen = BTreeSet::new();
        let mut boundary = VecDeque::from_iter(self.after(*a));
        while let Some(n) = boundary.pop_front() {
            if &n == b {
                return true;
            }
            seen.insert(n);
            boundary.extend(self.after(n).into_iter().filter(|a| !seen.contains(a)));
            if &n != a {
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

    fn insert(&mut self, n: Id) {
        self.before.entry(n).or_default();
        self.after.entry(n).or_default();
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

    pub fn iter(&self) -> TopoIter<'_> {
        TopoIter::new(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopoIter<'a> {
    topo: &'a Topo,
    waiting_stack: Vec<(&'a Id, Vec<&'a Id>)>,
}

impl<'a> TopoIter<'a> {
    fn new(topo: &'a Topo) -> Self {
        let mut iter = Self {
            topo,
            waiting_stack: Vec::new(),
        };

        for root in topo.roots().iter().rev() {
            iter.push_waiting(root);
        }

        iter
    }

    fn push_waiting(&mut self, n: &'a Id) {
        let mut deps = Vec::new();
        if let Some(befores) = self.topo.before.get(n) {
            deps.extend(befores)
        }
        self.waiting_stack.push((n, deps));
    }
}

impl<'a> Iterator for TopoIter<'a> {
    type Item = Id;

    fn next(&mut self) -> Option<Id> {
        let (n, deps) = self.waiting_stack.pop()?;

        if deps.is_empty() {
            // This node is free to be released, but first
            // queue up any nodes who come after this one
            if let Some(afters) = self.topo.after.get(n) {
                for after in afters.iter().rev() {
                    self.push_waiting(after);
                }
            }

            Some(*n)
        } else {
            // This node has dependencies that need to be
            // released before it.
            self.waiting_stack.push((n, Vec::new()));

            for dep in deps.into_iter().rev() {
                self.push_waiting(dep);
            }

            self.next()
        }
    }
}

#[cfg(test)]
mod tests {
    use quickcheck_macros::quickcheck;

    use super::*;

    fn n(n: u8) -> Id {
        [n; 32]
    }

    #[test]
    fn test_single() {
        let mut topo = Topo::default();

        topo.add_root(n(0));

        assert_eq!(Vec::from_iter(topo.iter()), vec![n(0)]);
    }

    #[test]
    fn test_one_insert() {
        let mut topo = Topo::default();

        topo.add_root(n(0));
        topo.add_after(n(0), n(1));

        assert_eq!(Vec::from_iter(topo.iter()), vec![n(0), n(1)]);

        let mut topo = Topo::default();

        topo.add_root(n(1));
        topo.add_after(n(1), n(0));

        assert_eq!(Vec::from_iter(topo.iter()), vec![n(1), n(0)]);
    }

    #[test]
    fn test_fork() {
        let mut topo = Topo::default();

        topo.add_root(n(0));
        topo.add_after(n(0), n(1));
        topo.add_after(n(0), n(2));

        assert_eq!(Vec::from_iter(topo.iter()), vec![n(0), n(1), n(2)]);
    }

    #[test]
    fn test_insert() {
        let mut topo = Topo::default();

        topo.add_root(n(0));
        topo.add_after(n(0), n(1));
        topo.add_before(n(1), n(2));

        let mut iter = topo.iter();
        assert_eq!(iter.next(), Some(n(0)));
        assert_eq!(iter.next(), Some(n(2)));
        assert_eq!(iter.next(), Some(n(1)));

        assert_eq!(Vec::from_iter(topo.iter()), vec![n(0), n(2), n(1)]);
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

        topo.add_root(n(0));
        topo.add_after(n(0), n(1));
        topo.add_after(n(1), n(4));
        topo.add_after(n(0), n(2));
        topo.add_after(n(2), n(3));

        assert_eq!(
            Vec::from_iter(topo.iter()),
            vec![n(0), n(1), n(4), n(2), n(3)]
        );
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
        topo.add_root(n(0));
        topo.add_after(n(0), n(2));
        topo.add_after(n(0), n(3));

        assert_eq!(Vec::from_iter(topo.iter()), vec![n(0), n(2), n(3)]);

        topo.add_before(n(3), n(1));

        let mut iter = topo.iter();
        assert_eq!(iter.next(), Some(n(0)));
        assert_eq!(Vec::from_iter(topo.iter()), vec![n(0), n(2), n(1), n(3)]);
    }

    #[test]
    fn test_forks_remain_in_order_despite_prepend_case_2() {
        //   3
        //  /
        // 0 - 2
        //    /
        //   1
        //
        // linearizes to 0123

        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_after(n(0), n(2));
        topo.add_after(n(0), n(3));

        assert_eq!(Vec::from_iter(topo.iter()), vec![n(0), n(2), n(3)]);

        topo.add_before(n(2), n(1));

        assert_eq!(Vec::from_iter(topo.iter()), vec![n(0), n(1), n(2), n(3)]);
    }

    #[test]
    fn test_adding_smaller_vertex_at_fork() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_after(n(0), n(2));
        topo.add_after(n(0), n(1));

        assert_eq!(topo.after(n(0)), BTreeSet::from_iter([n(1), n(2)]));

        assert_eq!(Vec::from_iter(topo.iter()), vec![n(0), n(1), n(2)]);
    }

    #[test]
    fn test_adding_smaller_vertex_at_full_fork() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_after(n(0), n(2));
        topo.add_after(n(0), n(3));
        topo.add_after(n(0), n(1));

        assert_eq!(topo.after(n(0)), BTreeSet::from_iter([n(1), n(2), n(3)]));
        assert_eq!(topo.after(n(1)), BTreeSet::from_iter([]));
        assert_eq!(topo.after(n(2)), BTreeSet::from_iter([]));
        assert_eq!(topo.after(n(3)), BTreeSet::from_iter([]));

        assert_eq!(Vec::from_iter(topo.iter()), vec![n(0), n(1), n(2), n(3)]);
    }

    #[test]
    fn test_adding_concurrent_middle_vertex() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_after(n(0), n(1));
        topo.add_after(n(0), n(3));

        assert_eq!(topo.after(n(0)), BTreeSet::from_iter([n(1), n(3)]));

        topo.add_after(n(0), n(2));

        assert_eq!(topo.after(n(0)), BTreeSet::from_iter([n(1), n(2), n(3)]));

        assert_eq!(Vec::from_iter(topo.iter()), vec![n(0), n(1), n(2), n(3)]);
    }

    #[test]
    fn test_adding_concurrent_bigger_vertex() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_after(n(0), n(1));
        topo.add_after(n(0), n(2));

        assert_eq!(topo.after(n(0)), BTreeSet::from_iter([n(1), n(2)]));

        topo.add_after(n(0), n(3));

        assert_eq!(topo.after(n(0)), BTreeSet::from_iter([n(1), n(2), n(3)]));

        assert_eq!(Vec::from_iter(topo.iter()), vec![n(0), n(1), n(2), n(3)]);
    }

    #[test]
    fn test_insert_before_root() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_before(n(0), n(1));

        assert_eq!(topo.after(n(0)), BTreeSet::from_iter([]));
        assert_eq!(topo.before(n(0)), BTreeSet::from_iter([n(1)]));

        assert_eq!(Vec::from_iter(topo.iter()), vec![n(1), n(0)]);
    }

    #[test]
    fn test_insert_before_root_twice() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_before(n(0), n(1));
        topo.add_before(n(0), n(2));

        assert_eq!(topo.after(n(0)), BTreeSet::from_iter([]));
        assert_eq!(topo.before(n(0)), BTreeSet::from_iter([n(1), n(2)]));

        assert_eq!(Vec::from_iter(topo.iter()), vec![n(1), n(2), n(0)]);
    }

    #[test]
    fn test_insert_before_root_out_of_order() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_before(n(0), n(2));
        topo.add_before(n(0), n(3));
        topo.add_before(n(0), n(1));

        assert_eq!(topo.after(n(0)), BTreeSet::from_iter([]));
        assert_eq!(topo.before(n(0)), BTreeSet::from_iter([n(1), n(2), n(3)]));

        assert_eq!(Vec::from_iter(topo.iter()), vec![n(1), n(2), n(3), n(0)]);
    }

    #[test]
    fn test_insert_between_root_and_element() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_after(n(0), n(1));
        topo.add_before(n(1), n(2));

        assert_eq!(Vec::from_iter(topo.iter()), vec![n(0), n(2), n(1)]);
    }

    #[test]
    fn test_insert_between_root_and_element_twice() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_after(n(0), n(1));
        topo.add_before(n(1), n(2));
        topo.add_before(n(1), n(3));

        assert_eq!(Vec::from_iter(topo.iter()), vec![n(0), n(2), n(3), n(1)]);
    }

    #[test]
    fn test_concurrent_inserts() {
        let mut topo = Topo::default();

        topo.add_root(n(0));
        topo.add_root(n(1));

        assert_eq!(Vec::from_iter(topo.iter()), vec![n(0), n(1)]);
    }

    #[test]
    fn test_concurrent_inserts_with_run() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_after(n(0), n(1));
        topo.add_root(n(2));

        assert_eq!(topo.after(n(0)), BTreeSet::from_iter([n(1)]));

        assert_eq!(Vec::from_iter(topo.iter()), vec![n(0), n(1), n(2)]);

        let mut topo_different_order = Topo::default();
        topo_different_order.add_root(n(2));
        topo_different_order.add_root(n(0));
        topo_different_order.add_after(n(0), n(1));

        assert_eq!(topo, topo_different_order);
    }

    #[test]
    fn test_triple_concurrent_roots() {
        let mut topo = Topo::default();

        topo.add_root(n(0));
        topo.add_root(n(1));
        topo.add_root(n(2));

        assert_eq!(Vec::from_iter(topo.iter()), vec![n(0), n(1), n(2)]);
    }

    #[test]
    fn test_prepend_to_larger_branch() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_root(n(1));
        topo.add_root(n(2));
        topo.add_before(n(2), n(3));
        assert_eq!(Vec::from_iter(topo.iter()), vec![n(0), n(1), n(3), n(2)]);
    }

    #[test]
    fn test_new_root_after_a_run() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_before(n(0), n(2));
        topo.add_root(n(1));

        assert_eq!(Vec::from_iter(topo.iter()), vec![n(2), n(0), n(1)]);

        let mut topo_different_order = Topo::default();
        topo_different_order.add_root(n(1));
        topo_different_order.add_root(n(0));
        topo_different_order.add_before(n(0), n(2));

        assert_eq!(topo, topo_different_order);
    }

    #[test]
    fn test_concurrent_prepend_and_append_seperated_by_a_node() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_after(n(0), n(1));
        topo.add_before(n(1), n(2));
        topo.add_after(n(0), n(3));

        assert_eq!(Vec::from_iter(topo.iter()), vec![n(0), n(2), n(1), n(3)]);

        let mut topo_reverse_order = Topo::default();
        topo_reverse_order.add_root(n(0));
        topo_reverse_order.add_after(n(0), n(3));
        topo_reverse_order.add_after(n(0), n(1));
        topo_reverse_order.add_before(n(1), n(2));

        assert_eq!(topo, topo_reverse_order);
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
