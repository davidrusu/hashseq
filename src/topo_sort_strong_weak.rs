use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

use crate::Id;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Link {
    Leaf,
    Strong(Id),
    Weak(Id),
    Fork { strong: Id, weak: Id },
}

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct Tree {
    children: BTreeMap<Id, Link>,
    parent: BTreeMap<Id, Id>,
}

impl Tree {
    pub fn leaf(&mut self, v: Id) {
        self.children.insert(v, Link::Leaf);
    }

    pub fn strong(&mut self, v: Id, next: Id) {
        self.children.insert(v, Link::Strong(next));
        self.parent.insert(next, v);
    }

    pub fn weak(&mut self, v: Id, next: Id) {
        self.children.insert(v, Link::Weak(next));
        self.parent.insert(next, v);
    }

    pub fn fork(&mut self, v: Id, strong: Id, weak: Id) {
        self.children.insert(v, Link::Fork { strong, weak });
        self.parent.insert(strong, v);
        self.parent.insert(weak, v);
    }

    pub fn root(&self) -> Option<Id> {
        let mut roots = self
            .children
            .keys()
            .filter(|v| !self.parent.contains_key(v));

        let root = roots.next().copied();

        assert_eq!(roots.next(), None); // there should be only one

        root
    }

    pub fn parent(&self, v: &Id) -> Option<Id> {
        self.parent.get(v).copied()
    }

    pub fn children(&self, v: &Id) -> Option<Link> {
        self.children.get(v).copied()
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Topo {
    tree: Tree,
}

impl Topo {
    pub fn add(&mut self, left: Option<Id>, node: Id, right: Option<Id>) {
        // println!("add {left:?} {node} {right:?}");
        assert!(self.tree.children(&node).is_none()); // we are currently not idempotent

        match (left, right) {
            (None, None) => {
                if let Some(root) = self.tree.root() {
                    if root < node {
                        let mut parent = root;
                        loop {
                            match self.tree.children(&parent).unwrap() {
                                Link::Leaf => {
                                    self.tree.weak(parent, node);
                                    self.tree.leaf(node);
                                    break;
                                }
                                Link::Strong(strong) => {
                                    self.tree.fork(parent, strong, node);
                                    self.tree.leaf(node);
                                    break;
                                }
                                Link::Weak(weak) => {
                                    assert_ne!(weak, node);
                                    if weak < node {
                                        parent = weak;
                                    } else {
                                        self.tree.weak(parent, node);
                                        self.tree.weak(node, weak);
                                        break;
                                    }
                                }
                                Link::Fork { strong, weak } => {
                                    if weak < node {
                                        parent = weak;
                                    } else {
                                        self.tree.fork(parent, strong, node);
                                        self.tree.weak(node, weak);
                                        break;
                                    }
                                }
                            };
                        }
                    } else {
                        self.tree.weak(node, root);
                    }
                } else {
                    self.tree.leaf(node);
                }
            }
            (None, Some(right)) => {
                let mut child = right;

                loop {
                    match self.tree.parent(&child) {
                        Some(parent) => {
                            assert_ne!(parent, node);
                            if node < parent {
                                child = parent
                            } else {
                                self.tree.weak(parent, node);
                                break;
                            }
                        }
                        None => {
                            break;
                        }
                    }
                }

                if child == right {
                    self.tree.strong(node, child);
                } else {
                    self.tree.weak(node, child);
                }
            }
            (Some(left), None) => match self.tree.children(&left).unwrap() {
                // we can remove this children().unwrap() by treating None as
                Link::Leaf => {
                    self.tree.strong(left, node);
                    self.tree.leaf(node);
                }
                Link::Strong(next) => {
                    assert_ne!(next, node);
                    if node < next {
                        self.tree.fork(left, node, next);
                        self.tree.leaf(node); // and we could remove all these leaf inserts
                    } else {
                        self.tree.fork(left, next, node);
                        self.tree.leaf(node);
                    }
                }
                Link::Weak(next) => {
                    assert_ne!(next, node);
                    self.tree.fork(left, node, next);
                    self.tree.leaf(node);
                }
                Link::Fork { strong, weak } => {
                    assert_ne!(strong, node);
                    assert_ne!(weak, node);
                    if node < strong {
                        self.tree.fork(left, node, strong);
                        self.tree.weak(strong, weak);
                        self.tree.leaf(node);
                    } else if node < weak {
                        self.tree.fork(left, strong, node);
                        self.tree.weak(node, weak);
                    } else {
                        self.tree.weak(weak, node);
                        self.tree.leaf(node);
                    }
                }
            },
            (Some(left), Some(right)) => {
                let mut child = right;
                loop {
                    assert_ne!(child, node);
                    assert_ne!(child, left);

                    let parent = self.tree.parent(&child).unwrap();
                    if parent == left || parent < node {
                        break;
                    }
                    child = parent
                }

                let child_parent = self.tree.parent(&child).unwrap();

                match self.tree.children(&child_parent).unwrap() {
                    Link::Leaf => panic!("left does not have a link"),
                    Link::Strong(id) => {
                        assert_eq!(id, child);
                        if child_parent == left {
                            self.tree.strong(child_parent, node);
                        } else {
                            self.tree.weak(child_parent, node);
                        }
                    }
                    Link::Weak(id) => {
                        assert_eq!(id, child);
                        self.tree.strong(child_parent, node);
                    }
                    Link::Fork { strong, weak } => {
                        if weak == child {
                            self.tree.fork(child_parent, strong, node);
                        } else {
                            assert_eq!(strong, child);
                            self.tree.fork(child_parent, node, weak);
                        }
                    }
                }
                if child == right {
                    self.tree.strong(node, child);
                } else {
                    self.tree.weak(node, child);
                }
            }
        }
    }

    pub fn iter(&self) -> TopoIter<'_> {
        TopoIter::new(self)
    }
}

#[derive(Debug)]
pub struct TopoIter<'a> {
    topo: &'a Topo,
    boundary: Vec<Id>,
    waiting: BTreeMap<Id, BTreeSet<Id>>,
}

impl<'a> TopoIter<'a> {
    pub fn new(topo: &'a Topo) -> Self {
        let boundary = Vec::from_iter(topo.tree.root());
        Self {
            topo,
            boundary,
            waiting: Default::default(),
        }
    }
}

impl<'a> Iterator for TopoIter<'a> {
    type Item = Id;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(next) = self.boundary.pop() {
            let mut to_add_to_boundary = Vec::new();
            for link in self.topo.tree.children(&next) {
                let afters = match link {
                    Link::Leaf => vec![],
                    Link::Strong(id) | Link::Weak(id) => vec![id],
                    Link::Fork { strong, weak } => vec![strong, weak],
                };

                for after in afters {
                    let after_dependencies = self
                        .waiting
                        .entry(after)
                        .or_insert_with(|| BTreeSet::from_iter(self.topo.tree.parent(&after)));

                    after_dependencies.remove(&next);

                    if after_dependencies.is_empty() {
                        to_add_to_boundary.push(after);
                        self.waiting.remove(&after);
                    }
                }
            }

            // to_add_to_boundary.sort();

            self.boundary.extend(to_add_to_boundary.into_iter().rev());

            Some(next)
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

        dbg!(&topo);

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
        dbg!(&topo);
        topo.add(Some(0), 1, None);
        dbg!(&topo);
        topo.add(Some(1), 4, None);
        dbg!(&topo);
        topo.add(Some(0), 2, None);
        dbg!(&topo);
        topo.add(Some(2), 3, None);
        dbg!(&topo);

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

        assert_eq!(
            topo.tree.children(&0),
            Some(Link::Fork { strong: 1, weak: 2 })
        );

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 1, 2]);
    }

    #[test]
    fn test_adding_smaller_vertex_at_fork() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(Some(0), 2, None);
        topo.add(Some(0), 1, None);

        assert_eq!(
            topo.tree.children(&0),
            Some(Link::Fork { strong: 1, weak: 2 })
        );

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 1, 2]);
    }

    #[test]
    fn test_adding_smaller_vertex_at_full_fork() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(Some(0), 2, None);
        topo.add(Some(0), 3, None);
        topo.add(Some(0), 1, None);

        assert_eq!(
            topo.tree.children(&0),
            Some(Link::Fork { strong: 1, weak: 2 })
        );
        assert_eq!(topo.tree.children(&2), Some(Link::Weak(3)));

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_adding_middle_vertex_at_full_fork() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(Some(0), 1, None);
        topo.add(Some(0), 3, None);

        assert_eq!(
            topo.tree.children(&0),
            Some(Link::Fork { strong: 1, weak: 3 })
        );

        topo.add(Some(0), 2, None);

        assert_eq!(
            topo.tree.children(&0),
            Some(Link::Fork { strong: 1, weak: 2 })
        );

        assert_eq!(topo.tree.children(&2), Some(Link::Weak(3)));

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_adding_bigger_vertex_at_full_fork() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(Some(0), 1, None);
        topo.add(Some(0), 2, None);

        assert_eq!(
            topo.tree.children(&0),
            Some(Link::Fork { strong: 1, weak: 2 })
        );

        topo.add(Some(0), 3, None);

        assert_eq!(
            topo.tree.children(&0),
            Some(Link::Fork { strong: 1, weak: 2 })
        );

        assert_eq!(topo.tree.children(&2), Some(Link::Weak(3)));

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_insert_before_root() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(None, 1, Some(0));

        assert_eq!(topo.tree.children(&0), Some(Link::Leaf));
        assert_eq!(topo.tree.parent(&0), Some(1));
        assert_eq!(topo.tree.children(&1), Some(Link::Strong(0)));
        assert_eq!(Vec::from_iter(topo.iter()), vec![1, 0]);
    }

    #[test]
    fn test_insert_before_root_twice() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(None, 1, Some(0));
        topo.add(None, 2, Some(0));

        assert_eq!(topo.tree.children(&0), Some(Link::Leaf));
        assert_eq!(topo.tree.parent(&0), Some(2));
        assert_eq!(topo.tree.children(&2), Some(Link::Strong(0)));
        assert_eq!(topo.tree.parent(&2), Some(1));
        assert_eq!(topo.tree.children(&1), Some(Link::Weak(2)));
        assert_eq!(topo.tree.parent(&1), None);

        assert_eq!(Vec::from_iter(topo.iter()), vec![1, 2, 0]);
    }

    #[test]
    fn test_insert_before_root_out_of_order() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(None, 2, Some(0));
        topo.add(None, 3, Some(0));
        topo.add(None, 1, Some(0));

        assert_eq!(topo.tree.children(&0), Some(Link::Leaf));
        assert_eq!(topo.tree.parent(&0), Some(3));
        assert_eq!(topo.tree.children(&3), Some(Link::Strong(0)));
        assert_eq!(topo.tree.parent(&3), Some(2));
        assert_eq!(topo.tree.children(&2), Some(Link::Weak(3)));
        assert_eq!(topo.tree.parent(&2), Some(1));
        assert_eq!(topo.tree.children(&1), Some(Link::Weak(2)));
        assert_eq!(topo.tree.parent(&1), None);

        assert_eq!(Vec::from_iter(topo.iter()), vec![1, 2, 3, 0]);
    }

    #[test]
    fn test_insert_at_strong_link() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(Some(0), 1, None);

        assert_eq!(topo.tree.children(&0), Some(Link::Strong(1)));
        assert_eq!(topo.tree.parent(&1), Some(0));

        topo.add(Some(0), 2, Some(1));

        assert_eq!(topo.tree.children(&0), Some(Link::Strong(2)));
        assert_eq!(topo.tree.children(&2), Some(Link::Strong(1)));
        assert_eq!(topo.tree.parent(&1), Some(2));
        assert_eq!(topo.tree.parent(&2), Some(0));

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 2, 1]);
    }

    #[test]
    fn test_insert_at_strong_link_twice() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(Some(0), 1, None);

        assert_eq!(topo.tree.children(&0), Some(Link::Strong(1)));
        assert_eq!(topo.tree.parent(&1), Some(0));

        topo.add(Some(0), 2, Some(1));

        assert_eq!(topo.tree.children(&0), Some(Link::Strong(2)));
        assert_eq!(topo.tree.children(&2), Some(Link::Strong(1)));

        topo.add(Some(0), 3, Some(1));

        assert_eq!(topo.tree.children(&0), Some(Link::Strong(2)));
        assert_eq!(topo.tree.children(&2), Some(Link::Weak(3)));
        assert_eq!(topo.tree.children(&3), Some(Link::Strong(1)));

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 2, 3, 1]);
    }

    #[test]
    fn test_concurrent_inserts() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(None, 1, None);

        assert_eq!(topo.tree.children(&0), Some(Link::Weak(1)));

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 1]);
    }

    #[test]
    fn test_concurrent_inserts_with_run() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(Some(0), 1, None);
        topo.add(None, 2, None);

        assert_eq!(
            topo.tree.children(&0),
            Some(Link::Fork { strong: 1, weak: 2 })
        );

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 1, 2]);
    }

    #[test]
    fn test_triple_concurrent_roots() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(None, 1, None);
        topo.add(None, 2, None);

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 1, 2]);
    }

    #[test]
    fn test_insert_at_weak_link() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(None, 1, None);
        topo.add(None, 2, None);

        topo.add(Some(0), 3, Some(2));
        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 1, 3, 2]);
    }

    #[quickcheck]
    fn prop_order_preservation_across_forks() {
        // for nodes a, b
        // if there exists sequence s \in S, a,b \in s with a < b in s
        // then forall q \in S where a,b \in q, a < b in q

        // that is, if node `a` comes before `b` in some sequence, `a` comes before `b` in all sequences.

        assert!(false);
    }
}
