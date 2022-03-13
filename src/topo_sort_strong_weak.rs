use std::collections::{BTreeMap, BTreeSet};

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

    pub fn add(&mut self, left: Option<Id>, node: Id, right: Option<Id>) {
        println!("add {left:?} {node} {right:?}");
        assert!(self.children(&node).is_none()); // we are currently not idempotent

        match (left, right) {
            (None, None) => {
                if let Some(root) = dbg!(self.root()) {
                    // if dbg!(root < node) {
                    let mut parent = root;
                    loop {
                        dbg!(&parent);
                        match dbg!(self.children(&parent).unwrap()) {
                            Link::Leaf => {
                                if parent < node {
                                    self.weak(parent, node);
                                    self.leaf(node);
                                } else {
                                    self.weak(node, parent);
                                }
                                break;
                            }
                            Link::Strong(strong) => {
                                parent = strong;
                                // self.fork(parent, strong, node);
                                // self.leaf(node);
                                // break;
                            }
                            Link::Weak(weak) => {
                                assert_ne!(weak, node);
                                if weak < node {
                                    parent = weak;
                                } else {
                                    self.weak(parent, node);
                                    self.weak(node, weak);
                                    break;
                                }
                            }
                            Link::Fork { strong, weak } => {
                                if weak < node {
                                    parent = weak;
                                } else {
                                    self.fork(parent, strong, node);
                                    self.weak(node, weak);
                                    break;
                                }
                            }
                        };
                    }
                    // } else {
                    //     self.weak(node, root);
                    // }
                } else {
                    self.leaf(node);
                }
            }
            (None, Some(right)) => {
                let mut child = right;

                while let Some(parent) = self.parent(&child) {
                    assert_ne!(parent, node);
                    match self.children(&parent).unwrap() {
                        Link::Leaf => panic!("unexpected leaf parent"),
                        Link::Strong(strong) => {
                            assert_eq!(strong, child);
                        }
                        Link::Weak(_) => todo!(),
                        Link::Fork { strong, weak } => todo!(),
                    }
                    if node < parent {
                        child = parent
                    } else {
                        self.weak(parent, node);
                        break;
                    }
                }

                if child == right {
                    self.strong(node, child);
                } else {
                    self.weak(node, child);
                }
            }
            (Some(left), None) => match self.children(&left).unwrap() {
                // we can remove this children().unwrap() by treating None as
                Link::Leaf => {
                    self.strong(left, node);
                    self.leaf(node);
                }
                Link::Strong(next) => {
                    assert_ne!(next, node);
                    if node < next {
                        self.fork(left, node, next);
                        self.leaf(node); // and we could remove all these leaf inserts
                    } else {
                        self.fork(left, next, node);
                        self.leaf(node);
                    }
                }
                Link::Weak(next) => {
                    assert_ne!(next, node);
                    self.fork(left, node, next);
                    self.leaf(node);
                }
                Link::Fork { strong, weak } => {
                    assert_ne!(strong, node);
                    assert_ne!(weak, node);
                    if node < strong {
                        self.fork(left, node, strong);
                        self.weak(strong, weak);
                        self.leaf(node);
                    } else if node < weak {
                        self.fork(left, strong, node);
                        self.weak(node, weak);
                    } else {
                        self.weak(weak, node);
                        self.leaf(node);
                    }
                }
            },
            (Some(left), Some(right)) => {
                let mut child = right;
                loop {
                    assert_ne!(child, node);
                    assert_ne!(child, left);

                    let parent = self.parent(&child).unwrap();
                    if parent == left || parent < node {
                        break;
                    }
                    child = parent
                }

                let child_parent = self.parent(&child).unwrap();

                match self.children(&child_parent).unwrap() {
                    Link::Leaf => panic!("left does not have a link"),
                    Link::Strong(id) => {
                        assert_eq!(id, child);
                        if child_parent == left {
                            self.strong(child_parent, node);
                        } else {
                            self.weak(child_parent, node);
                        }
                    }
                    Link::Weak(id) => {
                        assert_eq!(id, child);
                        self.strong(child_parent, node);
                    }
                    Link::Fork { strong, weak } => {
                        if weak == child {
                            self.fork(child_parent, strong, node);
                        } else {
                            assert_eq!(strong, child);
                            self.fork(child_parent, node, weak);
                        }
                    }
                }
                if child == right {
                    self.strong(node, child);
                } else {
                    self.weak(node, child);
                }
            }
        }
    }

    pub fn iter(&self) -> TreeIter<'_> {
        TreeIter::new(self)
    }
}

#[derive(Debug)]
pub struct TreeIter<'a> {
    tree: &'a Tree,
    boundary: Vec<Id>,
    waiting: BTreeMap<Id, BTreeSet<Id>>,
}

impl<'a> TreeIter<'a> {
    pub fn new(tree: &'a Tree) -> Self {
        let boundary = Vec::from_iter(tree.root());
        Self {
            tree,
            boundary,
            waiting: Default::default(),
        }
    }
}

impl<'a> Iterator for TreeIter<'a> {
    type Item = Id;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(next) = self.boundary.pop() {
            let mut to_add_to_boundary = Vec::new();
            if let Some(link) = self.tree.children(&next) {
                let afters = match link {
                    Link::Leaf => vec![],
                    Link::Strong(id) | Link::Weak(id) => vec![id],
                    Link::Fork { strong, weak } => vec![strong, weak],
                };

                for after in afters {
                    let after_dependencies = self
                        .waiting
                        .entry(after)
                        .or_insert_with(|| BTreeSet::from_iter(self.tree.parent(&after)));

                    after_dependencies.remove(&next);

                    if after_dependencies.is_empty() {
                        to_add_to_boundary.push(after);
                        self.waiting.remove(&after);
                    }
                }
            }

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
        let mut tree = Tree::default();

        tree.add(None, 0, None);

        assert_eq!(Vec::from_iter(tree.iter()), vec![0]);
    }

    #[test]
    fn test_double() {
        let mut tree = Tree::default();

        tree.add(None, 0, None);
        tree.add(Some(0), 1, None);

        assert_eq!(Vec::from_iter(tree.iter()), vec![0, 1]);

        let mut tree = Tree::default();

        tree.add(None, 1, None);
        tree.add(Some(1), 0, None);

        assert_eq!(Vec::from_iter(tree.iter()), vec![1, 0]);
    }

    #[test]
    fn test_fork() {
        let mut tree = Tree::default();

        tree.add(None, 0, None);
        tree.add(Some(0), 1, None);
        tree.add(Some(0), 2, None);

        assert_eq!(Vec::from_iter(tree.iter()), vec![0, 1, 2]);
    }

    #[test]
    fn test_insert() {
        let mut tree = Tree::default();

        tree.add(None, 0, None);
        tree.add(Some(0), 1, None);
        tree.add(Some(0), 2, Some(1));

        assert_eq!(Vec::from_iter(tree.iter()), vec![0, 2, 1]);
    }

    #[test]
    fn test_runs_remain_uninterrupted() {
        //   1 - 4
        //  /
        // 0
        //  \
        //   2 - 3

        // linearizes to 01423

        let mut tree = Tree::default();

        tree.add(None, 0, None);
        tree.add(Some(0), 1, None);
        tree.add(Some(1), 4, None);
        tree.add(Some(0), 2, None);
        tree.add(Some(2), 3, None);

        assert_eq!(Vec::from_iter(tree.iter()), vec![0, 1, 4, 2, 3]);
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

        let mut tree = Tree::default();
        tree.add(None, 0, None);
        tree.add(Some(0), 2, None);
        tree.add(Some(0), 3, None);

        assert_eq!(Vec::from_iter(tree.iter()), vec![0, 2, 3]);

        tree.add(Some(0), 1, Some(3));

        assert_eq!(Vec::from_iter(tree.iter()), vec![0, 2, 1, 3]);
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

        let mut tree = Tree::default();
        tree.add(None, 0, None);
        tree.add(Some(0), 2, None);
        tree.add(Some(0), 3, None);

        assert_eq!(Vec::from_iter(tree.iter()), vec![0, 2, 3]);

        tree.add(Some(0), 1, Some(2));

        assert_eq!(Vec::from_iter(tree.iter()), vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_adding_larger_vertex_at_fork() {
        // a == b
        //  \ <---- weak
        //   c

        let mut tree = Tree::default();
        tree.add(None, 0, None);
        tree.add(Some(0), 1, None);
        tree.add(Some(0), 2, None);

        assert_eq!(tree.children(&0), Some(Link::Fork { strong: 1, weak: 2 }));

        assert_eq!(Vec::from_iter(tree.iter()), vec![0, 1, 2]);
    }

    #[test]
    fn test_adding_smaller_vertex_at_fork() {
        let mut tree = Tree::default();
        tree.add(None, 0, None);
        tree.add(Some(0), 2, None);
        tree.add(Some(0), 1, None);

        assert_eq!(tree.children(&0), Some(Link::Fork { strong: 1, weak: 2 }));

        assert_eq!(Vec::from_iter(tree.iter()), vec![0, 1, 2]);
    }

    #[test]
    fn test_adding_smaller_vertex_at_full_fork() {
        let mut tree = Tree::default();
        tree.add(None, 0, None);
        tree.add(Some(0), 2, None);
        tree.add(Some(0), 3, None);
        tree.add(Some(0), 1, None);

        assert_eq!(tree.children(&0), Some(Link::Fork { strong: 1, weak: 2 }));
        assert_eq!(tree.children(&2), Some(Link::Weak(3)));

        assert_eq!(Vec::from_iter(tree.iter()), vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_adding_middle_vertex_at_full_fork() {
        let mut tree = Tree::default();
        tree.add(None, 0, None);
        tree.add(Some(0), 1, None);
        tree.add(Some(0), 3, None);

        assert_eq!(tree.children(&0), Some(Link::Fork { strong: 1, weak: 3 }));

        tree.add(Some(0), 2, None);

        assert_eq!(tree.children(&0), Some(Link::Fork { strong: 1, weak: 2 }));

        assert_eq!(tree.children(&2), Some(Link::Weak(3)));

        assert_eq!(Vec::from_iter(tree.iter()), vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_adding_bigger_vertex_at_full_fork() {
        let mut tree = Tree::default();
        tree.add(None, 0, None);
        tree.add(Some(0), 1, None);
        tree.add(Some(0), 2, None);

        assert_eq!(tree.children(&0), Some(Link::Fork { strong: 1, weak: 2 }));

        tree.add(Some(0), 3, None);

        assert_eq!(tree.children(&0), Some(Link::Fork { strong: 1, weak: 2 }));

        assert_eq!(tree.children(&2), Some(Link::Weak(3)));

        assert_eq!(Vec::from_iter(tree.iter()), vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_insert_before_root() {
        let mut tree = Tree::default();
        tree.add(None, 0, None);
        tree.add(None, 1, Some(0));

        assert_eq!(tree.children(&0), Some(Link::Leaf));
        assert_eq!(tree.parent(&0), Some(1));
        assert_eq!(tree.children(&1), Some(Link::Strong(0)));
        assert_eq!(Vec::from_iter(tree.iter()), vec![1, 0]);
    }

    #[test]
    fn test_insert_before_root_twice() {
        let mut tree = Tree::default();
        tree.add(None, 0, None);
        tree.add(None, 1, Some(0));
        tree.add(None, 2, Some(0));

        assert_eq!(tree.children(&0), Some(Link::Leaf));
        assert_eq!(tree.parent(&0), Some(2));
        assert_eq!(tree.children(&2), Some(Link::Strong(0)));
        assert_eq!(tree.parent(&2), Some(1));
        assert_eq!(tree.children(&1), Some(Link::Weak(2)));
        assert_eq!(tree.parent(&1), None);

        assert_eq!(Vec::from_iter(tree.iter()), vec![1, 2, 0]);
    }

    #[test]
    fn test_insert_before_root_out_of_order() {
        let mut tree = Tree::default();
        tree.add(None, 0, None);
        tree.add(None, 2, Some(0));
        tree.add(None, 3, Some(0));
        tree.add(None, 1, Some(0));

        assert_eq!(tree.children(&0), Some(Link::Leaf));
        assert_eq!(tree.parent(&0), Some(3));
        assert_eq!(tree.children(&3), Some(Link::Strong(0)));
        assert_eq!(tree.parent(&3), Some(2));
        assert_eq!(tree.children(&2), Some(Link::Weak(3)));
        assert_eq!(tree.parent(&2), Some(1));
        assert_eq!(tree.children(&1), Some(Link::Weak(2)));
        assert_eq!(tree.parent(&1), None);

        assert_eq!(Vec::from_iter(tree.iter()), vec![1, 2, 3, 0]);
    }

    #[test]
    fn test_insert_at_strong_link() {
        let mut tree = Tree::default();
        tree.add(None, 0, None);
        tree.add(Some(0), 1, None);

        assert_eq!(tree.children(&0), Some(Link::Strong(1)));
        assert_eq!(tree.parent(&1), Some(0));

        tree.add(Some(0), 2, Some(1));

        assert_eq!(tree.children(&0), Some(Link::Strong(2)));
        assert_eq!(tree.children(&2), Some(Link::Strong(1)));
        assert_eq!(tree.parent(&1), Some(2));
        assert_eq!(tree.parent(&2), Some(0));

        assert_eq!(Vec::from_iter(tree.iter()), vec![0, 2, 1]);
    }

    #[test]
    fn test_insert_at_strong_link_twice() {
        let mut tree = Tree::default();
        tree.add(None, 0, None);
        tree.add(Some(0), 1, None);

        assert_eq!(tree.children(&0), Some(Link::Strong(1)));
        assert_eq!(tree.parent(&1), Some(0));

        tree.add(Some(0), 2, Some(1));

        assert_eq!(tree.children(&0), Some(Link::Strong(2)));
        assert_eq!(tree.children(&2), Some(Link::Strong(1)));

        tree.add(Some(0), 3, Some(1));

        assert_eq!(tree.children(&0), Some(Link::Strong(2)));
        assert_eq!(tree.children(&2), Some(Link::Weak(3)));
        assert_eq!(tree.children(&3), Some(Link::Strong(1)));

        assert_eq!(Vec::from_iter(tree.iter()), vec![0, 2, 3, 1]);
    }

    #[test]
    fn test_concurrent_inserts() {
        let mut tree = Tree::default();
        tree.add(None, 0, None);
        tree.add(None, 1, None);

        assert_eq!(tree.children(&0), Some(Link::Weak(1)));

        assert_eq!(Vec::from_iter(tree.iter()), vec![0, 1]);
    }

    #[test]
    fn test_concurrent_inserts_with_run() {
        let mut tree = Tree::default();
        tree.add(None, 0, None);
        tree.add(Some(0), 1, None);
        tree.add(None, 2, None);

        assert_eq!(tree.children(&0), Some(Link::Fork { strong: 1, weak: 2 }));

        assert_eq!(Vec::from_iter(tree.iter()), vec![0, 1, 2]);

        let mut tree_different_order = Tree::default();
        tree_different_order.add(None, 2, None);
        tree_different_order.add(None, 0, None);
        tree_different_order.add(Some(0), 1, None);

        assert_eq!(tree, tree_different_order);
    }

    #[test]
    fn test_triple_concurrent_roots() {
        let mut tree = Tree::default();
        tree.add(None, 0, None);
        tree.add(None, 1, None);
        tree.add(None, 2, None);

        assert_eq!(Vec::from_iter(tree.iter()), vec![0, 1, 2]);
    }

    #[test]
    fn test_insert_at_weak_link() {
        let mut tree = Tree::default();
        tree.add(None, 0, None);
        tree.add(None, 1, None);
        tree.add(None, 2, None);

        tree.add(Some(0), 3, Some(2));
        assert_eq!(Vec::from_iter(tree.iter()), vec![0, 1, 3, 2]);
    }

    #[test]
    fn test_new_root_after_a_strong_link() {
        let mut tree = Tree::default();
        tree.add(None, 0, None);
        tree.add(None, 2, Some(0));
        dbg!(&tree);
        tree.add(None, 1, None);

        dbg!(&tree);
        assert_eq!(Vec::from_iter(tree.iter()), vec![2, 0, 1]);

        assert_eq!(tree.children(&2), Some(Link::Strong(0)));
        assert_eq!(tree.children(&1), Some(Link::Leaf));
        assert_eq!(tree.children(&0), Some(Link::Weak(1)));

        let mut tree_different_order = Tree::default();
        tree_different_order.add(None, 1, None);
        tree_different_order.add(None, 0, None);
        tree_different_order.add(None, 2, Some(0));

        assert_eq!(tree, tree_different_order);
    }

    #[test]
    fn test_concurrent_prepend_and_append_seperated_by_a_node() {
        let mut tree = Tree::default();
        tree.add(None, 0, None);
        tree.add(Some(0), 1, None);
        tree.add(None, 2, Some(1));
        tree.add(Some(0), 3, None);

        assert_eq!(Vec::from_iter(tree.iter()), vec![0, 2, 1, 3]);

        let mut tree_reverse_order = Tree::default();
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
