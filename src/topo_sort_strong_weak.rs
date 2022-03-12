use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

use crate::Id;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Link {
    End,
    Strong(Id),
    Weak(Id),
    Fork { strong: Id, weak: Id },
}

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct BiMap {
    after: BTreeMap<Id, Link>,
    before: BTreeMap<Id, Id>,
}

impl BiMap {
    pub fn end(&mut self, v: Id) {
        self.after.insert(v, Link::End);
    }

    pub fn strong(&mut self, v: Id, next: Id) {
        self.after.insert(v, Link::Strong(next));
        self.before.insert(next, v);
    }

    pub fn weak(&mut self, v: Id, next: Id) {
        self.after.insert(v, Link::Weak(next));
        self.before.insert(next, v);
    }

    pub fn fork(&mut self, v: Id, strong: Id, weak: Id) {
        self.after.insert(v, Link::Fork { strong, weak });
        self.before.insert(strong, v);
        self.before.insert(weak, v);
    }

    pub fn first(&self) -> Option<Id> {
        let mut firsts = self.after.keys().filter(|v| !self.before.contains_key(v));
        let first = firsts.next().copied();

        assert_eq!(firsts.next(), None); // there should be only one

        first
    }

    pub fn before(&self, v: &Id) -> Option<Id> {
        self.before.get(v).copied()
    }

    pub fn after(&self, v: &Id) -> Option<Link> {
        self.after.get(v).copied()
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Topo {
    edges: BiMap,
}

impl Topo {
    pub fn add(&mut self, left: Option<Id>, elem: Id, right: Option<Id>) {
        println!("add {left:?} {elem} {right:?}");
        assert!(!self.edges.after.contains_key(&elem));

        match (left, right) {
            (None, None) => self.edges.end(elem),
            (None, Some(right)) => {
                let mut current_right = right;
                loop {
                    match self.edges.before(&current_right) {
                        Some(before) => {
                            if before < elem {
                                self.edges.weak(before, elem);
                                break;
                            } else if elem < before {
                                current_right = before
                            }
                        }
                        None => {
                            break;
                        }
                    }
                }

                if current_right == right {
                    self.edges.strong(elem, current_right);
                } else {
                    self.edges.weak(elem, current_right);
                }
            }
            (Some(left), None) => match self.edges.after(&left).unwrap() {
                Link::End => {
                    self.edges.strong(left, elem);
                    self.edges.end(elem);
                }
                Link::Strong(next) => match elem.cmp(&next) {
                    Ordering::Less => {
                        self.edges.fork(left, elem, next);
                        self.edges.end(elem);
                    }
                    Ordering::Equal => panic!("ids are equal"),
                    Ordering::Greater => {
                        self.edges.fork(left, next, elem);
                        self.edges.end(elem);
                    }
                },
                Link::Weak(next) => {
                    self.edges.fork(left, elem, next);
                    self.edges.end(elem);
                }
                Link::Fork { strong, weak } => {
                    if elem < strong {
                        self.edges.fork(left, elem, strong);
                        self.edges.weak(strong, weak);
                        self.edges.end(elem);
                    } else if elem > strong && elem < weak {
                        self.edges.fork(left, strong, elem);
                        self.edges.weak(elem, weak);
                    } else if elem > weak {
                        self.edges.weak(weak, elem);
                        self.edges.end(elem);
                    } else {
                        panic!("Unhandled case");
                    }
                }
            },
            (Some(left), Some(right)) => {
                let mut current_right = right;
                loop {
                    match self.edges.before(&current_right) {
                        Some(before) => {
                            if before == left || before < elem {
                                break;
                            } else if elem < before {
                                current_right = before
                            }
                        }
                        None => panic!("Unhandled case"),
                    }
                }

                let before_right = self.edges.before(&current_right).unwrap();

                match self.edges.after(&before_right).unwrap() {
                    Link::End => panic!("left does not have a link"),
                    Link::Strong(id) => {
                        assert_eq!(id, current_right);
                        if before_right == left {
                            self.edges.strong(before_right, elem);
                        } else {
                            self.edges.weak(before_right, elem);
                        }
                        if current_right == right {
                            self.edges.strong(elem, current_right);
                        } else {
                            self.edges.weak(elem, current_right);
                        }
                    }
                    Link::Weak(id) => {
                        assert_eq!(id, current_right);
                        self.edges.strong(before_right, elem);
                        if current_right == right {
                            self.edges.strong(elem, current_right);
                        } else {
                            self.edges.weak(elem, current_right);
                        }
                    }
                    Link::Fork { strong, weak } => {
                        if weak == current_right {
                            self.edges.fork(before_right, strong, elem);
                        } else {
                            assert_eq!(strong, current_right);
                            self.edges.fork(before_right, elem, weak);
                        }

                        if current_right == right {
                            self.edges.strong(elem, current_right);
                        } else {
                            self.edges.weak(elem, current_right);
                        }
                    }
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
        let mut boundary = Vec::from_iter(topo.edges.first());
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
        dbg!(&self);
        if let Some(next) = self.boundary.pop() {
            let mut to_add_to_boundary = Vec::new();
            for link in self.topo.edges.after(&next) {
                let afters = match link {
                    Link::End => vec![],
                    Link::Strong(id) | Link::Weak(id) => vec![id],
                    Link::Fork { strong, weak } => vec![strong, weak],
                };

                for after in afters {
                    let after_dependencies = self
                        .waiting
                        .entry(after)
                        .or_insert_with(|| BTreeSet::from_iter(self.topo.edges.before(&after)));

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
            topo.edges.after(&0),
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
            topo.edges.after(&0),
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
            topo.edges.after(&0),
            Some(Link::Fork { strong: 1, weak: 2 })
        );
        assert_eq!(topo.edges.after(&2), Some(Link::Weak(3)));

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_adding_middle_vertex_at_full_fork() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(Some(0), 1, None);
        topo.add(Some(0), 3, None);

        assert_eq!(
            topo.edges.after(&0),
            Some(Link::Fork { strong: 1, weak: 3 })
        );

        topo.add(Some(0), 2, None);

        assert_eq!(
            topo.edges.after(&0),
            Some(Link::Fork { strong: 1, weak: 2 })
        );

        assert_eq!(topo.edges.after(&2), Some(Link::Weak(3)));

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_adding_bigger_vertex_at_full_fork() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(Some(0), 1, None);
        topo.add(Some(0), 2, None);

        assert_eq!(
            topo.edges.after(&0),
            Some(Link::Fork { strong: 1, weak: 2 })
        );

        topo.add(Some(0), 3, None);

        assert_eq!(
            topo.edges.after(&0),
            Some(Link::Fork { strong: 1, weak: 2 })
        );

        assert_eq!(topo.edges.after(&2), Some(Link::Weak(3)));

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_insert_before_root() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(None, 1, Some(0));

        assert_eq!(topo.edges.after(&0), Some(Link::End));
        assert_eq!(topo.edges.before(&0), Some(1));
        assert_eq!(topo.edges.after(&1), Some(Link::Strong(0)));
        assert_eq!(Vec::from_iter(topo.iter()), vec![1, 0]);
    }

    #[test]
    fn test_insert_before_root_twice() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(None, 1, Some(0));
        topo.add(None, 2, Some(0));

        assert_eq!(topo.edges.after(&0), Some(Link::End));
        assert_eq!(topo.edges.before(&0), Some(2));
        assert_eq!(topo.edges.after(&2), Some(Link::Strong(0)));
        assert_eq!(topo.edges.before(&2), Some(1));
        assert_eq!(topo.edges.after(&1), Some(Link::Weak(2)));
        assert_eq!(topo.edges.before(&1), None);

        assert_eq!(Vec::from_iter(topo.iter()), vec![1, 2, 0]);
    }

    #[test]
    fn test_insert_before_root_out_of_order() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(None, 2, Some(0));
        topo.add(None, 3, Some(0));
        topo.add(None, 1, Some(0));

        assert_eq!(topo.edges.after(&0), Some(Link::End));
        assert_eq!(topo.edges.before(&0), Some(3));
        assert_eq!(topo.edges.after(&3), Some(Link::Strong(0)));
        assert_eq!(topo.edges.before(&3), Some(2));
        assert_eq!(topo.edges.after(&2), Some(Link::Weak(3)));
        assert_eq!(topo.edges.before(&2), Some(1));
        assert_eq!(topo.edges.after(&1), Some(Link::Weak(2)));
        assert_eq!(topo.edges.before(&1), None);

        assert_eq!(Vec::from_iter(topo.iter()), vec![1, 2, 3, 0]);
    }

    #[test]
    fn test_insert_at_strong_link() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(Some(0), 1, None);

        assert_eq!(topo.edges.after(&0), Some(Link::Strong(1)));
        assert_eq!(topo.edges.before(&1), Some(0));

        topo.add(Some(0), 2, Some(1));

        assert_eq!(topo.edges.after(&0), Some(Link::Strong(2)));
        assert_eq!(topo.edges.after(&2), Some(Link::Strong(1)));
        assert_eq!(topo.edges.before(&1), Some(2));
        assert_eq!(topo.edges.before(&2), Some(0));

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 2, 1]);
    }

    #[test]
    fn test_insert_at_strong_link_twice() {
        let mut topo = Topo::default();
        topo.add(None, 0, None);
        topo.add(Some(0), 1, None);

        assert_eq!(topo.edges.after(&0), Some(Link::Strong(1)));
        assert_eq!(topo.edges.before(&1), Some(0));

        topo.add(Some(0), 2, Some(1));

        assert_eq!(topo.edges.after(&0), Some(Link::Strong(2)));
        assert_eq!(topo.edges.after(&2), Some(Link::Strong(1)));

        topo.add(Some(0), 3, Some(1));

        assert_eq!(topo.edges.after(&0), Some(Link::Strong(2)));
        assert_eq!(topo.edges.after(&2), Some(Link::Weak(3)));
        assert_eq!(topo.edges.after(&3), Some(Link::Strong(1)));

        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 2, 3, 1]);
    }
}
