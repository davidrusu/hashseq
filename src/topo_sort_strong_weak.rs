use std::collections::{BTreeMap, BTreeSet};

use crate::Id;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct BiMap {
    after: BTreeMap<Id, BTreeSet<Id>>,
    before: BTreeMap<Id, BTreeSet<Id>>,
}

impl BiMap {
    pub fn vertex(&mut self, vertex: Id) {
        self.after.entry(vertex).or_default();
        self.before.entry(vertex).or_default();
    }

    pub fn link(&mut self, before: Id, after: Id) {
        self.after.entry(before).or_default().insert(after);
        self.before.entry(after).or_default().insert(before);
    }

    pub fn firsts(&self) -> impl Iterator<Item = Id> + '_ {
        self.before
            .iter()
            .filter(|(_, befores)| befores.is_empty())
            .map(|(v, _)| v)
            .copied()
    }

    pub fn before(&self, v: &Id) -> BTreeSet<Id> {
        self.before.get(v).cloned().unwrap_or_default()
    }

    pub fn after(&self, v: &Id) -> BTreeSet<Id> {
        self.after.get(v).cloned().unwrap_or_default()
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Topo {
    edges: BiMap,
}

impl Topo {
    pub fn add(&mut self, left: Option<Id>, elem: Id, right: Option<Id>) {
        assert!(!self.edges.before.contains_key(&elem));
        self.edges.vertex(elem);

        if let Some(left) = left {
            self.edges.link(left, elem);
        }

        if let Some(right) = right {
            self.edges.link(elem, right);
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
        let mut boundary = Vec::from_iter(topo.edges.firsts());
        boundary.sort();
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
            for after in self.topo.edges.after(&next).iter() {
                let after_dependencies = self
                    .waiting
                    .entry(*after)
                    .or_insert_with(|| self.topo.edges.before(after).clone());

                after_dependencies.remove(&next);

                if after_dependencies.is_empty() {
                    to_add_to_boundary.push(*after);
                    self.waiting.remove(after);
                }
            }

            to_add_to_boundary.sort();

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
        topo.add(Some(0), 1, Some(4));
        topo.add(Some(0), 2, Some(3));

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
}
