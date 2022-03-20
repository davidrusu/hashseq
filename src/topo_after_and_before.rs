use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Add,
};

use crate::Id;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct Entry {
    elem: Id,
    stack: Vec<Id>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Topo {
    roots: BTreeSet<Id>,
    afters: BTreeMap<Id, BTreeSet<Id>>,
    befores: BTreeMap<Id, BTreeSet<Id>>,
    order: Vec<Entry>,
}

impl Topo {
    fn add_root(&mut self, root: Id) {
        if self.roots.is_empty() {
            self.roots.insert(root);
            assert!(self.order.is_empty());
            let entry = Entry {
                elem: root,
                stack: Vec::new(),
            };
            self.order.insert(0, entry);
        }
    }
    pub fn add(&mut self, left: Option<Id>, node: Id, right: Option<Id>) {
        match (left, right) {
            (None, None) => self.add_root(node),
            (None, Some(right)) => {}
            (Some(left), None) => {}
            (Some(left), Some(right)) => {}
        }
    }

    fn iter(&self) -> impl Iterator<Item = Id> + '_ {
        self.order.iter().map(|e| e.elem)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty() {
        let topo = Topo::default();
        assert_eq!(Vec::from_iter(topo.iter()), vec![]);
    }

    #[test]
    fn test_single() {
        let topo = Topo::default();
        topo.add(None, 0, None);
        assert_eq!(Vec::from_iter(topo.iter()), vec![0]);
    }

    #[test]
    fn test_concurrent_roots() {
        let mut topo = Topo::default();

        topo.add(None, 1, None);
        assert_eq!(Vec::from_iter(topo.iter()), vec![1]);

        topo.add(None, 0, None);
        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 1]);

        topo.add(None, 2, None);
        assert_eq!(Vec::from_iter(topo.iter()), vec![0, 1, 2]);
    }
}
