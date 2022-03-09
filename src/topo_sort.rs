use std::collections::{BTreeMap, BTreeSet};

use crate::Id;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct TopoSort {
    after_constraints: BTreeMap<Id, BTreeSet<Id>>,
}

impl TopoSort {
    pub fn insert(&mut self, n: Id) {
        self.after_constraints.entry(n).or_default();
    }

    pub fn add_constraint(&mut self, before: Id, after: Id) {
        self.after_constraints
            .entry(after)
            .or_default()
            .insert(before);
    }

    pub fn remove_and_propagate_constraints(&mut self, node_to_delete: Id) {
        let afters_to_propagate = self
            .after_constraints
            .entry(node_to_delete)
            .or_default()
            .clone();

        for (_, afters) in self.after_constraints.iter_mut() {
            if afters.contains(&node_to_delete) {
                afters.extend(afters_to_propagate.clone());
                afters.remove(&node_to_delete);
            }
        }

        self.after_constraints.remove(&node_to_delete);
    }

    fn free_variables(&self) -> impl Iterator<Item = Id> + '_ {
        self.after_constraints
            .iter()
            .filter(|(_, befores)| befores.is_empty())
            .map(|(n, _)| *n)
    }

    pub fn iter<'a>(&'a self) -> TopoIter<'a> {
        TopoIter::new(self)
    }
}

pub struct TopoIter<'a> {
    topo: &'a TopoSort,
    used: BTreeSet<Id>,
    free_stack: Vec<Id>,
}

impl<'a> TopoIter<'a> {
    fn new(topo: &'a TopoSort) -> Self {
        let used = BTreeSet::new();
        let mut free_stack: Vec<Id> = topo.free_variables().collect();
        free_stack.sort();
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

            let mut newly_free = Vec::new();
            for (after, befores) in self.topo.after_constraints.iter() {
                if self.free_stack.contains(&after) {
                    continue;
                }
                if self.used.contains(&after) {
                    continue;
                }
                if befores.is_subset(&self.used) {
                    newly_free.push(after);
                }
            }

            newly_free.sort();

            self.free_stack.extend(newly_free);

            Some(n)
        } else {
            None
        }
    }
}
