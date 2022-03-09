use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashSet};

type Id = u64;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct Node {
    roots: BTreeSet<Id>,
    lefts: BTreeSet<Id>,
    rights: BTreeSet<Id>,
    value: char,
}
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct Remove {
    roots: BTreeSet<Id>,
    node: Id,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
enum Op {
    Insert(Node),
    Remove(Remove),
}

impl Node {
    fn hash(&self) -> Id {
        use std::hash::Hash;
        use std::hash::Hasher;
        let mut hasher = DefaultHasher::new();
        self.lefts.hash(&mut hasher);
        self.rights.hash(&mut hasher);
        self.roots.hash(&mut hasher);
        self.value.hash(&mut hasher);
        hasher.finish()
    }
}

impl Remove {
    fn hash(&self) -> Id {
        use std::hash::Hash;
        use std::hash::Hasher;
        let mut hasher = DefaultHasher::new();
        self.roots.hash(&mut hasher);
        self.node.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct TopoSort {
    after_constraints: BTreeMap<Id, BTreeSet<Id>>,
}

impl TopoSort {
    fn insert(&mut self, n: Id) {
        self.after_constraints.entry(n).or_default();
    }

    fn add_constraint(&mut self, before: Id, after: Id) {
        self.after_constraints
            .entry(after)
            .or_default()
            .insert(before);
    }

    fn remove_and_propagate_constraints(&mut self, node_to_delete: Id) {
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

    fn iter<'a>(&'a self) -> TopoIter<'a> {
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

#[derive(Debug, Default, PartialEq, Eq)]
struct HashSeq {
    topo: TopoSort,
    nodes: BTreeMap<Id, Node>,
    removed: BTreeMap<Id, Remove>,
    roots: BTreeSet<Id>,
    orphaned: HashSet<Op>,
}

impl HashSeq {
    fn len(&self) -> usize {
        self.nodes.len() - self.removed.len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn insert(&mut self, idx: usize, value: char) {
        let mut order: TopoIter<'_> = self.topo.iter();

        let lefts = if let Some(prev_idx) = idx.checked_sub(1) {
            for _ in 0..prev_idx {
                order.next();
            }
            let lefts = BTreeSet::from_iter(order.free_stack.iter().copied());
            order.next();
            lefts
        } else {
            BTreeSet::default()
        };

        let rights = BTreeSet::from_iter(order.free_stack.iter().copied());

        let node = Node {
            value,
            lefts,
            rights,
            roots: self.roots.clone(),
        };
        self.apply(Op::Insert(node))
            .expect("ERR: We constructed a faulty op using public API");
    }

    pub fn insert_batch(&mut self, idx: usize, batch: impl IntoIterator<Item = char>) {
        for (i, e) in batch.into_iter().enumerate() {
            self.insert(idx + i, e)
        }
    }

    pub fn remove(&mut self, idx: usize) {
        let mut order: TopoIter<'_> = self.topo.iter();

        for _ in 0..idx {
            order.next();
        }

        if let Some(node) = order.next() {
            self.apply(Op::Remove(Remove {
                node,
                roots: self.roots.clone(),
            }))
            .expect("ERR: We constructed a faulty op using public API")
        }
    }

    fn missing_dependencies(&self, op: &Op) -> BTreeSet<Id> {
        let dependencies = match op {
            Op::Insert(Node { roots, .. }) | Op::Remove(Remove { roots, .. }) => roots,
        };

        let mut missing_deps = BTreeSet::new();

        for dep in dependencies.iter() {
            if !self.nodes.contains_key(&dep) && !self.removed.contains_key(&dep) {
                missing_deps.insert(*dep);
            }
        }

        missing_deps
    }

    fn is_faulty_node(&self, node: &Node) -> bool {
        // Ensure there is no overlap between nodes on the left and nodes on the right
        let mut left_boundary = node.lefts.clone();
        let mut right_boundary = node.rights.clone();
        let mut nodes_on_left = BTreeSet::new();
        let mut nodes_on_right = BTreeSet::new();
        while !left_boundary.is_empty() || !right_boundary.is_empty() {
            for l in std::mem::take(&mut left_boundary) {
                if let Some(l_node) = self.nodes.get(&l) {
                    left_boundary.extend(l_node.lefts.clone());
                    nodes_on_left.insert(l);
                } else {
                    return true; // refers to a node that we have not seen.
                }
            }
            for r in std::mem::take(&mut right_boundary) {
                if let Some(r_node) = self.nodes.get(&r) {
                    right_boundary.extend(r_node.rights.clone());
                    nodes_on_right.insert(r);
                } else {
                    return true; // refers to a node that we have not seen.
                }
            }
            if nodes_on_left.intersection(&nodes_on_right).next().is_some() {
                return true; // left/right constraints are refer to overlapping sets
            }
        }

        false
    }

    fn is_faulty_remove(&self, remove: &Remove) -> bool {
        !self.nodes.contains_key(&remove.node)
    }

    pub fn apply(&mut self, op: Op) -> Result<(), Op> {
        let deps = self.missing_dependencies(&op);
        if !deps.is_empty() {
            self.orphaned.insert(op);
            return Ok(());
        }

        match op {
            Op::Insert(node) => {
                let id = node.hash();

                if self.nodes.contains_key(&id) {
                    return Ok(()); // Already processed node.
                }

                if self.is_faulty_node(&node) {
                    return Err(Op::Insert(node));
                }

                self.topo.insert(id);
                for l in node.lefts.iter() {
                    self.topo.add_constraint(*l, id);
                }
                for r in node.rights.iter() {
                    self.topo.add_constraint(id, *r);
                }

                let superseded_roots = BTreeSet::from_iter(
                    self.roots
                        .iter()
                        .filter(|r| node.roots.contains(r))
                        .copied(),
                );

                for r in superseded_roots {
                    self.roots.remove(&r);
                }

                self.nodes.insert(id, node);
                self.roots.insert(id);
            }
            Op::Remove(remove) => {
                let id = remove.hash();
                if self.removed.contains_key(&id) {
                    return Ok(());
                }

                if self.is_faulty_remove(&remove) {
                    return Err(Op::Remove(remove));
                }

                self.topo.remove_and_propagate_constraints(remove.node);

                let superseded_roots = BTreeSet::from_iter(
                    self.roots
                        .iter()
                        .filter(|r| remove.roots.contains(r))
                        .copied(),
                );

                for r in superseded_roots {
                    self.roots.remove(&r);
                }

                self.removed.insert(id, remove);
                self.roots.insert(id);
            }
        }

        let orphans = std::mem::take(&mut self.orphaned);

        for orphan in orphans {
            self.apply(orphan)?;
        }

        Ok(())
    }

    pub fn merge(&mut self, other: Self) -> Result<(), Op> {
        for node in other.nodes.into_values() {
            self.apply(Op::Insert(node))?;
        }
        for rm in other.removed.into_values() {
            self.apply(Op::Remove(rm))?;
        }
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = char> + '_ {
        self.topo.iter().filter_map(|id| {
            if self.removed.contains_key(&id) {
                None
            } else {
                self.nodes.get(&id).map(|n| n.value)
            }
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
        assert_eq!(&seq.iter().collect::<String>(), "abc");
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
        seq_b.insert_batch(0, "this together, ".chars());

        seq_a.merge(seq_b).expect("Faulty merge");

        assert_eq!(&seq_a.iter().collect::<String>(), "this together, we wrote");
    }

    #[test]
    fn test_common_prefix_isnt_duplicated() {
        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        seq_a.insert_batch(0, "hello my name is david".chars());
        seq_b.insert_batch(0, "hello my name is zameena".chars());

        seq_a.merge(seq_b).expect("Faulty merge");

        assert_eq!(
            &seq_a.iter().collect::<String>(),
            "hello my name is zameenadavid"
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

        seq_a.merge(seq_b).expect("Faulty merge");
        assert_eq!(&seq_a.iter().collect::<String>(), "azaba");
    }

    #[test]
    fn test_insert_different_chars_at_front() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(0, 'b');

        dbg!(&seq);

        assert_eq!(&String::from_iter(seq.iter()), "ba");
    }

    #[test]
    fn test_insert_same_char_at_front() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(0, 'a');

        dbg!(&seq);

        assert_eq!(&String::from_iter(seq.iter()), "aa");
    }

    #[test]
    fn test_insert_delete_then_reinsert() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.remove(0);
        seq.insert(0, 'a');

        dbg!(&seq);

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
    fn test_faulty_if_node_refers_to_non_existant_nodes() {
        let mut seq = HashSeq::default();

        assert!(seq
            .apply(Op::Insert(Node {
                value: 'a',
                lefts: BTreeSet::from_iter([0]),
                rights: BTreeSet::default(),
                roots: BTreeSet::default(),
            }))
            .is_err());

        assert_eq!(seq, HashSeq::default());

        assert!(seq
            .apply(Op::Insert(Node {
                value: 'a',
                lefts: BTreeSet::default(),
                rights: BTreeSet::from_iter([0]),
                roots: BTreeSet::default(),
            }))
            .is_err());

        assert_eq!(seq, HashSeq::default());
    }

    #[test]
    fn test_faulty_if_left_right_constraints_overlap() {
        let mut seq = HashSeq::default();

        seq.insert_batch(0, "ab".chars());

        let mut topo_seq = seq.topo.iter();

        let a_id = topo_seq.next().unwrap();
        let b_id = topo_seq.next().unwrap();

        // engineer a faulty op where `b` is on our left and `a` is on our right.

        assert!(seq
            .apply(Op::Insert(Node {
                value: 'a',
                lefts: BTreeSet::from_iter([b_id]),
                rights: BTreeSet::from_iter([a_id]),
                roots: BTreeSet::default(),
            }))
            .is_err());

        let mut expected_seq = HashSeq::default();
        expected_seq.insert_batch(0, "ab".chars());

        assert_eq!(seq, expected_seq);
    }

    #[test]
    fn test_faulty_remove() {
        let mut seq = HashSeq::default();

        // Attempting to remove node with id 0, with the empty DAG roots.
        // This is faulty since the empty DAG roots should not have seen
        // any nodes, let alone a node with id 0.
        assert!(seq
            .apply(Op::Remove(Remove {
                node: 0,
                roots: BTreeSet::new()
            }))
            .is_err());

        assert_eq!(seq, HashSeq::default());
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
}

fn main() {
    let mut seq = HashSeq::default();
    seq.insert(0, 'a');
    seq.insert(1, 'b');
    seq.insert(2, 'c');
    dbg!(&seq);
    println!("{}", seq.iter().collect::<String>());
}
