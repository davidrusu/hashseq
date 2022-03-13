use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashSet};

// use crate::topo_sort::{TopoIter, TopoSort};
use crate::topo_sort_strong_weak::Tree;
use crate::Id;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Insert {
    extra_dependencies: BTreeSet<Id>,
    left: Option<Id>,
    right: Option<Id>,
    value: char,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Remove {
    extra_dependencies: BTreeSet<Id>,
    insert: Id,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum Op {
    Insert(Insert),
    Remove(Remove),
}

impl Op {
    fn dependencies(&self) -> Box<dyn Iterator<Item = Id> + '_> {
        match &self {
            Op::Insert(i) => Box::new(
                i.extra_dependencies
                    .iter()
                    .chain(&i.left)
                    .chain(&i.right)
                    .copied(),
            ),
            Op::Remove(r) => Box::new(
                std::iter::once(&r.insert)
                    .chain(r.extra_dependencies.iter())
                    .copied(),
            ),
        }
    }
}

impl Insert {
    fn hash(&self) -> Id {
        use std::hash::Hash;
        use std::hash::Hasher;
        let mut hasher = DefaultHasher::new();
        self.left.hash(&mut hasher);
        self.right.hash(&mut hasher);
        self.extra_dependencies.hash(&mut hasher);
        self.value.hash(&mut hasher);
        hasher.finish()
    }
}

impl Remove {
    fn hash(&self) -> Id {
        use std::hash::Hash;
        use std::hash::Hasher;
        let mut hasher = DefaultHasher::new();
        self.extra_dependencies.hash(&mut hasher);
        self.insert.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct HashSeq {
    tree: Tree,
    inserts: BTreeMap<Id, Insert>,
    removed: BTreeMap<Id, Remove>,
    removed_inserts: BTreeSet<Id>,
    roots: BTreeSet<Id>,
    orphaned: HashSet<Op>,
}

impl HashSeq {
    pub fn len(&self) -> usize {
        self.inserts.len() - self.removed.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn insert(&mut self, idx: usize, value: char) {
        let mut order = self
            .tree
            .iter()
            .filter(|id| !self.removed_inserts.contains(id));

        let left = if let Some(prev_idx) = idx.checked_sub(1) {
            for _ in 0..prev_idx {
                order.next();
            }
            order.next()
        } else {
            None
        };

        let right = order.next();

        let mut extra_dependencies = self.roots.clone();

        if let Some(l) = left.as_ref() {
            extra_dependencies.remove(l); // left will already be seen a dependency.
        }
        if let Some(r) = right.as_ref() {
            extra_dependencies.remove(r); //  right will already be seen a dependency.
        }

        let insert = Insert {
            value,
            left,
            right,
            extra_dependencies,
        };

        self.apply(Op::Insert(insert))
            .expect("ERR: We constructed a faulty op using public API");
    }

    pub fn insert_batch(&mut self, idx: usize, batch: impl IntoIterator<Item = char>) {
        for (i, e) in batch.into_iter().enumerate() {
            self.insert(idx + i, e)
        }
    }

    pub fn remove(&mut self, idx: usize) {
        let mut order = self
            .tree
            .iter()
            .filter(|id| !self.removed_inserts.contains(id));

        for _ in 0..idx {
            order.next();
        }

        if let Some(insert) = order.next() {
            let mut extra_dependencies = self.roots.clone();
            extra_dependencies.remove(&insert); // insert will already be seen as a dependency;

            self.apply(Op::Remove(Remove {
                insert,
                extra_dependencies,
            }))
            .expect("ERR: We constructed a faulty op using public API")
        }
    }

    fn any_missing_dependencies(&self, deps: &BTreeSet<Id>) -> bool {
        for dep in deps.iter() {
            if !self.inserts.contains_key(dep) && !self.removed.contains_key(dep) {
                return true;
            }
        }

        false
    }

    fn is_faulty_insert(&self, insert: &Insert) -> bool {
        // Ensure there is no overlap between inserts on the left and inserts on the right
        match (&insert.left, &insert.right) {
            (Some(l), Some(r)) => {
                let mut l_idx = None;
                let mut r_idx = None;
                for (idx, id) in self.tree.iter().enumerate() {
                    if *l == id {
                        l_idx = Some(idx);
                    }
                    if *r == id {
                        r_idx = Some(idx);
                    }
                }

                match (l_idx, r_idx) {
                    (Some(l_idx), Some(r_idx)) => l_idx > r_idx,
                    _ => true,
                }
            }
            _ => false,
        }
    }

    fn is_faulty_remove(&self, remove: &Remove) -> bool {
        !self.inserts.contains_key(&remove.insert)
    }

    pub fn apply(&mut self, op: Op) -> Result<(), Op> {
        let op_dependencies = BTreeSet::from_iter(op.dependencies());
        if self.any_missing_dependencies(&op_dependencies) {
            self.orphaned.insert(op);
            return Ok(());
        }

        match op {
            Op::Insert(insert) => {
                let id = insert.hash();

                if self.inserts.contains_key(&id) {
                    return Ok(()); // Already processed insert.
                }

                if self.is_faulty_insert(&insert) {
                    return Err(Op::Insert(insert));
                }

                self.tree.add(insert.left, id, insert.right);

                let superseded_roots = Vec::from_iter(
                    self.roots
                        .iter()
                        .filter(|r| op_dependencies.contains(r))
                        .copied(),
                );

                for r in superseded_roots {
                    self.roots.remove(&r);
                }

                self.inserts.insert(id, insert);
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

                let superseded_roots = Vec::from_iter(
                    self.roots
                        .iter()
                        .filter(|r| op_dependencies.contains(r))
                        .copied(),
                );

                for r in superseded_roots {
                    self.roots.remove(&r);
                }

                self.removed_inserts.insert(remove.insert);
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
        for insert in other.inserts.into_values() {
            self.apply(Op::Insert(insert))?;
        }
        for rm in other.removed.into_values() {
            self.apply(Op::Remove(rm))?;
        }
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = char> + '_ {
        self.tree.iter().filter_map(|id| {
            if self.removed_inserts.contains(&id) {
                None
            } else {
                self.inserts.get(&id).map(|l| l.value)
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

        seq_a.insert_batch(0, "we wrote ".chars());
        seq_b.insert_batch(0, "this together".chars());

        seq_a.merge(seq_b).expect("Faulty merge");

        assert_eq!(&seq_a.iter().collect::<String>(), "we wrote this together");
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
            "hello my name is davidzameena"
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
    fn test_faulty_if_insert_refers_to_non_existant_inserts() {
        let mut seq = HashSeq::default();

        assert!(seq
            .apply(Op::Insert(Insert {
                value: 'a',
                left: Some(0),
                right: None,
                extra_dependencies: BTreeSet::default(),
            }))
            .is_err());

        assert_eq!(seq, HashSeq::default());

        assert!(seq
            .apply(Op::Insert(Insert {
                value: 'a',
                left: None,
                right: Some(0),
                extra_dependencies: BTreeSet::default(),
            }))
            .is_err());

        assert_eq!(seq, HashSeq::default());
    }

    #[test]
    fn test_faulty_if_left_right_constraints_overlap() {
        let mut seq = HashSeq::default();

        seq.insert_batch(0, "ab".chars());

        let mut tree_iter = seq.tree.iter();

        let a_id = tree_iter.next().unwrap();
        let b_id = tree_iter.next().unwrap();

        // engineer a faulty op where `b` is on our left and `a` is on our right.

        assert!(seq
            .apply(Op::Insert(Insert {
                value: 'a',
                left: Some(b_id),
                right: Some(a_id),
                extra_dependencies: BTreeSet::default(),
            }))
            .is_err());

        let mut expected_seq = HashSeq::default();
        expected_seq.insert_batch(0, "ab".chars());

        assert_eq!(seq, expected_seq);
    }

    #[test]
    fn test_faulty_remove() {
        let mut seq = HashSeq::default();

        // Attempting to remove insert with id 0, with the empty DAG roots.
        // This is faulty since the empty DAG roots should not have seen
        // any inserts, let alone a insert with id 0.
        assert!(seq
            .apply(Op::Remove(Remove {
                insert: 0,
                roots: BTreeSet::new()
            }))
            .is_err());

        assert_eq!(seq, HashSeq::default());
    }

    #[test]
    fn test_cycle_resolution() {
        let mut seq = HashSeq::default();
        let a = Insert {
            roots: Default::default(),
            left: None,
            right: None,
            value: 'a',
        };

        let b = Insert {
            roots: Default::default(),
            left: None,
            right: None,
            value: 'b',
        };

        seq.apply(Op::Insert(a.clone())).unwrap();
        seq.apply(Op::Insert(b.clone())).unwrap();

        let a_c_b = Insert {
            roots: BTreeSet::from_iter([a.hash(), b.hash()]),
            left: Some(a.hash()),
            right: Some(b.hash()),
            value: 'c',
        };

        let b_d_a = Insert {
            roots: BTreeSet::from_iter([a.hash(), b.hash()]),
            left: Some(b.hash()),
            right: Some(a.hash()),
            value: 'd',
        };

        seq.apply(Op::Insert(a_c_b)).unwrap();

        assert_eq!(&String::from_iter(seq.iter()), "acb");

        seq.apply(Op::Insert(b_d_a)).unwrap();

        assert_eq!(&String::from_iter(seq.iter()), "abcd");
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
