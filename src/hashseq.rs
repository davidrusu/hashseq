use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::topo_sort::{TopoIter, TopoSort};
use crate::Id;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Insert {
    roots: BTreeSet<Id>,
    lefts: BTreeSet<Id>,
    rights: BTreeSet<Id>,
    value: char,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Remove {
    roots: BTreeSet<Id>,
    insert: Id,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum Op {
    Insert(Insert),
    Remove(Remove),
}

impl Insert {
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
        self.insert.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct HashSeq {
    topo: TopoSort,
    inserts: BTreeMap<Id, Insert>,
    removed: BTreeMap<Id, Remove>,
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
        let mut order: TopoIter<'_> = self.topo.iter();

        let lefts = if let Some(prev_idx) = idx.checked_sub(1) {
            for _ in 0..prev_idx {
                order.next();
            }
            let lefts = BTreeSet::from_iter(order.next_candidates());
            order.next();
            lefts
        } else {
            BTreeSet::default()
        };

        let rights = BTreeSet::from_iter(order.next_candidates());

        let insert = Insert {
            value,
            lefts,
            rights,
            roots: self.roots.clone(),
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
        let mut order: TopoIter<'_> = self.topo.iter();

        for _ in 0..idx {
            order.next();
        }

        if let Some(insert) = order.next() {
            self.apply(Op::Remove(Remove {
                insert,
                roots: self.roots.clone(),
            }))
            .expect("ERR: We constructed a faulty op using public API")
        }
    }

    fn missing_dependencies(&self, op: &Op) -> BTreeSet<Id> {
        let dependencies = match op {
            Op::Insert(Insert { roots, .. }) | Op::Remove(Remove { roots, .. }) => roots,
        };

        let mut missing_deps = BTreeSet::new();

        for dep in dependencies.iter() {
            if !self.inserts.contains_key(&dep) && !self.removed.contains_key(&dep) {
                missing_deps.insert(*dep);
            }
        }

        missing_deps
    }

    fn is_faulty_insert(&self, insert: &Insert) -> bool {
        // Ensure there is no overlap between inserts on the left and inserts on the right
        let mut left_boundary = insert.lefts.clone();
        let mut right_boundary = insert.rights.clone();
        let mut inserts_on_left = BTreeSet::new();
        let mut inserts_on_right = BTreeSet::new();
        while !left_boundary.is_empty() || !right_boundary.is_empty() {
            for l in std::mem::take(&mut left_boundary) {
                if let Some(l_insert) = self.inserts.get(&l) {
                    left_boundary.extend(l_insert.lefts.clone());
                    inserts_on_left.insert(l);
                } else {
                    return true; // refers to a insert that we have not seen.
                }
            }
            for r in std::mem::take(&mut right_boundary) {
                if let Some(r_insert) = self.inserts.get(&r) {
                    right_boundary.extend(r_insert.rights.clone());
                    inserts_on_right.insert(r);
                } else {
                    return true; // refers to a left that we have not seen.
                }
            }
        }

        // TODO: if we're careful, we can move this into the above loop for an early out.
        if inserts_on_left
            .intersection(&inserts_on_right)
            .next()
            .is_some()
        {
            true // left/right constraints are refer to overlapping sets
        } else {
            false
        }
    }

    fn is_faulty_remove(&self, remove: &Remove) -> bool {
        !self.inserts.contains_key(&remove.insert)
    }

    pub fn apply(&mut self, op: Op) -> Result<(), Op> {
        let deps = self.missing_dependencies(&op);
        if !deps.is_empty() {
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

                self.topo.insert(id);
                for l in insert.lefts.iter() {
                    self.topo.add_constraint(*l, id);
                }
                for r in insert.rights.iter() {
                    self.topo.add_constraint(id, *r);
                }

                let superseded_roots = BTreeSet::from_iter(
                    self.roots
                        .iter()
                        .filter(|r| insert.roots.contains(r))
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

                self.topo.remove_and_propagate_constraints(remove.insert);

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
        for insert in other.inserts.into_values() {
            self.apply(Op::Insert(insert))?;
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
    fn test_faulty_if_insert_refers_to_non_existant_inserts() {
        let mut seq = HashSeq::default();

        assert!(seq
            .apply(Op::Insert(Insert {
                value: 'a',
                lefts: BTreeSet::from_iter([0]),
                rights: BTreeSet::default(),
                roots: BTreeSet::default(),
            }))
            .is_err());

        assert_eq!(seq, HashSeq::default());

        assert!(seq
            .apply(Op::Insert(Insert {
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
            .apply(Op::Insert(Insert {
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