use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::topo_sort::Topo;
// use crate::topo_sort_strong_weak::Tree;
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

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct HashSeq {
    topo: Topo,
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

    pub fn orphans(&self) -> &HashSet<Op> {
        &self.orphaned
    }

    pub fn insert(&mut self, idx: usize, value: char) {
        let mut order = self
            .topo
            .iter()
            .filter(|id| !self.removed_inserts.contains(id));

        let mut left = if let Some(prev_idx) = idx.checked_sub(1) {
            for _ in 0..prev_idx {
                order.next();
            }
            order.next()
        } else {
            None
        };

        let mut right = order.next();

        if let (Some(l), Some(r)) = (left, right) {
            if self.topo.is_causally_before(l, r) {
                left = None
            } else {
                right = None
            }
        }

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
            .topo
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
                for (idx, id) in self.topo.iter().enumerate() {
                    if *l == id {
                        l_idx = Some(idx);
                    }
                    if *r == id {
                        r_idx = Some(idx);
                    }
                }

                match (l_idx, r_idx) {
                    (Some(l_idx), Some(r_idx)) => l_idx >= r_idx,
                    _ => true,
                }
            }
            _ => false,
        }
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
                // println!("apply({:?} = {:?})", insert, id);

                if self.inserts.contains_key(&id) {
                    return Ok(()); // Already processed insert.
                }

                if self.is_faulty_insert(&insert) {
                    return Err(Op::Insert(insert));
                }

                self.topo.add(insert.left, id, insert.right);

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
                // println!("apply({:?} = {:?})", remove, id);
                if self.removed.contains_key(&id) {
                    return Ok(());
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
        self.topo.iter().filter_map(|id| {
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

        assert_eq!(&String::from_iter(seq.iter()), "ba");
    }

    #[test]
    fn test_insert_same_char_at_front() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(0, 'a');

        assert_eq!(&String::from_iter(seq.iter()), "aa");
    }

    #[test]
    fn test_insert_delete_then_reinsert() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.remove(0);
        seq.insert(0, 'a');

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
    fn test_inserts_refering_to_out_of_order_inserts_are_cached() {
        let mut seq = HashSeq::default();

        let insert = Insert {
            value: 'b',
            left: None,
            right: None,
            extra_dependencies: BTreeSet::default(),
        };

        assert!(seq
            .apply(Op::Insert(Insert {
                value: 'a',
                left: Some(insert.hash()),
                right: None,
                extra_dependencies: BTreeSet::default(),
            }))
            .is_ok());

        assert_eq!(seq.orphans().len(), 1);
        assert_eq!(seq.len(), 0);

        assert!(seq
            .apply(Op::Insert(Insert {
                value: 'a',
                left: None,
                right: Some(insert.hash()),
                extra_dependencies: BTreeSet::default(),
            }))
            .is_ok());

        assert_eq!(seq.orphans().len(), 2);
        assert_eq!(seq.len(), 0);

        assert!(seq.apply(Op::Insert(insert)).is_ok());

        assert_eq!(seq.orphans().len(), 0);
        assert_eq!(seq.len(), 3);

        assert_eq!(&String::from_iter(seq.iter()), "aba");
    }

    #[test]
    fn test_faulty_if_left_right_constraints_overlap() {
        let mut seq = HashSeq::default();

        seq.insert_batch(0, "ab".chars());

        let mut tree_iter = seq.topo.iter();

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
    fn test_out_of_order_remove_is_cached() {
        let mut seq = HashSeq::default();

        // Attempting to remove insert that doesn't yet exist.
        // We expect the remove operation to be cached and applied
        // once we see the insert.

        let insert = Insert {
            left: None,
            value: 'a',
            right: None,
            extra_dependencies: BTreeSet::new(),
        };

        assert!(seq
            .apply(Op::Remove(Remove {
                insert: insert.hash(),
                extra_dependencies: BTreeSet::new()
            }))
            .is_ok());

        assert_eq!(seq.orphans().len(), 1);
        assert!(seq.apply(Op::Insert(insert)).is_ok());
        assert_eq!(seq.orphans().len(), 0);
        assert_eq!(&String::from_iter(seq.iter()), "");
    }

    #[test]
    fn test_prop_associative_qc1() {
        // ([(true, 0, '\u{0}'), (true, 0, '\u{0}')], [], [(true, 0, '\u{3}')])

        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        seq_a.insert(0, 'a');
        seq_a.insert(0, 'a');

        seq_b.insert(0, 'b');

        let mut ab = seq_a.clone();
        ab.merge(seq_b.clone()).unwrap();

        let mut ba = seq_b.clone();
        ba.merge(seq_a.clone()).unwrap();

        dbg!(&ab);
        dbg!(&ba);

        assert_eq!(ab, ba);
    }

    #[quickcheck]
    fn prop_reflexive(ops: Vec<(bool, u8, char)>) {
        let mut seq = HashSeq::default();

        for (insert_or_remove, idx, elem) in ops {
            let idx = idx as usize;
            match insert_or_remove {
                true => {
                    // insert
                    seq.insert(idx.min(seq.len()), elem);
                }
                false => {
                    // remove
                    if !seq.is_empty() {
                        seq.remove(idx.min(seq.len() - 1));
                    }
                }
            }
        }

        // merge(a, a) == a

        let mut merge_self = seq.clone();
        merge_self.merge(seq.clone()).unwrap();

        assert_eq!(merge_self, seq);
    }

    #[quickcheck]
    fn prop_commutative(a: Vec<(bool, u8, char)>, b: Vec<(bool, u8, char)>) {
        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        for (insert_or_remove, idx, elem) in a {
            let idx = idx as usize;
            match insert_or_remove {
                true => {
                    // insert
                    seq_a.insert(idx.min(seq_a.len()), elem);
                }
                false => {
                    // remove
                    if !seq_a.is_empty() {
                        seq_a.remove(idx.min(seq_a.len() - 1));
                    }
                }
            }
        }

        for (insert_or_remove, idx, elem) in b {
            let idx = idx as usize;
            match insert_or_remove {
                true => {
                    // insert
                    seq_b.insert(idx.min(seq_b.len()), elem);
                }
                false => {
                    // remove
                    if !seq_b.is_empty() {
                        seq_b.remove(idx.min(seq_b.len() - 1));
                    }
                }
            }
        }

        // merge(a, b) == merge(b, a)

        let mut merge_a_b = seq_a.clone();
        merge_a_b.merge(seq_b.clone()).unwrap();

        let mut merge_b_a = seq_b.clone();
        merge_b_a.merge(seq_a.clone()).unwrap();

        assert_eq!(merge_a_b, merge_b_a);
    }

    #[quickcheck]
    fn prop_associative(
        a: Vec<(bool, u8, char)>,
        b: Vec<(bool, u8, char)>,
        c: Vec<(bool, u8, char)>,
    ) {
        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();
        let mut seq_c = HashSeq::default();

        for (insert_or_remove, idx, elem) in a {
            let idx = idx as usize;
            match insert_or_remove {
                true => {
                    // insert
                    seq_a.insert(idx.min(seq_a.len()), elem);
                }
                false => {
                    // remove
                    if !seq_a.is_empty() {
                        seq_a.remove(idx.min(seq_a.len() - 1));
                    }
                }
            }
        }

        for (insert_or_remove, idx, elem) in b {
            let idx = idx as usize;
            match insert_or_remove {
                true => {
                    // insert
                    seq_b.insert(idx.min(seq_b.len()), elem);
                }
                false => {
                    // remove
                    if !seq_b.is_empty() {
                        seq_b.remove(idx.min(seq_b.len() - 1));
                    }
                }
            }
        }

        for (insert_or_remove, idx, elem) in c {
            let idx = idx as usize;
            match insert_or_remove {
                true => {
                    // insert
                    seq_c.insert(idx.min(seq_c.len()), elem);
                }
                false => {
                    // remove
                    if !seq_c.is_empty() {
                        seq_c.remove(idx.min(seq_c.len() - 1));
                    }
                }
            }
        }

        // merge(merge(a, b), c) == merge(a, merge(b, c))

        let mut ab_then_c = seq_a.clone();
        ab_then_c.merge(seq_b.clone()).unwrap();
        ab_then_c.merge(seq_c.clone()).unwrap();

        let mut bc_then_a = seq_b.clone();
        bc_then_a.merge(seq_c.clone()).unwrap();
        bc_then_a.merge(seq_a.clone()).unwrap();

        assert_eq!(ab_then_c, bc_then_a);

        // TODO: once insert returns an Op, check that we are op associative as well.
    }

    #[test]
    fn test_prop_vec_model_qc1() {
        let mut model = Vec::new();
        let mut seq = HashSeq::default();

        for (insert_or_remove, idx, elem) in [(true, 0, 'b'), (true, 0, 'b'), (true, 1, 'a')] {
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

    #[test]
    fn test_prop_vec_model_qc2() {
        let mut model = Vec::new();
        let mut seq = HashSeq::default();

        for (insert_or_remove, idx, elem) in [
            (true, 0, 'b'),
            (true, 0, 'b'),
            (true, 1, 'b'),
            (true, 2, 'a'),
        ] {
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

    #[test]
    fn test_prop_vec_model_qc3() {
        let mut model = Vec::new();
        let mut seq = HashSeq::default();

        for (insert_or_remove, idx, elem) in [
            (true, 0, 'c'),
            (true, 1, 'c'),
            (true, 2, 'c'),
            (false, 1, 'c'),
            (true, 1, 'b'),
        ] {
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

    #[test]
    fn test_prop_vec_model_qc4() {
        let mut seq = HashSeq::default();

        for (idx, elem) in [(0, 'a'), (1, 'a'), (2, 'a'), (3, 'a'), (3, 'a'), (3, 'd')] {
            seq.insert(idx, elem);
        }

        assert_eq!(seq.iter().collect::<String>(), "aaadaa");
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
