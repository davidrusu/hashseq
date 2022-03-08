use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashSet};

type Id = u64;

#[derive(Debug)]
struct Node {
    lefts: BTreeSet<Id>,
    rights: BTreeSet<Id>,
    value: char,
}

impl Node {
    fn hash(&self) -> Id {
        use std::hash::Hash;
        use std::hash::Hasher;
        // Hash an input incrementally.
        let mut hasher = DefaultHasher::new();
        self.lefts.hash(&mut hasher);
        self.rights.hash(&mut hasher);
        self.value.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Debug, Default)]
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
        println!("{:#?}", topo);
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

#[derive(Debug, Default)]
struct HashSeq {
    topo: TopoSort,
    nodes: BTreeMap<Id, Node>,
    // orphans: HashSet<Node>,
    // faulty: HashSet<Node>,
}

impl HashSeq {
    fn insert(&mut self, idx: usize, value: char) {
        let mut order: TopoIter<'_> = self.topo.iter();

        if let Some(prev_idx) = idx.checked_sub(1) {
            for _ in 0..prev_idx {
                order.next();
            }
        }

        let lefts = BTreeSet::from_iter(order.free_stack.iter().copied());
        order.next();
        let rights = BTreeSet::from_iter(order.free_stack.iter().copied());

        let node = Node {
            value,
            lefts,
            rights,
        };
        self.apply(node);
    }

    fn insert_batch(&mut self, idx: usize, batch: impl IntoIterator<Item = char>) {
        for (i, e) in batch.into_iter().enumerate() {
            self.insert(idx + i, e)
        }
    }

    fn apply(&mut self, node: Node) {
        let id = node.hash();
        self.topo.insert(id);
        for l in node.lefts.iter() {
            self.topo.add_constraint(*l, id);
        }
        for r in node.rights.iter() {
            self.topo.add_constraint(id, *r);
        }
        self.nodes.insert(id, node);
    }

    fn merge(&mut self, other: Self) {
        for node in other.nodes.into_values() {
            self.apply(node);
        }
    }

    fn iter(&self) -> impl Iterator<Item = char> + '_ {
        self.topo
            .iter()
            .map(|id| self.nodes.get(&id).unwrap().value)
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

        seq_a.insert_batch(0, "we wrote this ".chars());
        seq_b.insert_batch(0, "at the same time".chars());

        seq_a.merge(seq_b);

        assert_eq!(
            &seq_a.iter().collect::<String>(),
            "we wrote this at the same time"
        );
    }

    #[test]
    fn test_common_prefix_isnt_duplicated() {
        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        seq_a.insert_batch(0, "hello my name is david".chars());
        seq_b.insert_batch(0, "hello my name is zameena".chars());

        seq_a.merge(seq_b);

        assert_eq!(
            &seq_a.iter().collect::<String>(),
            "hello my name is zameenadavid"
        );
    }
    #[quickcheck]
    fn prop_vec_model(instructions: Vec<(u8, char)>) {
        let mut model = Vec::new();
        let mut seq = HashSeq::default();

        for (idx, elem) in instructions {
            let idx = idx as usize;
            model.insert(idx.min(model.len()), elem);
            seq.insert(idx.min(seq.len()), elem);
        }

        assert_eq!(seq.iter().collect::<Vec<_>>(), model);
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
