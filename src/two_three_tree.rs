#[derive(Debug)]
enum Node {
    Leaf(char),
    Two {
        count: usize,
        left: Box<Node>,
        right: Box<Node>,
    },
}

impl Node {
    fn is_leaf(&self) -> bool {
        matches!(self, Node::Leaf(_))
    }

    fn count(&self) -> usize {
        match self {
            Node::Leaf(_) => 1,
            Node::Two { count, .. } => *count,
        }
    }

    fn insert(&mut self, idx: usize, value: char) {
        assert!(idx <= self.count());
        match self {
            Node::Leaf(other) => {
                let (left, right) = if idx == 0 {
                    (value, *other)
                } else {
                    (*other, value)
                };
                let left = Box::new(Node::Leaf(left));
                let right = Box::new(Node::Leaf(right));
                *self = Node::Two {
                    count: 2,
                    left,
                    right,
                };
            }
            Node::Two { left, right, count } => {
                if idx <= left.count() {
                    left.insert(idx, value);
                } else {
                    right.insert(idx - left.count(), value)
                }
                *count += 1;
            }
        }
    }

    fn remove(&mut self, idx: usize) {
        assert!(idx < self.count());

        match self {
            Node::Leaf(_) => panic!("Parent should have removed us"),
            Node::Two { count, left, right } => {
                if idx < left.count() {
                    if left.is_leaf() {
                        let n = std::mem::replace(right, Box::new(Node::Leaf('a')));
                        *self = *n;
                    } else {
                        left.remove(idx);
                        *count -= 1;
                    }
                } else {
                    if right.is_leaf() {
                        let n = std::mem::replace(left, Box::new(Node::Leaf('a')));
                        *self = *n;
                    } else {
                        right.remove(idx - left.count());
                        *count -= 1;
                    }
                }
            }
        }
    }

    fn height(&self) -> usize {
        match &self {
            Node::Leaf(_) => 0,
            Node::Two { left, right, .. } => left.height().max(right.height()) + 1,
        }
    }

    fn is_balanced(&self) -> bool {
        match self {
            Node::Leaf(_) => true,
            Node::Two { left, right, .. } => left.height() == right.height(),
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = char>> {
        match self {
            Node::Leaf(v) => Box::new(std::iter::once(*v)),
            Node::Two { left, right, .. } => Box::new(left.iter().chain(right.iter())),
        }
    }
}

#[derive(Default, Debug)]
struct Tree {
    root: Option<Node>,
}

impl Tree {
    fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    fn len(&self) -> usize {
        match &self.root {
            Some(root) => root.count(),
            None => 0,
        }
    }

    fn insert(&mut self, idx: usize, value: char) {
        match &mut self.root {
            None => self.root = Some(Node::Leaf(value)),
            Some(root) => root.insert(idx, value),
        }
    }

    fn remove(&mut self, idx: usize) {
        assert!(idx <= self.len());
        match &mut self.root {
            Some(Node::Leaf(_)) => {
                self.root = None;
            }
            Some(r) => {
                r.remove(idx);
            }
            None => (),
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = char>> {
        match &self.root {
            None => Box::new(std::iter::empty()),
            Some(root) => root.iter(),
        }
    }

    fn is_balanced(&self) -> bool {
        match &self.root {
            Some(root) => root.is_balanced(),
            None => true,
        }
    }

    fn height(&self) -> usize {
        match &self.root {
            Some(r) => r.height(),
            None => 0,
        }
    }
}

mod test {
    use super::*;

    use quickcheck_macros::quickcheck;

    #[test]
    fn test_insert() {
        let mut seq = Tree::default();
        seq.insert(0, 'a');

        assert_eq!(String::from_iter(seq.iter()), "a");
    }

    #[test]
    fn test_insert_twice() {
        let mut seq = Tree::default();
        seq.insert(0, 'a');
        seq.insert(1, 'b');

        assert_eq!(String::from_iter(seq.iter()), "ab");
    }

    #[test]
    fn test_insert_thrice() {
        let mut seq = Tree::default();
        seq.insert(0, 'a');
        seq.insert(1, 'b');
        seq.insert(2, 'c');

        assert_eq!(String::from_iter(seq.iter()), "abc");
        dbg!(&seq);
        assert_eq!(seq.height(), 2);
    }

    #[test]
    fn test_insert_five_times() {
        let mut seq = Tree::default();
        seq.insert(0, 'a');
        seq.insert(0, 'b');
        seq.insert(0, 'c');
        seq.insert(0, 'd');
        seq.insert(0, 'e');

        assert_eq!(String::from_iter(seq.iter()), "edcba");
        dbg!(&seq);
        assert_eq!(seq.height(), 3);
    }

    #[test]
    fn test_insert_twice_than_remove() {
        let mut seq = Tree::default();

        seq.insert(0, 'a');
        seq.insert(1, 'b');
        seq.remove(1);

        assert_eq!(String::from_iter(seq.iter()), "a");
    }

    #[quickcheck]
    fn prop_vec_model(instructions: Vec<(bool, u8, char)>) {
        let mut model = Vec::new();
        let mut seq = Tree::default();

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
        // assert!(seq.is_balanced());
        if !seq.is_empty() {
            let h = seq.height();
            let expected_height = seq.len().ilog(2usize) as usize + 1;
            println!("{} expected_h: {expected_height}, got: {h}", seq.len());
            assert!(h <= expected_height, "{h} <= {expected_height}");
        }
    }
}
