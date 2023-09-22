use std::cell::RefCell;

#[derive(Default)]
struct PosList {
    root: Option<RefCell<Box<Node>>>,
}

impl PosList {
    fn new() -> Self {
        Self::default()
    }

    fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    fn len(&self) -> usize {
        match &self.root {
            Some(node) => node.borrow().len(),
            None => 0,
        }
    }

    fn insert(&mut self, idx: usize, value: char) {
        const HEIGHT: usize = 20;

        if idx == 0 {
            match std::mem::take(&mut self.root) {
                Some(node) => {
                    if node.borrow().is_leaf() {
                        let leaf = Node::Leaf {
                            value,
                            next: Some(node),
                        };
                        self.root = Some(RefCell::new(Box::new(leaf)));
                    } else {
                        todo!();
                    }
                }

                None => {
                    self.root = Some(RefCell::new(Box::new(Node::Leaf { value, next: None })));
                }
            }
        } else {
            match self.root.as_ref() {
                Some(root) => root.borrow_mut().insert(idx - 1, value),
                None => panic!("Attempt to insert out of bounds"),
            }
        }
    }

    fn remove(&self, idx: usize) {}
}

struct PosListIntoIter {
    root: Option<RefCell<Box<Node>>>,
}

impl Iterator for PosListIntoIter {
    type Item = char;

    fn next(&mut self) -> Option<Self::Item> {
        match std::mem::take(&mut self.root) {
            Some(node) => match node.borrow().as_ref() {
                Node::Skip { length, skip, down } => {
                    // self.root = Some((*down).clone());
                    self.next()
                }
                Node::Leaf { value, next } => {
                    self.root = next.as_ref().map(|n| *n.clone());
                    Some(*value)
                }
            },
            None => None,
        }
    }
}

impl IntoIterator for PosList {
    type Item = char;

    type IntoIter = PosListIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        PosListIntoIter { root: self.root }
    }
}

enum Node {
    Skip {
        length: usize,
        skip: RefCell<Box<Node>>,
        down: RefCell<Box<Node>>,
    },
    Leaf {
        value: char,
        next: Option<RefCell<Box<Node>>>,
    },
}
impl Node {
    fn len(&self) -> usize {
        match self {
            Node::Skip { length, skip, down } => length + skip.borrow().len(),
            Node::Leaf { value, next } => 1 + next.as_ref().map(|n| n.borrow().len()).unwrap_or(0),
        }
    }

    fn is_leaf(&self) -> bool {
        matches!(&self, Node::Leaf { .. })
    }

    fn height(&self) -> usize {
        match self {
            Node::Skip { length, skip, down } => todo!(),
            Node::Leaf { value, next } => todo!(),
        }
    }

    pub fn insert(&mut self, idx: usize, value: char) {
        match self {
            Node::Skip { length, skip, down } => todo!(),
            Node::Leaf { next, .. } => {
                if idx > 0 {
                    match next.as_mut() {
                        Some(node) => node.borrow_mut().insert(idx - 1, value),
                        None => panic!("Attempt to insert out of bounds"),
                    }
                } else if idx == 0 {
                    let next_next = std::mem::take(next);
                    let new_next = Node::Leaf {
                        value,
                        next: next_next,
                    };
                    *next = Some(RefCell::new(Box::new(new_next)));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use quickcheck_macros::quickcheck;

    use super::*;

    #[test]
    fn test_empty() {
        let empty = PosList::new();

        assert_eq!(empty.len(), 0);
        assert!(empty.is_empty());
    }

    #[test]
    fn test_insert() {
        let mut seq = PosList::new();
        seq.insert(0, 'a');

        assert_eq!(String::from_iter(seq), "a");
    }

    #[test]
    fn test_insert_at_front_twice() {
        let mut seq = PosList::new();
        seq.insert(0, 'a');
        seq.insert(0, 'b');

        assert_eq!(String::from_iter(seq), "ba");
    }
    #[test]
    fn test_insert_in_order() {
        let mut seq = PosList::new();
        seq.insert(0, 'a');
        seq.insert(1, 'b');

        assert_eq!(String::from_iter(seq), "ab");
    }

    #[quickcheck]
    fn prop_vec_model(instructions: Vec<(/* bool, */ u8, char)>) {
        let mut model = Vec::new();
        let mut seq = PosList::default();

        for (/* insert_or_remove, */ idx, elem) in instructions {
            let idx = idx as usize;
            match /* insert_or_remove */ true {
                true => {
                    // insert
                    model.insert(idx.min(model.len()), elem);
                    seq.insert(idx.min(seq.len()), elem);
                }
                false => {
                    // remove
                    // assert_eq!(seq.is_empty(), model.is_empty());
                    // if !seq.is_empty() {
                    //     model.remove(idx.min(model.len() - 1));
                    //     seq.remove(idx.min(seq.len() - 1));
                    // }
                }
            }
        }

        assert_eq!(seq.len(), model.len());
        assert_eq!(seq.is_empty(), model.is_empty());
        assert_eq!(Vec::from_iter(seq), model);
    }
}
