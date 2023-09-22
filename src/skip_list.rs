use std::{cell::RefCell, rc::Rc};

struct Node {
    value: char,
    prev: Box<Option<Rc<RefCell<Node>>>>,
    next: Box<Option<Rc<RefCell<Node>>>>,
    removed: bool,
}

impl Node {
    fn new(value: char) -> Self {
        Self {
            value,
            prev: Box::new(None),
            next: Box::new(None),
            removed: false,
        }
    }

    fn mark_removed(&mut self) {
        self.prev = Box::new(None);
        self.next = Box::new(None);
        self.removed = true;
    }
}

#[derive(Default)]
struct SkipList {
    root: Option<Rc<RefCell<Node>>>,
}

impl SkipList {
    fn len(&self) -> usize {
        self.iter_nodes().count()
    }

    fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    fn insert(&mut self, idx: usize, value: char) {
        assert!(idx <= self.len());

        let node = Rc::new(RefCell::new(Node::new(value)));

        if idx == 0 {
            if let Some(head) = self.root.clone() {
                node.borrow_mut().next = Box::new(Some(head.clone()));
                head.borrow_mut().prev = Box::new(Some(node.clone()));
            }

            self.root = Some(node);

            return;
        }

        let prev = self.iter_nodes().nth(idx - 1).unwrap();

        node.borrow_mut().next = prev.borrow().next.clone();
        node.borrow_mut().prev = Box::new(Some(prev.clone()));
        prev.borrow_mut().next = Box::new(Some(node));
    }

    fn remove(&mut self, idx: usize) -> Rc<RefCell<Node>> {
        let removed_node = if idx == 0 {
            let removed_node = self.root.clone().unwrap();
            self.root = *(removed_node.borrow().next.clone());

            removed_node
        } else {
            let prev = self.iter_nodes().nth(idx - 1).unwrap();
            let removed_node = prev.borrow().next.clone().unwrap();
            let next_next = removed_node.borrow().next.clone();
            prev.borrow_mut().next = next_next;

            removed_node
        };

        removed_node.borrow_mut().mark_removed();
        removed_node
    }

    fn iter_nodes(&self) -> SkipListIter {
        SkipListIter {
            node: self.root.clone(),
        }
    }

    fn iter(&self) -> impl Iterator<Item = char> {
        self.iter_nodes().map(|n| n.borrow().value)
    }
}

struct SkipListIter {
    node: Option<Rc<RefCell<Node>>>,
}

impl Iterator for SkipListIter {
    type Item = Rc<RefCell<Node>>;

    fn next(&mut self) -> Option<Self::Item> {
        match &self.node {
            Some(n) => {
                let current = n.clone();
                let next = n.borrow().next.clone();
                self.node = *next;

                Some(current)
            }
            None => None,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use quickcheck_macros::quickcheck;

    #[test]
    fn test_add_then_remove() {
        let mut seq = SkipList::default();
        seq.insert(0, 'a');
        seq.remove(0);

        assert_eq!(String::from_iter(seq.iter()), "");
        assert_eq!(seq.len(), 0);
        assert!(seq.is_empty());
    }

    #[quickcheck]
    fn prop_vec_model(instructions: Vec<(bool, u8, char)>) {
        let mut model = Vec::new();
        let mut seq = SkipList::default();

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
