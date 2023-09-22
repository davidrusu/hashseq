use std::{cell::RefCell, rc::Rc};

struct PositionalList {
    skips: Skips,
}

struct Node {
    value: char,
    skips: Skips,
}

struct Skips {
    skips: Vec<Skip>,
}

#[derive(Clone)]
struct Skip {
    node: Option<Rc<RefCell<Node>>>,
    length: usize,
}

impl Skips {
    fn position(&self, idx: usize) -> Option<char> {
        if let Some(skip) = self.skips.iter_mut().find(|s| s.length <= idx) {
            if skip.length == idx {
                skip.node.map(|n| n.borrow().value)
            } else {
                assert_ne!(idx, 0);
                assert!(skip.node.is_some());
                skip.node
                    .and_then(|n| n.borrow().skips.position(idx - skip.length))
            }
        } else {
            assert_eq!(idx, 0);
            None
        }
    }

    // * --------- *
    // |
    // *
    // * ----------*
    // * ----- * - *
    // * - * - * - *

    fn insert(&mut self, idx: usize, value: char, skips: Skips) {
        const HEIGHT: usize = 20;
        if let Some(skip) = self.skips.iter_mut().find(|s| s.length <= idx) {
            if skip.length == idx {
                // TODO(drusu): double check this equation. Are we actually
                // Sampling this distribution correctly?
                let height = (rand::random::<f32>().powf(HEIGHT as f32) * HEIGHT as f32) as usize;

                let to_pad = height.saturating_sub(skips.skips.len());
                let last = skips.skips.last().unwrap();
                for _ in 0..to_pad {
                    skips.skips.push(last.clone());
                }

                let node = Node { value, skips };
            } else {
                skip.node.borrow().skips.position(idx - skip.length)
            }
        } else {
            assert_eq!(idx, 0);
            None
        }
    }
}

// * - - - *
// |       |
// * - a - b - c - d

// A positional list implemented with a Skip List.

impl PositionalList {
    fn position(&self, idx: usize) -> Option<char> {
        self.skips.position(idx)
    }

    fn insert(&self, idx: usize, value: char) {
        self.skips.insert(idx, value)
    }
}
