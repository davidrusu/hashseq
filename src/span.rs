use std::{collections::BTreeSet, rc::Rc};

use crate::{HashNode, Id, Op};
use rle::{HasLength, MergableSpan, SplitableSpanHelpers};

#[derive(Debug, Clone, Copy, Default)]
pub enum SpanDir {
    #[default]
    After,
    Before,
}

#[derive(Debug, Clone, Default)]
pub struct Span {
    pub direction: SpanDir,
    pub first: Id,
    pub first_extra_deps: Rc<BTreeSet<Id>>,
    pub last: Id,
    pub content: Rc<String>,
}

impl Copy for Span {}

impl SplitableSpanHelpers for Span {
    fn truncate_h(&mut self, at: usize) -> Self {
        let op_builder = match self.direction {
            SpanDir::Before => Op::Before,
            SpanDir::After => Op::After,
        };

        let chars = match self.direction {
            SpanDir::Before => self.content.chars().rev(),
            SpanDir::After => self.content.chars(),
        };

        let first_node = HashNode {
            extra_dependencies: self.first_extra_deps.clone(),
            op: op_builder(chars().next().unwrap()),
        };
        let mut prev_node = first_node;

        for c in chars.take(at - 1) {
            let node = HashNode {
                extra_dependencies: Default::default(),
                op: op_builder(prev_node.id(), c),
            };

            prev_node = node;
        }

        let other = match self.direction {
            SpanDir::Before => {
                // Content is stored reversed when direction is Before
                let (right, left) = self.content.split_at(self.content.len() - at);
                self.content = left.to_string();

                Self {
                    direction: self.direction,
                    first: prev_node.id(),
                    first_extra_deps: Default::default(),
                    last: self.last,
                    content: right.to_string(),
                }
            }
            SpanDir::After => {
                let (left, right) = self.content.split_at(at);

                self.content = left.to_string();

                Self {
                    direction: self.direction,
                    first: prev_node.id(),
                    first_extra_deps: Default::default(),
                    last: self.last,
                    content: right.to_string(),
                }
            }
        };

        self.last = prev_node.id();

        other
    }
}

impl MergableSpan for Span {
    fn can_append(&self, other: &Self) -> bool {
        self.direction == other.direction && self.last == other.first
    }

    fn append(&mut self, other: Self) {
        self.content.extend(other.content);
        self.last = other.last
    }
}

impl HasLength for Span {
    fn len(&self) -> usize {
        self.content.len()
    }
}
