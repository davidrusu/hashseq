use crate::{Anchor, HashNode, Id, Op, Payload};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Why a wire run block could not be reconstructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunError {
    /// No text: a run has at least one element.
    Empty,
    /// An element's deps repeat its chain anchor (non-normalized pins).
    RedundantDep,
    /// An interior-deps offset addresses no element after the first.
    DepOffsetOutOfRange(usize),
}

/// A run represents a sequence of consecutive characters that can be compressed
/// together instead of storing each as an individual HashNode.
///
/// The first element is anchored according to `first_op` (After or Before). All
/// subsequent elements chain `InsertAfter` from the previous element in the run.
///
/// Both kinds live in HashSeq's internal storage: a lone `InsertBefore` is just a
/// 1-char `FirstOp::Before` run, extended in place as the typing burst continues.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Run {
    pub at: Anchor,
    /// Pins for the first element of the run
    pub first_pins: BTreeSet<Id>,
    /// Pins of interior elements (offset >= 1), sparse: only
    /// non-empty sets are stored. This is what lets a typing burst keep
    /// extending its run across a remove elsewhere — the next char carries
    /// `pins = {remove_id}` without starting a new run.
    pub interior_pins: BTreeMap<usize, BTreeSet<Id>>,
    /// The string content of this run
    pub run: String,
    /// Cached element IDs for O(1) lookup (avoids recomputing hashes)
    pub elements: Vec<Id>,
}

impl Run {
    /// Create a new InsertAfter-rooted run.
    pub fn new(at: Anchor, first_pins: BTreeSet<Id>, first: char) -> Self {
        let first_node = HashNode {
            pins: first_pins.clone(),
            op: Op::insert(at, Payload::Char(first)),
        };
        let first_id = first_node.id();
        Self {
            at,
            first_pins,
            interior_pins: BTreeMap::new(),
            run: first.to_string(),
            elements: vec![first_id],
        }
    }

    /// Reconstruct a run from its anchor and full text: the first char is anchored
    /// per `first_op`, the rest chain `InsertAfter`, carrying any interior pins
    /// at their offsets (which participate in each element's id).
    ///
    /// This is the wire path, so the fields are validated rather than
    /// trusted: deps must not repeat the element's own chain anchor (nodes
    /// are stored normalized, `pins = refs ∖ named`), and every interior
    /// offset must address an element after the first. Both are checked as
    /// the chain is derived, since element `i`'s id is what offset `i + 1`'s
    /// deps may not name.
    pub fn from_text(
        at: Anchor,
        first_pins: BTreeSet<Id>,
        text: &str,
        mut interior_pins: BTreeMap<usize, BTreeSet<Id>>,
    ) -> Result<Self, RunError> {
        let mut chars = text.chars();
        let first = chars.next().ok_or(RunError::Empty)?;
        if first_pins.contains(&at.id()) {
            return Err(RunError::RedundantDep);
        }
        let mut run = Self::new(at, first_pins, first);
        for (i, ch) in chars.enumerate() {
            let deps = interior_pins.remove(&(i + 1)).unwrap_or_default();
            if deps.contains(run.elements.last().unwrap()) {
                return Err(RunError::RedundantDep);
            }
            run.extend_with_pins(ch, deps);
        }
        if let Some((&offset, _)) = interior_pins.first_key_value() {
            return Err(RunError::DepOffsetOutOfRange(offset));
        }
        Ok(run)
    }

    /// Get the number of characters in this run (O(1) using cached elements)
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// Check if this run is empty (should never happen for valid runs)
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Decompress the run into individual HashNodes
    /// This reconstructs the full node information for each character
    pub fn decompress(&self) -> Vec<HashNode> {
        self.decompress_with_ids()
            .into_iter()
            .map(|(_, node)| node)
            .collect()
    }

    /// Decompress with each node's (cached) id — the chain anchors and the
    /// returned ids come from `self.elements`, so no hashing happens here.
    ///
    /// The ids are only as trustworthy as `self.elements`: runs built by this
    /// module (`new`/`extend`/`from_text`) compute them from content, so
    /// internal callers (merge, decode) may apply without rehashing; anything
    /// else should verify (applying via `HashSeq::apply` re-derives ids).
    pub fn decompress_with_ids(&self) -> Vec<(Id, HashNode)> {
        let mut nodes = Vec::with_capacity(self.elements.len());

        let mut chars = self.run.chars();

        let first = chars.next().unwrap(); // we always have at least one char in the run
        nodes.push((self.elements[0], self.first_node_with_char(first)));

        for (i, ch) in chars.enumerate() {
            let pins = self
                .interior_pins
                .get(&(i + 1))
                .cloned()
                .unwrap_or_default();
            nodes.push((
                self.elements[i + 1],
                HashNode {
                    pins,
                    op: Op::insert_after(self.elements[i], ch),
                },
            ));
        }

        nodes
    }

    fn first_node_with_char(&self, first: char) -> HashNode {
        HashNode {
            pins: self.first_pins.clone(),
            op: Op::insert(self.at, Payload::Char(first)),
        }
    }

    /// Get the ID of the first character in the run
    pub fn first_id(&self) -> Id {
        self.elements[0]
    }

    /// Get the ID of the last character in the run
    pub fn last_id(&self) -> Id {
        *self.elements.last().unwrap()
    }

    /// Extend this run by appending a character and return the new element's ID
    /// The new character will be InsertAfter(current_last_character, ch)
    pub fn extend(&mut self, ch: char) -> Id {
        self.extend_with_pins(ch, BTreeSet::new())
    }

    /// Extend with pins on the new element (they participate in
    /// its id and are stored sparsely at its offset).
    pub fn extend_with_pins(&mut self, ch: char, deps: BTreeSet<Id>) -> Id {
        let prev_id = *self.elements.last().unwrap();
        let new_node = HashNode {
            pins: deps,
            op: Op::insert_after(prev_id, ch),
        };
        let new_id = new_node.id();
        if !new_node.pins.is_empty() {
            self.interior_pins
                .insert(self.elements.len(), new_node.pins);
        }
        self.extend_with_id(new_id, ch);
        new_id
    }

    /// Extend this run with a pre-computed ID (avoids hash computation)
    pub fn extend_with_id(&mut self, id: Id, ch: char) {
        self.run.push(ch);
        self.elements.push(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::{Arbitrary, Gen};

    fn test_id(n: u8) -> Id {
        let mut id = [0u8; 32];
        id[0] = n;
        Id(id)
    }

    impl Arbitrary for Run {
        fn arbitrary(g: &mut Gen) -> Self {
            // Generate a random string of 1-100 characters
            let len = (u8::arbitrary(g) as usize % 100).max(1);
            let chars: Vec<char> = (0..len)
                .map(|_| {
                    // Generate printable ASCII characters
                    let c = (u8::arbitrary(g) % 95) + 32;
                    c as char
                })
                .collect();

            // Generate a random insert_after Id
            let mut insert_after = [0u8; 32];
            for byte in &mut insert_after {
                *byte = u8::arbitrary(g);
            }

            // Create the run with the first character
            let mut run = Run::new(Anchor::After(Id(insert_after)), BTreeSet::new(), chars[0]);

            // Extend with remaining characters; sprinkle interior pins
            // (the burst-across-a-delete shape) on ~1 in 4 elements.
            for &ch in &chars[1..] {
                if u8::arbitrary(g) % 4 == 0 {
                    let mut dep = [0u8; 32];
                    for byte in &mut dep {
                        *byte = u8::arbitrary(g);
                    }
                    run.extend_with_pins(ch, BTreeSet::from_iter([Id(dep)]));
                } else {
                    run.extend(ch);
                }
            }

            run
        }
    }

    /// Interior deps participate in element ids and survive decompress.
    #[test]
    fn test_interior_deps_decompress() {
        let dep = test_id(7);
        let mut run = Run::new(Anchor::After(test_id(0)), BTreeSet::new(), 'a');
        run.extend('b');
        run.extend_with_pins('c', BTreeSet::from_iter([dep]));
        run.extend('d');

        let nodes = run.decompress();
        assert_eq!(nodes[2].pins, BTreeSet::from_iter([dep]));
        assert_eq!(nodes[3].pins, BTreeSet::new());
        // element ids match the cached ones (deps are in the preimage)
        for (i, node) in nodes.iter().enumerate() {
            assert_eq!(node.id(), run.elements[i]);
        }
        // and from_text with the same interior map reconstructs identically
        let rebuilt = Run::from_text(
            run.at,
            run.first_pins.clone(),
            &run.run,
            run.interior_pins.clone(),
        )
        .unwrap();
        assert_eq!(rebuilt, run);
    }

    /// Splitting at or around an interior-deps offset preserves ids: deps at
    /// the split point become the right run's first deps.
    #[test]
    fn test_new_run() {
        let anchor = Anchor::After(test_id(0));
        let mut run = Run::new(anchor, BTreeSet::new(), 'a');
        run.extend('b');
        run.extend('c');

        assert_eq!(run.len(), 3);
        assert_eq!(run.run, "abc");
        assert_eq!(run.at, anchor);
    }

    #[test]
    fn test_decompress() {
        let anchor = Anchor::After(test_id(0));
        let mut run = Run::new(anchor, BTreeSet::new(), 'a');
        run.extend('b');

        let nodes = run.decompress();
        assert_eq!(nodes.len(), 2);

        // Verify each node is correct
        let expected_node_a = HashNode {
            pins: BTreeSet::new(),
            op: Op::insert(anchor, Payload::Char('a')),
        };
        assert_eq!(nodes[0], expected_node_a);

        let expected_node_b = HashNode {
            pins: BTreeSet::new(),
            op: Op::insert_after(nodes[0].id(), 'b'),
        };
        assert_eq!(nodes[1], expected_node_b);
    }

    #[test]
    fn test_extend() {
        let mut run = Run::new(Anchor::After(test_id(0)), BTreeSet::new(), 'a');

        run.extend('b');

        assert_eq!(run.len(), 2);
        assert_eq!(run.run, "ab");
    }

    #[test]
    fn test_first_and_last_id() {
        let mut run = Run::new(Anchor::After(test_id(0)), BTreeSet::new(), 'a');
        run.extend('b');
        run.extend('c');
        let nodes = run.decompress();

        assert_eq!(run.first_id(), nodes[0].id());
        assert_eq!(run.last_id(), nodes[2].id());
    }
}
