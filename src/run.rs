use crate::{HashNode, Id, Op};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// How the first element of a run is anchored relative to its `anchor` node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FirstOp {
    /// First char is `InsertAfter(anchor, ch)`. Subsequent chars chain InsertAfter.
    After,
    /// First char is `InsertBefore(anchor, ch)`. Subsequent chars chain InsertAfter
    /// from the first char.
    Before,
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
    /// The node this run is anchored against (semantic role depends on `first_op`).
    pub anchor: Id,
    /// How the first element relates to `anchor`.
    pub first_op: FirstOp,
    /// Extra dependencies for the first element of the run
    /// This is needed to correctly reconstruct the node's hash when decompressing
    pub first_extra_deps: BTreeSet<Id>,
    /// Extra dependencies of interior elements (offset >= 1), sparse: only
    /// non-empty sets are stored. This is what lets a typing burst keep
    /// extending its run across a remove elsewhere — the next char carries
    /// `extra_deps = {remove_id}` without starting a new run.
    pub interior_extra_deps: BTreeMap<usize, BTreeSet<Id>>,
    /// The string content of this run
    pub run: String,
    /// Cached element IDs for O(1) lookup (avoids recomputing hashes)
    pub elements: Vec<Id>,
}

impl Run {
    /// Create a new InsertAfter-rooted run.
    pub fn new(insert_after: Id, first_extra_deps: BTreeSet<Id>, first: char) -> Self {
        Self::with_first_op(insert_after, FirstOp::After, first_extra_deps, first)
    }

    /// Create a new InsertBefore-rooted run. The first character is constrained to
    /// appear immediately before `anchor`; subsequent characters chain InsertAfter
    /// from the first.
    pub fn new_before(anchor: Id, first_extra_deps: BTreeSet<Id>, first: char) -> Self {
        Self::with_first_op(anchor, FirstOp::Before, first_extra_deps, first)
    }

    /// Reconstruct a run from its anchor and full text: the first char is anchored
    /// per `first_op`, the rest chain `InsertAfter`, carrying any interior extra
    /// deps at their offsets (which participate in each element's id).
    /// Returns `None` for empty text.
    pub fn from_text(
        anchor: Id,
        first_op: FirstOp,
        first_extra_deps: BTreeSet<Id>,
        text: &str,
        mut interior_extra_deps: BTreeMap<usize, BTreeSet<Id>>,
    ) -> Option<Self> {
        let mut chars = text.chars();
        let mut run = Self::with_first_op(anchor, first_op, first_extra_deps, chars.next()?);
        for (i, ch) in chars.enumerate() {
            let deps = interior_extra_deps.remove(&(i + 1)).unwrap_or_default();
            run.extend_with_deps(ch, deps);
        }
        Some(run)
    }

    fn with_first_op(
        anchor: Id,
        first_op: FirstOp,
        first_extra_deps: BTreeSet<Id>,
        first: char,
    ) -> Self {
        let first_node = HashNode {
            extra_dependencies: first_extra_deps.clone(),
            op: match first_op {
                FirstOp::After => Op::InsertAfter(anchor, first),
                FirstOp::Before => Op::InsertBefore(anchor, first),
            },
        };
        let first_id = first_node.id();
        Self {
            anchor,
            first_op,
            first_extra_deps,
            interior_extra_deps: BTreeMap::new(),
            run: first.to_string(),
            elements: vec![first_id],
        }
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
        let mut nodes = Vec::with_capacity(self.run.len());

        let mut chars = self.run.chars();

        let first = chars.next().unwrap(); // we always have at least one char in the run
        nodes.push(self.first_node_with_char(first));

        for (i, ch) in chars.enumerate() {
            let extra_dependencies = self
                .interior_extra_deps
                .get(&(i + 1))
                .cloned()
                .unwrap_or_default();
            nodes.push(HashNode {
                extra_dependencies,
                op: Op::InsertAfter(nodes[nodes.len() - 1].id(), ch),
            });
        }

        nodes
    }

    pub fn first_node(&self) -> HashNode {
        let first = self.run.chars().next().unwrap();
        self.first_node_with_char(first)
    }

    fn first_node_with_char(&self, first: char) -> HashNode {
        HashNode {
            extra_dependencies: self.first_extra_deps.clone(),
            op: match self.first_op {
                FirstOp::After => Op::InsertAfter(self.anchor, first),
                FirstOp::Before => Op::InsertBefore(self.anchor, first),
            },
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

    /// Get the run's ID (same as the first character's ID)
    pub fn run_id(&self) -> Id {
        self.first_id()
    }

    /// Find the position of a given ID within this run
    pub fn find_position(&self, id: &Id) -> Option<usize> {
        self.elements.iter().position(|elem_id| elem_id == id)
    }

    /// Extend this run by appending a character and return the new element's ID
    /// The new character will be InsertAfter(current_last_character, ch)
    pub fn extend(&mut self, ch: char) -> Id {
        self.extend_with_deps(ch, BTreeSet::new())
    }

    /// Extend with extra dependencies on the new element (they participate in
    /// its id and are stored sparsely at its offset).
    pub fn extend_with_deps(&mut self, ch: char, deps: BTreeSet<Id>) -> Id {
        let prev_id = *self.elements.last().unwrap();
        let new_node = HashNode {
            extra_dependencies: deps,
            op: Op::InsertAfter(prev_id, ch),
        };
        let new_id = new_node.id();
        if !new_node.extra_dependencies.is_empty() {
            self.interior_extra_deps
                .insert(self.elements.len(), new_node.extra_dependencies);
        }
        self.extend_with_id(new_id, ch);
        new_id
    }

    /// Extend this run with a pre-computed ID (avoids hash computation)
    pub fn extend_with_id(&mut self, id: Id, ch: char) {
        self.run.push(ch);
        self.elements.push(id);
    }

    /// Split this run at the given position, returning the right portion
    /// The left portion remains in self, the right portion is returned
    ///
    /// Example: run "abc" split at position 1 becomes "a" and "bc"
    /// The right run's anchor becomes the ID of the last element of the left run
    /// and is always `FirstOp::After` (the chain extends after the left tail).
    pub fn split_at(&mut self, position: usize) -> Run {
        assert!(
            position > 0 && position < self.len(),
            "Invalid split position"
        );

        let right_elements = self.elements.split_off(position);
        let right_anchor = *self.elements.last().unwrap();

        let byte_pos = self.run.char_indices().nth(position).unwrap().0;
        let right_str = self.run.split_off(byte_pos);

        // Interior deps at the split point become the right run's first deps
        // (preserving the element's id); later offsets rebase.
        let mut right_interior = self.interior_extra_deps.split_off(&position);
        let right_first_deps = right_interior.remove(&position).unwrap_or_default();
        let right_interior: BTreeMap<usize, BTreeSet<Id>> = right_interior
            .into_iter()
            .map(|(k, v)| (k - position, v))
            .collect();

        Run {
            anchor: right_anchor,
            first_op: FirstOp::After,
            first_extra_deps: right_first_deps,
            interior_extra_deps: right_interior,
            run: right_str,
            elements: right_elements,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::{Arbitrary, Gen};
    use quickcheck_macros::quickcheck;

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
            let mut run = Run::new(Id(insert_after), BTreeSet::new(), chars[0]);

            // Extend with remaining characters; sprinkle interior extra-deps
            // (the burst-across-a-delete shape) on ~1 in 4 elements.
            for &ch in &chars[1..] {
                if u8::arbitrary(g) % 4 == 0 {
                    let mut dep = [0u8; 32];
                    for byte in &mut dep {
                        *byte = u8::arbitrary(g);
                    }
                    run.extend_with_deps(ch, BTreeSet::from_iter([Id(dep)]));
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
        let mut run = Run::new(test_id(0), BTreeSet::new(), 'a');
        run.extend('b');
        run.extend_with_deps('c', BTreeSet::from_iter([dep]));
        run.extend('d');

        let nodes = run.decompress();
        assert_eq!(nodes[2].extra_dependencies, BTreeSet::from_iter([dep]));
        assert_eq!(nodes[3].extra_dependencies, BTreeSet::new());
        // element ids match the cached ones (deps are in the preimage)
        for (i, node) in nodes.iter().enumerate() {
            assert_eq!(node.id(), run.elements[i]);
        }
        // and from_text with the same interior map reconstructs identically
        let rebuilt = Run::from_text(
            run.anchor,
            run.first_op,
            run.first_extra_deps.clone(),
            &run.run,
            run.interior_extra_deps.clone(),
        )
        .unwrap();
        assert_eq!(rebuilt, run);
    }

    /// Splitting at or around an interior-deps offset preserves ids: deps at
    /// the split point become the right run's first deps.
    #[test]
    fn test_split_at_interior_deps_boundary() {
        let dep = test_id(9);
        let mut run = Run::new(test_id(0), BTreeSet::new(), 'a');
        run.extend('b');
        run.extend_with_deps('c', BTreeSet::from_iter([dep]));
        run.extend('d');
        let original = run.decompress();

        for at in 1..run.len() {
            let mut left = run.clone();
            let right = left.split_at(at);
            let mut combined = left.decompress();
            combined.extend(right.decompress());
            assert_eq!(combined, original, "split at {at} changed identities");
        }
    }

    #[test]
    fn test_new_run() {
        let anchor = test_id(0);
        let mut run = Run::new(anchor, BTreeSet::new(), 'a');
        run.extend('b');
        run.extend('c');

        assert_eq!(run.len(), 3);
        assert_eq!(run.run, "abc");
        assert_eq!(run.anchor, anchor);
        assert_eq!(run.first_op, FirstOp::After);
    }

    #[test]
    fn test_decompress() {
        let anchor = test_id(0);
        let mut run = Run::new(anchor, BTreeSet::new(), 'a');
        run.extend('b');

        let nodes = run.decompress();
        assert_eq!(nodes.len(), 2);

        // Verify each node is correct
        let expected_node_a = HashNode {
            extra_dependencies: BTreeSet::new(),
            op: Op::InsertAfter(anchor, 'a'),
        };
        assert_eq!(nodes[0], expected_node_a);

        let expected_node_b = HashNode {
            extra_dependencies: BTreeSet::new(),
            op: Op::InsertAfter(nodes[0].id(), 'b'),
        };
        assert_eq!(nodes[1], expected_node_b);
    }

    #[test]
    fn test_extend() {
        let mut run = Run::new(test_id(0), BTreeSet::new(), 'a');

        run.extend('b');

        assert_eq!(run.len(), 2);
        assert_eq!(run.run, "ab");
    }

    #[test]
    fn test_split_at() {
        let mut run = Run::new(test_id(0), BTreeSet::new(), 'a');
        run.extend('b');
        run.extend('c');

        // Get IDs before split
        let nodes_before = run.decompress();

        let right_run = run.split_at(1);

        // Left run should have 'a'
        assert_eq!(run.run, "a");

        // Right run should have 'bc' with anchor = ID of 'a'
        assert_eq!(right_run.run, "bc");
        assert_eq!(right_run.anchor, nodes_before[0].id());
        assert_eq!(right_run.first_op, FirstOp::After);
    }

    #[test]
    fn test_first_and_last_id() {
        let mut run = Run::new(test_id(0), BTreeSet::new(), 'a');
        run.extend('b');
        run.extend('c');
        let nodes = run.decompress();

        assert_eq!(run.first_id(), nodes[0].id());
        assert_eq!(run.last_id(), nodes[2].id());
        assert_eq!(run.run_id(), nodes[0].id());
    }

    #[test]
    fn test_find_position() {
        let mut run = Run::new(test_id(0), BTreeSet::new(), 'a');
        run.extend('b');
        run.extend('c');
        let nodes = run.decompress();

        assert_eq!(run.find_position(&nodes[0].id()), Some(0));
        assert_eq!(run.find_position(&nodes[1].id()), Some(1));
        assert_eq!(run.find_position(&nodes[2].id()), Some(2));
        assert_eq!(run.find_position(&test_id(99)), None);
    }

    #[quickcheck]
    fn prop_split_preserves_decompress(run: Run, idx: usize) -> bool {
        // split_at requires: 0 < position < len
        // So valid range is 1..run.len()
        if run.len() < 2 {
            // Can't split a run with only 1 element
            return true;
        }

        // Clamp idx to valid range [1, run.len())
        let position = (idx % (run.len() - 1)).max(1);

        // Get original decompressed nodes
        let original_nodes = run.decompress();

        // Split the run
        let mut run_a = run.clone();
        let run_b = run_a.split_at(position);

        // Get decompressed nodes from both parts
        let nodes_a = run_a.decompress();
        let nodes_b = run_b.decompress();

        // Concatenate the decompressed nodes
        let mut combined_nodes = nodes_a;
        combined_nodes.extend(nodes_b);

        // Verify they match the original
        original_nodes == combined_nodes
    }
}
