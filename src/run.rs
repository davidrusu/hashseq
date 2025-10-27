use crate::{HashNode, Id, Op};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// A run represents a sequence of consecutive characters that can be compressed
/// together instead of storing each as an individual HashNode.
///
/// For example, inserting "abc" after node X creates a run containing "abc"
/// where 'a' is InsertAfter(X), 'b' is InsertAfter('a'), 'c' is InsertAfter('b').
///
/// INVARIANT: All runs must start with an InsertAfter operation. This means:
/// - The first element is InsertAfter(insert_after, first_char)
/// - Subsequent elements are InsertAfter(previous_element, char)
/// - Runs can never start with InsertRoot or InsertBefore
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Run {
    /// The node that comes before this run (the anchor for the first character)
    pub insert_after: Id,
    /// Extra dependencies for the first element of the run
    /// This is needed to correctly reconstruct the node's hash when decompressing
    pub first_extra_deps: BTreeSet<Id>,
    /// The string content of this run
    pub run: String,
}

impl Run {
    /// Create a new run from a string
    pub fn new(insert_after: Id, first_extra_deps: BTreeSet<Id>, first: char) -> Self {
        Self {
            insert_after,
            first_extra_deps,
            run: first.to_string(),
        }
    }

    /// Get the number of characters in this run
    pub fn len(&self) -> usize {
        self.run.chars().count()
    }

    /// Check if this run is empty (should never happen for valid runs)
    pub fn is_empty(&self) -> bool {
        self.run.is_empty()
    }

    /// Decompress the run into individual HashNodes
    /// This reconstructs the full node information for each character
    pub fn decompress(&self) -> Vec<HashNode> {
        let mut nodes = Vec::with_capacity(self.run.len());

        let mut chars = self.run.chars();

        let first = chars.next().unwrap(); // we always have at least one char in the run
        nodes.push(HashNode {
            extra_dependencies: self.first_extra_deps.clone(),
            op: Op::InsertAfter(self.insert_after, first),
        });

        for ch in chars {
            nodes.push(HashNode {
                extra_dependencies: BTreeSet::new(),
                op: Op::InsertAfter(nodes[nodes.len() - 1].id(), ch),
            });
        }

        nodes
    }

    /// Get the ID of the first character in the run
    pub fn first_id(&self) -> Id {
        self.decompress()[0].id()
    }

    /// Get the ID of the last character in the run
    pub fn last_id(&self) -> Id {
        let nodes = self.decompress();
        nodes[nodes.len() - 1].id()
    }

    /// Get the run's ID (same as the first character's ID)
    pub fn run_id(&self) -> Id {
        self.first_id()
    }

    /// Find the position of a given ID within this run
    pub fn find_position(&self, id: &Id) -> Option<usize> {
        self.decompress().iter().position(|node| &node.id() == id)
    }

    /// Extend this run by appending a character
    /// The new character will be InsertAfter(current_last_character, ch)
    pub fn extend(&mut self, ch: char) {
        self.run.push(ch);
    }

    /// Split this run at the given position, returning the right portion
    /// The left portion remains in self, the right portion is returned
    ///
    /// Example: run "abc" split at position 1 becomes "a" and "bc"
    /// The right run's insert_after becomes the ID of the last element of the left run
    pub fn split_at(&mut self, position: usize) -> Run {
        assert!(
            position > 0 && position < self.len(),
            "Invalid split position"
        );

        // Get the ID of the last character in the left portion
        let left_nodes = self.decompress();
        let right_insert_after = left_nodes[position - 1].id();

        // Split the string
        let right_run_str = self.run.split_off(position);

        // Create the right run
        // The right portion has no extra dependencies since it's anchored to an existing node
        let mut right_chars = right_run_str.chars();
        let first_char = right_chars.next().unwrap();
        let mut right_run = Run::new(right_insert_after, BTreeSet::new(), first_char);

        // Extend with remaining characters
        for ch in right_chars {
            right_run.extend(ch);
        }

        right_run
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
        id
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
            let mut run = Run::new(insert_after, BTreeSet::new(), chars[0]);

            // Extend with remaining characters
            for &ch in &chars[1..] {
                run.extend(ch);
            }

            run
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
        assert_eq!(run.insert_after, anchor);
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
        assert_eq!(run.len(), 1);
        assert_eq!(run.run, "a");

        // Right run should have 'bc' with insert_after = ID of 'a'
        assert_eq!(right_run.len(), 2);
        assert_eq!(right_run.run, "bc");
        assert_eq!(right_run.insert_after, nodes_before[0].id());
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
