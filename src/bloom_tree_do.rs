use std::hash::Hash;

// BloomFilter implementation remains unchanged
#[derive(Debug, Clone)]
struct BloomFilter {
    bits: Vec<bool>,
    size: usize,
    num_hashes: usize,
}

impl BloomFilter {
    /// Create a new Bloom filter with specified size and number of hash functions
    #[inline]
    fn new(size: usize, num_hashes: usize) -> Self {
        assert!(size > 0 && num_hashes > 0);
        Self {
            bits: vec![false; size],
            size,
            num_hashes,
        }
    }

    /// Insert an item into the Bloom filter
    #[inline]
    fn insert(&mut self, item: &impl Hash) {
        for i in 0..self.num_hashes {
            let idx = self.hash(item, i);
            self.bits[idx] = true;
        }
    }

    /// Test if an item might be in the set
    #[inline]
    fn might_contain(&self, item: &impl Hash) -> bool {
        (0..self.num_hashes).all(|i| self.bits[self.hash(item, i)])
    }

    /// Calculate hash for a given item and seed
    #[inline]
    fn hash(&self, item: &impl Hash, seed: usize) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;
        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        seed.hash(&mut hasher);
        hasher.finish() as usize % self.size
    }
}

/// Node in the Bloom filter tree structure
#[derive(Debug, Clone)]
struct Node<T> {
    element: T,
    filter: BloomFilter,
    left_size: usize,
    left: Option<usize>,  // Index into nodes vec
    right: Option<usize>, // Index into nodes vec
    height: usize,
}

/// Tree structure augmented with Bloom filters for efficient position queries
#[derive(Debug, Clone)]
pub struct BloomTree<T> {
    nodes: Vec<Node<T>>,
    root: Option<usize>, // Index of root node
    size: usize,         // Number of elements in tree
}

impl<T: Hash + Clone + Eq + std::fmt::Debug> Node<T> {
    #[inline]
    fn new(element: T, filter_size: usize) -> Self {
        let mut filter = BloomFilter::new(filter_size, 4);
        filter.insert(&element);
        Self {
            element,
            filter,
            left_size: 0,
            left: None,
            right: None,
            height: 1,
        }
    }

    #[inline]
    fn update_height(&mut self, left_height: usize, right_height: usize) {
        self.height = 1 + std::cmp::max(left_height, right_height);
    }
}

impl<T: Hash + Clone + Eq + std::fmt::Debug> BloomTree<T> {
    #[inline]
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            root: None,
            size: 0,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    fn update_filter(&mut self, node_idx: usize) {
        // Collect elements in-order without recursion
        let mut elements = Vec::new();
        let mut stack = Vec::new();
        let mut current = Some(node_idx);

        while !stack.is_empty() || current.is_some() {
            // Traverse left as far as possible
            while let Some(idx) = current {
                stack.push(idx);
                current = self.nodes[idx].left;
            }

            if let Some(idx) = stack.pop() {
                // Process current node
                elements.push(self.nodes[idx].element.clone());
                // Move to right subtree
                current = self.nodes[idx].right;
            }
        }

        // Update filter with collected elements
        let filter_size = self.nodes[node_idx].filter.size;
        let mut new_filter = BloomFilter::new(filter_size, 4);
        for element in &elements {
            new_filter.insert(element);
        }
        self.nodes[node_idx].filter = new_filter;
    }

    pub fn insert(&mut self, position: usize, element: T) {
        assert!(position <= self.size);

        match self.root {
            Some(root_idx) => {
                let filter_size = 256 * (1 << (self.nodes[root_idx].height / 2));
                self.insert_at(root_idx, position, element, filter_size);
            }
            None => {
                let node = Node::new(element, 256);
                self.nodes.push(node);
                self.root = Some(self.nodes.len() - 1);
            }
        }
        self.size += 1;
    }

    pub fn position(&self, element: &T) -> Option<usize> {
        self.root
            .and_then(|root_idx| self.position_recursive(root_idx, element))
    }

    fn position_recursive(&self, node_idx: usize, element: &T) -> Option<usize> {
        let node = &self.nodes[node_idx];

        // Early exit if element definitely not in subtree
        if !node.filter.might_contain(element) {
            return None;
        }

        // First check left subtree
        if let Some(left_idx) = node.left {
            if self.nodes[left_idx].filter.might_contain(element) {
                if let Some(pos) = self.position_recursive(left_idx, element) {
                    return Some(pos);
                }
            }
        }

        // Then check current node
        if &node.element == element {
            return Some(node.left_size);
        }

        // Finally check right subtree
        if let Some(right_idx) = node.right {
            if self.nodes[right_idx].filter.might_contain(element) {
                return self
                    .position_recursive(right_idx, element)
                    .map(|pos| node.left_size + 1 + pos);
            }
        }

        None
    }

    fn update_node_height(&mut self, node_idx: usize) {
        // Get heights before modifying the node
        let left_height = self.nodes[node_idx]
            .left
            .map(|idx| self.nodes[idx].height)
            .unwrap_or(0);
        let right_height = self.nodes[node_idx]
            .right
            .map(|idx| self.nodes[idx].height)
            .unwrap_or(0);

        // Now update the height
        self.nodes[node_idx].update_height(left_height, right_height);
    }

    fn insert_at(&mut self, node_idx: usize, position: usize, element: T, filter_size: usize) {
        let left_size = self.nodes[node_idx].left_size;

        if position <= left_size {
            // Insert into left subtree
            match self.nodes[node_idx].left {
                Some(left_idx) => {
                    self.insert_at(left_idx, position, element, filter_size);
                }
                None => {
                    let new_node = Node::new(element, filter_size);
                    self.nodes.push(new_node);
                    self.nodes[node_idx].left = Some(self.nodes.len() - 1);
                }
            }
            self.nodes[node_idx].left_size += 1;
        } else {
            // Insert into right subtree
            match self.nodes[node_idx].right {
                Some(right_idx) => {
                    self.insert_at(right_idx, position - left_size - 1, element, filter_size);
                }
                None => {
                    let new_node = Node::new(element, filter_size);
                    self.nodes.push(new_node);
                    self.nodes[node_idx].right = Some(self.nodes.len() - 1);
                }
            }
        }

        // Update height and filter
        self.update_node_height(node_idx);
        self.update_filter(node_idx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};

    const DEBUG: bool = false;

    macro_rules! debug {
    ($($arg:tt)*) => ({
        if DEBUG {
            println!("[DEBUG] {}", format!($($arg)*));
        }
    })
}

    #[derive(Debug, Clone)]
    enum Action {
        Insert(usize, u32),
        Position(u32),
    }

    impl Arbitrary for Action {
        fn arbitrary(g: &mut Gen) -> Self {
            let size = usize::arbitrary(g) % 100;
            if bool::arbitrary(g) {
                Action::Insert(size, u32::arbitrary(g))
            } else {
                Action::Position(u32::arbitrary(g))
            }
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            match self {
                Action::Insert(pos, val) => {
                    // Create a vector to hold all possible shrunk values
                    let mut shrunk = Vec::new();

                    // Shrink position towards 0
                    if *pos > 0 {
                        shrunk.push(Action::Insert(0, *val));
                        shrunk.push(Action::Insert(pos / 2, *val));
                    }

                    // Shrink value towards 0, 1
                    if *val > 0 {
                        shrunk.push(Action::Insert(*pos, 0));
                        shrunk.push(Action::Insert(*pos, 1));
                        shrunk.push(Action::Insert(*pos, val / 2));
                    }

                    Box::new(shrunk.into_iter())
                }
                Action::Position(val) => {
                    let mut shrunk = Vec::new();

                    // Shrink value towards 0, 1
                    if *val > 0 {
                        shrunk.push(Action::Position(0));
                        shrunk.push(Action::Position(1));
                        shrunk.push(Action::Position(val / 2));
                    }

                    Box::new(shrunk.into_iter())
                }
            }
        }
    }

    // Property test remains the same but will now benefit from better shrinking
    #[test]
    fn test_position_semantics() {
        fn property(actions: Vec<Action>) -> TestResult {
            let mut tree = BloomTree::new();
            let mut reference = Vec::new();

            for (i, action) in actions.iter().enumerate() {
                match action {
                    Action::Insert(pos, value) => {
                        let pos = pos % (reference.len() + 1);
                        tree.insert(pos, *value);
                        reference.insert(pos, *value);
                        // Add debug output for insertion
                        debug!("Step {}: Insert {} at position {}", i, value, pos);
                        debug!("Reference after insert: {:?}", reference);
                    }
                    Action::Position(value) => {
                        let tree_pos = tree.position(value);
                        let ref_pos = reference.iter().position(|x| x == value);
                        // Add debug output for position query
                        debug!(
                            "Step {}: Position query for {}: tree={:?}, ref={:?}",
                            i, value, tree_pos, ref_pos
                        );

                        if tree_pos != ref_pos {
                            return TestResult::error(format!(
                                "Position mismatch at step {}: value={}, tree={:?}, reference={:?}\nFull reference: {:?}",
                                i, value, tree_pos, ref_pos, reference
                            ));
                        }
                    }
                }
            }
            TestResult::passed()
        }

        QuickCheck::new()
            .tests(1000)
            .max_tests(2000)
            .quickcheck(property as fn(Vec<Action>) -> TestResult);
    }

    /// Helper function to validate the entire tree structure
    fn validate_tree<T: Hash + Clone + Eq + std::fmt::Debug>(
        tree: &BloomTree<T>,
    ) -> Result<(), String> {
        // 1. Validate empty tree
        if tree.is_empty() {
            if tree.root.is_some() {
                return Err("Empty tree should have no root".into());
            }
            return Ok(());
        }

        // 2. Validate node indices
        if let Some(root_idx) = tree.root {
            if root_idx >= tree.nodes.len() {
                return Err(format!(
                    "Root index {} out of bounds (len: {})",
                    root_idx,
                    tree.nodes.len()
                ));
            }
            validate_subtree(tree, root_idx, 0, tree.size)?;
        }

        Ok(())
    }

    /// Recursively validates a subtree, ensuring height and size invariants
    fn validate_subtree<T: Hash + Clone + Eq + std::fmt::Debug>(
        tree: &BloomTree<T>,
        node_idx: usize,
        min_pos: usize,
        max_pos: usize,
    ) -> Result<(usize, usize), String> {
        // Returns (height, size)
        let node = &tree.nodes[node_idx];
        let mut subtree_size = 1;
        let mut left_height = 0;
        let mut right_height = 0;

        // Validate left subtree
        if let Some(left_idx) = node.left {
            if left_idx >= tree.nodes.len() {
                return Err(format!("Left child index {} out of bounds", left_idx));
            }
            if node.left_size == 0 {
                return Err("Node with left child must have non-zero left_size".into());
            }
            let (height, size) =
                validate_subtree(tree, left_idx, min_pos, min_pos + node.left_size)?;
            left_height = height;
            subtree_size += size;
        } else if node.left_size != 0 {
            return Err("Node without left child must have zero left_size".into());
        }

        // Validate right subtree
        if let Some(right_idx) = node.right {
            if right_idx >= tree.nodes.len() {
                return Err(format!("Right child index {} out of bounds", right_idx));
            }
            let right_min_pos = min_pos + node.left_size + 1;
            let (height, size) = validate_subtree(tree, right_idx, right_min_pos, max_pos)?;
            right_height = height;
            subtree_size += size;
        }

        // Validate height
        let expected_height = 1 + std::cmp::max(left_height, right_height);
        if node.height != expected_height {
            return Err(format!(
                "Height mismatch at node {}: expected {}, got {}",
                node_idx, expected_height, node.height
            ));
        }

        Ok((expected_height, subtree_size))
    }

    #[test]
    fn test_tree_growth() {
        // Test 1: Empty tree validation
        let tree: BloomTree<i32> = BloomTree::new();
        assert!(validate_tree(&tree).is_ok());

        // Test 2: Sequential insertion at end
        let mut tree = BloomTree::new();
        for i in 0..5 {
            tree.insert(i, i as i32);
            assert!(
                validate_tree(&tree).is_ok(),
                "Failed after inserting {} at end",
                i
            );

            // Verify size
            assert_eq!(tree.len(), i + 1);

            // Verify position lookups
            for j in 0..=i {
                assert_eq!(tree.position(&(j as i32)), Some(j));
            }
        }

        // Test 3: Insertions at start (prepend)
        let mut tree = BloomTree::new();
        for i in 0..5 {
            tree.insert(0, i as i32);
            assert!(
                validate_tree(&tree).is_ok(),
                "Failed after inserting {} at start",
                i
            );

            // Verify elements shifted correctly
            for j in 0..=i {
                assert_eq!(tree.position(&(j as i32)), Some(i - j));
            }
        }

        // Test 4: Alternating insertions (middle positions)
        let mut tree = BloomTree::new();
        tree.insert(0, 0); // [0]
        tree.insert(1, 1); // [0,1]
        tree.insert(1, 2); // [0,2,1]
        tree.insert(2, 3); // [0,2,3,1]
        assert!(
            validate_tree(&tree).is_ok(),
            "Failed after mixed insertions"
        );

        // Verify final positions
        assert_eq!(tree.position(&0), Some(0));
        assert_eq!(tree.position(&2), Some(1));
        assert_eq!(tree.position(&3), Some(2));
        assert_eq!(tree.position(&1), Some(3));

        // Test 5: Stress test with larger sequence
        let mut tree = BloomTree::new();
        let size = 100;
        for i in 0..size {
            let pos = if i % 2 == 0 { tree.len() } else { 0 };
            tree.insert(pos, i as i32);
            assert!(
                validate_tree(&tree).is_ok(),
                "Failed during stress test at i={}",
                i
            );
        }
        assert_eq!(tree.len(), size);
    }
}
