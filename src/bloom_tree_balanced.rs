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
        println!(
            "\nChecking position at node {}: {:?}",
            node_idx, node.element
        );

        // Early exit if element definitely not in subtree
        if !node.filter.might_contain(element) {
            println!("  Filter excludes element at node {}", node_idx);
            return None;
        }
        println!("  Filter includes element at node {}", node_idx);

        // First check left subtree
        if let Some(left_idx) = node.left {
            println!("  Checking left child {}", left_idx);
            if self.nodes[left_idx].filter.might_contain(element) {
                println!("  Left child {} might contain element", left_idx);
                if let Some(pos) = self.position_recursive(left_idx, element) {
                    return Some(pos);
                }
            } else {
                println!(
                    "  Left child {} definitely doesn't contain element",
                    left_idx
                );
            }
        }

        // Then check current node
        if &node.element == element {
            println!(
                "  Found element at current node {}, left_size={}",
                node_idx, node.left_size
            );
            return Some(node.left_size);
        }

        // Finally check right subtree
        if let Some(right_idx) = node.right {
            println!("  Checking right child {}", right_idx);
            if self.nodes[right_idx].filter.might_contain(element) {
                println!("  Right child {} might contain element", right_idx);
                return self.position_recursive(right_idx, element).map(|pos| {
                    let final_pos = node.left_size + 1 + pos;
                    println!(
                        "  Found in right subtree at relative position {}, final position {}",
                        pos, final_pos
                    );
                    final_pos
                });
            } else {
                println!(
                    "  Right child {} definitely doesn't contain element",
                    right_idx
                );
            }
        }

        println!("  Element not found in node {} or its subtrees", node_idx);
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
                    // Check if rebalancing needed after recursive insert
                    self.rebalance(left_idx);
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
                    // Check if rebalancing needed after recursive insert
                    self.rebalance(right_idx);
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

        // Check if current node needs rebalancing
        self.rebalance(node_idx);
    }
}

impl<T: Hash + Clone + Eq + std::fmt::Debug> BloomTree<T> {
    // Weight-balance threshold
    const ALPHA: f64 = 0.25;

    /// Calculate total size of subtree rooted at node_idx
    fn subtree_size(&self, node_idx: usize) -> usize {
        let node = &self.nodes[node_idx];
        let left_size = node.left_size;
        let right_size = match node.right {
            Some(right_idx) => self.subtree_size(right_idx),
            None => 0,
        };
        left_size + right_size + 1
    }

    /// Calculate balance ratio for a node
    fn balance_ratio(&self, node_idx: usize) -> f64 {
        let node = &self.nodes[node_idx];
        let left_weight = node.left_size;
        let total_weight = self.subtree_size(node_idx);
        let right_weight = total_weight - left_weight - 1;

        f64::min(left_weight as f64, right_weight as f64) / total_weight as f64
    }

    fn debug_print_tree(&self) -> String {
        match self.root {
            None => "Empty tree".to_string(),
            Some(root_idx) => self.debug_print_node(root_idx, 0),
        }
    }

    fn debug_print_node(&self, node_idx: usize, indent: usize) -> String {
        let node = &self.nodes[node_idx];
        let mut result = format!(
            "{:indent$}Node({:?}, left_size={}, height={})\n",
            "",
            node.element,
            node.left_size,
            node.height,
            indent = indent
        );

        if let Some(left_idx) = node.left {
            result.push_str(&self.debug_print_node(left_idx, indent + 2));
        }
        if let Some(right_idx) = node.right {
            result.push_str(&self.debug_print_node(right_idx, indent + 2));
        }
        result
    }

    fn rotate_left(&mut self, node_idx: usize) {
        println!("\nBefore left rotation at node {}:", node_idx);
        println!("{}", self.debug_print_tree());

        let right_idx = self.nodes[node_idx].right.unwrap();
        let right_left = self.nodes[right_idx].left;

        // Step 1: Compute initial sizes
        let old_node_left_size = self.nodes[node_idx].left_size;
        let right_node_left_size = self.nodes[right_idx].left_size;
        let right_left_subtree_size = right_left.map_or(0, |idx| self.subtree_size(idx));

        println!("Initial sizes:");
        println!("  node({}).left_size = {}", node_idx, old_node_left_size);
        println!("  node({}).left_size = {}", right_idx, right_node_left_size);
        println!("  right_left_subtree_size = {}", right_left_subtree_size);

        // Step 2: Update parent pointers
        if self.root == Some(node_idx) {
            println!("  Updating root from {} to {}", node_idx, right_idx);
            self.root = Some(right_idx);
        }

        // Step 3: Perform rotation
        self.nodes[right_idx].left = Some(node_idx);
        self.nodes[node_idx].right = right_left;

        // Step 4: Update sizes
        // The left size of the original node becomes the size of the right's left subtree
        self.nodes[node_idx].left_size = right_left_subtree_size;

        // The left size of the new root includes:
        // - Original node's left subtree
        // - Original node itself
        self.nodes[right_idx].left_size = old_node_left_size + 1;

        println!("Final sizes:");
        println!(
            "  node({}).left_size = {}",
            node_idx, self.nodes[node_idx].left_size
        );
        println!(
            "  node({}).left_size = {}",
            right_idx, self.nodes[right_idx].left_size
        );

        // Step 5: Update heights
        self.update_node_height(node_idx);
        self.update_node_height(right_idx);

        // Step 6: Update filters bottom-up
        if let Some(left_child) = right_left {
            self.update_filter(left_child);
        }
        self.update_filter(node_idx);
        self.update_filter(right_idx);

        println!("\nAfter left rotation:");
        println!("{}", self.debug_print_tree());
    }

    fn rotate_right(&mut self, node_idx: usize) {
        let left_idx = self.nodes[node_idx].left.unwrap();
        let left_right = self.nodes[left_idx].right;

        // Step 1: Calculate initial sizes
        let left_right_size = left_right.map_or(0, |idx| self.subtree_size(idx));

        // Step 2: Update parent links
        if self.root == Some(node_idx) {
            self.root = Some(left_idx);
        }

        // Step 3: Perform structural rotation
        self.nodes[left_idx].right = Some(node_idx);
        self.nodes[node_idx].left = left_right;

        // Step 4: Update size information
        self.nodes[node_idx].left_size = left_right_size;
        // left_idx.left_size remains unchanged

        // Step 5: Update filters bottom-up
        if let Some(right_child) = left_right {
            self.update_filter(right_child);
        }
        self.update_filter(node_idx);
        self.update_filter(left_idx);

        // Step 6: Update heights bottom-up
        self.update_node_height(node_idx);
        self.update_node_height(left_idx);
    }

    fn update_filter(&mut self, node_idx: usize) {
        println!("Updating filter for node {}", node_idx);
        // Create new filter with same parameters
        let filter_size = self.nodes[node_idx].filter.size;
        let num_hashes = self.nodes[node_idx].filter.num_hashes;
        let mut new_filter = BloomFilter::new(filter_size, num_hashes);

        // First collect all elements in the subtree
        let mut elements = Vec::new();
        self.collect_elements(node_idx, &mut elements);

        println!("  Collected elements for filter: {:?}", elements);

        // Add all elements to the filter
        for element in elements {
            new_filter.insert(&element);
        }

        // Update node's filter
        self.nodes[node_idx].filter = new_filter;
        println!("  Filter updated for node {}", node_idx);
    }

    fn collect_elements(&self, node_idx: usize, elements: &mut Vec<T>) {
        let node = &self.nodes[node_idx];

        // Add current node's element
        elements.push(node.element.clone());

        // Recursively collect from children
        if let Some(left_idx) = node.left {
            self.collect_elements(left_idx, elements);
        }
        if let Some(right_idx) = node.right {
            self.collect_elements(right_idx, elements);
        }
    }

    fn calculate_subtree_weights(&self, node_idx: usize) -> (usize, usize) {
        let node = &self.nodes[node_idx];

        // Calculate actual weights of subtrees
        let left_weight = node.left.map_or(0, |idx| self.subtree_size(idx));

        let right_weight = node.right.map_or(0, |idx| self.subtree_size(idx));

        (left_weight, right_weight)
    }

    fn rebalance(&mut self, node_idx: usize) {
        if !self.needs_rebalance(node_idx) {
            return;
        }

        println!("\nRebalancing node {}", node_idx);
        println!("Tree before rebalance:\n{}", self.debug_print_tree());

        // Calculate actual weights of subtrees
        let (left_weight, right_weight) = self.calculate_subtree_weights(node_idx);
        let total_weight = left_weight + right_weight + 1;

        println!(
            "Weights: left={}, right={}, total={}",
            left_weight, right_weight, total_weight
        );

        if left_weight < right_weight {
            // Right-heavy case
            let right_idx = self.nodes[node_idx]
                .right
                .expect("Right child must exist for right-heavy node");
            println!("Right-heavy case. Right child: {}", right_idx);

            let (right_left_weight, right_right_weight) = self.calculate_subtree_weights(right_idx);
            println!(
                "Right subtree weights: left={}, right={}",
                right_left_weight, right_right_weight
            );

            if right_left_weight > right_right_weight {
                println!("Performing right-left double rotation");
                self.rotate_right(right_idx);
                self.rotate_left(node_idx);
            } else {
                println!("Performing single left rotation");
                self.rotate_left(node_idx);
            }
        } else if left_weight > right_weight {
            // Changed to explicit comparison
            // Left-heavy case
            let left_idx = self.nodes[node_idx]
                .left
                .expect("Left child must exist for left-heavy node");
            println!("Left-heavy case. Left child: {}", left_idx);

            let (left_left_weight, left_right_weight) = self.calculate_subtree_weights(left_idx);
            println!(
                "Left subtree weights: left={}, right={}",
                left_left_weight, left_right_weight
            );

            if left_right_weight > left_left_weight {
                println!("Performing left-right double rotation");
                self.rotate_left(left_idx);
                self.rotate_right(node_idx);
            } else {
                println!("Performing single right rotation");
                self.rotate_right(node_idx);
            }
        } else {
            // Equal weights - no rebalancing needed
            println!("Weights are equal, no rebalancing needed");
            return;
        }

        // Update filters for the entire subtree after rebalancing
        self.update_subtree_filters(node_idx);
        println!("Tree after rebalance:\n{}", self.debug_print_tree());
    }

    fn needs_rebalance(&self, node_idx: usize) -> bool {
        let (left_weight, right_weight) = self.calculate_subtree_weights(node_idx);
        let total_weight = left_weight + right_weight + 1;

        if total_weight <= 1 {
            return false;
        }

        let min_weight = f64::min(left_weight as f64, right_weight as f64);
        let balance_ratio = min_weight / (total_weight as f64);

        balance_ratio < Self::ALPHA
    }

    fn update_subtree_filters(&mut self, node_idx: usize) {
        // Update filters in post-order traversal
        if let Some(left_idx) = self.nodes[node_idx].left {
            self.update_subtree_filters(left_idx);
        }
        if let Some(right_idx) = self.nodes[node_idx].right {
            self.update_subtree_filters(right_idx);
        }
        self.update_filter(node_idx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};

    const DEBUG: bool = true;

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

    #[test]
    fn test_rotations() {
        let mut tree = BloomTree::new();
        tree.insert(0, 1);
        tree.insert(1, 2);
        tree.insert(2, 3);
        // Force a rotation by making it right-heavy
        assert_eq!(tree.position(&2), Some(1));
        assert_eq!(tree.position(&3), Some(2));
    }
    #[test]
    fn test_complex_rotations() {
        let mut tree = BloomTree::new();

        // Test right-heavy case
        tree.insert(0, 1);
        tree.insert(1, 2);
        tree.insert(2, 3);
        assert_eq!(
            tree.position(&1),
            Some(0),
            "Position of 1 after right-heavy insertion"
        );
        assert_eq!(
            tree.position(&2),
            Some(1),
            "Position of 2 after right-heavy insertion"
        );
        assert_eq!(
            tree.position(&3),
            Some(2),
            "Position of 3 after right-heavy insertion"
        );

        // Test left-heavy case
        tree.insert(0, 0);
        assert_eq!(
            tree.position(&0),
            Some(0),
            "Position of 0 after left-heavy adjustment"
        );
        assert_eq!(
            tree.position(&1),
            Some(1),
            "Position of 1 after left-heavy adjustment"
        );
        assert_eq!(
            tree.position(&2),
            Some(2),
            "Position of 2 after left-heavy adjustment"
        );
        assert_eq!(
            tree.position(&3),
            Some(3),
            "Position of 3 after left-heavy adjustment"
        );
    }
    #[test]
    fn test_rotations_with_root_updates() {
        let mut tree = BloomTree::new();

        // Test 1: Right rotation case
        tree.insert(0, 1); // [1]
        tree.insert(1, 2); // [1,2]
        tree.insert(2, 3); // [1,2,3] -> triggers rotation

        // Verify structure after right-heavy rotation
        assert_eq!(tree.position(&1), Some(0), "1 should be at position 0");
        assert_eq!(tree.position(&2), Some(1), "2 should be at position 1");
        assert_eq!(tree.position(&3), Some(2), "3 should be at position 2");

        // Test 2: Left rotation case
        tree.insert(0, 0); // [0,1,2,3]
        assert_eq!(tree.position(&0), Some(0), "0 should be at position 0");
        assert_eq!(tree.position(&1), Some(1), "1 should be at position 1");
        assert_eq!(tree.position(&2), Some(2), "2 should be at position 2");
        assert_eq!(tree.position(&3), Some(3), "3 should be at position 3");

        // Validate internal structure
        assert!(
            validate_tree(&tree).is_ok(),
            "Tree should maintain valid structure"
        );
    }

    #[test]
    fn test_minimal_rotation_case() {
        let mut tree = BloomTree::new();

        // Step 1: Create initial tree [1,2]
        tree.insert(0, 1); // [1]
        tree.insert(1, 2); // [1,2]

        // Verify initial structure
        assert_eq!(tree.position(&1), Some(0), "1 should be at position 0");
        assert_eq!(tree.position(&2), Some(1), "2 should be at position 1");

        // Step 2: This insert triggers the problematic rebalancing
        tree.insert(0, 0); // Should become [0,1,2]

        // Verify final positions
        assert_eq!(tree.position(&0), Some(0), "0 should be at position 0");
        assert_eq!(tree.position(&1), Some(1), "1 should be at position 1");
        assert_eq!(tree.position(&2), Some(2), "2 should be at position 2");
    }
}
