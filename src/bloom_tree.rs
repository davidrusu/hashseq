use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// A space-efficient probabilistic data structure for testing set membership
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
            let i_h = self.hash(item, i);
            self.bits[i_h] = true;
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
    left: Option<Box<Node<T>>>,
    right: Option<Box<Node<T>>>,
    height: usize,
}

/// Tree structure augmented with Bloom filters for efficient position queries
#[derive(Debug, Clone)]
pub struct BloomTree<T> {
    root: Option<Box<Node<T>>>,
    size: usize,
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
    fn update_height(&mut self) {
        self.height = 1 + std::cmp::max(
            self.left.as_ref().map_or(0, |n| n.height),
            self.right.as_ref().map_or(0, |n| n.height),
        );
    }

    fn update_filter(&mut self) {
        // First collect all elements in the subtree in-order
        let mut elements = Vec::new();
        if let Some(left) = &self.left {
            elements.extend(left.collect_all_elements());
        }
        elements.push(self.element.clone());
        if let Some(right) = &self.right {
            elements.extend(right.collect_all_elements());
        }

        // Create new filter with all elements
        self.filter = BloomFilter::new(self.filter.size, 4);
        for element in &elements {
            self.filter.insert(element);
        }
    }

    fn collect_all_elements(&self) -> Vec<T> {
        let mut elements = Vec::new();
        if let Some(left) = &self.left {
            elements.extend(left.collect_all_elements());
        }
        elements.push(self.element.clone());
        if let Some(right) = &self.right {
            elements.extend(right.collect_all_elements());
        }
        elements
    }

    fn position(&self, element: &T) -> Option<usize> {
        // Early exit if element definitely not in subtree
        if !self.filter.might_contain(element) {
            return None;
        }

        // First check left subtree
        if let Some(left) = &self.left {
            if left.filter.might_contain(element) {
                if let Some(pos) = left.position(element) {
                    return Some(pos);
                }
            }
        }

        // Then check current node
        if &self.element == element {
            return Some(self.left_size);
        }

        // Finally check right subtree
        if let Some(right) = &self.right {
            if right.filter.might_contain(element) {
                return right.position(element).map(|pos| self.left_size + 1 + pos);
            }
        }

        None
    }
}

impl<T: Hash + Clone + Eq + std::fmt::Debug> BloomTree<T> {
    #[inline]
    pub fn new() -> Self {
        Self {
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

        match self.root.take() {
            Some(mut root) => {
                let filter_size = 256 * (1 << (root.height / 2));
                self.insert_at(&mut root, position, element, filter_size);
                self.root = Some(root);
            }
            None => {
                self.root = Some(Box::new(Node::new(element, 256)));
            }
        }
        self.size += 1;
    }

    fn insert_at(&self, node: &mut Box<Node<T>>, position: usize, element: T, filter_size: usize) {
        if position <= node.left_size {
            // Insert into left subtree
            match node.left.take() {
                Some(mut left) => {
                    self.insert_at(&mut left, position, element, filter_size);
                    node.left = Some(left);
                }
                None => {
                    node.left = Some(Box::new(Node::new(element, filter_size)));
                }
            }
            node.left_size += 1;
        } else {
            // Insert into right subtree
            match node.right.take() {
                Some(mut right) => {
                    self.insert_at(
                        &mut right,
                        position - node.left_size - 1,
                        element,
                        filter_size,
                    );
                    node.right = Some(right);
                }
                None => {
                    node.right = Some(Box::new(Node::new(element, filter_size)));
                }
            }
        }

        // Update height first
        node.update_height();

        // Then update filter with all elements in the subtree
        node.update_filter();
    }

    #[inline]
    pub fn position(&self, element: &T) -> Option<usize> {
        self.root.as_ref().and_then(|root| root.position(element))
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
            .tests(100000)
            .max_tests(200000)
            .quickcheck(property as fn(Vec<Action>) -> TestResult);
    }
}
