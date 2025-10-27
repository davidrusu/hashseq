use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use associative_positional_list::AssociativePositionalList;

use crate::topo_sort::{Topo, TopoIter};
use crate::{HashNode, Id, Op, Run};

/// Location information for where a node ID can be found
#[derive(Debug, Clone)]
pub enum NodeLocation {
    /// Node is part of a run at the given position
    InRun { run_id: Id, position: usize },
    /// Node is a standalone HashNode
    Individual(Id),
}

#[derive(Debug, Default, Clone)]
pub struct HashSeq {
    pub topo: Topo,

    // Hybrid storage: runs for sequential elements, individual nodes for complex operations
    pub runs: BTreeMap<Id, Run>,
    pub individual_nodes: BTreeMap<Id, HashNode>,

    // ID resolution index for O(1) lookup of any node
    pub id_to_location: HashMap<Id, NodeLocation>,

    pub removed_inserts: HashSet<Id>,
    pub(crate) roots: BTreeSet<Id>,
    pub(crate) orphaned: HashSet<HashNode>,
    index: AssociativePositionalList<Id>,
}

impl PartialEq for HashSeq {
    fn eq(&self, other: &Self) -> bool {
        // For equality, we compare the semantic content, not the storage organization
        // Two HashSeqs are equal if they have:
        // 1. The same sequence content (visible characters)
        // 2. The same removed_inserts
        // 3. The same orphaned nodes
        // 4. The same topological structure

        // Compare the visible content
        let self_content: Vec<char> = self.iter().collect();
        let other_content: Vec<char> = other.iter().collect();

        if self_content != other_content {
            return false;
        }

        // Compare removed inserts and orphaned
        if self.removed_inserts != other.removed_inserts || self.orphaned != other.orphaned {
            return false;
        }

        // Compare topological structure by checking that both have the same nodes
        // and the same ordering relationships
        let self_ids: BTreeSet<Id> = self.iter_ids().collect();
        let other_ids: BTreeSet<Id> = other.iter_ids().collect();

        if self_ids != other_ids {
            return false;
        }

        // Check that the topological ordering is the same
        // by comparing the sequence of IDs
        let self_id_seq: Vec<Id> = self.iter_ids().collect();
        let other_id_seq: Vec<Id> = other.iter_ids().collect();

        self_id_seq == other_id_seq
    }
}

impl Eq for HashSeq {}

impl HashSeq {
    /// Check if a node ID exists (either in runs or individual nodes)
    pub fn contains_node(&self, id: &Id) -> bool {
        self.id_to_location.contains_key(id)
    }

    /// Get the character value for a given node ID
    pub fn get_node_char(&self, id: &Id) -> Option<char> {
        match self.id_to_location.get(id)? {
            NodeLocation::InRun { run_id, position } => {
                let run = self.runs.get(run_id)?;
                run.run.chars().nth(*position)
            }
            NodeLocation::Individual(node_id) => {
                let node = self.individual_nodes.get(node_id)?;
                match &node.op {
                    Op::InsertRoot(c) | Op::InsertAfter(_, c) | Op::InsertBefore(_, c) => Some(*c),
                    Op::Remove(_) => None,
                }
            }
        }
    }

    /// Add a node to the storage
    /// All InsertAfter operations are automatically promoted to runs
    fn add_individual_node(&mut self, node: HashNode) {
        let id = node.id();

        // Auto-promote all InsertAfter operations to runs
        if let Op::InsertAfter(parent_id, ch) = &node.op {
            // Case 1: Parent is the last element of an existing run AND no extra deps - extend it
            if node.extra_dependencies.is_empty() {
                if let Some(NodeLocation::InRun { run_id, position }) =
                    self.id_to_location.get(parent_id)
                {
                    if let Some(run) = self.runs.get_mut(run_id) {
                        if *position == run.len() - 1 {
                            // Parent is the last element, we can extend
                            run.extend(*ch);
                            self.id_to_location.insert(
                                id,
                                NodeLocation::InRun {
                                    run_id: *run_id,
                                    position: run.len() - 1,
                                },
                            );
                            return;
                        }
                    }
                }
            }

            // Case 2: Create a new run (with or without extra dependencies)
            let new_run = Run::new(*parent_id, node.extra_dependencies.clone(), *ch);
            self.add_run(new_run);
            return;
        }

        // Only InsertRoot, InsertBefore, and Remove are stored as individual nodes
        self.individual_nodes.insert(id, node);
        self.id_to_location.insert(id, NodeLocation::Individual(id));
    }

    /// Add a run to storage and update index for all its elements
    fn add_run(&mut self, run: Run) {
        let run_id = run.run_id();
        let nodes = run.decompress();

        // Update index for all elements in the run
        for (position, node) in nodes.iter().enumerate() {
            self.id_to_location
                .insert(node.id(), NodeLocation::InRun { run_id, position });
        }

        self.runs.insert(run_id, run);
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub fn orphans(&self) -> &HashSet<HashNode> {
        &self.orphaned
    }

    fn neighbours(&mut self, idx: usize) -> (Option<Id>, Option<Id>) {
        let left = idx
            .checked_sub(1)
            .and_then(|prev_idx| self.index.get(prev_idx).copied());

        let right = self.index.get(idx).copied();

        (left, right)
    }

    pub fn insert(&mut self, idx: usize, value: char) {
        self.insert_batch(idx, [value]);
    }

    pub fn insert_batch(&mut self, idx: usize, batch: impl IntoIterator<Item = char>) {
        let chars: Vec<char> = batch.into_iter().collect();

        if chars.is_empty() {
            return;
        }

        // Single character inserts always use individual nodes
        if chars.len() == 1 {
            self.insert_single_char(idx, chars[0]);
            return;
        }

        let (left, right) = self.neighbours(idx);

        match (left, right) {
            (Some(left_id), _) => {
                // Check if we can extend an existing run
                // Conditions:
                // 1. left_id is the last entry of a run
                // 2. roots is a singleton set containing exactly left_id
                let can_extend_existing_run =
                    if let Some(NodeLocation::InRun { run_id, position }) =
                        self.id_to_location.get(&left_id).cloned()
                    {
                        self.runs
                            .get(&run_id)
                            .map(|run| {
                                position == run.len() - 1  // left_id is last entry of run
                                && self.roots.len() == 1  // roots is singleton
                                && self.roots.contains(&left_id) // roots contains exactly left_id
                            })
                            .unwrap_or(false)
                    } else {
                        false
                    };

                if can_extend_existing_run {
                    // Extend the existing run
                    let location = self.id_to_location.get(&left_id).cloned().unwrap();
                    if let NodeLocation::InRun {
                        run_id,
                        position: _,
                    } = location
                    {
                        let mut prev_id = left_id;

                        // Extend the run with all new characters
                        for (i, &ch) in chars.iter().enumerate() {
                            let new_id = self.generate_node_id_for_char(ch, &prev_id);

                            if let Some(run) = self.runs.get_mut(&run_id) {
                                run.extend(ch);

                                // Update the ID index
                                self.id_to_location.insert(
                                    new_id,
                                    NodeLocation::InRun {
                                        run_id,
                                        position: run.len() - 1,
                                    },
                                );
                            }

                            // Update topology
                            self.topo.add_after(prev_id, new_id);
                            self.update_position_index(new_id, idx + i);

                            // Update roots
                            self.roots.remove(&prev_id);
                            self.roots.insert(new_id);

                            prev_id = new_id;
                        }
                    }
                } else if self.can_safely_merge_after(&left_id) {
                    // Check if left is an individual node we can convert to a run
                    if let Some(left_node) = self.individual_nodes.get(&left_id).cloned() {
                        // INVARIANT: Runs must start with InsertAfter
                        if let Op::InsertAfter(left_anchor, left_char) = left_node.op {
                            // Check if roots is a singleton containing exactly left_id
                            if self.roots.len() == 1 && self.roots.contains(&left_id) {
                                // Convert the individual node to a run with all the new characters
                                let mut run = Run::new(
                                    left_anchor,
                                    left_node.extra_dependencies.clone(),
                                    left_char,
                                );

                                // Extend with all new characters
                                for &ch in &chars {
                                    run.extend(ch);
                                }

                                // Remove the individual node
                                self.individual_nodes.remove(&left_id);

                                // Generate IDs and update topology
                                let mut prev_id = left_id;
                                let nodes = run.decompress();

                                // Start from index 1 since node 0 is the left_id we already have
                                for (i, node) in nodes.iter().enumerate().skip(1) {
                                    let node_id = node.id();
                                    self.topo.add_after(prev_id, node_id);
                                    self.update_position_index(node_id, idx + i - 1);

                                    // Update roots
                                    self.roots.remove(&prev_id);
                                    self.roots.insert(node_id);

                                    prev_id = node_id;
                                }

                                // Add the run
                                self.add_run(run);
                                return;
                            }
                        }
                    }

                    // Fall through to creating a new run
                    // Create the run with insert_after = left_id
                    // No extra dependencies needed when inserting sequentially
                    let mut run = Run::new(left_id, BTreeSet::new(), chars[0]);

                    // Extend with remaining characters
                    for &ch in &chars[1..] {
                        run.extend(ch);
                    }

                    let nodes = run.decompress();
                    let mut prev_id = left_id;
                    for (i, node) in nodes.iter().enumerate() {
                        if i == 0 {
                            self.insert_after_anchor(node.id(), prev_id);
                        } else {
                            self.topo.add_after(prev_id, node.id());
                        }
                        // Update position index for each character
                        self.update_position_index(node.id(), idx + i);
                        prev_id = node.id();
                    }

                    // Add the run to our storage
                    self.add_run(run);

                    // Update roots

                    let dependencies = BTreeSet::from_iter(nodes[0].dependencies());
                    let superseded_roots = Vec::from_iter(
                        self.roots
                            .iter()
                            .filter(|r| dependencies.contains(*r))
                            .copied(),
                    );

                    for r in superseded_roots {
                        self.roots.remove(&r);
                    }

                    self.roots.insert(nodes[nodes.len() - 1].id());
                } else {
                    // Cannot safely merge - create a new run
                    // Use roots as extra dependencies to maintain causal consistency
                    let mut run = Run::new(left_id, self.roots.clone(), chars[0]);

                    // Extend with remaining characters
                    for &ch in &chars[1..] {
                        run.extend(ch);
                    }

                    let nodes = run.decompress();
                    let mut prev_id = left_id;
                    for (i, node) in nodes.iter().enumerate() {
                        if i == 0 {
                            self.insert_after_anchor(node.id(), prev_id);
                        } else {
                            self.topo.add_after(prev_id, node.id());
                        }
                        // Update position index for each character
                        self.update_position_index(node.id(), idx + i);
                        prev_id = node.id();
                    }

                    // Add the run to our storage
                    self.add_run(run);

                    // Update roots

                    let dependencies = BTreeSet::from_iter(nodes[0].dependencies());
                    let superseded_roots = Vec::from_iter(
                        self.roots
                            .iter()
                            .filter(|r| dependencies.contains(*r))
                            .copied(),
                    );

                    for r in superseded_roots {
                        self.roots.remove(&r);
                    }

                    self.roots.insert(nodes[nodes.len() - 1].id());
                }
            }
            (None, _) => {
                // No left neighbor - fall back to individual node insertions
                // This handles InsertRoot and InsertBefore cases
                for (i, &ch) in chars.iter().enumerate() {
                    self.insert_single_char(idx + i, ch);
                }
            }
        }
    }

    /// Insert a single character using individual node storage
    /// Attempts to merge with existing nodes/runs when safe to do so
    fn insert_single_char(&mut self, idx: usize, value: char) {
        let (left, right) = self.neighbours(idx);

        // Check if we can safely merge with an existing node when inserting after
        if let Some(left_id) = left {
            if self.can_safely_merge_after(&left_id) {
                // Check if the left node is part of a run we can extend
                if let Some(NodeLocation::InRun { run_id, position }) =
                    self.id_to_location.get(&left_id).cloned()
                {
                    // Extend the run if this character comes at the end
                    let can_extend = self
                        .runs
                        .get(&run_id)
                        .map(|run| position == run.len() - 1)
                        .unwrap_or(false);

                    if can_extend {
                        // Generate the new ID before any mutable borrows
                        let new_id = self.generate_node_id_for_char(value, &left_id);

                        // Now extend the run
                        if let Some(run) = self.runs.get_mut(&run_id) {
                            run.extend(value);

                            // Update the ID index
                            self.id_to_location.insert(
                                new_id,
                                NodeLocation::InRun {
                                    run_id,
                                    position: run.len() - 1,
                                },
                            );

                            // Update topology
                            self.topo.add_after(left_id, new_id);
                            self.update_position_index(new_id, idx);

                            // Update roots: remove left_id, add new_id
                            self.roots.remove(&left_id);
                            self.roots.insert(new_id);

                            return;
                        }
                    }
                } else if let Some(left_node) = self.individual_nodes.get(&left_id) {
                    // Convert individual node to a run with the new character
                    // INVARIANT: Runs must start with InsertAfter, so we can only convert
                    // if the left node is an InsertAfter operation
                    if let Op::InsertAfter(left_anchor, left_char) = left_node.op {
                        let new_id = self.generate_node_id_for_char(value, &left_id);

                        // Create a new run with both characters
                        // First element: InsertAfter(left_anchor, left_char)
                        // Second element: InsertAfter(left_id, value)

                        // Preserve the left node's extra_dependencies for the first element
                        let mut run =
                            Run::new(left_anchor, left_node.extra_dependencies.clone(), left_char);

                        // Extend with the second character
                        run.extend(value);

                        // Remove the individual node
                        self.individual_nodes.remove(&left_id);

                        // Add the run
                        self.add_run(run);

                        // Update topology for the new character
                        self.topo.add_after(left_id, new_id);
                        self.update_position_index(new_id, idx);

                        // Update roots: remove left_id, add new_id
                        self.roots.remove(&left_id);
                        self.roots.insert(new_id);

                        return;
                    }
                    // If left_node is InsertRoot or InsertBefore, we can't convert it to a run
                    // Fall through to the standard individual node insertion below
                }
            }
        }

        // Fallback to standard individual node insertion
        let op = match (left, right) {
            (Some(l), Some(r)) => {
                if self.topo.is_causally_before(&l, &r) {
                    Op::InsertBefore(r, value)
                } else {
                    Op::InsertAfter(l, value)
                }
            }
            (Some(l), None) => Op::InsertAfter(l, value),
            (None, Some(r)) => Op::InsertBefore(r, value),
            (None, None) => Op::InsertRoot(value),
        };

        let extra_dependencies =
            BTreeSet::from_iter(self.roots.difference(&op.dependencies()).cloned());

        let node = HashNode {
            extra_dependencies,
            op,
        };

        self.apply_with_known_position(node, idx);
    }

    /// Check if it's safe to merge a character after the given node
    /// Safe means there are no other nodes chaining off the same parent
    fn can_safely_merge_after(&self, node_id: &Id) -> bool {
        // Get all nodes that come after this node
        let after_nodes = self.topo.after(*node_id);

        // Safe to merge if there are no successors, meaning no other nodes
        // are chaining off this node
        after_nodes.is_empty()
    }

    /// Generate a consistent node ID for a character following another character  
    fn generate_node_id_for_char(&self, value: char, after_id: &Id) -> Id {
        use crate::hash_node::hash_op;

        let op = crate::Op::InsertAfter(*after_id, value);
        let extra_dependencies = BTreeSet::new(); // No extra dependencies for run extensions

        hash_op(&op, &extra_dependencies)
    }

    pub fn remove(&mut self, idx: usize) {
        self.remove_batch(idx, 1);
    }

    pub fn remove_batch(&mut self, idx: usize, amount: usize) {
        if amount == 0 {
            // Nothing to remove
            return;
        }

        let mut to_remove = BTreeSet::new();
        for pos in idx..(idx + amount) {
            if let Some(id) = self.index.get(pos) {
                to_remove.insert(*id);
            } else {
                break;
            }
        }
        let op = Op::Remove(to_remove);

        let extra_dependencies =
            BTreeSet::from_iter(self.roots.difference(&op.dependencies()).cloned());

        let node = HashNode {
            extra_dependencies,
            op,
        };

        self.apply(node);
    }

    fn any_missing_dependencies(&self, deps: &BTreeSet<Id>) -> bool {
        for dep in deps.iter() {
            if !self.contains_node(dep) {
                return true;
            }
        }

        false
    }

    fn insert_root(&mut self, root_id: Id) {
        let position = if let Some(next_root) = self
            .topo
            .roots()
            .range(root_id..)
            .find(|id| !self.removed_inserts.contains(*id))
        {
            // new root is inserted just before the next biggest root
            self.index.find(next_root).unwrap()
        } else {
            // otherwise if there is no bigger root, the new root is
            // inserted at end of list
            self.len()
        };
        self.insert_root_with_known_position(root_id, position);
    }

    fn insert_root_with_known_position(&mut self, id: Id, position: usize) {
        self.index.insert(position, id);
        self.topo.add_root(id);
    }

    fn insert_after_anchor(&mut self, id: Id, anchor: Id) {
        let position = if let Some(next_node) = self
            .topo
            .after(anchor)
            .range(id..)
            .find(|id| !self.removed_inserts.contains(*id))
        {
            // new node is inserted just before the other node after our anchor node that is
            // bigger than the new node
            self.index.find(next_node)
        } else {
            // otherwise the new node is inserted after our anchor node (unless it has been removed)
            self.index.find(&anchor).map(|p| p + 1)
        };

        self.topo.add_after(anchor, id);

        let position = position.unwrap_or_else(|| {
            // fall back to iterating over the entire sequence if the anchor node has been removed
            // or if next_node is not yet in the index (can happen during merge)
            let (position, _) = self.iter_ids().enumerate().find(|(_, n)| n == &id).unwrap();
            position
        });
        self.update_position_index(id, position);
    }

    fn update_position_index(&mut self, id: Id, position: usize) {
        self.index.insert(position, id);
    }

    fn remove_nodes(&mut self, nodes: &BTreeSet<Id>) {
        // TODO: if self.nodes.get(node) is not an insert op, then drop this remove.
        //       Are you sure? looks like we would mark this op as an orphan if we hadn't
        //       seen a node yet.
        for n in nodes {
            if let Some(p) = self.index.find(n) {
                self.index.remove(p);
            }
        }
        self.removed_inserts.extend(nodes);
    }

    fn insert_before_anchor(&mut self, id: Id, anchor: Id) {
        let position = if let Some(next_node) = self
            .topo
            .before(anchor)
            .range(id..)
            .find(|id| !self.removed_inserts.contains(*id))
        {
            // new node is inserted just before the other node before our anchor node that is
            // bigger than the new node
            Some(self.index.find(next_node).unwrap())
        } else {
            // otherwise the new node is inserted before our anchor node
            self.index.find(&anchor)
        };

        self.topo.add_before(anchor, id);

        let position = position.unwrap_or_else(|| {
            // fall back to iterating over the entire sequence if the anchor node has been removed
            let (position, _) = self.iter_ids().enumerate().find(|(_, n)| n == &id).unwrap();
            position
        });
        self.update_position_index(id, position);
    }

    pub fn apply_with_known_position(&mut self, node: HashNode, position: usize) {
        let id = node.id();

        if self.contains_node(&id) {
            return; // Already processed this node
        }

        let dependencies = BTreeSet::from_iter(node.dependencies());
        if self.any_missing_dependencies(&dependencies) {
            self.orphaned.insert(node);
            return;
        }

        match &node.op {
            Op::InsertRoot(_) => self.insert_root_with_known_position(id, position),
            Op::InsertAfter(anchor, _) => {
                self.topo.add_after(*anchor, id);
                self.update_position_index(id, position)
            }
            Op::InsertBefore(anchor, _) => {
                self.topo.add_before(*anchor, id);
                self.update_position_index(id, position)
            }
            Op::Remove(nodes) => self.remove_nodes(nodes),
        }

        self.add_individual_node(node);

        let superseded_roots = Vec::from_iter(
            self.roots
                .iter()
                .filter(|r| dependencies.contains(*r))
                .copied(),
        );

        for r in superseded_roots {
            self.roots.remove(&r);
        }

        self.roots.insert(id);

        let orphans = std::mem::take(&mut self.orphaned);

        for orphan in orphans {
            self.apply(orphan);
        }
    }

    pub fn apply(&mut self, node: HashNode) {
        let id = node.id();

        if self.contains_node(&id) {
            return; // Already processed this node
        }

        // Only check operation dependencies, not extra_dependencies
        // Extra dependencies are for topological ordering but shouldn't block insertion
        let op_dependencies = node.op.dependencies();
        if self.any_missing_dependencies(&op_dependencies) {
            self.orphaned.insert(node);
            return;
        }

        match &node.op {
            Op::InsertRoot(_) => self.insert_root(id),
            Op::InsertAfter(anchor, _) => self.insert_after_anchor(id, *anchor),
            Op::InsertBefore(anchor, _) => self.insert_before_anchor(id, *anchor),
            Op::Remove(nodes) => self.remove_nodes(nodes),
        }

        self.add_individual_node(node.clone());

        // For superseding roots, we need to consider all dependencies (op + extra)
        let dependencies = BTreeSet::from_iter(node.dependencies());
        let superseded_roots = Vec::from_iter(
            self.roots
                .iter()
                .filter(|r| dependencies.contains(*r))
                .copied(),
        );

        for r in superseded_roots {
            self.roots.remove(&r);
        }

        self.roots.insert(id);

        let orphans = std::mem::take(&mut self.orphaned);

        for orphan in orphans {
            self.apply(orphan);
        }
    }

    pub fn merge(&mut self, other: Self) {
        // Simple merge: decompress all nodes from other and apply them
        // The apply function will rebuild runs when possible

        // Collect all nodes from individual_nodes
        for (_id, node) in other.individual_nodes {
            self.apply(node);
        }

        // Collect all nodes from runs by decompressing them
        for (_run_id, run) in other.runs {
            let nodes = run.decompress();
            for node in nodes {
                self.apply(node);
            }
        }

        // Apply all orphaned nodes
        for orphan in other.orphaned {
            self.apply(orphan);
        }
    }

    pub fn iter_ids(&self) -> TopoIter<'_, '_> {
        self.topo.iter(&self.removed_inserts)
    }

    pub fn iter(&self) -> impl Iterator<Item = char> + '_ {
        self.iter_ids().filter_map(|id| self.get_node_char(&id))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use quickcheck_macros::quickcheck;

    #[test]
    fn test_insert_at_end() {
        let mut seq = HashSeq::default();
        seq.insert(0, 'a');
        seq.insert(1, 'b');
        seq.insert(2, 'c');

        assert_eq!(seq.iter().collect::<String>(), "abc");
    }

    #[test]
    fn test_insert_batch() {
        let mut seq = HashSeq::default();
        seq.insert_batch(0, "abc".chars());
        assert_eq!(&seq.iter().collect::<String>(), "abc");
    }

    #[test]
    fn test_insert_batch_vs_single_inserts() {
        // Test that inserting one character at a time produces the same result
        // as using insert_batch

        let test_string = "hello world";

        // Insert one character at a time
        let mut seq_single = HashSeq::default();
        for (i, ch) in test_string.chars().enumerate() {
            seq_single.insert(i, ch);
        }

        // Insert as a batch
        let mut seq_batch = HashSeq::default();
        seq_batch.insert_batch(0, test_string.chars());

        // Verify they produce the same output
        let result_single: String = seq_single.iter().collect();
        let result_batch: String = seq_batch.iter().collect();

        assert_eq!(result_single, test_string);
        assert_eq!(result_batch, test_string);
        assert_eq!(result_single, result_batch);

        // Test inserting in the middle
        let mut seq_single_mid = HashSeq::default();
        seq_single_mid.insert(0, 'a');
        seq_single_mid.insert(1, 'z');
        seq_single_mid.insert(1, 'b');
        seq_single_mid.insert(2, 'c');
        seq_single_mid.insert(3, 'd');

        let mut seq_batch_mid = HashSeq::default();
        seq_batch_mid.insert(0, 'a');
        seq_batch_mid.insert(1, 'z');
        seq_batch_mid.insert_batch(1, "bcd".chars());

        assert_eq!(seq_single_mid.iter().collect::<String>(), "abcdz");
        assert_eq!(seq_batch_mid.iter().collect::<String>(), "abcdz");
    }

    #[test]
    fn test_split_batch_inserts() {
        // Test that insert_batch("abcd") produces the same internal structure as
        // insert_batch("ab") followed by insert_batch("cd")
        // This verifies that runs are collapsed identically

        let test_string = "abcd";

        // Insert entire string as one batch
        let mut seq_single_batch = HashSeq::default();
        seq_single_batch.insert_batch(0, test_string.chars());

        // Insert as two separate batches
        let mut seq_split_batch = HashSeq::default();
        seq_split_batch.insert_batch(0, "ab".chars());
        seq_split_batch.insert_batch(2, "cd".chars());

        // Verify internal structure is identical
        assert_eq!(
            seq_single_batch.runs.len(),
            seq_split_batch.runs.len(),
            "Number of runs should be identical"
        );
        assert_eq!(
            seq_single_batch.individual_nodes.len(),
            seq_split_batch.individual_nodes.len(),
            "Number of individual nodes should be identical"
        );
        assert_eq!(
            seq_single_batch.runs, seq_split_batch.runs,
            "Runs should be identical"
        );
        assert_eq!(
            seq_single_batch.individual_nodes, seq_split_batch.individual_nodes,
            "Individual nodes should be identical"
        );
        assert_eq!(
            seq_single_batch.roots, seq_split_batch.roots,
            "Roots should be identical"
        );

        // Verify output is also the same
        assert_eq!(seq_single_batch.iter().collect::<String>(), test_string);
        assert_eq!(seq_split_batch.iter().collect::<String>(), test_string);

        // Test with longer strings and different split points
        let long_string = "hello world";

        let mut seq1 = HashSeq::default();
        seq1.insert_batch(0, long_string.chars());

        let mut seq2 = HashSeq::default();
        seq2.insert_batch(0, "hello ".chars());
        seq2.insert_batch(6, "world".chars());

        // Verify internal structure matches
        assert_eq!(
            seq1.runs, seq2.runs,
            "Runs should be identical for split 'hello world'"
        );
        assert_eq!(seq1.individual_nodes, seq2.individual_nodes);
        assert_eq!(seq1.iter().collect::<String>(), long_string);
        assert_eq!(seq2.iter().collect::<String>(), long_string);

        // Test with three batches
        let mut seq3 = HashSeq::default();
        seq3.insert_batch(0, "hel".chars());
        seq3.insert_batch(3, "lo ".chars());
        seq3.insert_batch(6, "world".chars());

        assert_eq!(
            seq1.runs, seq3.runs,
            "Runs should be identical for three-way split"
        );
        assert_eq!(seq1.individual_nodes, seq3.individual_nodes);
        assert_eq!(seq3.iter().collect::<String>(), long_string);
    }

    #[test]
    fn test_batch_split_null_chars() {
        // Regression test for bug found by prop_batch_split_equivalence
        // Issue: inserting "\0\0\0" as single batch vs split ["\0", "\0\0"]
        // produced different first_extra_deps in the run
        let text = "\0\0\0";

        // seq1: insert entire string as one batch
        let mut seq1 = HashSeq::default();
        seq1.insert_batch(0, text.chars());

        // seq2: split into "\0" at position 0, then "\0\0" at position 1
        let mut seq2 = HashSeq::default();
        seq2.insert_batch(0, "\0".chars());
        seq2.insert_batch(1, "\0\0".chars());

        // Verify internal structures are identical
        assert_eq!(seq1.runs, seq2.runs, "Runs should be identical");
        assert_eq!(
            seq1.individual_nodes, seq2.individual_nodes,
            "Individual nodes should be identical"
        );
        assert_eq!(seq1.roots, seq2.roots, "Roots should be identical");
    }

    #[test]
    fn test_no_individual_insert_after_ops() {
        // Test that all InsertAfter operations are stored in runs, never as individual nodes
        let mut seq = HashSeq::default();
        seq.insert(0, 'a');
        seq.insert(1, 'b');
        seq.insert(2, 'c');
        seq.insert(3, 'd');

        // Verify no individual nodes contain InsertAfter operations
        for (_id, node) in seq.individual_nodes.iter() {
            assert!(
                !matches!(node.op, Op::InsertAfter(_, _)),
                "Found InsertAfter in individual_nodes: {:?}",
                node
            );
        }

        // Verify we have runs and roots
        assert!(!seq.runs.is_empty(), "Should have runs for InsertAfter ops");
        assert_eq!(seq.individual_nodes.len(), 1, "Should only have root node");
    }

    #[test]
    fn test_merge_batch_preserves_structure() {
        // Test that merging a HashSeq with "abcd" into an empty HashSeq
        // results in the same structure: root node 'a' + run "bcd"
        let mut seq_with_abcd = HashSeq::default();
        seq_with_abcd.insert_batch(0, "abcd".chars());

        let mut empty_seq = HashSeq::default();
        empty_seq.merge(seq_with_abcd.clone());

        // Verify internal structures are identical
        assert_eq!(
            seq_with_abcd.runs, empty_seq.runs,
            "Runs should be identical after merge"
        );
        assert_eq!(
            seq_with_abcd.individual_nodes, empty_seq.individual_nodes,
            "Individual nodes should be identical after merge"
        );
        assert_eq!(
            seq_with_abcd.roots, empty_seq.roots,
            "Roots should be identical after merge"
        );

        // Verify the structure is as expected:
        // - Should have 1 root node for 'a'
        assert_eq!(
            seq_with_abcd.individual_nodes.len(),
            1,
            "Should have 1 individual node (root 'a')"
        );

        // - Should have 1 run containing "bcd"
        assert_eq!(seq_with_abcd.runs.len(), 1, "Should have 1 run");
        let run = seq_with_abcd.runs.values().next().unwrap();
        assert_eq!(run.run, "bcd", "Run should contain 'bcd'");

        // Verify the text is correct
        assert_eq!(
            seq_with_abcd.iter().collect::<String>(),
            "abcd",
            "Text should be 'abcd'"
        );
        assert_eq!(
            empty_seq.iter().collect::<String>(),
            "abcd",
            "Merged seq should also have 'abcd'"
        );
    }

    #[quickcheck]
    fn prop_batch_split_equivalence(text: String, split_points: Vec<usize>) -> bool {
        // Property: inserting a string as a single batch produces the same internal
        // structure as splitting it into multiple batches and inserting sequentially

        if text.is_empty() {
            return true;
        }

        // Convert text to character count for proper indexing
        let chars: Vec<char> = text.chars().collect();
        let char_len = chars.len();

        // Normalize split points to valid positions within character boundaries
        let mut splits: Vec<usize> = split_points
            .iter()
            .filter_map(|&p| {
                if char_len > 0 {
                    Some((p % char_len.max(1)).min(char_len))
                } else {
                    None
                }
            })
            .collect();

        // Sort and deduplicate
        splits.sort_unstable();
        splits.dedup();

        // Ensure boundaries are included
        if splits.is_empty() || splits[0] != 0 {
            splits.insert(0, 0);
        }
        if splits[splits.len() - 1] != char_len {
            splits.push(char_len);
        }

        // Remove consecutive duplicates that might have been created
        splits.dedup();

        // If we only have start and end (no actual splits), treat as single batch
        if splits.len() <= 2 {
            return true; // This is a trivial case
        }

        // Create seq1: insert entire string as one batch
        let mut seq1 = HashSeq::default();
        seq1.insert_batch(0, text.chars());

        // Create seq2: insert string split into batches sequentially
        let mut seq2 = HashSeq::default();
        let mut current_pos = 0;

        for i in 0..splits.len() - 1 {
            let start = splits[i];
            let end = splits[i + 1];

            if start < end {
                let substring: String = chars[start..end].iter().collect();
                seq2.insert_batch(current_pos, substring.chars());
                current_pos += end - start;
            }
        }

        // Verify internal structures are identical
        seq1.runs == seq2.runs
            && seq1.individual_nodes == seq2.individual_nodes
            && seq1.roots == seq2.roots
    }

    #[test]
    fn test_run_creation() {
        let mut seq = HashSeq::default();

        // Single characters should create individual nodes
        seq.insert(0, 'x');
        assert_eq!(seq.runs.len(), 0);
        assert_eq!(seq.individual_nodes.len(), 1);

        // Multi-character batch should create a run
        seq.insert_batch(1, "abc".chars());
        assert_eq!(seq.runs.len(), 1);
        assert_eq!(seq.individual_nodes.len(), 1);

        // Verify the run contains the right data
        let run = seq.runs.values().next().unwrap();
        assert_eq!(run.run, "abc");
        assert_eq!(run.len(), 3);

        // Verify the final string
        assert_eq!(&seq.iter().collect::<String>(), "xabc");
    }

    #[test]
    fn test_run_memory_efficiency() {
        let mut seq = HashSeq::default();

        // Create a long sequence using batch insert
        let long_string = "The quick brown fox jumps over the lazy dog. ".repeat(10);
        seq.insert_batch(0, long_string.chars());

        // Should create one run
        assert_eq!(seq.runs.len(), 1);
        assert_eq!(seq.individual_nodes.len(), 1);

        let run = seq.runs.values().next().unwrap();
        assert_eq!(run.len(), long_string.len() - 1); // First char becomes a root (not included in run)

        // Verify content
        assert_eq!(seq.iter().collect::<String>(), long_string);
    }

    #[test]
    fn test_concurrent_inserts() {
        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        seq_a.insert_batch(0, "we wrote".chars());
        seq_b.insert_batch(0, "this together ".chars());

        seq_a.merge(seq_b);

        assert_eq!(&seq_a.iter().collect::<String>(), "this together we wrote");
    }

    #[test]
    fn test_common_prefix_is_deduplicated() {
        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        seq_a.insert_batch(0, "hello my name is david".chars());
        seq_b.insert_batch(0, "hello my name is zameena".chars());

        seq_a.merge(seq_b);

        let merged = seq_a.iter().collect::<String>();
        assert!(
            &merged == "hello my name is zameenadavid"
                || &merged == "hello my name is davidzameena"
        );
    }

    #[test]
    fn test_common_prefix_is_deduplicated_simple() {
        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        seq_a.insert_batch(0, "aba".chars());
        assert_eq!(&seq_a.iter().collect::<String>(), "aba");

        seq_b.insert_batch(0, "aza".chars());
        assert_eq!(&seq_b.iter().collect::<String>(), "aza");

        seq_a.merge(seq_b);
        assert_eq!(&seq_a.iter().collect::<String>(), "azaba");
    }

    #[test]
    fn test_insert_different_chars_at_front() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(0, 'b');

        assert_eq!(&String::from_iter(seq.iter()), "ba");
    }

    #[test]
    fn test_insert_same_char_at_front() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(0, 'a');

        assert_eq!(&String::from_iter(seq.iter()), "aa");
    }

    #[test]
    fn test_insert_delete_then_reinsert() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.remove(0);
        seq.insert(0, 'a');

        assert_eq!(&String::from_iter(seq.iter()), "a");
    }

    #[test]
    fn test_add_twice_then_remove_both() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(0, 'a');
        seq.remove(0);
        seq.remove(0);

        assert_eq!(&String::from_iter(seq.iter()), "");
        assert_eq!(seq.len(), 0);
    }

    #[test]
    fn test_inserts_refering_to_out_of_order_inserts_are_cached() {
        let mut seq = HashSeq::default();

        let insert = HashNode {
            op: Op::InsertRoot('b'),
            extra_dependencies: BTreeSet::default(),
        };

        seq.apply(HashNode {
            op: Op::InsertAfter(insert.id(), 'a'),
            extra_dependencies: BTreeSet::default(),
        });

        assert_eq!(seq.orphans().len(), 1);
        assert_eq!(seq.len(), 0);

        seq.apply(HashNode {
            op: Op::InsertBefore(insert.id(), 'a'),
            extra_dependencies: BTreeSet::default(),
        });

        assert_eq!(seq.orphans().len(), 2);
        assert_eq!(seq.len(), 0);

        seq.apply(insert);

        assert_eq!(seq.orphans().len(), 0);
        assert_eq!(seq.len(), 3);

        assert_eq!(&String::from_iter(seq.iter()), "aba");
    }

    #[test]
    fn test_out_of_order_remove_is_cached() {
        let mut seq = HashSeq::default();

        // Attempting to remove insert that doesn't yet exist.
        // We expect the remove operation to be cached and applied
        // once we see the insert.

        let insert = HashNode {
            op: Op::InsertRoot('a'),
            extra_dependencies: BTreeSet::new(),
        };

        seq.apply(HashNode {
            op: Op::Remove(BTreeSet::from_iter([insert.id()])),
            extra_dependencies: BTreeSet::new(),
        });

        assert_eq!(seq.orphans().len(), 1);
        seq.apply(insert);
        assert_eq!(seq.orphans().len(), 0);
        assert_eq!(&String::from_iter(seq.iter()), "");
    }

    #[test]
    fn test_prop_associative_qc1() {
        // ([(true, 0, '\u{0}'), (true, 0, '\u{0}')], [], [(true, 0, '\u{3}')])

        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        seq_a.insert(0, 'a');
        seq_a.insert(0, 'a');

        seq_b.insert(0, 'b');

        let mut ab = seq_a.clone();
        ab.merge(seq_b.clone());

        let mut ba = seq_b.clone();
        ba.merge(seq_a.clone());

        assert_eq!(ab, ba);
    }

    #[test]
    fn test_prop_commutative_qc1() {
        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        seq_a.insert(0, 'a');
        seq_a.remove(0);
        assert_eq!(String::from_iter(seq_a.iter()), "");

        seq_b.insert(0, 'a');
        seq_b.insert(0, 'b');
        assert_eq!(String::from_iter(seq_b.iter()), "ba");

        // merge(a, b) == merge(b, a)

        let mut merge_a_b = seq_a.clone();
        merge_a_b.merge(seq_b.clone());
        let mut merge_b_a = seq_b.clone();
        merge_b_a.merge(seq_a.clone());

        assert_eq!(merge_a_b, merge_b_a);
    }

    #[test]
    fn test_prop_commutative_insert_remove() {
        // Failing case: a = [], b = [(true, 0, '\0'), (false, 0, '\0')]
        let seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        // seq_a is empty

        // seq_b: insert then remove
        seq_b.insert(0, '\0');
        seq_b.remove(0);

        // merge(a, b) == merge(b, a)
        let mut merge_a_b = seq_a.clone();
        merge_a_b.merge(seq_b.clone());
        let mut merge_b_a = seq_b.clone();
        merge_b_a.merge(seq_a.clone());

        assert_eq!(merge_a_b, merge_b_a);
    }

    #[test]
    fn test_prop_commutative_two_inserts() {
        // Failing case: a = [(true, 0, '\0'), (true, 1, '\0')], b = []
        let mut seq_a = HashSeq::default();
        let seq_b = HashSeq::default();

        // seq_a: two inserts
        seq_a.insert(0, '\0');
        seq_a.insert(1, '\0');

        // seq_b is empty

        // merge(a, b) == merge(b, a)
        let mut merge_a_b = seq_a.clone();
        merge_a_b.merge(seq_b.clone());

        let mut merge_b_a = seq_b.clone();
        merge_b_a.merge(seq_a.clone());

        assert_eq!(merge_a_b, merge_b_a);
    }

    #[test]
    fn test_prop_commutative_four_inserts() {
        // Failing case: a = [], b = [(true, 0, '\0'), (true, 1, '\0'), (true, 1, '\0'), (true, 2, '\0')]
        let seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        // seq_b: four inserts
        seq_b.insert(0, '\0');
        seq_b.insert(1, '\0');
        seq_b.insert(1, '\0');
        seq_b.insert(2, '\0');

        // seq_a is empty

        // merge(a, b) == merge(b, a)
        let mut merge_a_b = seq_a.clone();
        merge_a_b.merge(seq_b.clone());

        let mut merge_b_a = seq_b.clone();
        merge_b_a.merge(seq_a.clone());

        assert_eq!(merge_a_b, merge_b_a);
    }

    #[test]
    fn test_prop_commutative_insert_insert_remove() {
        // Failing case: a = [], b = [(true, 0, '\0'), (true, 1, '\0'), (false, 0, '\0')]
        let seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        // seq_b: insert at 0, insert at 1, remove at 0
        seq_b.insert(0, '\0');
        seq_b.insert(1, '\0');
        seq_b.remove(0);

        // seq_a is empty

        // merge(a, b) == merge(b, a)
        let mut merge_a_b = seq_a.clone();
        merge_a_b.merge(seq_b.clone());

        let mut merge_b_a = seq_b.clone();
        merge_b_a.merge(seq_a.clone());

        assert_eq!(merge_a_b, merge_b_a);
    }

    #[quickcheck]
    fn prop_reflexive(ops: Vec<(bool, u8, char)>) {
        let mut seq = HashSeq::default();

        for (insert_or_remove, idx, elem) in ops {
            let idx = idx as usize;
            match insert_or_remove {
                true => {
                    // insert
                    seq.insert(idx.min(seq.len()), elem);
                }
                false => {
                    // remove
                    if !seq.is_empty() {
                        seq.remove(idx.min(seq.len() - 1));
                    }
                }
            }
        }

        // merge(a, a) == a

        let mut merge_self = seq.clone();
        merge_self.merge(seq.clone());

        assert_eq!(merge_self, seq);
    }

    #[quickcheck]
    fn prop_commutative(a: Vec<(bool, u8, char)>, b: Vec<(bool, u8, char)>) {
        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        for (insert_or_remove, idx, elem) in a {
            let idx = idx as usize;
            match insert_or_remove {
                true => {
                    // insert
                    seq_a.insert(idx.min(seq_a.len()), elem);
                }
                false => {
                    // remove
                    if !seq_a.is_empty() {
                        seq_a.remove(idx.min(seq_a.len() - 1));
                    }
                }
            }
        }

        for (insert_or_remove, idx, elem) in b {
            let idx = idx as usize;
            match insert_or_remove {
                true => {
                    // insert
                    seq_b.insert(idx.min(seq_b.len()), elem);
                }
                false => {
                    // remove
                    if !seq_b.is_empty() {
                        seq_b.remove(idx.min(seq_b.len() - 1));
                    }
                }
            }
        }

        // merge(a, b) == merge(b, a)

        let mut merge_a_b = seq_a.clone();
        merge_a_b.merge(seq_b.clone());

        let mut merge_b_a = seq_b.clone();
        merge_b_a.merge(seq_a.clone());

        assert_eq!(merge_a_b, merge_b_a);
    }

    #[quickcheck]
    fn prop_associative(
        a: Vec<(bool, u8, char)>,
        b: Vec<(bool, u8, char)>,
        c: Vec<(bool, u8, char)>,
    ) {
        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();
        let mut seq_c = HashSeq::default();

        for (insert_or_remove, idx, elem) in a {
            let idx = idx as usize;
            match insert_or_remove {
                true => {
                    // insert
                    seq_a.insert(idx.min(seq_a.len()), elem);
                }
                false => {
                    // remove
                    if !seq_a.is_empty() {
                        seq_a.remove(idx.min(seq_a.len() - 1));
                    }
                }
            }
        }

        for (insert_or_remove, idx, elem) in b {
            let idx = idx as usize;
            match insert_or_remove {
                true => {
                    // insert
                    seq_b.insert(idx.min(seq_b.len()), elem);
                }
                false => {
                    // remove
                    if !seq_b.is_empty() {
                        seq_b.remove(idx.min(seq_b.len() - 1));
                    }
                }
            }
        }

        for (insert_or_remove, idx, elem) in c {
            let idx = idx as usize;
            match insert_or_remove {
                true => {
                    // insert
                    seq_c.insert(idx.min(seq_c.len()), elem);
                }
                false => {
                    // remove
                    if !seq_c.is_empty() {
                        seq_c.remove(idx.min(seq_c.len() - 1));
                    }
                }
            }
        }

        // merge(merge(a, b), c) == merge(a, merge(b, c))

        let mut ab_then_c = seq_a.clone();
        ab_then_c.merge(seq_b.clone());
        ab_then_c.merge(seq_c.clone());

        let mut bc_then_a = seq_b.clone();
        bc_then_a.merge(seq_c.clone());
        bc_then_a.merge(seq_a.clone());

        assert_eq!(ab_then_c, bc_then_a);

        // TODO: once insert returns an Op, check that we are op associative as well.
    }

    #[test]
    fn test_prop_vec_model_qc1() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'c');
        seq.insert(0, 'b');
        seq.insert(1, 'a');

        assert_eq!(String::from_iter(seq.iter()), "bac");
    }

    #[test]
    fn test_prop_vec_model_qc2() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(0, 'b');
        seq.insert(1, 'c');
        seq.insert(2, 'd');

        assert_eq!(String::from_iter(seq.iter()), "bcda");
    }

    #[test]
    fn test_prop_vec_model_qc3() {
        let mut model = Vec::new();
        let mut seq = HashSeq::default();

        for (insert_or_remove, idx, elem) in [
            (true, 0, 'c'),
            (true, 1, 'c'),
            (true, 2, 'c'),
            (false, 1, 'c'),
            (true, 1, 'b'),
        ] {
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

    #[test]
    fn test_prop_vec_model_qc4() {
        let mut seq = HashSeq::default();

        for (idx, elem) in [(0, 'a'), (1, 'a'), (2, 'a'), (3, 'a'), (3, 'a'), (3, 'd')] {
            seq.insert(idx, elem);
        }

        assert_eq!(seq.iter().collect::<String>(), "aaadaa");
    }

    #[test]
    fn test_prop_vec_model_qc5() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(0, 'a');
        seq.remove(0);
        seq.insert(1, 'b');

        assert_eq!(String::from_iter(seq.iter()), "ab");
    }

    #[test]
    fn test_prop_vec_model_qc6() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(1, 'b');
        seq.insert(0, 'c');
        seq.remove(2);

        assert_eq!(String::from_iter(seq.iter()), "ca");
    }

    #[test]
    fn test_prop_vec_model_qc7() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(0, 'b');
        seq.remove(1);

        assert_eq!(String::from_iter(seq.iter()), "b");
    }

    #[test]
    fn test_prop_vec_model_qc8() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(0, 'b');
        seq.insert(1, 'c');
        seq.remove(0);
        seq.insert(2, 'd');

        assert_eq!(String::from_iter(seq.iter()), "cad");
    }

    #[test]
    fn test_prop_vec_model_qc9() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(1, 'a');
        seq.insert(1, 'b');
        seq.insert(1, 'a');
        seq.remove(2);

        assert_eq!(String::from_iter(seq.iter()), "aaa");
    }

    #[test]
    fn test_prop_vec_model_qc10() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(0, 'b');
        seq.insert(1, 'c');
        seq.remove(2);

        assert_eq!(String::from_iter(seq.iter()), "bc");
    }

    #[test]
    fn test_prop_vec_model_qc11() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(0, 'b');
        seq.insert(0, 'c');
        seq.insert(0, 'd');
        seq.remove(3);

        assert_eq!(String::from_iter(seq.iter()), "dcb");
    }

    #[test]
    fn test_prop_vec_model_qc12() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(0, 'a');
        seq.remove(0);
        seq.remove(0);
        seq.insert(0, 'a');
        seq.remove(0);

        assert_eq!(String::from_iter(seq.iter()), "");
    }

    #[test]
    fn test_prop_vec_model_qc13() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(1, 'a');
        seq.insert(1, 'a');
        seq.insert(1, 'b');

        assert_eq!(String::from_iter(seq.iter()), "abaa");
    }

    #[test]
    fn test_prop_vec_model_qc14_missing_char() {
        let mut seq = HashSeq::default();

        // Regression test for bug where multi-byte UTF-8 characters were not handled correctly
        // in runs. The bug was that Run::len() returned byte length instead of character count,
        // causing position calculation errors for characters like '\u{80}' (2 bytes in UTF-8).
        seq.insert(0, '\0');
        seq.insert(1, '\0');
        seq.insert(2, '\0');
        seq.insert(3, '\u{80}');

        let result: Vec<char> = seq.iter().collect();
        assert_eq!(result, vec!['\0', '\0', '\0', '\u{80}']);
    }

    #[test]
    fn test_insert_remove_and_reinsert() {
        let mut seq = HashSeq::default();
        seq.insert(0, 'b');
        seq.remove(0);
        seq.insert(0, 'b');
        assert_eq!(String::from_iter(seq.iter()), "b");
    }

    #[test]
    fn test_removing_an_element_twice() {
        let mut seq = HashSeq::default();
        seq.insert(0, 'a');
        seq.insert(0, 'b');
        let removed = seq.iter_ids().nth(1).unwrap();
        seq.remove(1);

        seq.apply(HashNode {
            op: Op::Remove(BTreeSet::from_iter([removed])),
            extra_dependencies: BTreeSet::new(),
        });

        assert_eq!(String::from_iter(seq.iter()), "b");
    }

    #[quickcheck]
    fn prop_vec_model(instructions: Vec<(bool, u8, char)>) {
        let mut model = Vec::new();
        let mut seq = HashSeq::default();

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

    #[quickcheck]
    fn prop_order_is_stable(a: Vec<(bool, u8, char)>, b: Vec<(bool, u8, char)>) {
        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();
        let mut removed = BTreeSet::new();

        for (insert_or_remove, idx, elem) in a {
            let idx = idx as usize;
            match insert_or_remove {
                true => {
                    // insert
                    seq_a.insert(idx.min(seq_a.len()), elem);
                }
                false => {
                    // remove
                    if !seq_a.is_empty() {
                        let idx = idx.min(seq_a.len() - 1);
                        removed.insert(seq_a.iter_ids().nth(idx).unwrap());
                        seq_a.remove(idx);
                    }
                }
            }
        }

        for (insert_or_remove, idx, elem) in b {
            let idx = idx as usize;
            match insert_or_remove {
                true => {
                    // insert
                    seq_b.insert(idx.min(seq_b.len()), elem);
                }
                false => {
                    // remove
                    if !seq_b.is_empty() {
                        let idx = idx.min(seq_b.len() - 1);
                        removed.insert(seq_b.iter_ids().nth(idx).unwrap());
                        seq_b.remove(idx);
                    }
                }
            }
        }

        let mut merged = seq_a.clone();
        merged.merge(seq_b.clone());

        for r in removed {
            seq_a.apply(HashNode {
                op: Op::Remove(BTreeSet::from_iter([r])),
                extra_dependencies: BTreeSet::new(),
            });
            seq_b.apply(HashNode {
                op: Op::Remove(BTreeSet::from_iter([r])),
                extra_dependencies: BTreeSet::new(),
            });
        }
        let mut iter_a = seq_a.iter_ids();
        let mut iter_b = seq_b.iter_ids();
        let mut next_a = iter_a.next();
        let mut next_b = iter_b.next();

        for id in merged.iter_ids() {
            if Some(id) == next_a {
                next_a = iter_a.next();
            }
            if Some(id) == next_b {
                next_b = iter_b.next();
            }
        }
        assert_eq!(next_a, None);
        assert_eq!(next_b, None);
    }

    #[test]
    fn test_order_is_stable_minimal() {
        // Failing case from quickcheck: a = [], b = [(true, 0, '\0'), (true, 0, '\0'), (true, 2, '\0')]
        let a: Vec<(bool, u8, char)> = vec![];
        let b = [(true, 0, '\0'), (true, 0, '\0'), (true, 2, '\0')];

        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();
        let mut removed = BTreeSet::new();

        for (insert_or_remove, idx, elem) in a {
            let idx = idx as usize;
            match insert_or_remove {
                true => {
                    seq_a.insert(idx.min(seq_a.len()), elem);
                }
                false => {
                    if !seq_a.is_empty() {
                        let idx = idx.min(seq_a.len() - 1);
                        removed.insert(seq_a.iter_ids().nth(idx).unwrap());
                        seq_a.remove(idx);
                    }
                }
            }
        }

        for (insert_or_remove, idx, elem) in b.iter() {
            let idx = *idx as usize;
            match insert_or_remove {
                true => {
                    let insert_idx = idx.min(seq_b.len());
                    seq_b.insert(insert_idx, *elem);
                }
                false => {
                    if !seq_b.is_empty() {
                        let idx = idx.min(seq_b.len() - 1);
                        removed.insert(seq_b.iter_ids().nth(idx).unwrap());
                        seq_b.remove(idx);
                    }
                }
            }
        }

        let mut merged = seq_a.clone();
        merged.merge(seq_b.clone());

        for r in removed {
            seq_a.apply(HashNode {
                op: Op::Remove(BTreeSet::from_iter([r])),
                extra_dependencies: BTreeSet::new(),
            });
            seq_b.apply(HashNode {
                op: Op::Remove(BTreeSet::from_iter([r])),
                extra_dependencies: BTreeSet::new(),
            });
        }

        let mut iter_a = seq_a.iter_ids();
        let mut iter_b = seq_b.iter_ids();
        let mut next_a = iter_a.next();
        let mut next_b = iter_b.next();

        for id in merged.iter_ids() {
            if Some(id) == next_a {
                next_a = iter_a.next();
            }
            if Some(id) == next_b {
                next_b = iter_b.next();
            }
        }

        assert_eq!(next_a, None);
        assert_eq!(next_b, None);
    }

    #[test]
    fn test_order_is_stable_4_inserts() {
        // Failing case: a = [], b = [(true, 0, '\0'), (true, 1, '\0'), (true, 1, '\0'), (true, 2, '\0')]
        let a: Vec<(bool, u8, char)> = vec![];
        let b = [
            (true, 0, '\0'),
            (true, 1, '\0'),
            (true, 1, '\0'),
            (true, 2, '\0'),
        ];

        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();
        let mut removed = BTreeSet::new();

        for (insert_or_remove, idx, elem) in a {
            let idx = idx as usize;
            match insert_or_remove {
                true => {
                    seq_a.insert(idx.min(seq_a.len()), elem);
                }
                false => {
                    if !seq_a.is_empty() {
                        let idx = idx.min(seq_a.len() - 1);
                        removed.insert(seq_a.iter_ids().nth(idx).unwrap());
                        seq_a.remove(idx);
                    }
                }
            }
        }

        for (insert_or_remove, idx, elem) in b.iter() {
            let idx = *idx as usize;
            match insert_or_remove {
                true => {
                    let insert_idx = idx.min(seq_b.len());
                    seq_b.insert(insert_idx, *elem);
                }
                false => {
                    if !seq_b.is_empty() {
                        let idx = idx.min(seq_b.len() - 1);
                        removed.insert(seq_b.iter_ids().nth(idx).unwrap());
                        seq_b.remove(idx);
                    }
                }
            }
        }

        let mut merged = seq_a.clone();
        merged.merge(seq_b.clone());

        for r in removed {
            seq_a.apply(HashNode {
                op: Op::Remove(BTreeSet::from_iter([r])),
                extra_dependencies: BTreeSet::new(),
            });
            seq_b.apply(HashNode {
                op: Op::Remove(BTreeSet::from_iter([r])),
                extra_dependencies: BTreeSet::new(),
            });
        }

        let mut iter_a = seq_a.iter_ids();
        let mut iter_b = seq_b.iter_ids();
        let mut next_a = iter_a.next();
        let mut next_b = iter_b.next();

        for id in merged.iter_ids() {
            if Some(id) == next_a {
                next_a = iter_a.next();
            }
            if Some(id) == next_b {
                next_b = iter_b.next();
            }
        }

        assert_eq!(next_a, None);
        assert_eq!(next_b, None);
    }

    #[test]
    fn test_order_is_stable_with_removes() {
        // Failing case: a = [], b = [(true, 0, '\0'), (true, 1, '\0'), (true, 1, '\0'), (false, 0, '\0'), (false, 1, '\0')]
        let a: Vec<(bool, u8, char)> = vec![];
        let b = [
            (true, 0, '\0'),
            (true, 1, '\0'),
            (true, 1, '\0'),
            (false, 0, '\0'),
            (false, 1, '\0'),
        ];

        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();
        let mut removed = BTreeSet::new();

        for (insert_or_remove, idx, elem) in a {
            let idx = idx as usize;
            match insert_or_remove {
                true => {
                    seq_a.insert(idx.min(seq_a.len()), elem);
                }
                false => {
                    if !seq_a.is_empty() {
                        let idx = idx.min(seq_a.len() - 1);
                        removed.insert(seq_a.iter_ids().nth(idx).unwrap());
                        seq_a.remove(idx);
                    }
                }
            }
        }

        for (insert_or_remove, idx, elem) in b.iter() {
            let idx = *idx as usize;
            match insert_or_remove {
                true => {
                    let insert_idx = idx.min(seq_b.len());
                    seq_b.insert(insert_idx, *elem);
                }
                false => {
                    if !seq_b.is_empty() {
                        let idx = idx.min(seq_b.len() - 1);
                        let removed_id = seq_b.iter_ids().nth(idx).unwrap();
                        removed.insert(removed_id);
                        seq_b.remove(idx);
                    }
                }
            }
        }

        let mut merged = seq_a.clone();
        merged.merge(seq_b.clone());

        for r in removed {
            seq_a.apply(HashNode {
                op: Op::Remove(BTreeSet::from_iter([r])),
                extra_dependencies: BTreeSet::new(),
            });
            seq_b.apply(HashNode {
                op: Op::Remove(BTreeSet::from_iter([r])),
                extra_dependencies: BTreeSet::new(),
            });
        }

        let mut iter_a = seq_a.iter_ids();
        let mut iter_b = seq_b.iter_ids();
        let mut next_a = iter_a.next();
        let mut next_b = iter_b.next();

        for id in merged.iter_ids() {
            if Some(id) == next_a {
                next_a = iter_a.next();
            }
            if Some(id) == next_b {
                next_b = iter_b.next();
            }
        }

        assert_eq!(next_a, None);
        assert_eq!(next_b, None);
    }

    #[test]
    fn test_prop_commutative_failing_case() {
        // Failing case from quickcheck: ([(true, 0, '\0'), (true, 0, '\0'), (false, 1, '\0')], [(true, 0, '@')])
        // Seq A: insert at 0, insert at 0, remove at 1
        // Seq B: insert at 0 ('@')

        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        // Build seq_a: insert at 0, insert at 0, remove at 1
        seq_a.insert(0, '\0');
        seq_a.insert(0, '\0');
        seq_a.remove(1);

        // Build seq_b: insert at 0 ('@')
        seq_b.insert(0, '@');

        // Test commutativity
        let mut merge_a_b = seq_a.clone();
        merge_a_b.merge(seq_b.clone());

        let mut merge_b_a = seq_b.clone();
        merge_b_a.merge(seq_a.clone());

        assert_eq!(merge_a_b, merge_b_a);
    }

    #[test]
    fn test_hash_collision_check() {
        use crate::hash_node::hash_op;
        use std::collections::BTreeSet;

        let id_176 = [
            176u8, 0, 196, 149, 199, 28, 222, 36, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,
        ];

        let op1 = Op::InsertRoot('\0');
        let op2 = Op::InsertBefore(id_176, '\0');

        let hash1 = hash_op(&op1, &BTreeSet::new());
        let hash2 = hash_op(&op2, &BTreeSet::new());

        assert_ne!(
            hash1, hash2,
            "These operations should have different hashes!"
        );
    }

    #[test]
    fn test_prop_commutative_run_vs_individual() {
        // Failing case: ([], [(true, 0, '\0'), (true, 0, '\0'), (true, 1, '\0'), (true, 2, '\0')])
        // Seq A: empty
        // Seq B: insert at 0, insert at 0, insert at 1, insert at 2

        let seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        // seq_a is empty

        // Build seq_b with 4 inserts
        seq_b.insert(0, '\0');
        seq_b.insert(0, '\0');
        seq_b.insert(1, '\0');
        seq_b.insert(2, '\0');

        // Test commutativity
        let mut merge_a_b = seq_a.clone();
        merge_a_b.merge(seq_b.clone());

        let mut merge_b_a = seq_b.clone();
        merge_b_a.merge(seq_a.clone());

        // Compare content and IDs
        let merge_a_b_content: Vec<char> = merge_a_b.iter().collect();
        let merge_b_a_content: Vec<char> = merge_b_a.iter().collect();
        let merge_a_b_ids: Vec<Id> = merge_a_b.iter_ids().collect();
        let merge_b_a_ids: Vec<Id> = merge_b_a.iter_ids().collect();
        assert_eq!(merge_a_b_content, merge_b_a_content);
        assert_eq!(merge_a_b_ids, merge_b_a_ids);
        assert_eq!(merge_a_b, merge_b_a);
    }

    #[test]
    fn test_prop_commutative_remove_with_run() {
        // Failing case: ([(true, 0, '\0'), (true, 0, '\0'), (false, 1, '\0'), (true, 1, '\0'), (true, 2, '\0')], [])
        // Seq A: insert at 0, insert at 0, remove at 1, insert at 1, insert at 2
        // Seq B: empty

        let mut seq_a = HashSeq::default();
        let seq_b = HashSeq::default();

        // Build seq_a with the operations
        seq_a.insert(0, '\0'); // Insert at 0
        seq_a.insert(0, '\0'); // Insert at 0
        seq_a.remove(1); // Remove at 1
        seq_a.insert(1, '\0'); // Insert at 1
        seq_a.insert(2, '\0'); // Insert at 2

        // Test commutativity
        let mut merge_a_b = seq_a.clone();
        merge_a_b.merge(seq_b.clone());

        let mut merge_b_a = seq_b.clone();
        merge_b_a.merge(seq_a.clone());

        assert_eq!(merge_a_b, merge_b_a);
    }
}
