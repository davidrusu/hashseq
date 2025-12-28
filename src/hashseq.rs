use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use associative_positional_list::AssociativePositionalList;

use crate::{HashNode, Id, Op, Run};

#[derive(Debug, Clone)]
pub struct TopoIter<'a> {
    seq: &'a HashSeq,
    waiting_stack: Vec<(Id, Vec<Id>)>,
}

impl<'a> TopoIter<'a> {
    fn new(seq: &'a HashSeq) -> Self {
        let mut iter = Self {
            seq,
            waiting_stack: Vec::new(),
        };

        let mut roots_vec: Vec<Id> = seq.root_nodes.keys().copied().collect();
        roots_vec.sort();
        for root in roots_vec.into_iter().rev() {
            iter.push_waiting(root);
        }

        iter
    }

    fn push_waiting(&mut self, n: Id) {
        let mut deps: Vec<Id> = self.seq.befores(&n).into_iter().cloned().collect();
        deps.sort();
        deps.reverse();
        self.waiting_stack.push((n, deps));
    }
}

impl<'a> Iterator for TopoIter<'a> {
    type Item = &'a Id;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (_, deps) = self.waiting_stack.last_mut()?;

            if let Some(dep) = deps.pop() {
                // This node has dependencies that need to be
                // released ahead of itself.
                self.push_waiting(dep);
            } else {
                let (n, _) = self.waiting_stack.pop().expect("Failed to pop");
                // This node is free to be released, but first
                // queue up any nodes who come after this one
                if let Some(afters) = self.seq.afters.get(&n) {
                    // Sort by Id value
                    let mut afters_sorted: Vec<Id> = afters.clone();
                    afters_sorted.sort();
                    for s in afters_sorted.into_iter().rev() {
                        self.push_waiting(s);
                    }
                } else if let Some(run_pos) = self.seq.run_index.get(&n) {
                    // Check if n is the first element of this run
                    if run_pos.position == 0 {
                        // Push remaining run elements (skip first which is n)
                        if let Some(elements) = self.seq.run_elements.get(&run_pos.run_id) {
                            for id in elements.iter().skip(1).rev() {
                                // Use push_waiting to properly handle befores
                                self.push_waiting(*id);
                            }
                        }
                    }
                }
                // Return reference from the nodes set
                if let Some(id_ref) = self.seq.nodes.get(&n)
                    && !self.seq.removed_inserts.contains(id_ref)
                {
                    return Some(id_ref);
                }
            }
        }
    }
}

/// Location information for where a node ID can be found
#[derive(Debug, Clone, Copy)]
pub struct RunPosition {
    pub run_id: Id,
    pub position: usize,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CausalInsert {
    pub extra_dependencies: BTreeSet<Id>,
    pub anchor: Id,
    pub ch: char,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CausalRemove {
    pub extra_dependencies: BTreeSet<Id>,
    pub nodes: BTreeSet<Id>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CausalRoot {
    pub extra_dependencies: BTreeSet<Id>,
    pub ch: char,
}

#[derive(Debug, Default, Clone)]
pub struct HashSeq {
    // All node IDs for stable reference storage (used by TopoIter)
    pub nodes: BTreeSet<Id>,

    // Hybrid storage: runs for sequential elements, individual nodes for complex operations
    pub runs: HashMap<Id, Run>,
    pub root_nodes: BTreeMap<Id, CausalRoot>,
    pub before_nodes: HashMap<Id, CausalInsert>,
    // Reverse index: anchor -> list of nodes inserted before that anchor
    pub befores_by_anchor: HashMap<Id, Vec<Id>>,
    pub remove_nodes: HashMap<Id, CausalRemove>,

    // ID resolution index for O(1) lookup of any node
    pub run_index: HashMap<Id, RunPosition>,

    // Cache of decompressed run element IDs for O(1) lookup in get_afters
    // Maps run_id -> list of element IDs in that run
    pub run_elements: HashMap<Id, Vec<Id>>,

    // Fork tracking: maps anchor ID to list of IDs that fork from it
    pub afters: HashMap<Id, Vec<Id>>,

    pub removed_inserts: HashSet<Id>,
    pub(crate) tips: BTreeSet<Id>,
    pub(crate) orphaned: HashSet<HashNode>,
    index: AssociativePositionalList<Id>,
}

impl PartialEq for HashSeq {
    fn eq(&self, other: &Self) -> bool {
        self.tips == other.tips
    }
}

impl Eq for HashSeq {}

impl HashSeq {
    /// Check if a node ID exists (either in runs or individual nodes)
    pub fn contains_node(&self, id: &Id) -> bool {
        self.root_nodes.contains_key(id)
            || self.remove_nodes.contains_key(id)
            || self.before_nodes.contains_key(id)
            || self.run_index.contains_key(id)
    }

    /// Get the character value for a given node ID
    pub fn get_node_char(&self, id: &Id) -> char {
        if let Some(root) = self.root_nodes.get(id) {
            return root.ch;
        }
        if let Some(before) = self.before_nodes.get(id) {
            return before.ch;
        }
        let run_pos = &self.run_index[id];

        self.runs[&run_pos.run_id]
            .run
            .chars()
            .nth(run_pos.position)
            .unwrap()
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

    /// Get nodes that come after this one. Uses both explicit afters and run data.
    pub fn afters(&self, id: &Id) -> Vec<&Id> {
        match self.afters.get(id) {
            Some(ns) => {
                let mut result: Vec<&Id> = ns.iter().collect();
                result.sort();
                result
            }
            None => {
                // Check if this node is in a run and not the last element
                if let Some(run_pos) = self.run_index.get(id) {
                    if let Some(elements) = self.run_elements.get(&run_pos.run_id) {
                        if run_pos.position + 1 < elements.len() {
                            let next_id = &elements[run_pos.position + 1];
                            // Look up the reference in run_index for stable lifetime
                            if let Some((id_ref, _)) = self.run_index.get_key_value(next_id) {
                                return vec![id_ref];
                            }
                        }
                    }
                }
                Vec::new()
            }
        }
    }

    /// Get nodes that come before this one (inserted with InsertBefore).
    pub fn befores(&self, id: &Id) -> Vec<&Id> {
        match self.befores_by_anchor.get(id) {
            Some(ns) => {
                let mut result: Vec<&Id> = ns.iter().collect();
                result.sort();
                result
            }
            None => Vec::new(),
        }
    }

    /// Check if node `a` is causally before node `b`.
    fn is_causally_before(&self, a: &Id, b: &Id) -> bool {
        let mut seen = BTreeSet::new();
        let mut boundary: Vec<Id> = self.afters(a).into_iter().cloned().collect();
        while let Some(n) = boundary.pop() {
            if &n == b {
                return true;
            }

            seen.insert(n);
            boundary.extend(
                self.afters(&n)
                    .into_iter()
                    .cloned()
                    .filter(|x| !seen.contains(x)),
            );
            if &n != a {
                boundary.extend(
                    self.befores(&n)
                        .into_iter()
                        .cloned()
                        .filter(|x| !seen.contains(x)),
                );
            }
        }

        false
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

        let (left, right) = self.neighbours(idx);

        match (left, right) {
            (Some(left_id), Some(right_id)) => {
                let mut chars_iter = chars.into_iter();
                let mut extra_dependencies = self.tips.clone();
                extra_dependencies.remove(&left_id);
                let first_ch = chars_iter.next().unwrap();
                let mut first_node = HashNode {
                    extra_dependencies,
                    op: Op::InsertAfter(left_id, first_ch),
                };

                if self.is_causally_before(&left_id, &right_id) {
                    // Using InsertAfter for the first node doesn't work.
                    // use InsertBefore right_id instead
                    let mut extra_dependencies = self.tips.clone();
                    extra_dependencies.remove(&right_id);
                    first_node = HashNode {
                        extra_dependencies,
                        op: Op::InsertBefore(right_id, first_ch),
                    };
                }
                let mut prev_id = first_node.id();
                self.apply(first_node);
                for ch in chars_iter {
                    let mut extra_dependencies = self.tips.clone();
                    extra_dependencies.remove(&prev_id);
                    let node = HashNode {
                        extra_dependencies,
                        op: Op::InsertAfter(prev_id, ch),
                    };
                    prev_id = node.id();
                    self.apply(node);
                }
            }
            (Some(left_id), None) => {
                // there is no right node, we just chain from left
                let mut prev_id = left_id;
                for ch in chars.into_iter() {
                    let mut extra_dependencies = self.tips.clone();
                    extra_dependencies.remove(&prev_id);
                    let node = HashNode {
                        extra_dependencies,
                        op: Op::InsertAfter(prev_id, ch),
                    };
                    prev_id = node.id();

                    self.apply(node);
                }
            }
            (None, Some(right_id)) => {
                let mut chars_iter = chars.into_iter();
                let mut extra_dependencies = self.tips.clone();
                extra_dependencies.remove(&right_id);

                let first_node = HashNode {
                    extra_dependencies,
                    op: Op::InsertBefore(right_id, chars_iter.next().unwrap()),
                };

                let mut prev_id = first_node.id();
                self.apply(first_node);

                for ch in chars_iter {
                    let mut extra_dependencies = self.tips.clone();
                    extra_dependencies.remove(&prev_id);
                    let node = HashNode {
                        extra_dependencies,
                        op: Op::InsertAfter(prev_id, ch),
                    };
                    prev_id = node.id();
                    self.apply(node);
                }
            }
            (None, None) => {
                // seq is empty
                let mut chars_iter = chars.into_iter();

                let first_node = HashNode {
                    extra_dependencies: self.tips.clone(),
                    op: Op::InsertRoot(chars_iter.next().unwrap()),
                };

                let mut prev_id = first_node.id();
                self.apply(first_node);

                for ch in chars_iter {
                    let mut extra_dependencies = self.tips.clone();
                    extra_dependencies.remove(&prev_id);
                    let node = HashNode {
                        extra_dependencies,
                        op: Op::InsertAfter(prev_id, ch),
                    };
                    prev_id = node.id();
                    self.apply(node);
                }
            }
        }
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
            BTreeSet::from_iter(self.tips.difference(&op.dependencies()).cloned());

        let node = HashNode {
            extra_dependencies,
            op,
        };

        self.apply(node);
    }

    fn any_missing_dependencies<'a>(&self, deps: impl IntoIterator<Item = &'a Id>) -> bool {
        for dep in deps {
            if !self.contains_node(dep) {
                return true;
            }
        }

        false
    }

    fn insert_root(&mut self, root_id: Id, root: CausalRoot) {
        let position = if let Some(next_root) = self
            .root_nodes
            .keys()
            .filter(|id| *id >= &root_id)
            .find(|id| !self.removed_inserts.contains(*id))
        {
            // new root is inserted just before the next biggest root
            self.index.find(next_root).unwrap()
        } else {
            // otherwise if there is no bigger root, the new root is
            // inserted at end of list
            self.len()
        };
        self.insert_root_with_known_position(root_id, root, position);
    }

    fn insert_root_with_known_position(&mut self, id: Id, root: CausalRoot, position: usize) {
        self.index.insert(position, id);
        self.nodes.insert(id);  // For TopoIter reference storage
        self.root_nodes.insert(id, root);
    }

    fn insert_after(&mut self, id: Id, after: CausalInsert) {
        let afters_for_anchor = self.afters(&after.anchor);
        let position = if let Some(next_node) = BTreeSet::from_iter(afters_for_anchor.iter().copied())
            .range(id..)
            .find(|id| !self.removed_inserts.contains(**id))
        {
            // new node is inserted just before the other node after our anchor node that is
            // bigger than the new node
            self.index.find(next_node)
        } else {
            // otherwise the new node is inserted after our anchor node (unless it has been removed)
            self.index.find(&after.anchor).map(|p| p + 1)
        };

        let is_run_extension = if let Some(run_pos) = self.run_index.get(&after.anchor).copied() {
            // We are inserting after a node that is in a run.
            // need to decide if we can extend the run or if we need to split it
            if self.runs[&run_pos.run_id].len() == run_pos.position + 1
                && afters_for_anchor.is_empty()
                && after.extra_dependencies.is_empty()
            {
                // we are inserting at the end of a run, we can safely extend the run
                self.runs.get_mut(&run_pos.run_id).unwrap().extend(after.ch);
                self.run_index.insert(
                    id,
                    RunPosition {
                        run_id: run_pos.run_id,
                        position: run_pos.position + 1,
                    },
                );
                // Update run_elements cache
                self.run_elements.get_mut(&run_pos.run_id).unwrap().push(id);
                true // This is a run extension
            } else {
                if run_pos.position + 1 < self.runs[&run_pos.run_id].len() {
                    let run = self.runs.get_mut(&run_pos.run_id).unwrap();
                    let right_run = run.split_at(run_pos.position + 1);
                    debug_assert_eq!(run.last_id(), after.anchor);

                    // Decompress the right run to get element IDs
                    let right_nodes = right_run.decompress();
                    let right_run_first_id = right_run.first_id();

                    // re-index the right run
                    let mut right_elements = Vec::with_capacity(right_nodes.len());
                    for (idx, node) in right_nodes.into_iter().enumerate() {
                        let node_id = node.id();
                        self.run_index.insert(
                            node_id,
                            RunPosition {
                                run_id: right_run_first_id,
                                position: idx,
                            },
                        );
                        right_elements.push(node_id);
                    }

                    // Update run_elements for left portion (truncate)
                    self.run_elements.get_mut(&run_pos.run_id).unwrap().truncate(run_pos.position + 1);

                    // The split-off portion needs to be tracked in afters
                    self.afters.entry(after.anchor).or_default().push(right_run_first_id);
                    self.nodes.insert(right_run_first_id);
                    self.runs.insert(right_run_first_id, right_run);
                    self.run_elements.insert(right_run_first_id, right_elements);
                }
                self.runs.insert(
                    id,
                    Run::new(after.anchor, after.extra_dependencies.clone(), after.ch),
                );
                self.run_index.insert(
                    id,
                    RunPosition {
                        run_id: id,
                        position: 0,
                    },
                );
                // Add run_elements for the new run
                self.run_elements.insert(id, vec![id]);
                false // This is a fork, not a run extension
            }
        } else {
            // Either anchor is not a run, or we can't extend from it for some reason, start a new run
            self.runs.insert(
                id,
                Run::new(after.anchor, after.extra_dependencies.clone(), after.ch),
            );
            self.run_index.insert(
                id,
                RunPosition {
                    run_id: id,
                    position: 0,
                },
            );
            // Add run_elements for the new run
            self.run_elements.insert(id, vec![id]);
            false // This is a fork, not a run extension
        };

        // Only add to afters if this is a fork (not a run extension)
        if is_run_extension {
            // For run extensions, just add to nodes (no afters entry needed)
            self.nodes.insert(id);
        } else {
            // For forks, add to both afters and nodes
            self.afters.entry(after.anchor).or_default().push(id);
            self.nodes.insert(id);
        }

        let position = position.unwrap_or_else(|| {
            // fall back to iterating over the entire sequence if the anchor node has been removed
            // or if next_node is not yet in the index (can happen during merge)
            let (position, _) = self
                .iter_ids()
                .enumerate()
                .find(|(_, n)| n == &&id)
                .unwrap();
            position
        });
        self.update_position_index(id, position);
    }

    fn update_position_index(&mut self, id: Id, position: usize) {
        self.index.insert(position, id);
    }

    fn remove_nodes(&mut self, id: Id, remove: CausalRemove) {
        // TODO: if self.nodes.get(node) is not an insert op, then drop this remove.
        //       Are you sure? looks like we would mark this op as an orphan if we hadn't
        //       seen a node yet.
        for n in remove.nodes.iter() {
            if let Some(p) = self.index.find(n) {
                self.index.remove(p);
            }
        }
        self.removed_inserts.extend(&remove.nodes);
        self.remove_nodes.insert(id, remove);
    }

    fn insert_before(&mut self, id: Id, before: CausalInsert) {
        let befores_set: BTreeSet<Id> = self.befores(&before.anchor)
            .into_iter()
            .copied()
            .collect();
        let position = if let Some(next_node) = befores_set
            .range(id..)
            .find(|id| !self.removed_inserts.contains(*id))
        {
            // new node is inserted just before the other node before our anchor node that is
            // bigger than the new node
            Some(self.index.find(next_node).unwrap())
        } else {
            // otherwise the new node is inserted before our anchor node
            self.index.find(&before.anchor)
        };

        if let Some(run_pos) = self.run_index.get(&before.anchor).copied()
            && run_pos.position > 0
        {
            let run = self.runs.get_mut(&run_pos.run_id).unwrap();
            // Get the last ID of the left portion from run_elements cache
            let left_last_id = self.run_elements[&run_pos.run_id][run_pos.position - 1];
            let right_run = run.split_at(run_pos.position);
            let right_run_id = right_run.first_id();
            debug_assert_eq!(right_run_id, before.anchor);

            // Decompress the right run to get element IDs
            let right_nodes = right_run.decompress();

            // re-index the right run
            let mut right_elements = Vec::with_capacity(right_nodes.len());
            for (idx, node) in right_nodes.into_iter().enumerate() {
                let node_id = node.id();
                self.run_index.insert(
                    node_id,
                    RunPosition {
                        run_id: right_run_id,
                        position: idx,
                    },
                );
                right_elements.push(node_id);
            }

            // Update run_elements for left portion (truncate)
            self.run_elements.get_mut(&run_pos.run_id).unwrap().truncate(run_pos.position);

            self.runs.insert(right_run_id, right_run);
            self.run_elements.insert(right_run_id, right_elements);
            // Track the split in afters so iteration can find the right portion
            self.afters.entry(left_last_id).or_default().push(right_run_id);
            self.nodes.insert(right_run_id);
        }

        self.nodes.insert(id);
        self.befores_by_anchor.entry(before.anchor).or_default().push(id);

        self.before_nodes.insert(id, before);

        let position = position.unwrap_or_else(|| {
            // fall back to iterating over the entire sequence if the anchor node has been removed
            let (position, _) = self
                .iter_ids()
                .enumerate()
                .find(|(_, n)| n == &&id)
                .unwrap();
            position
        });
        self.update_position_index(id, position);
    }

    pub fn apply(&mut self, node: HashNode) {
        let id = node.id();

        if self.contains_node(&id) {
            return; // Already processed this node
        }

        let dependencies = node.dependencies();
        if self.any_missing_dependencies(&dependencies) {
            self.orphaned.insert(node);
            return;
        }

        match node.op {
            Op::InsertRoot(ch) => self.insert_root(
                id,
                CausalRoot {
                    extra_dependencies: node.extra_dependencies,
                    ch,
                },
            ),
            Op::InsertAfter(anchor, ch) => self.insert_after(
                id,
                CausalInsert {
                    extra_dependencies: node.extra_dependencies,
                    anchor,
                    ch,
                },
            ),
            Op::InsertBefore(anchor, ch) => self.insert_before(
                id,
                CausalInsert {
                    extra_dependencies: node.extra_dependencies,
                    anchor,
                    ch,
                },
            ),
            Op::Remove(nodes) => self.remove_nodes(
                id,
                CausalRemove {
                    extra_dependencies: node.extra_dependencies,
                    nodes,
                },
            ),
        }

        for tip in dependencies {
            self.tips.remove(&tip);
        }
        self.tips.insert(id);

        for orphan in std::mem::take(&mut self.orphaned) {
            self.apply(orphan);
        }
    }

    pub fn merge(&mut self, other: Self) {
        // Simple merge: decompress all nodes from other and apply them
        // The apply function will rebuild runs when possible

        for (id, root) in other.root_nodes {
            let node = HashNode {
                extra_dependencies: root.extra_dependencies,
                op: Op::InsertRoot(root.ch),
            };
            debug_assert_eq!(id, node.id());
            self.apply(node)
        }

        for (_run_id, run) in other.runs {
            for node in run.decompress() {
                self.apply(node);
            }
        }

        for (id, causal_insert) in other.before_nodes {
            let node = HashNode {
                extra_dependencies: causal_insert.extra_dependencies,
                op: Op::InsertBefore(causal_insert.anchor, causal_insert.ch),
            };
            debug_assert_eq!(id, node.id());
            self.apply(node)
        }

        for (id, causal_remove) in other.remove_nodes {
            let node = HashNode {
                extra_dependencies: causal_remove.extra_dependencies,
                op: Op::Remove(causal_remove.nodes),
            };
            debug_assert_eq!(id, node.id());
            self.apply(node)
        }

        // Apply all orphaned nodes
        for orphan in other.orphaned {
            self.apply(orphan);
        }
    }

    pub fn iter_ids(&self) -> TopoIter<'_> {
        TopoIter::new(self)
    }

    pub fn iter(&self) -> impl Iterator<Item = char> + '_ {
        self.iter_ids().map(|id| self.get_node_char(id))

        // self.index.iter().map(|id| self.get_node_char(&id).unwrap())
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
    fn test_insert_after_before() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'a');
        seq.insert(0, 'b');
        seq.insert(1, 'c');

        assert_eq!(String::from_iter(seq.iter()), "bca");
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

        // Insert entire string as one batch
        let mut seq_single_batch = HashSeq::default();
        seq_single_batch.insert_batch(0, "abcd".chars());

        // Insert as two separate batches
        let mut seq_split_batch = HashSeq::default();
        seq_split_batch.insert_batch(0, "ab".chars());
        seq_split_batch.insert_batch(2, "cd".chars());

        // Verify internal structure is identical
        assert_eq!(
            seq_single_batch.runs, seq_split_batch.runs,
            "Runs should be identical"
        );
        assert_eq!(
            seq_single_batch.nodes, seq_split_batch.nodes,
            "Topo tree should be identical"
        );
        assert_eq!(
            seq_single_batch.tips, seq_split_batch.tips,
            "Tips should be identical"
        );

        // Verify output is also the same
        assert_eq!(seq_single_batch.iter().collect::<String>(), "abcd");
        assert_eq!(seq_split_batch.iter().collect::<String>(), "abcd");
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
        assert_eq!(seq1.tips, seq2.tips, "Tips should be identical");
        assert_eq!(seq1.nodes, seq2.nodes, "Nodes should be identical");
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
            seq_with_abcd.tips, empty_seq.tips,
            "tips should be identical after merge"
        );

        // Verify the structure is as expected:
        // - Should have 1 root node for 'a'
        assert_eq!(
            seq_with_abcd.root_nodes.len(),
            1,
            "Should have 1 individual node (root 'a')"
        );

        // - Should have 1 run containing "bcd"
        assert_eq!(seq_with_abcd.runs.len(), 1, "Should have 1 run");
        let run = seq_with_abcd.runs.values().next().unwrap();
        assert_eq!(run.run, "bcd", "Run should contain 'bcd'");

        // Verify the text is correct
        assert_eq!(seq_with_abcd.iter().collect::<String>(), "abcd");
        assert_eq!(empty_seq.iter().collect::<String>(), "abcd");
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
        assert_eq!(seq1.runs, seq2.runs);
        assert_eq!(seq1.root_nodes, seq2.root_nodes);
        assert_eq!(seq1.before_nodes, seq2.before_nodes);
        assert_eq!(seq1.remove_nodes, seq2.remove_nodes);
        assert_eq!(seq1.nodes, seq2.nodes);
        assert_eq!(seq1.tips, seq2.tips);

        true
    }

    #[test]
    fn test_run_creation() {
        let mut seq = HashSeq::default();

        // Single characters should create individual nodes
        seq.insert(0, 'x');
        assert_eq!(seq.runs.len(), 0);
        assert_eq!(seq.root_nodes.len(), 1);

        // Multi-character batch should create a run
        seq.insert_batch(1, "abc".chars());
        assert_eq!(seq.runs.len(), 1);
        assert_eq!(seq.root_nodes.len(), 1);

        // Verify the run contains the right data
        let run = seq.runs.values().next().unwrap();
        assert_eq!(run.run, "abc");

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
        assert_eq!(seq.root_nodes.len(), 1);

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
        assert_eq!(merged, "hello my name is zameenadavid");
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
    fn test_common_prefix_is_deduplicated_simple_2() {
        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        seq_a.insert_batch(0, "aaab".chars());
        seq_b.insert_batch(0, "aaac".chars());

        seq_a.merge(seq_b);

        let merged = seq_a.iter().collect::<String>();
        assert_eq!(merged, "aaabc");
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

    #[test]
    fn test_reflexive_merge_with_remove() {
        // Failing case: [(true, 0, '\0'), (true, 1, '\u{80}'), (true, 2, '\0'), (false, 0, '\0'), (true, 1, '\0')]
        let mut seq = HashSeq::default();

        seq.insert(0, '\0');
        seq.insert(1, '\u{80}');
        seq.insert(2, '\0');
        seq.remove(0);
        seq.insert(1, '\0');

        // merge(a, a) == a
        let mut merge_self = seq.clone();
        merge_self.merge(seq.clone());

        assert_eq!(merge_self, seq);
    }

    #[test]
    fn test_reflexive_regression() {
        // Regression test from quickcheck failure:
        // [(true, 0, '\0'), (true, 1, '\0'), (false, 0, '\0'), (true, 1, '\0')]
        let mut seq = HashSeq::default();

        seq.insert(0, 'a'); // op 1: idx=0, len=0 -> insert at 0
        dbg!(&seq.run_index, &seq.removed_inserts);

        seq.insert(1, 'b'); // op 2: idx=1, len=1 -> insert at 1
        dbg!(&seq.run_index, &seq.removed_inserts);

        seq.remove(0); // op 3: idx=0, len=2 -> remove at 0
        dbg!(&seq.run_index, &seq.removed_inserts);

        seq.insert(1, 'c'); // op 4: idx=1, len=1 -> insert at 1
        dbg!(&seq.run_index, &seq.removed_inserts);

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
    fn test_prop_vec_model_qc3_debug() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'c'); // "c"
        println!("After insert(0, 'c'): '{}'", seq.iter().collect::<String>());
        assert_eq!(seq.iter().collect::<String>(), "c");

        seq.insert(1, 'c'); // "cc"
        println!("After insert(1, 'c'): '{}'", seq.iter().collect::<String>());
        println!("  runs: {:?}", seq.runs.keys().collect::<Vec<_>>());
        println!("  afters: {:?}", seq.afters);
        assert_eq!(seq.iter().collect::<String>(), "cc");

        seq.insert(2, 'c'); // "ccc"
        println!("After insert(2, 'c'): '{}'", seq.iter().collect::<String>());
        println!("  runs: {:?}", seq.runs.keys().collect::<Vec<_>>());
        println!("  afters: {:?}", seq.afters);
        assert_eq!(seq.iter().collect::<String>(), "ccc");

        // Print all node IDs
        println!("  root_nodes: {:?}", seq.root_nodes.keys().collect::<Vec<_>>());
        println!("  run_index: {:?}", seq.run_index.iter().collect::<Vec<_>>());

        seq.remove(1); // "cc"
        println!("After remove(1): '{}'", seq.iter().collect::<String>());
        println!("  removed_inserts: {:?}", seq.removed_inserts);
        assert_eq!(seq.iter().collect::<String>(), "cc");

        // Debug: check what after returns for each node
        for id in seq.root_nodes.keys() {
            let afters = seq.afters(id);
            println!("  seq.afters({:?}) = {:?}", id, afters.iter().map(|x| **x).collect::<Vec<_>>());
        }

        seq.insert(1, 'b'); // "cbc"
        println!("After insert(1, 'b'): '{}'", seq.iter().collect::<String>());
        println!("  before_nodes: {:?}", seq.before_nodes.keys().collect::<Vec<_>>());
        println!("  befores_by_anchor: {:?}", seq.befores_by_anchor);
        println!("  nodes: {:?}", seq.nodes.iter().collect::<Vec<_>>());
        println!("  afters: {:?}", seq.afters);

        // Check if ef6 (the third c) is in nodes
        for (id, pos) in seq.run_index.iter() {
            println!("  run_index entry: id={:?} at run {:?} pos {}", id, pos.run_id, pos.position);
            println!("    in nodes? {}", seq.nodes.contains(id));
        }

        println!("  iter_ids count: {}", seq.iter_ids().count());
        for id in seq.iter_ids() {
            println!("    id: {:?}", id);
        }
        assert_eq!(seq.iter().collect::<String>(), "cbc");
    }

    #[test]
    fn test_prop_vec_model_qc3() {
        let mut seq = HashSeq::default();

        seq.insert(0, 'c'); // "c"
        seq.insert(1, 'c'); // "cc"
        seq.insert(2, 'c'); // "ccc"
        seq.remove(1); // "cc"
        seq.insert(1, 'b'); // "cbc"

        assert_eq!(seq.iter().collect::<String>(), "cbc");
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
        let removed = seq.iter_ids().nth(1).copied().unwrap();
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
                        removed.insert(*seq_a.iter_ids().nth(idx).unwrap());
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
                        removed.insert(*seq_b.iter_ids().nth(idx).unwrap());
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
                        removed.insert(*seq_a.iter_ids().nth(idx).unwrap());
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
                        removed.insert(*seq_b.iter_ids().nth(idx).unwrap());
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
                        removed.insert(*seq_a.iter_ids().nth(idx).unwrap());
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
                        removed.insert(*seq_b.iter_ids().nth(idx).unwrap());
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
    fn test_order_is_stable_remove_then_insert() {
        // Failing case: a = [], b = [(true, 0, '\0'), (true, 1, '\0'), (true, 2, '\0'), (false, 2, '\0'), (true, 2, '\u{97}')]
        let mut seq_a = HashSeq::default();
        let mut seq_b = HashSeq::default();

        // seq_a remains empty (a = [])

        // seq_b operations:
        seq_b.insert(0, '\0');
        seq_b.insert(1, '\0');
        seq_b.insert(2, '\0');

        // (false, 2, '\0') - remove at index 2
        let removed_id = *seq_b.iter_ids().nth(2).unwrap();
        seq_b.remove(2);

        // (true, 2, '\u{97}') - insert at index 2
        seq_b.insert(2, '\u{97}');

        let mut merged = seq_a.clone();
        merged.merge(seq_b.clone());

        seq_a.apply(HashNode {
            op: Op::Remove(BTreeSet::from_iter([removed_id])),
            extra_dependencies: BTreeSet::new(),
        });

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
                        removed.insert(*seq_a.iter_ids().nth(idx).unwrap());
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
                        removed.insert(*removed_id);
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
        let merge_a_b_ids: Vec<&Id> = merge_a_b.iter_ids().collect();
        let merge_b_a_ids: Vec<&Id> = merge_b_a.iter_ids().collect();
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

    // Tests for runs (spans have been removed and runs are now the source of truth)
    #[test]
    fn test_runs_basic() {
        let mut seq = HashSeq::default();
        seq.insert(0, 'a'); // This is a root, not in runs
        seq.insert(1, 'b'); // This starts a run
        seq.insert(2, 'c'); // This extends the run

        // First character is a root, remaining two should be in a single run
        assert_eq!(seq.root_nodes.len(), 1);
        assert_eq!(seq.runs.len(), 1);
        let run = seq.runs.values().next().unwrap();
        assert_eq!(run.len(), 2);
        assert_eq!(run.run, "bc");
        assert_eq!(String::from_iter(seq.iter()), "abc");
    }

    #[test]
    fn test_runs_with_fork() {
        let mut seq = HashSeq::default();
        seq.insert(0, 'a'); // a (root)
        seq.insert(0, 'b'); // ba (insert before 'a')

        // 'b' is an InsertBefore, which creates a before_node
        assert_eq!(String::from_iter(seq.iter()), "ba");
    }
}
