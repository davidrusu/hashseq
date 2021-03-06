use std::collections::BTreeMap;

use generational_arena::{Arena, Index};

use crate::Id;

#[derive(Clone, Copy, Debug)]
enum Node {
    Leaf,
    Two(Index, Id, Index),
    Three(Index, Id, Index, Id, Index),
}

#[derive(Clone, Copy, Debug)]
struct NodeWithMeta {
    node: Node,
    size: usize,
}

#[derive(Debug)]
struct Tree {
    root: Index,
    nodes: Arena<NodeWithMeta>,
    /// we share pointers to the leaf nodes to avoid allocations
    leaf_idx: Index,
    id_to_node: BTreeMap<Id, Index>,
}

impl Default for NodeWithMeta {
    fn default() -> Self {
        Self {
            node: Node::Leaf,
            size: 0,
        }
    }
}

impl Default for Tree {
    fn default() -> Self {
        let mut nodes = Arena::new();
        let leaf_idx = nodes.insert(NodeWithMeta::default());
        Self {
            root: leaf_idx,
            nodes,
            leaf_idx,
            id_to_node: BTreeMap::default(),
        }
    }
}

impl Tree {
    fn len(&self) -> usize {
        self.nodes[self.root].size
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn two_node(&mut self, left: Index, val: Id, right: Index) -> Index {
        self.nodes.insert(NodeWithMeta {
            node: Node::Two(left, val, right),
            size: self.nodes[left].size + 1 + self.nodes[right].size,
        })
    }

    fn three_node(&mut self, l: Index, lv: Id, m: Index, rv: Id, r: Index) -> Index {
        self.nodes.insert(NodeWithMeta {
            node: Node::Three(l, lv, m, rv, r),
            size: self.nodes[l].size + 1 + self.nodes[m].size + 1 + self.nodes[r].size,
        })
    }

    fn insert(&mut self, idx: usize, value: Id) {
        match self.insert_rec(idx, value, 0, self.root) {
            Some((left, value, right)) => {
                if self.root != self.leaf_idx {
                    self.nodes.remove(self.root);
                }
                self.root = self.nodes.insert(NodeWithMeta {
                    node: Node::Two(left, value, right),
                    size: self.nodes[left].size + 1 + self.nodes[right].size,
                })
            }
            None => (),
        }
    }

    fn insert_rec(
        &mut self,
        idx: usize,
        value: Id,
        prefix_len: usize,
        root: Index,
    ) -> Option<(Index, Id, Index)> {
        // println!(
        //     "insert_rec({idx}, {value}, {prefix_len}, {root:?}={:?})",
        //     self.nodes[root]
        // );
        match self.nodes[root].node {
            Node::Leaf => {
                assert_eq!(prefix_len, idx);
                assert_eq!(root, self.leaf_idx);
                Some((self.leaf_idx, value, self.leaf_idx))
            }
            Node::Two(l, v, r) => {
                let left_bound = self.nodes[l].size + prefix_len;
                if idx <= left_bound {
                    match self.insert_rec(idx, value, prefix_len, l) {
                        Some((cl, cv, cr)) => {
                            self.nodes[root].node = Node::Three(cl, cv, cr, v, r);
                        }
                        None => (),
                    }
                } else {
                    match self.insert_rec(idx, value, left_bound + 1, r) {
                        Some((cl, cv, cr)) => {
                            self.nodes[root].node = Node::Three(l, v, cl, cv, cr);
                        }
                        None => (),
                    }
                }
                self.nodes[root].size += 1;
                None
            }
            Node::Three(l, lv, m, rv, r) => {
                let left_bound = self.nodes[l].size + prefix_len;
                let mid_bound = left_bound + 1 + self.nodes[m].size;
                if idx <= left_bound {
                    match self.insert_rec(idx, value, prefix_len, l) {
                        Some((cl, cv, cr)) => {
                            self.nodes.remove(root);
                            let nl = self.nodes.insert(NodeWithMeta {
                                node: Node::Two(cl, cv, cr),
                                size: self.nodes[cl].size + 1 + self.nodes[cr].size,
                            });
                            let nr = self.nodes.insert(NodeWithMeta {
                                node: Node::Two(m, rv, r),
                                size: self.nodes[m].size + 1 + self.nodes[r].size,
                            });
                            Some((nl, lv, nr))
                        }
                        None => {
                            self.nodes[root].size += 1;
                            None
                        }
                    }
                } else if idx <= mid_bound {
                    match self.insert_rec(idx, value, left_bound + 1, m) {
                        Some((cl, cv, cr)) => {
                            self.nodes.remove(root);
                            let nl = self.nodes.insert(NodeWithMeta {
                                node: Node::Two(l, lv, cl),
                                size: self.nodes[l].size + 1 + self.nodes[cl].size,
                            });
                            let nr = self.nodes.insert(NodeWithMeta {
                                node: Node::Two(cr, rv, r),
                                size: self.nodes[cr].size + 1 + self.nodes[r].size,
                            });
                            Some((nl, cv, nr))
                        }
                        None => {
                            self.nodes[root].size += 1;
                            None
                        }
                    }
                } else {
                    match self.insert_rec(idx, value, mid_bound + 1, r) {
                        Some((cl, cv, cr)) => {
                            self.nodes.remove(root);
                            let nl = self.nodes.insert(NodeWithMeta {
                                node: Node::Two(l, lv, m),
                                size: self.nodes[l].size + 1 + self.nodes[m].size,
                            });
                            let nr = self.nodes.insert(NodeWithMeta {
                                node: Node::Two(cl, cv, cr),
                                size: self.nodes[cl].size + 1 + self.nodes[cr].size,
                            });
                            Some((nl, rv, nr))
                        }
                        None => {
                            self.nodes[root].size += 1;
                            None
                        }
                    }
                }
            }
        }
    }

    fn get(&self, idx: usize) -> Option<Id> {
        self.get_rec(idx, 0, self.root)
    }

    fn get_rec(&self, idx: usize, prefix_len: usize, root: Index) -> Option<Id> {
        // println!(
        //     "get_rec({idx}, {prefix_len}, {root:?}={:?})",
        //     self.nodes[root]
        // );
        match self.nodes[root].node {
            Node::Leaf => None,
            Node::Two(l, v, r) => {
                let left_bound = self.nodes[l].size + prefix_len;
                if idx < left_bound {
                    self.get_rec(idx, prefix_len, l)
                } else if idx == left_bound {
                    Some(v)
                } else {
                    self.get_rec(idx, left_bound + 1, r)
                }
            }
            Node::Three(l, lv, m, rv, r) => {
                let left_bound = self.nodes[l].size + prefix_len;
                let mid_bound = left_bound + 1 + self.nodes[m].size;
                if idx < left_bound {
                    self.get_rec(idx, prefix_len, l)
                } else if idx == left_bound {
                    Some(lv)
                } else if idx < mid_bound {
                    self.get_rec(idx, left_bound + 1, m)
                } else if idx == mid_bound {
                    Some(rv)
                } else {
                    self.get_rec(idx, mid_bound + 1, r)
                }
            }
        }
    }

    fn iter_node(&self, node: Index) -> Box<dyn Iterator<Item = Id> + '_> {
        match self.nodes[node].node {
            Node::Leaf => Box::new(std::iter::empty()),
            Node::Two(l, v, r) => Box::new(
                self.iter_node(l)
                    .chain(std::iter::once(v))
                    .chain(std::iter::once_with(move || self.iter_node(r)).flatten()),
            ),
            Node::Three(l, lv, m, rv, r) => Box::new(
                self.iter_node(l)
                    .chain(std::iter::once(lv))
                    .chain(std::iter::once_with(move || self.iter_node(m)).flatten())
                    .chain(std::iter::once(rv))
                    .chain(std::iter::once_with(move || self.iter_node(r)).flatten()),
            ),
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Id> + '_> {
        self.iter_node(self.root)
    }

    fn pprint(&self, root: Index) -> String {
        let meta_node = &self.nodes[root];
        match meta_node.node {
            Node::Leaf => "*".to_string(),
            Node::Two(l, v, r) => format!(
                "Two(size={}, {}, {v}, {})",
                meta_node.size,
                self.pprint(l),
                self.pprint(r)
            ),
            Node::Three(l, lv, m, rv, r) => {
                format!(
                    "Three(size={}, {}, {lv}, {}, {rv}, {})",
                    meta_node.size,
                    self.pprint(l),
                    self.pprint(m),
                    self.pprint(r)
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck_macros::quickcheck;

    fn test_empty() {
        let tree = Tree::default();

        assert_eq!(tree.len(), 0);
        assert!(tree.is_empty());
    }

    #[test]
    fn test_insert_one_value() {
        let mut tree = Tree::default();
        tree.insert(0, 0);
        assert_eq!(Vec::from_iter(tree.iter()), vec![0]);
        assert_eq!(tree.get(0), Some(0));
    }

    #[test]
    fn test_insert_at_front() {
        let mut tree = Tree::default();
        tree.insert(0, 0);
        tree.insert(0, 1);

        assert_eq!(Vec::from_iter(tree.iter()), vec![1, 0]);
    }

    #[test]
    fn test_insert_at_end() {
        let mut tree = Tree::default();

        tree.insert(0, 0);
        tree.insert(1, 0);

        assert_eq!(Vec::from_iter(tree.iter()), vec![0, 0]);
    }

    #[test]
    fn test_insert_in_middle() {
        let mut tree = Tree::default();

        tree.insert(0, 0);
        tree.insert(0, 0);
        tree.insert(1, 0);

        assert_eq!(Vec::from_iter(tree.iter()), vec![0, 0, 0]);
    }

    #[test]
    fn test_prop_vec_model_qc1() {
        let mut model = Vec::new();
        let mut tree = Tree::default();

        model.insert(0, 0);
        tree.insert(0, 0);

        assert_eq!(tree.len(), 1);

        model.insert(0, 0);
        tree.insert(0, 0);

        assert_eq!(tree.len(), 2);

        model.insert(0, 0);
        tree.insert(0, 0);

        assert_eq!(tree.len(), 3);

        model.insert(0, 0);
        tree.insert(0, 0);

        assert_eq!(tree.len(), 4);

        model.insert(0, 0);
        tree.insert(0, 0);

        assert_eq!(tree.len(), 5);

        model.insert(0, 0);
        tree.insert(0, 0);

        assert_eq!(tree.len(), 6);

        model.insert(6, 0);
        tree.insert(6, 0);

        assert_eq!(tree.len(), 7);

        assert!(model.iter().copied().eq(tree.iter()));
    }

    #[test]
    fn test_prop_vec_model_qc2() {
        let mut model = Vec::new();
        let mut tree = Tree::default();

        model.insert(0, 0);
        tree.insert(0, 0);

        model.insert(0, 0);
        tree.insert(0, 0);

        assert_eq!(model.get(1).cloned(), tree.get(1));

        assert!(model.iter().copied().eq(tree.iter()));
    }

    #[quickcheck]
    fn test_vec_model_qc4() {
        let inserts = [
            (0, 0, 0),
            (0, 0, 0),
            (0, 0, 0),
            (0, 0, 0),
            (0, 0, 0),
            (0, 0, 0),
            (0, 0, 0),
            (1, 5, 0),
        ];
        let mut model = Vec::new();
        let mut tree = Tree::default();

        for (mut instruction, mut idx, value) in inserts {
            instruction = instruction % 2;

            assert_eq!(idx.min(model.len()), idx.min(tree.len()));
            idx = idx.min(tree.len());

            match instruction {
                0 => {
                    model.insert(idx, value);
                    tree.insert(idx, value);
                }
                1 => {
                    assert_eq!(model.get(idx).cloned(), tree.get(idx))
                }
                i => panic!("Unexpected instruction {i}"),
            }
        }

        assert!(model.iter().copied().eq(tree.iter()));
    }

    #[test]
    fn test_vec_model_qc5() {
        let inserts = [
            (0, 0, 0),
            (0, 0, 1),
            (0, 0, 2),
            (0, 0, 3),
            (0, 3, 4),
            (0, 3, 5),
        ];
        let mut model = Vec::new();
        let mut tree = Tree::default();

        for (mut instruction, mut idx, value) in inserts {
            instruction = instruction % 2;

            assert_eq!(idx.min(model.len()), idx.min(tree.len()));
            idx = idx.min(tree.len());

            match instruction {
                0 => {
                    model.insert(idx, value);
                    tree.insert(idx, value);
                }
                1 => {}
                i => panic!("Unexpected instruction {i}"),
            }
        }

        println!("{}", tree.pprint(tree.root));
        assert_eq!(model.get(6).cloned(), tree.get(6));

        assert!(model.iter().copied().eq(tree.iter()));
    }

    #[test]
    fn test_vec_model_qc6() {
        let inserts = [
            (0, 0, 6),
            (0, 0, 5),
            (0, 0, 4),
            (0, 0, 3),
            (0, 0, 2),
            (0, 0, 1),
            (0, 0, 0),
        ];
        let mut model = Vec::new();
        let mut tree = Tree::default();

        for (mut instruction, mut idx, value) in inserts {
            instruction = instruction % 2;

            assert_eq!(idx.min(model.len()), idx.min(tree.len()));
            idx = idx.min(tree.len());

            match instruction {
                0 => {
                    model.insert(idx, value);
                    tree.insert(idx, value);
                }
                1 => {
                    assert_eq!(model.get(idx).cloned(), tree.get(idx))
                }
                i => panic!("Unexpected instruction {i}"),
            }
        }
        println!("{}", tree.pprint(tree.root));
        assert_eq!(model.get(6).cloned(), tree.get(6));

        assert!(model.iter().copied().eq(tree.iter()));
    }

    #[quickcheck]
    fn prop_vec_model(inserts: Vec<(u8, usize, Id)>) {
        let mut model = Vec::new();
        let mut tree = Tree::default();

        for (mut instruction, mut idx, value) in inserts {
            instruction = instruction % 2;

            assert_eq!(idx.min(model.len()), idx.min(tree.len()));
            idx = idx.min(tree.len());

            match instruction {
                0 => {
                    model.insert(idx, value);
                    tree.insert(idx, value);
                }
                1 => {
                    assert_eq!(model.get(idx).cloned(), tree.get(idx))
                }
                i => panic!("Unexpected instruction {i}"),
            }
        }

        assert!(model.iter().copied().eq(tree.iter()));
    }
}
