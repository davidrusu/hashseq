use std::collections::{BTreeSet, HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::Id;

#[derive(Debug, Default, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct SpanNode {
    pub span: Vec<Id>,
}

impl SpanNode {
    fn new(span: Vec<Id>) -> Self {
        debug_assert!(!span.is_empty());
        Self { span }
    }

    fn id(&self) -> Id {
        self.first()
    }

    fn first(&self) -> Id {
        self.span[0]
    }

    fn last(&self) -> Id {
        self.span[self.span.len() - 1]
    }

    // Splits this span in place after the given node; returns the span that was split off
    fn split_after(&mut self, node: Id) -> SpanNode {
        // first find the position of the node
        let p = self.span.iter().position(|n| n == &node).unwrap();
        let new_span = self.span.split_off(p + 1);

        SpanNode::new(new_span)
    }

    // Splits this span in place before the given node; returns the span that was split off
    fn split_before(&mut self, node: Id) -> SpanNode {
        // first find the position of the node
        let p = self.span.iter().position(|n| n == &node).unwrap();
        let new_span = self.span.split_off(p);

        SpanNode::new(new_span)
    }

}

#[derive(Debug, Default, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Topo {
    // All node IDs for stable reference storage
    pub nodes: BTreeSet<Id>,
    // roots designate the independent causal trees.
    pub roots: BTreeSet<Id>,
    pub befores: HashMap<Id, Vec<Id>>,
    pub afters: HashMap<Id, Vec<Id>>,
    pub span_index: HashMap<Id, Id>,
    pub spans: HashMap<Id, SpanNode>,
}

impl Topo {
    pub fn is_causally_before(&self, a: &Id, b: &Id) -> bool {
        let mut seen = BTreeSet::new();
        let mut boundary: Vec<Id> = self.after(a).into_iter().cloned().collect();
        while let Some(n) = boundary.pop() {
            if &n == b {
                return true;
            }

            seen.insert(n);
            boundary.extend(
                self.after(&n)
                    .into_iter()
                    .cloned()
                    .filter(|x| !seen.contains(x)),
            );
            if &n != a {
                boundary.extend(
                    self.before(&n)
                        .into_iter()
                        .cloned()
                        .filter(|x| !seen.contains(x)),
                );
            }
        }

        false
    }

    pub fn add_root(&mut self, node: Id) {
        debug_assert!(!self.spans.contains_key(&node));
        self.nodes.insert(node);
        self.roots.insert(node);
    }

    pub fn add_after(&mut self, anchor: Id, node: Id, has_deps: bool) {
        self.nodes.insert(node);
        match self.span_index.get(&anchor) {
            Some(span_id) => {
                let span_id = *span_id;
                // the anchor is already part of a span
                let span = self.spans.get_mut(&span_id).unwrap();
                if span.last() == anchor
                    && !self.afters.contains_key(&anchor)
                    && !has_deps
                {
                    // we can extend the span
                    span.span.push(node);
                    self.span_index.insert(node, span_id);
                } else if span.last() == anchor {
                    // the span forks at anchor
                    self.spans.insert(node, SpanNode::new(vec![node]));
                    self.span_index.insert(node, node);
                    self.afters.entry(anchor).or_default().push(node);
                } else {
                    // the anchor is somewhere inside the span
                    // need to split the span at the anchor and create a fork
                    let new_span = span.split_after(anchor);
                    let new_span_id = new_span.id();
                    // re-index the new span
                    for id in new_span.span.iter() {
                        self.span_index.insert(*id, new_span_id);
                    }
                    self.afters.entry(anchor).or_default().push(new_span_id);
                    self.spans.insert(new_span_id, new_span);

                    self.spans.insert(node, SpanNode::new(vec![node]));
                    self.span_index.insert(node, node);
                    self.afters.entry(anchor).or_default().push(node);
                }
            }
            None => {
                // begin a new span with this node
                self.spans.insert(node, SpanNode::new(vec![node]));
                self.span_index.insert(node, node);
                self.afters.entry(anchor).or_default().push(node);
            }
        }
    }

    pub fn add_before(&mut self, anchor: Id, node: Id) {
        self.nodes.insert(node);
        match self.span_index.get(&anchor) {
            Some(span_id) => {
                let span_id = *span_id;
                // the anchor is part of a span
                let span = self.spans.get_mut(&span_id).unwrap();
                if span.first() == anchor {
                    self.befores.entry(anchor).or_default().push(node);
                } else {
                    // the anchor is somewhere inside the span
                    // need to split the span at the anchor and create a fork
                    let new_span = span.split_before(anchor);
                    debug_assert_eq!(new_span.id(), anchor);

                    // re-index the new span
                    for id in new_span.span.iter() {
                        self.span_index.insert(*id, anchor);
                    }
                    self.afters.entry(span.last()).or_default().push(anchor);
                    self.spans.insert(anchor, new_span);

                    self.befores.entry(anchor).or_default().push(node);
                }
            }
            None => {
                self.befores.entry(anchor).or_default().push(node);
            }
        }
    }

    pub fn roots(&self) -> &BTreeSet<Id> {
        &self.roots
    }

    pub fn after(&self, id: &Id) -> Vec<&Id> {
        match self.afters.get(id) {
            Some(ns) => {
                let mut result: Vec<&Id> = ns.iter().collect();
                result.sort();
                result
            }
            None => match self.span_index.get(id) {
                Some(span_id) => {
                    let span = &self.spans[span_id];
                    if span.last() == *id {
                        Vec::new()
                    } else {
                        // Find position in span and return next element
                        let pos = span.span.iter().position(|n| n == id).unwrap();
                        vec![&span.span[pos + 1]]
                    }
                }
                None => Vec::new(),
            },
        }
    }

    pub fn before(&self, id: &Id) -> Vec<&Id> {
        match self.befores.get(id) {
            Some(ns) => {
                let mut result: Vec<&Id> = ns.iter().collect();
                result.sort();
                result
            }
            None => Vec::new(),
        }
    }

    pub fn iter<'a, 'b>(&'a self, removed: &'b HashSet<Id>) -> TopoIter<'a, 'b> {
        TopoIter::new(self, removed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopoIter<'a, 'b> {
    topo: &'a Topo,
    waiting_stack: Vec<(Id, Vec<Id>)>,
    removed: &'b HashSet<Id>,
}

impl<'a, 'b> TopoIter<'a, 'b> {
    fn new(topo: &'a Topo, removed: &'b HashSet<Id>) -> Self {
        let mut iter = Self {
            topo,
            waiting_stack: Vec::new(),
            removed,
        };

        for root in topo.roots().iter().rev() {
            iter.push_waiting(*root);
        }

        iter
    }

    fn push_waiting(&mut self, n: Id) {
        let mut deps: Vec<Id> = self.topo.before(&n).into_iter().cloned().collect();
        deps.sort();
        deps.reverse();
        self.waiting_stack.push((n, deps));
    }
}

impl<'a, 'b> Iterator for TopoIter<'a, 'b> {
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
                if let Some(afters) = self.topo.afters.get(&n) {
                    // Sort by Id value
                    let mut afters_sorted: Vec<Id> = afters.clone();
                    afters_sorted.sort();
                    for s in afters_sorted.into_iter().rev() {
                        self.push_waiting(s);
                    }
                } else if let Some(span_id) = self.topo.span_index.get(&n) {
                    let span = &self.topo.spans[span_id];
                    // Check if n is the first element of this span
                    if span.first() == n {
                        // Push remaining span elements (skip first which is n)
                        for s in span.span.iter().skip(1).rev() {
                            self.waiting_stack.push((*s, Vec::new()));
                        }
                    }
                }
                // Return reference from the nodes set
                if let Some(id_ref) = self.topo.nodes.get(&n)
                    && !self.removed.contains(id_ref)
                {
                    return Some(id_ref);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(n: u8) -> Id {
        let mut id = [0u8; 32];
        id[0] = n;
        Id(id)
    }

    #[test]
    fn test_single() {
        let mut topo = Topo::default();

        topo.add_root(n(0));

        assert_eq!(Vec::from_iter(topo.iter(&Default::default())), vec![&n(0)]);
    }

    #[test]
    fn test_one_insert() {
        let mut topo = Topo::default();

        topo.add_root(n(0));
        topo.add_after(n(0), n(1), false);

        let removed = Default::default();
        let mut iter = topo.iter(&removed);
        assert_eq!(iter.next(), Some(&n(0)));
        assert_eq!(iter.next(), Some(&n(1)));
        assert_eq!(iter.next(), None);

        let mut topo = Topo::default();

        topo.add_root(n(1));
        topo.add_after(n(1), n(0), false);

        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            vec![&n(1), &n(0)]
        );
    }

    #[test]
    fn test_fork() {
        let mut topo = Topo::default();

        topo.add_root(n(0));
        topo.add_after(n(0), n(1), false);
        topo.add_after(n(0), n(2), false);

        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            vec![&n(0), &n(1), &n(2)]
        );
    }

    #[test]
    fn test_insert() {
        let mut topo = Topo::default();

        topo.add_root(n(0));
        topo.add_after(n(0), n(1), false);
        topo.add_before(n(1), n(2));

        assert_eq!(topo.after(&n(0)), vec![&n(1)]);
        assert!(topo.before(&n(0)).is_empty());
        assert!(dbg!(topo.after(&n(1))).is_empty());
        assert_eq!(topo.before(&n(1)), vec![&n(2)]);

        assert!(topo.after(&n(2)).is_empty());
        assert!(topo.before(&n(2)).is_empty());

        let removed = Default::default();
        let mut iter = topo.iter(&removed);
        assert_eq!(iter.next(), Some(&n(0)));
        assert_eq!(iter.next(), Some(&n(2)));
        assert_eq!(iter.next(), Some(&n(1)));

        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            vec![&n(0), &n(2), &n(1)]
        );
    }

    #[test]
    fn test_runs_remain_uninterrupted() {
        //   1 - 4
        //  /
        // 0
        //  \
        //   2 - 3

        // linearizes to 01423

        let mut topo = Topo::default();

        topo.add_root(n(0));
        topo.add_after(n(0), n(1), false);
        topo.add_after(n(1), n(4), false);
        topo.add_after(n(0), n(2), false);
        topo.add_after(n(2), n(3), false);

        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            vec![&n(0), &n(1), &n(4), &n(2), &n(3)]
        );
    }

    #[test]
    fn test_forks_remain_in_order_despite_prepend() {
        //   2
        //  /
        // 0 - 3
        //    /
        //   1
        //
        // linearizes to 0213

        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_after(n(0), n(2), false);
        topo.add_after(n(0), n(3), false);

        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            vec![&n(0), &n(2), &n(3)]
        );

        topo.add_before(n(3), n(1));

        let removed = Default::default();
        let mut iter = topo.iter(&removed);
        assert_eq!(iter.next(), Some(&n(0)));
        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            vec![&n(0), &n(2), &n(1), &n(3)]
        );
    }

    #[test]
    fn test_forks_remain_in_order_despite_prepend_case_2() {
        //   3
        //  /
        // 0 - 2
        //    /
        //   1
        //
        // linearizes to 0123

        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_after(n(0), n(2), false);
        topo.add_after(n(0), n(3), false);

        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            vec![&n(0), &n(2), &n(3)]
        );

        topo.add_before(n(2), n(1));

        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            vec![&n(0), &n(1), &n(2), &n(3)]
        );
    }

    #[test]
    fn test_adding_smaller_vertex_at_fork() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_after(n(0), n(2), false);
        topo.add_after(n(0), n(1), false);

        assert_eq!(topo.after(&n(0)), vec![&n(1), &n(2)]);

        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            vec![&n(0), &n(1), &n(2)]
        );
    }

    #[test]
    fn test_adding_smaller_vertex_at_full_fork() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_after(n(0), n(2), false);
        topo.add_after(n(0), n(3), false);
        topo.add_after(n(0), n(1), false);

        assert_eq!(topo.after(&n(0)), vec![&n(1), &n(2), &n(3)]);
        assert!(topo.after(&n(1)).is_empty());
        assert!(topo.after(&n(2)).is_empty());
        assert!(topo.after(&n(3)).is_empty());

        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            vec![&n(0), &n(1), &n(2), &n(3)]
        );
    }

    #[test]
    fn test_adding_concurrent_middle_vertex() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_after(n(0), n(1), false);
        topo.add_after(n(0), n(3), false);

        assert_eq!(topo.after(&n(0)), vec![&n(1), &n(3)]);

        topo.add_after(n(0), n(2), false);

        assert_eq!(topo.after(&n(0)), vec![&n(1), &n(2), &n(3)]);

        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            vec![&n(0), &n(1), &n(2), &n(3)]
        );
    }

    #[test]
    fn test_adding_concurrent_bigger_vertex() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_after(n(0), n(1), false);
        topo.add_after(n(0), n(2), false);

        assert_eq!(topo.after(&n(0)), vec![&n(1), &n(2)]);

        topo.add_after(n(0), n(3), false);

        assert_eq!(topo.after(&n(0)), vec![&n(1), &n(2), &n(3)]);

        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            vec![&n(0), &n(1), &n(2), &n(3)]
        );
    }

    #[test]
    fn test_insert_before_root() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_before(n(0), n(1));

        assert!(topo.after(&n(0)).is_empty());
        assert_eq!(topo.before(&n(0)), vec![&n(1)]);

        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            vec![&n(1), &n(0)]
        );
    }

    #[test]
    fn test_insert_before_root_twice() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_before(n(0), n(1));
        topo.add_before(n(0), n(2));

        assert!(topo.after(&n(0)).is_empty());
        assert_eq!(topo.before(&n(0)), vec![&n(1), &n(2)]);

        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            vec![&n(1), &n(2), &n(0)]
        );
    }

    #[test]
    fn test_insert_before_root_out_of_order() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_before(n(0), n(2));
        topo.add_before(n(0), n(3));
        topo.add_before(n(0), n(1));

        assert!(topo.after(&n(0)).is_empty());
        assert_eq!(topo.before(&n(0)), vec![&n(1), &n(2), &n(3)]);

        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            vec![&n(1), &n(2), &n(3), &n(0)]
        );
    }

    #[test]
    fn test_insert_between_root_and_element() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_after(n(0), n(1), false);
        topo.add_before(n(1), n(2));

        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            vec![&n(0), &n(2), &n(1)]
        );
    }

    #[test]
    fn test_insert_between_root_and_element_twice() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_after(n(0), n(1), false);
        topo.add_before(n(1), n(2));
        topo.add_before(n(1), n(3));

        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            vec![&n(0), &n(2), &n(3), &n(1)]
        );
    }

    #[test]
    fn test_concurrent_inserts() {
        let mut topo = Topo::default();

        topo.add_root(n(0));
        topo.add_root(n(1));

        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            vec![&n(0), &n(1)]
        );
    }

    #[test]
    fn test_concurrent_inserts_with_run() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_after(n(0), n(1), false);
        topo.add_root(n(2));

        assert_eq!(topo.after(&n(0)), vec![&n(1)]);

        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            vec![&n(0), &n(1), &n(2)]
        );

        let mut topo_different_order = Topo::default();
        topo_different_order.add_root(n(2));
        topo_different_order.add_root(n(0));
        topo_different_order.add_after(n(0), n(1), false);

        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            Vec::from_iter(topo_different_order.iter(&Default::default()))
        );
    }

    #[test]
    fn test_triple_concurrent_roots() {
        let mut topo = Topo::default();

        topo.add_root(n(0));
        topo.add_root(n(1));
        topo.add_root(n(2));

        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            vec![&n(0), &n(1), &n(2)]
        );
    }

    #[test]
    fn test_prepend_to_larger_branch() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_root(n(1));
        topo.add_root(n(2));
        topo.add_before(n(2), n(3));
        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            vec![&n(0), &n(1), &n(3), &n(2)]
        );
    }

    #[test]
    fn test_new_root_after_a_run() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_before(n(0), n(2));
        topo.add_root(n(1));

        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            vec![&n(2), &n(0), &n(1)]
        );

        let mut topo_different_order = Topo::default();
        topo_different_order.add_root(n(1));
        topo_different_order.add_root(n(0));
        topo_different_order.add_before(n(0), n(2));

        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            Vec::from_iter(topo_different_order.iter(&Default::default()))
        );
    }

    #[test]
    fn test_concurrent_prepend_and_append_seperated_by_a_node() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_after(n(0), n(1), false);
        topo.add_before(n(1), n(2));
        topo.add_after(n(0), n(3), false);

        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            vec![&n(0), &n(2), &n(1), &n(3)]
        );

        let mut topo_reverse_order = Topo::default();
        topo_reverse_order.add_root(n(0));
        topo_reverse_order.add_after(n(0), n(3), false);
        topo_reverse_order.add_after(n(0), n(1), false);
        topo_reverse_order.add_before(n(1), n(2));

        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            Vec::from_iter(topo_reverse_order.iter(&Default::default()))
        );
    }

    #[test]
    fn test_span_split() {
        let mut topo = Topo::default();
        topo.add_root(n(0));
        topo.add_after(n(0), n(1), false);
        topo.add_after(n(1), n(2), false);
        topo.add_after(n(2), n(3), false);
        topo.add_after(n(3), n(4), false);
        topo.add_after(n(2), n(5), false);
        topo.add_after(n(5), n(6), false);

        assert_eq!(
            Vec::from_iter(topo.iter(&Default::default())),
            vec![&n(0), &n(1), &n(2), &n(3), &n(4), &n(5), &n(6)]
        );
    }

    #[ignore]
    #[test]
    fn prop_order_preservation_across_forks() {
        // for nodes a, b
        // if there exists sequence s \in S, a,b \in s with a < b in s
        // then forall q \in S where a,b \in q, a < b in q

        // that is, if node `a` comes before `b` in some sequence, `a` comes before `b` in all sequences.
    }
}
