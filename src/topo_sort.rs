use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use crate::hashseq::RunPosition;
use crate::{Id, Run};

#[derive(Debug, Default, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Topo {
    // All node IDs for stable reference storage
    pub nodes: BTreeSet<Id>,
}

impl Topo {
    pub fn is_causally_before(
        &self,
        a: &Id,
        b: &Id,
        afters: &HashMap<Id, Vec<Id>>,
        befores: &HashMap<Id, Vec<Id>>,
        runs: &HashMap<Id, Run>,
        run_index: &HashMap<Id, RunPosition>,
    ) -> bool {
        let mut seen = BTreeSet::new();
        let mut boundary: Vec<Id> = Self::after(a, afters, runs, run_index)
            .into_iter()
            .cloned()
            .collect();
        while let Some(n) = boundary.pop() {
            if &n == b {
                return true;
            }

            seen.insert(n);
            boundary.extend(
                Self::after(&n, afters, runs, run_index)
                    .into_iter()
                    .cloned()
                    .filter(|x| !seen.contains(x)),
            );
            if &n != a {
                boundary.extend(
                    Self::before_from_map(&n, befores)
                        .into_iter()
                        .cloned()
                        .filter(|x| !seen.contains(x)),
                );
            }
        }

        false
    }

    pub fn add_root(&mut self, node: Id) {
        self.nodes.insert(node);
    }

    pub fn add_before(&mut self, anchor: Id, node: Id, befores: &mut HashMap<Id, Vec<Id>>) {
        self.nodes.insert(node);
        befores.entry(anchor).or_default().push(node);
    }

    /// Get nodes that come after this one. Uses both explicit afters and run data.
    pub fn after<'a>(
        id: &Id,
        afters: &'a HashMap<Id, Vec<Id>>,
        runs: &'a HashMap<Id, Run>,
        run_index: &'a HashMap<Id, RunPosition>,
    ) -> Vec<&'a Id> {
        match afters.get(id) {
            Some(ns) => {
                let mut result: Vec<&Id> = ns.iter().collect();
                result.sort();
                result
            }
            None => {
                // Check if this node is in a run and not the last element
                if let Some(run_pos) = run_index.get(id) {
                    let run = &runs[&run_pos.run_id];
                    let run_len = run.len();
                    if run_pos.position + 1 < run_len {
                        // There's a next element in this run - get its ID from run_index
                        if let Some((next_id, _)) = run_index.iter().find(|(_, pos)| {
                            pos.run_id == run_pos.run_id && pos.position == run_pos.position + 1
                        }) {
                            return vec![next_id];
                        }
                    }
                }
                Vec::new()
            }
        }
    }

    pub fn before_from_map<'a>(id: &Id, befores: &'a HashMap<Id, Vec<Id>>) -> Vec<&'a Id> {
        match befores.get(id) {
            Some(ns) => {
                let mut result: Vec<&Id> = ns.iter().collect();
                result.sort();
                result
            }
            None => Vec::new(),
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hashseq::{get_afters, before_from_map, TopoIter, RunPosition};

    fn n(n: u8) -> Id {
        let mut id = [0u8; 32];
        id[0] = n;
        Id(id)
    }

    // Helper to create empty runs/run_index/run_elements for tests
    fn empty_run_data() -> (HashMap<Id, RunPosition>, HashMap<Id, Vec<Id>>) {
        (HashMap::new(), HashMap::new())
    }

    // Helper function to add an after relationship
    fn add_after(topo: &mut Topo, afters: &mut HashMap<Id, Vec<Id>>, anchor: Id, node: Id) {
        topo.nodes.insert(node);
        afters.entry(anchor).or_default().push(node);
    }

    // Helper function to call get_afters with empty runs
    fn after_no_runs(afters: &HashMap<Id, Vec<Id>>, id: &Id) -> Vec<Id> {
        let (run_index, run_elements) = empty_run_data();
        get_afters(id, afters, &run_index, &run_elements)
            .into_iter()
            .cloned()
            .collect()
    }

    #[test]
    fn test_single() {
        let mut topo = Topo::default();
        let mut roots = BTreeSet::new();
        let afters = HashMap::new();
        let befores = HashMap::new();
        let (run_index, run_elements) = empty_run_data();

        topo.add_root(n(0));
        roots.insert(n(0));

        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
            vec![&n(0)]
        );
    }

    #[test]
    fn test_one_insert() {
        let mut topo = Topo::default();
        let mut roots = BTreeSet::new();
        let mut afters = HashMap::new();
        let befores = HashMap::new();
        let (run_index, run_elements) = empty_run_data();

        topo.add_root(n(0));
        roots.insert(n(0));
        add_after(&mut topo, &mut afters, n(0), n(1));

        let removed = Default::default();
        let mut iter = TopoIter::new(&topo.nodes, &roots, &removed, &afters, &befores, &run_index, &run_elements);
        assert_eq!(iter.next(), Some(&n(0)));
        assert_eq!(iter.next(), Some(&n(1)));
        assert_eq!(iter.next(), None);

        let mut topo = Topo::default();
        let mut roots = BTreeSet::new();
        let mut afters = HashMap::new();
        let befores = HashMap::new();

        topo.add_root(n(1));
        roots.insert(n(1));
        add_after(&mut topo, &mut afters, n(1), n(0));

        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
            vec![&n(1), &n(0)]
        );
    }

    #[test]
    fn test_fork() {
        let mut topo = Topo::default();
        let mut roots = BTreeSet::new();
        let mut afters = HashMap::new();
        let befores = HashMap::new();
        let (run_index, run_elements) = empty_run_data();

        topo.add_root(n(0));
        roots.insert(n(0));
        add_after(&mut topo, &mut afters, n(0), n(1));
        add_after(&mut topo, &mut afters, n(0), n(2));

        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
            vec![&n(0), &n(1), &n(2)]
        );
    }

    #[test]
    fn test_insert() {
        let mut topo = Topo::default();
        let mut roots = BTreeSet::new();
        let mut afters = HashMap::new();
        let mut befores = HashMap::new();
        let (run_index, run_elements) = empty_run_data();

        topo.add_root(n(0));
        roots.insert(n(0));
        add_after(&mut topo, &mut afters, n(0), n(1));
        topo.add_before(n(1), n(2), &mut befores);

        assert_eq!(after_no_runs(&afters, &n(0)), vec![n(1)]);
        assert!(before_from_map(&n(0), &befores).is_empty());
        assert!(after_no_runs(&afters, &n(1)).is_empty());
        assert_eq!(before_from_map(&n(1), &befores), vec![&n(2)]);

        assert!(after_no_runs(&afters, &n(2)).is_empty());
        assert!(before_from_map(&n(2), &befores).is_empty());

        let removed = Default::default();
        let mut iter = TopoIter::new(&topo.nodes, &roots, &removed, &afters, &befores, &run_index, &run_elements);
        assert_eq!(iter.next(), Some(&n(0)));
        assert_eq!(iter.next(), Some(&n(2)));
        assert_eq!(iter.next(), Some(&n(1)));

        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
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
        let mut roots = BTreeSet::new();
        let mut afters = HashMap::new();
        let befores = HashMap::new();
        let (run_index, run_elements) = empty_run_data();

        topo.add_root(n(0));
        roots.insert(n(0));
        add_after(&mut topo, &mut afters, n(0), n(1));
        add_after(&mut topo, &mut afters, n(1), n(4));
        add_after(&mut topo, &mut afters, n(0), n(2));
        add_after(&mut topo, &mut afters, n(2), n(3));

        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
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
        let mut roots = BTreeSet::new();
        let mut afters = HashMap::new();
        let mut befores = HashMap::new();
        let (run_index, run_elements) = empty_run_data();

        topo.add_root(n(0));
        roots.insert(n(0));
        add_after(&mut topo, &mut afters, n(0), n(2));
        add_after(&mut topo, &mut afters, n(0), n(3));

        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
            vec![&n(0), &n(2), &n(3)]
        );

        topo.add_before(n(3), n(1), &mut befores);

        let removed = Default::default();
        let mut iter = TopoIter::new(&topo.nodes, &roots, &removed, &afters, &befores, &run_index, &run_elements);
        assert_eq!(iter.next(), Some(&n(0)));
        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
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
        let mut roots = BTreeSet::new();
        let mut afters = HashMap::new();
        let mut befores = HashMap::new();
        let (run_index, run_elements) = empty_run_data();

        topo.add_root(n(0));
        roots.insert(n(0));
        add_after(&mut topo, &mut afters, n(0), n(2));
        add_after(&mut topo, &mut afters, n(0), n(3));

        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
            vec![&n(0), &n(2), &n(3)]
        );

        topo.add_before(n(2), n(1), &mut befores);

        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
            vec![&n(0), &n(1), &n(2), &n(3)]
        );
    }

    #[test]
    fn test_adding_smaller_vertex_at_fork() {
        let mut topo = Topo::default();
        let mut roots = BTreeSet::new();
        let mut afters = HashMap::new();
        let befores = HashMap::new();
        let (run_index, run_elements) = empty_run_data();

        topo.add_root(n(0));
        roots.insert(n(0));
        add_after(&mut topo, &mut afters, n(0), n(2));
        add_after(&mut topo, &mut afters, n(0), n(1));

        assert_eq!(after_no_runs(&afters, &n(0)), vec![n(1), n(2)]);

        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
            vec![&n(0), &n(1), &n(2)]
        );
    }

    #[test]
    fn test_adding_smaller_vertex_at_full_fork() {
        let mut topo = Topo::default();
        let mut roots = BTreeSet::new();
        let mut afters = HashMap::new();
        let befores = HashMap::new();
        let (run_index, run_elements) = empty_run_data();

        topo.add_root(n(0));
        roots.insert(n(0));
        add_after(&mut topo, &mut afters, n(0), n(2));
        add_after(&mut topo, &mut afters, n(0), n(3));
        add_after(&mut topo, &mut afters, n(0), n(1));

        assert_eq!(after_no_runs(&afters, &n(0)), vec![n(1), n(2), n(3)]);
        assert!(after_no_runs(&afters, &n(1)).is_empty());
        assert!(after_no_runs(&afters, &n(2)).is_empty());
        assert!(after_no_runs(&afters, &n(3)).is_empty());

        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
            vec![&n(0), &n(1), &n(2), &n(3)]
        );
    }

    #[test]
    fn test_adding_concurrent_middle_vertex() {
        let mut topo = Topo::default();
        let mut roots = BTreeSet::new();
        let mut afters = HashMap::new();
        let befores = HashMap::new();
        let (run_index, run_elements) = empty_run_data();

        topo.add_root(n(0));
        roots.insert(n(0));
        add_after(&mut topo, &mut afters, n(0), n(1));
        add_after(&mut topo, &mut afters, n(0), n(3));

        assert_eq!(after_no_runs(&afters, &n(0)), vec![n(1), n(3)]);

        add_after(&mut topo, &mut afters, n(0), n(2));

        assert_eq!(after_no_runs(&afters, &n(0)), vec![n(1), n(2), n(3)]);

        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
            vec![&n(0), &n(1), &n(2), &n(3)]
        );
    }

    #[test]
    fn test_adding_concurrent_bigger_vertex() {
        let mut topo = Topo::default();
        let mut roots = BTreeSet::new();
        let mut afters = HashMap::new();
        let befores = HashMap::new();
        let (run_index, run_elements) = empty_run_data();

        topo.add_root(n(0));
        roots.insert(n(0));
        add_after(&mut topo, &mut afters, n(0), n(1));
        add_after(&mut topo, &mut afters, n(0), n(2));

        assert_eq!(after_no_runs(&afters, &n(0)), vec![n(1), n(2)]);

        add_after(&mut topo, &mut afters, n(0), n(3));

        assert_eq!(after_no_runs(&afters, &n(0)), vec![n(1), n(2), n(3)]);

        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
            vec![&n(0), &n(1), &n(2), &n(3)]
        );
    }

    #[test]
    fn test_insert_before_root() {
        let mut topo = Topo::default();
        let mut roots = BTreeSet::new();
        let afters = HashMap::new();
        let mut befores = HashMap::new();
        let (run_index, run_elements) = empty_run_data();

        topo.add_root(n(0));
        roots.insert(n(0));
        topo.add_before(n(0), n(1), &mut befores);

        assert!(after_no_runs(&afters, &n(0)).is_empty());
        assert_eq!(before_from_map(&n(0), &befores), vec![&n(1)]);

        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
            vec![&n(1), &n(0)]
        );
    }

    #[test]
    fn test_insert_before_root_twice() {
        let mut topo = Topo::default();
        let mut roots = BTreeSet::new();
        let afters = HashMap::new();
        let mut befores = HashMap::new();
        let (run_index, run_elements) = empty_run_data();

        topo.add_root(n(0));
        roots.insert(n(0));
        topo.add_before(n(0), n(1), &mut befores);
        topo.add_before(n(0), n(2), &mut befores);

        assert!(after_no_runs(&afters, &n(0)).is_empty());
        assert_eq!(before_from_map(&n(0), &befores), vec![&n(1), &n(2)]);

        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
            vec![&n(1), &n(2), &n(0)]
        );
    }

    #[test]
    fn test_insert_before_root_out_of_order() {
        let mut topo = Topo::default();
        let mut roots = BTreeSet::new();
        let afters = HashMap::new();
        let mut befores = HashMap::new();
        let (run_index, run_elements) = empty_run_data();

        topo.add_root(n(0));
        roots.insert(n(0));
        topo.add_before(n(0), n(2), &mut befores);
        topo.add_before(n(0), n(3), &mut befores);
        topo.add_before(n(0), n(1), &mut befores);

        assert!(after_no_runs(&afters, &n(0)).is_empty());
        assert_eq!(
            before_from_map(&n(0), &befores),
            vec![&n(1), &n(2), &n(3)]
        );

        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
            vec![&n(1), &n(2), &n(3), &n(0)]
        );
    }

    #[test]
    fn test_insert_between_root_and_element() {
        let mut topo = Topo::default();
        let mut roots = BTreeSet::new();
        let mut afters = HashMap::new();
        let mut befores = HashMap::new();
        let (run_index, run_elements) = empty_run_data();

        topo.add_root(n(0));
        roots.insert(n(0));
        add_after(&mut topo, &mut afters, n(0), n(1));
        topo.add_before(n(1), n(2), &mut befores);

        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
            vec![&n(0), &n(2), &n(1)]
        );
    }

    #[test]
    fn test_insert_between_root_and_element_twice() {
        let mut topo = Topo::default();
        let mut roots = BTreeSet::new();
        let mut afters = HashMap::new();
        let mut befores = HashMap::new();
        let (run_index, run_elements) = empty_run_data();

        topo.add_root(n(0));
        roots.insert(n(0));
        add_after(&mut topo, &mut afters, n(0), n(1));
        topo.add_before(n(1), n(2), &mut befores);
        topo.add_before(n(1), n(3), &mut befores);

        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
            vec![&n(0), &n(2), &n(3), &n(1)]
        );
    }

    #[test]
    fn test_concurrent_inserts() {
        let mut topo = Topo::default();
        let mut roots = BTreeSet::new();
        let afters = HashMap::new();
        let befores = HashMap::new();
        let (run_index, run_elements) = empty_run_data();

        topo.add_root(n(0));
        roots.insert(n(0));
        topo.add_root(n(1));
        roots.insert(n(1));

        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
            vec![&n(0), &n(1)]
        );
    }

    #[test]
    fn test_concurrent_inserts_with_run() {
        let mut topo = Topo::default();
        let mut roots = BTreeSet::new();
        let mut afters = HashMap::new();
        let befores = HashMap::new();
        let (run_index, run_elements) = empty_run_data();

        topo.add_root(n(0));
        roots.insert(n(0));
        add_after(&mut topo, &mut afters, n(0), n(1));
        topo.add_root(n(2));
        roots.insert(n(2));

        assert_eq!(after_no_runs(&afters, &n(0)), vec![n(1)]);

        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
            vec![&n(0), &n(1), &n(2)]
        );

        let mut topo_different_order = Topo::default();
        let mut roots2 = BTreeSet::new();
        let mut afters2 = HashMap::new();
        let befores2 = HashMap::new();
        topo_different_order.add_root(n(2));
        roots2.insert(n(2));
        topo_different_order.add_root(n(0));
        roots2.insert(n(0));
        add_after(&mut topo_different_order, &mut afters2, n(0), n(1));

        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
            Vec::from_iter(TopoIter::new(
                &topo_different_order.nodes,
                &roots2,
                &Default::default(),
                &afters2,
                &befores2,
                &run_index,
                &run_elements
            ))
        );
    }

    #[test]
    fn test_triple_concurrent_roots() {
        let mut topo = Topo::default();
        let mut roots = BTreeSet::new();
        let afters = HashMap::new();
        let befores = HashMap::new();
        let (run_index, run_elements) = empty_run_data();

        topo.add_root(n(0));
        roots.insert(n(0));
        topo.add_root(n(1));
        roots.insert(n(1));
        topo.add_root(n(2));
        roots.insert(n(2));

        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
            vec![&n(0), &n(1), &n(2)]
        );
    }

    #[test]
    fn test_prepend_to_larger_branch() {
        let mut topo = Topo::default();
        let mut roots = BTreeSet::new();
        let afters = HashMap::new();
        let mut befores = HashMap::new();
        let (run_index, run_elements) = empty_run_data();

        topo.add_root(n(0));
        roots.insert(n(0));
        topo.add_root(n(1));
        roots.insert(n(1));
        topo.add_root(n(2));
        roots.insert(n(2));
        topo.add_before(n(2), n(3), &mut befores);
        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
            vec![&n(0), &n(1), &n(3), &n(2)]
        );
    }

    #[test]
    fn test_new_root_after_a_run() {
        let mut topo = Topo::default();
        let mut roots = BTreeSet::new();
        let afters = HashMap::new();
        let mut befores = HashMap::new();
        let (run_index, run_elements) = empty_run_data();

        topo.add_root(n(0));
        roots.insert(n(0));
        topo.add_before(n(0), n(2), &mut befores);
        topo.add_root(n(1));
        roots.insert(n(1));

        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
            vec![&n(2), &n(0), &n(1)]
        );

        let mut topo_different_order = Topo::default();
        let mut roots2 = BTreeSet::new();
        let afters2 = HashMap::new();
        let mut befores2 = HashMap::new();
        topo_different_order.add_root(n(1));
        roots2.insert(n(1));
        topo_different_order.add_root(n(0));
        roots2.insert(n(0));
        topo_different_order.add_before(n(0), n(2), &mut befores2);

        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
            Vec::from_iter(TopoIter::new(
                &topo_different_order.nodes,
                &roots2,
                &Default::default(),
                &afters2,
                &befores2,
                &run_index,
                &run_elements
            ))
        );
    }

    #[test]
    fn test_concurrent_prepend_and_append_seperated_by_a_node() {
        let mut topo = Topo::default();
        let mut roots = BTreeSet::new();
        let mut afters = HashMap::new();
        let mut befores = HashMap::new();
        let (run_index, run_elements) = empty_run_data();

        topo.add_root(n(0));
        roots.insert(n(0));
        add_after(&mut topo, &mut afters, n(0), n(1));
        topo.add_before(n(1), n(2), &mut befores);
        add_after(&mut topo, &mut afters, n(0), n(3));

        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
            vec![&n(0), &n(2), &n(1), &n(3)]
        );

        let mut topo_reverse_order = Topo::default();
        let mut roots2 = BTreeSet::new();
        let mut afters2 = HashMap::new();
        let mut befores2 = HashMap::new();
        topo_reverse_order.add_root(n(0));
        roots2.insert(n(0));
        add_after(&mut topo_reverse_order, &mut afters2, n(0), n(3));
        add_after(&mut topo_reverse_order, &mut afters2, n(0), n(1));
        topo_reverse_order.add_before(n(1), n(2), &mut befores2);

        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
            Vec::from_iter(TopoIter::new(
                &topo_reverse_order.nodes,
                &roots2,
                &Default::default(),
                &afters2,
                &befores2,
                &run_index,
                &run_elements
            ))
        );
    }

    #[test]
    fn test_span_split() {
        let mut topo = Topo::default();
        let mut roots = BTreeSet::new();
        let mut afters = HashMap::new();
        let befores = HashMap::new();
        let (run_index, run_elements) = empty_run_data();

        topo.add_root(n(0));
        roots.insert(n(0));
        add_after(&mut topo, &mut afters, n(0), n(1));
        add_after(&mut topo, &mut afters, n(1), n(2));
        add_after(&mut topo, &mut afters, n(2), n(3));
        add_after(&mut topo, &mut afters, n(3), n(4));
        add_after(&mut topo, &mut afters, n(2), n(5));
        add_after(&mut topo, &mut afters, n(5), n(6));

        assert_eq!(
            Vec::from_iter(TopoIter::new(&topo.nodes, &roots, &Default::default(), &afters, &befores, &run_index, &run_elements)),
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
