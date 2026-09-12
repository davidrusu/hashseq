//! HashKv: the key-value projection (HASHKV_SPEC.md).
//!
//! One register per key id: `heads(k)` = the puts on `k` not named in any
//! other put-on-`k`'s `overwrites`. A conflict is `|heads| > 1` —
//! non-supersession, surfaced as MVR. Resolution confers nothing on the
//! conflicted path: reads expose every head; there is no LWW and no
//! id-order winner (FRAMEWORK.md locality dividing line).
//!
//! Keys and values are ids of content-addressed value artifacts
//! (or op-node / origin ids — links). Artifact bytes ride a side store;
//! an absent artifact is the `pending` state, never papered over.

use std::collections::{BTreeSet, BinaryHeap};

use rustc_hash::FxHashMap;

use crate::delivery::Delivery;
use crate::hashseq::IdMap;
use crate::placement::PlacementRegister;
use crate::value::{TOMBSTONE, Value};
use crate::{HashNode, Id, Op};

/// A key's register state: the live put heads, in id order.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct KeyState {
    /// Put node ids not superseded by any other put on this key. Sorted by
    /// id (convergence-safe order; never replica-local).
    heads: Vec<Id>,
}

/// What a read sees for a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Read {
    /// No live head (never written, or the single head is a tombstone).
    Absent,
    /// Exactly one live head with this value id.
    One(Id),
    /// Multiple heads — a conflict, surfaced whole (MVR). Value ids in
    /// head-id order. The caller decides display policy; nothing here picks
    /// a winner (grinding and fabricated conflicts must buy nothing).
    Conflict(Vec<Id>),
}

#[derive(Debug, Clone)]
pub struct HashKv {
    /// The origin anchor: the arbitrary 32-byte value this object's
    /// creator chose (often another op's id — the composition convention).
    origin: Id,
    /// Applied puts by node id (the register history — retention: keep all;
    /// the supersession spine is what the read rules walk).
    pub(crate) nodes: IdMap<HashNode>,
    /// key value-id -> register. Keyed by the key's id — already a BLAKE3
    /// output, so FxHash is safe (the HASHKV_SPEC key rule: adversarial key
    /// bytes cost their author derivation, never a table).
    keys: IdMap<KeyState>,
    /// Value-artifact side store: artifact bytes by value id, for the ids
    /// this replica has seen bytes for. Reads without bytes are `pending`.
    /// Standalone this is the whole store; inside a `HashWeb` it is the
    /// per-object view of the store-wide one — the web mirrors in the
    /// artifacts each delivered put names (`HashKv::hydrate`), and the
    /// canonical snapshot carries the union.
    pub(crate) values: IdMap<Vec<u8>>,
    pub(crate) tips: BTreeSet<Id>,
    /// Applied node ids in apply order — the map's arena, append-only
    /// (orphaned and gated nodes never enter). Deps precede dependents, so
    /// the position is the clock walk's order (`delta_for`).
    pub(crate) order: Vec<Id>,
    /// Arena position by id (`order[slot[id]] == id`).
    pub(crate) slot: IdMap<u32>,
    /// Delta sync is on (`HashWeb::enable_delta_sync`): minted small
    /// artifacts are tracked in `new_artifacts`. Deltas themselves are a
    /// DAG diff against a peer clock and need no switch.
    pub(crate) delta_sync: bool,
    /// Small artifacts minted here since the last drain — the kv-level
    /// half of `HashWeb::new_artifacts`; recorded only while delta sync
    /// is on, drained by `HashWeb::take_new_artifacts`.
    pub(crate) new_artifacts: Vec<Id>,
    /// The containment register — where does this object live
    /// (PLACEMENT_SPEC.md). `Place` is valid in any object kind.
    pub(crate) placement: PlacementRegister,
    /// Orphans (non-map ops are refused and dropped — the edge table).
    pub(crate) delivery: Delivery,
}

impl PartialEq for HashKv {
    fn eq(&self, other: &Self) -> bool {
        self.tips == other.tips
    }
}
impl Eq for HashKv {}

impl Default for HashKv {
    fn default() -> Self {
        Self::new(Id::default())
    }
}

impl HashKv {
    pub fn new(origin: Id) -> Self {
        let mut kv = Self {
            origin,
            nodes: IdMap::default(),
            keys: IdMap::default(),
            values: IdMap::default(),
            tips: BTreeSet::new(),
            order: Vec::new(),
            slot: IdMap::default(),
            delta_sync: false,
            new_artifacts: Vec::new(),
            placement: PlacementRegister::default(),
            delivery: Delivery::default(),
        };
        // The origin is axiomatically present: the map's frontier begins at
        // it, so a fresh map's first put pins {origin}.
        kv.tips.insert(origin);
        kv
    }

    pub fn origin(&self) -> Id {
        self.origin
    }

    pub fn tips(&self) -> &BTreeSet<Id> {
        &self.tips
    }

    pub(crate) fn contains_node(&self, id: &Id) -> bool {
        *id == self.origin || self.nodes.contains_key(id)
    }

    /// Store a value artifact's bytes (resolves `pending` reads of its id).
    pub fn provide_value(&mut self, v: &Value) -> Id {
        let id = v.value_id();
        if let std::collections::hash_map::Entry::Vacant(e) = self.values.entry(id) {
            let bytes = v.encoded();
            if self.delta_sync && bytes.len() <= crate::encoding::WIRE_ARTIFACT_MAX {
                self.new_artifacts.push(id);
            }
            e.insert(bytes);
        }
        id
    }

    /// Mirror into this object's store the artifacts `node` names (a put's
    /// key and value) that `store` holds — the web-side hydration that
    /// keeps `get`/`resolve` on an object inside a `HashWeb` from missing
    /// values the web holds. Bounded by what the node references.
    pub(crate) fn hydrate(&mut self, node: &HashNode, store: &IdMap<Vec<u8>>) {
        let Op::Put { key, value, .. } = &node.op else {
            return;
        };
        for id in [key, value] {
            if !self.values.contains_key(id)
                && let Some(bytes) = store.get(id)
            {
                self.values.insert(*id, bytes.clone());
            }
        }
    }

    /// `hydrate` over every node this object holds (applied and orphaned).
    pub(crate) fn hydrate_all(&mut self, store: &IdMap<Vec<u8>>) {
        let nodes: Vec<HashNode> = self
            .nodes
            .values()
            .cloned()
            .chain(self.delivery.orphans().map(|(_, n)| n.clone()))
            .collect();
        for node in &nodes {
            self.hydrate(node, store);
        }
    }

    /// Resolve a value id to its artifact, if this replica holds the bytes.
    /// `None` = pending/unavailable (or the id names an op/origin — a link).
    pub fn resolve(&self, value_id: &Id) -> Option<Value> {
        self.values.get(value_id).and_then(|b| Value::decode(b))
    }

    // ---- local authoring ----

    /// Write `key = value`, superseding the heads this replica sees.
    /// Returns the applied node (for re-broadcast).
    pub fn put(&mut self, key: Value, value: Value) -> HashNode {
        let key_id = self.provide_value(&key);
        let value_id = self.provide_value(&value);
        self.put_ids(key_id, value_id)
    }

    /// Build (without applying) the put node this replica would author.
    pub fn make_put(&self, key: Id, value: Id) -> HashNode {
        let overwrites: BTreeSet<Id> = self
            .keys
            .get(&key)
            .map(|ks| ks.heads.iter().copied().collect())
            .unwrap_or_default();
        // pins = frontier ∖ named (normalized storage of refs = pins ∪ named)
        let pins: BTreeSet<Id> = BTreeSet::from_iter(self.tips.difference(&overwrites).cloned());
        HashNode {
            pins,
            op: Op::Put {
                key,
                value,
                overwrites,
            },
        }
    }

    /// `put` by raw ids (links, already-provided artifacts, tombstone).
    pub fn put_ids(&mut self, key: Id, value: Id) -> HashNode {
        let node = self.make_put(key, value);
        self.apply_with_id(node.id(), node.clone());
        node
    }

    /// Delete a key: a put of the tombstone artifact.
    pub fn del(&mut self, key: Value) -> HashNode {
        let key_id = self.provide_value(&key);
        self.put_ids(key_id, *TOMBSTONE)
    }

    // ---- reads (arbitration happens here, per Law II) ----

    /// The live head set of `key` (put node ids, id-ordered).
    pub fn heads(&self, key: &Id) -> &[Id] {
        self.keys
            .get(key)
            .map(|k| k.heads.as_slice())
            .unwrap_or(&[])
    }

    /// MVR read: the value ids of the live heads.
    pub fn read_id(&self, key: &Id) -> Read {
        let heads = self.heads(key);
        let live: Vec<Id> = heads
            .iter()
            .filter_map(|h| match &self.nodes[h].op {
                Op::Put { value, .. } => Some(*value),
                _ => unreachable!("heads hold puts"),
            })
            .collect();
        match live.as_slice() {
            [] => Read::Absent,
            [one] if *one == *TOMBSTONE => Read::Absent,
            [one] => Read::One(*one),
            _ => Read::Conflict(live),
        }
    }

    /// Convenience read by key value.
    pub fn read(&self, key: &Value) -> Read {
        self.read_id(&key.value_id())
    }

    /// Convenience single-value get: `Some(value)` iff exactly one live,
    /// resolvable, non-tombstone head. Conflicts and pending values are
    /// **not** collapsed — use `read`/`resolve` to surface them.
    pub fn get(&self, key: &Value) -> Option<Value> {
        match self.read(key) {
            Read::One(vid) => self.resolve(&vid),
            _ => None,
        }
    }

    /// Iterate live keys (key value ids) — id order is the only total order
    /// (key bytes can be pending); display ordering is a render concern.
    pub fn keys(&self) -> impl Iterator<Item = &Id> {
        self.keys
            .iter()
            .filter(|(k, _)| !matches!(self.read_id(k), Read::Absent))
            .map(|(k, _)| k)
    }

    // ---- apply ----

    pub fn apply(&mut self, node: HashNode) -> bool {
        let id = node.id();
        self.apply_with_id(id, node)
    }

    /// Apply with a pre-computed id (`id` must be the node's true hash).
    /// Iterative worklist: applying a node wakes exactly the orphans waiting
    /// on its id.
    /// Returns whether `node` was news (see `HashSeq::apply_with_id`).
    pub fn apply_with_id(&mut self, id: Id, node: HashNode) -> bool {
        debug_assert_eq!(id, node.id(), "apply_with_id called with a wrong id");
        if self.contains_node(&id) {
            return false;
        }
        let mut queue: Vec<(Id, HashNode)> = Vec::new();
        let news = self.orphan_or_dispatch(id, node, &mut queue);
        while let Some((id, node)) = queue.pop() {
            self.orphan_or_dispatch(id, node, &mut queue);
        }
        news
    }

    /// The ref an orphan is keyed on: the first ref in `iter_refs` order
    /// that is not applied here (see `HashSeq::canonical_orphan_dependency`
    /// for why that key is stable while the node stays orphaned).
    pub(crate) fn canonical_orphan_dependency(&self, node: &HashNode) -> Option<Id> {
        node.iter_refs().find(|d| !self.contains_node(d)).copied()
    }

    /// One step of the worklist: orphan `node` on its canonical missing
    /// ref (a re-delivered orphan lands in the bucket it is already in),
    /// or interpret it and wake its waiters. A refused node wakes nothing
    /// — its dependents stay orphaned (the refusal cascades).
    fn orphan_or_dispatch(
        &mut self,
        id: Id,
        node: HashNode,
        queue: &mut Vec<(Id, HashNode)>,
    ) -> bool {
        if let Some(key) = self.canonical_orphan_dependency(&node) {
            return self.delivery.orphan(key, id, node);
        }
        // A refused node (Err) is dropped; its dependents stay orphaned.
        let applied = self.interpret(id, node).is_ok();
        if applied {
            self.delivery.wake(&id, queue);
        }
        applied
    }

    /// Interpret one node whose refs are all applied — this projection's
    /// edge-table rows. `Err` hands the node back; the caller drops it.
    #[allow(clippy::result_large_err)]
    fn interpret(&mut self, id: Id, node: HashNode) -> Result<(), HashNode> {
        // Place is admitted in any object kind (PLACEMENT_SPEC.md): the
        // containment register concerns the object's placement, not its
        // content projection. placed_at is a commitment — nothing to gate.
        if let Op::Place {
            placed_at,
            overwrites,
        } = &node.op
        {
            for r in node.iter_refs() {
                self.tips.remove(r);
            }
            self.tips.insert(id);
            self.placement.apply(id, *placed_at, overwrites.clone());
            self.admit(id, node);
            return Ok(());
        }

        // Edge-table gate: only map ops are admitted here (a seq op in a
        // Map is ill-typed — stable, permanent).
        let Op::Put {
            key, overwrites, ..
        } = &node.op
        else {
            return Err(node);
        };
        let key = *key;

        // tips update: everything referenced leaves the frontier.
        for r in node.iter_refs() {
            self.tips.remove(r);
        }
        self.tips.insert(id);

        // heads(k) = heads(k) − overwrites(u) ∪ {u}, with the definitional
        // same-key filter: we only touch THIS key's head list, so an
        // overwrite naming a put on another key (or a non-put) simply isn't
        // here — ignored, never an error (it cannot corrupt another
        // register).
        let ks = self.keys.entry(key).or_default();
        ks.heads.retain(|h| !overwrites.contains(h));
        let pos = ks.heads.binary_search(&id).unwrap_or_else(|p| p);
        ks.heads.insert(pos, id);

        self.admit(id, node);
        Ok(())
    }

    /// Store an admitted node: the register history plus the arena slot.
    fn admit(&mut self, id: Id, node: HashNode) {
        self.nodes.insert(id, node);
        self.slot.insert(id, self.order.len() as u32);
        self.order.push(id);
    }

    /// Every applied node in apply order (causally safe: each node's refs
    /// precede it).
    pub fn nodes_in_apply_order(&self) -> impl Iterator<Item = (Id, HashNode)> + '_ {
        self.order.iter().map(|id| (*id, self.nodes[id].clone()))
    }

    /// This replica's clock for the object: what a peer that has
    /// everything we have holds. Sent as the hello, and kept as "last
    /// sent" for a peer after a drain.
    pub fn clock(&self) -> crate::Clock {
        crate::Clock(self.tips().clone())
    }

    /// The delta for the peer behind `clock`: every applied node outside
    /// the peer's causal closure, in apply order (see `HashSeq::delta_for`).
    /// Same sweep as `HashSeq::delta_for` over the map's arena (`order`,
    /// apply order; `slot` for the reverse lookup). The origin has no
    /// slot, so it is never walked or shipped.
    pub fn delta_for(&self, clock: &crate::Clock) -> Vec<HashNode> {
        const OURS: u8 = 1;
        const PEER: u8 = 2;
        let ours = self.tips();
        let mut heap: BinaryHeap<usize> = BinaryHeap::new();
        let mut colour: FxHashMap<usize, u8> = FxHashMap::default();
        let mut pending_ours = 0usize;
        for i in ours.iter().filter_map(|t| self.walk_idx(t)) {
            if colour.insert(i, OURS).is_none() {
                heap.push(i);
                pending_ours += 1;
            }
        }
        for i in clock.0.iter().filter_map(|t| self.walk_idx(t)) {
            match colour.insert(i, PEER) {
                None => heap.push(i),
                Some(OURS) => pending_ours -= 1,
                Some(_) => {}
            }
        }
        let mut out = Vec::new();
        let mut deps = Vec::new();
        while pending_ours > 0 {
            let Some(i) = heap.pop() else { break };
            deps.clear();
            self.walk_deps(i, &mut deps);
            if colour[&i] & PEER != 0 {
                // Deps have lower handles, so they are still in the heap
                // or unseen — never already popped.
                for &d in &deps {
                    match colour.insert(d, PEER) {
                        None => heap.push(d),
                        Some(OURS) => pending_ours -= 1,
                        Some(_) => {}
                    }
                }
            } else {
                pending_ours -= 1;
                out.push(i);
                for &d in &deps {
                    match colour.insert(d, OURS) {
                        None => {
                            heap.push(d);
                            pending_ours += 1;
                        }
                        Some(prev) => {
                            colour.insert(d, prev | OURS);
                        }
                    }
                }
            }
        }
        out.reverse();
        out.into_iter()
            .filter_map(|i| Some(self.nodes[&self.order[i]].clone()))
            .collect()
    }

    #[inline]
    fn walk_idx(&self, id: &Id) -> Option<usize> {
        self.slot.get(id).map(|s| *s as usize)
    }

    fn walk_deps(&self, i: usize, out: &mut Vec<usize>) {
        let node = &self.nodes[&self.order[i]];
        out.extend(node.iter_refs().filter_map(|r| self.walk_idx(r)));
    }

    pub fn merge(&mut self, other: Self) {
        assert_eq!(
            self.origin, other.origin,
            "cannot merge maps with different origins"
        );
        // Value artifacts merge by union (content-addressed — no conflicts).
        for (vid, bytes) in other.values {
            self.values.entry(vid).or_insert(bytes);
        }
        // Apply in causal-safe order via the orphan machinery: node ids were
        // computed on the other side, reuse them.
        for (id, node) in other.nodes {
            self.apply_with_id(id, node);
        }
        for (id, node) in other.delivery.into_orphans() {
            self.apply_with_id(id, node);
        }
    }

    /// The containment register (PLACEMENT_SPEC.md read surface).
    pub fn placement(&self) -> &PlacementRegister {
        &self.placement
    }

    /// Author a `Place` claiming `placed_at`, superseding the placement
    /// heads this replica sees. Returns the applied node (re-broadcast).
    pub fn place(&mut self, placed_at: Id) -> HashNode {
        let overwrites: BTreeSet<Id> = self.placement.heads().iter().copied().collect();
        let pins: BTreeSet<Id> = BTreeSet::from_iter(self.tips.difference(&overwrites).cloned());
        let node = HashNode {
            pins,
            op: Op::Place {
                placed_at,
                overwrites,
            },
        };
        self.apply_with_id(node.id(), node.clone());
        node
    }

    pub fn orphans(&self) -> impl Iterator<Item = &HashNode> {
        self.delivery.orphans().map(|(_, n)| n)
    }

    /// Every applied node as `(id, HashNode)` (orphaned/gated not included).
    pub fn all_nodes(&self) -> Vec<(Id, HashNode)> {
        self.nodes.iter().map(|(id, n)| (*id, n.clone())).collect()
    }

    /// Value artifacts this replica holds bytes for.
    pub fn value_store(&self) -> impl Iterator<Item = (&Id, &Vec<u8>)> {
        self.values.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck_macros::quickcheck;

    fn s(v: &str) -> Value {
        Value::String(v.into())
    }

    #[test]
    fn put_get_roundtrip() {
        let mut kv = HashKv::default();
        kv.put(s("name"), s("david"));
        assert_eq!(kv.get(&s("name")), Some(s("david")));
        assert_eq!(kv.get(&s("missing")), None);
    }

    #[test]
    fn sequential_puts_supersede() {
        let mut kv = HashKv::default();
        kv.put(s("k"), s("a"));
        kv.put(s("k"), s("b"));
        assert_eq!(kv.get(&s("k")), Some(s("b")));
        assert_eq!(kv.heads(&s("k").value_id()).len(), 1);
    }

    #[test]
    fn del_makes_absent() {
        let mut kv = HashKv::default();
        kv.put(s("k"), s("a"));
        kv.del(s("k"));
        assert_eq!(kv.read(&s("k")), Read::Absent);
        assert_eq!(kv.get(&s("k")), None);
    }

    #[test]
    fn concurrent_puts_conflict_and_next_put_resolves() {
        let mut a = HashKv::default();
        let mut b = HashKv::default();
        a.put(s("k"), s("from-a"));
        b.put(s("k"), s("from-b"));

        let mut merged = a.clone();
        merged.merge(b.clone());
        // MVR: both heads live, surfaced, no winner.
        match merged.read(&s("k")) {
            Read::Conflict(vals) => assert_eq!(vals.len(), 2),
            other => panic!("expected conflict, got {other:?}"),
        }
        assert_eq!(merged.get(&s("k")), None); // get never collapses

        // The next put (naming both heads) dominates and resolves.
        merged.put(s("k"), s("resolved"));
        assert_eq!(merged.get(&s("k")), Some(s("resolved")));
        assert_eq!(merged.heads(&s("k").value_id()).len(), 1);
    }

    #[test]
    fn withholding_fabricates_conflict_but_confers_nothing() {
        // A put that saw a head but omits it from overwrites (and pins)
        // leaves both as heads — the fabrication; resolution just surfaces.
        let mut kv = HashKv::default();
        kv.put(s("k"), s("honest"));
        let key_id = s("k").value_id();
        let vid = kv.provide_value(&s("sneaky"));
        // Byzantine op: fresh put refing only the origin.
        let node = HashNode {
            pins: BTreeSet::from_iter([kv.origin()]),
            op: Op::Put {
                key: key_id,
                value: vid,
                overwrites: BTreeSet::new(),
            },
        };
        kv.apply(node);
        assert!(matches!(kv.read(&s("k")), Read::Conflict(_)));
    }

    #[test]
    fn cross_key_overwrites_are_ignored() {
        // A put on key B whose overwrites name a put on key A must not
        // disturb A's register (the definitional same-key filter).
        let mut kv = HashKv::default();
        let put_a = kv.put(s("a"), s("va"));
        let a_head = put_a.id();

        let key_b = kv.provide_value(&s("b"));
        let vb = kv.provide_value(&s("vb"));
        let node = HashNode {
            pins: BTreeSet::from_iter(
                kv.tips()
                    .iter()
                    .filter(|t| **t != a_head)
                    .cloned()
                    .collect::<Vec<_>>(),
            ),
            op: Op::Put {
                key: key_b,
                value: vb,
                overwrites: BTreeSet::from_iter([a_head]),
            },
        };
        kv.apply(node);
        // A's register is untouched.
        assert_eq!(kv.get(&s("a")), Some(s("va")));
        assert_eq!(kv.get(&s("b")), Some(s("vb")));
    }

    #[test]
    fn non_map_ops_are_refused() {
        let mut kv = HashKv::default();
        let origin = kv.origin();
        let node = HashNode {
            pins: BTreeSet::new(),
            op: Op::insert_after(origin, 'x'),
        };
        let id = node.id();
        kv.apply(node);
        assert!(!kv.contains_node(&id));
        assert_eq!(kv.delivery.orphans().count(), 0, "dropped, not orphaned");
        assert!(kv.tips().len() == 1, "refused ops never enter tips");
    }

    #[test]
    fn pending_value_is_not_papered_over() {
        let mut a = HashKv::default();
        let key = a.provide_value(&s("k"));
        // Value id whose bytes we never provide (e.g. a large blob).
        let mystery = Id([9; 32]);
        a.put_ids(key, mystery);
        match a.read_id(&key) {
            Read::One(vid) => {
                assert_eq!(vid, mystery);
                assert_eq!(a.resolve(&vid), None, "pending, not fabricated");
            }
            other => panic!("expected One, got {other:?}"),
        }
    }

    /// Drive two replicas with per-replica op scripts, merge both ways, and
    /// require identical reads — the commutativity harness.
    fn script(kv: &mut HashKv, ops: &[(u8, u8, bool)]) {
        for &(k, v, del) in ops {
            let key = Value::Int(k as i64 % 4);
            if del {
                kv.del(key);
            } else {
                kv.put(key, Value::Int(v as i64));
            }
        }
    }

    fn reads(kv: &HashKv) -> Vec<(i64, Read)> {
        (0..4).map(|k| (k, kv.read(&Value::Int(k)))).collect()
    }

    #[quickcheck]
    fn prop_merge_commutative(a: Vec<(u8, u8, bool)>, b: Vec<(u8, u8, bool)>) -> bool {
        let mut kv_a = HashKv::default();
        let mut kv_b = HashKv::default();
        script(&mut kv_a, &a);
        script(&mut kv_b, &b);

        let mut ab = kv_a.clone();
        ab.merge(kv_b.clone());
        let mut ba = kv_b.clone();
        ba.merge(kv_a.clone());

        reads(&ab) == reads(&ba)
    }

    #[quickcheck]
    fn prop_merge_associative(
        a: Vec<(u8, u8, bool)>,
        b: Vec<(u8, u8, bool)>,
        c: Vec<(u8, u8, bool)>,
    ) -> bool {
        let mk = |ops: &[(u8, u8, bool)]| {
            let mut kv = HashKv::default();
            script(&mut kv, ops);
            kv
        };
        let (kv_a, kv_b, kv_c) = (mk(&a), mk(&b), mk(&c));

        let mut ab_c = kv_a.clone();
        ab_c.merge(kv_b.clone());
        ab_c.merge(kv_c.clone());

        let mut a_bc = kv_b.clone();
        a_bc.merge(kv_c.clone());
        let mut left = kv_a.clone();
        left.merge(a_bc);

        reads(&ab_c) == reads(&left)
    }

    #[quickcheck]
    fn prop_merge_idempotent(a: Vec<(u8, u8, bool)>) -> bool {
        let mut kv = HashKv::default();
        script(&mut kv, &a);
        let mut twice = kv.clone();
        twice.merge(kv.clone());
        reads(&twice) == reads(&kv)
    }
}
