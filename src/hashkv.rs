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

use std::collections::BTreeSet;

use crate::hashseq::IdMap;
use crate::delivery::Delivery;
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
    pub(crate) values: IdMap<Vec<u8>>,
    pub(crate) tips: BTreeSet<Id>,
    /// Authored-ops outbox (delta sync) — see `HashSeq::outbox`.
    pub(crate) outbox: Option<Vec<HashNode>>,
    /// The containment register — where does this object live
    /// (PLACEMENT_SPEC.md). `Place` is valid in any object kind.
    pub(crate) placement: PlacementRegister,
    /// Parked orphans + the gate (non-map ops quarantine — the edge table).
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
            outbox: None,
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
        self.values.entry(id).or_insert_with(|| v.encoded());
        id
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
        let pins: BTreeSet<Id> =
            BTreeSet::from_iter(self.tips.difference(&overwrites).cloned());
        HashNode {
            pins,
            op: Op::Put {
                key,
                value,
                overwrites,
            },
        }
    }

    /// Record a locally-authored node (delta sync) — authoring paths only.
    #[inline]
    pub(crate) fn record_authored(&mut self, node: &HashNode) {
        if let Some(ob) = &mut self.outbox {
            ob.push(node.clone());
        }
    }

    /// `put` by raw ids (links, already-provided artifacts, tombstone).
    pub fn put_ids(&mut self, key: Id, value: Id) -> HashNode {
        let node = self.make_put(key, value);
        self.record_authored(&node);
        self.apply(node.clone());
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
        self.keys.get(key).map(|k| k.heads.as_slice()).unwrap_or(&[])
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

    pub fn apply(&mut self, node: HashNode) {
        let id = node.id();
        self.apply_with_id(id, node);
    }

    /// Apply with a pre-computed id (`id` must be the node's true hash).
    /// Iterative worklist: applying a node wakes exactly the orphans parked
    /// on its id.
    pub fn apply_with_id(&mut self, id: Id, node: HashNode) {
        debug_assert_eq!(id, node.id(), "apply_with_id called with a wrong id");
        if self.contains_node(&id) || self.delivery.holds(&id) {
            return;
        }
        let mut queue: Vec<(Id, HashNode)> = Vec::new();
        self.park_or_dispatch(id, node, &mut queue);
        while let Some((id, node)) = queue.pop() {
            self.park_or_dispatch(id, node, &mut queue);
        }
    }

    /// One step of the worklist: park `node` on its first missing ref, or
    /// interpret it and wake its waiters. A gated node wakes nothing — its
    /// dependents stay parked (the quarantine cascade).
    fn park_or_dispatch(&mut self, id: Id, node: HashNode, queue: &mut Vec<(Id, HashNode)>) {
        let missing = node
            .iter_refs()
            .find(|d| !self.contains_node(d))
            .copied();
        if let Some(missing) = missing {
            self.delivery.park(missing, id, node);
            return;
        }
        self.delivery.unpark(&id);
        match self.interpret(id, node) {
            Ok(()) => self.delivery.wake(&id, queue),
            Err(node) => self.delivery.gate(id, node),
        }
    }

    /// Interpret one node whose refs are all applied — this projection's
    /// edge-table rows. `Err` hands the node back for quarantine.
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
            self.nodes.insert(id, node);
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

        self.nodes.insert(id, node);
        Ok(())
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
        for (id, node) in other.delivery.into_held() {
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
        let overwrites: BTreeSet<Id> =
            self.placement.heads().iter().copied().collect();
        let pins: BTreeSet<Id> =
            BTreeSet::from_iter(self.tips.difference(&overwrites).cloned());
        let node = HashNode {
            pins,
            op: Op::Place {
                placed_at,
                overwrites,
            },
        };
        self.record_authored(&node);
        self.apply(node.clone());
        node
    }

    pub fn orphans(&self) -> impl Iterator<Item = &HashNode> {
        self.delivery.orphans()
    }

    /// Every applied node as `(id, HashNode)` (parked/gated not included).
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
    fn non_map_ops_gate() {
        let mut kv = HashKv::default();
        let origin = kv.origin();
        kv.apply(HashNode {
            pins: BTreeSet::new(),
            op: Op::insert_after(origin, 'x'),
        });
        assert_eq!(kv.delivery.gated.len(), 1);
        assert!(kv.tips().len() == 1, "gated ops never enter tips");
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
        (0..4)
            .map(|k| (k, kv.read(&Value::Int(k))))
            .collect()
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
