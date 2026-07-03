//! HashWeb: a flat store of every object this replica knows about.
//!
//! The store has **no identity and no semantics of its own** — it is not a
//! datastructure. All composition lives in the ops: a `Put`/`Insert` whose
//! value is a creation artifact births a child object (`object_id` of the
//! creating op); a link is an id carried as a value. What the store does
//! is mechanics: instantiate objects, route each incoming op to its object
//! (derived from the op's refs — never a field), buffer store-wide on
//! missing refs, and quarantine ops whose refs determine no single object.
//!
//! Objects are **holonic**: each is a complete replica rooted at its own
//! origin. A root object's `(origin, kind)` is chosen out-of-band —
//! agreeing on the kind is the same class of agreement as the 32 origin
//! bytes themselves, since no creation op exists to commit it; child
//! objects' origins and kinds are hash-committed by their creation ops.
//! Every op commits transitively to its object-closure's root origin, so
//! one object's ops can never merge into another — the commitment domain
//! is per object, and store merge is unconditional: a union of knowledge.

use rustc_hash::FxHashMap;

use crate::hashkv::HashKv;
use crate::hashseq::IdMap;
use crate::delivery::Delivery;
use crate::value::{NEW_MAP, NEW_SEQ, Value, object_id};
use crate::{HashNode, HashSeq, Id, Op, Payload};

/// A routed projection.
#[derive(Debug, Clone)]
pub enum Object {
    Seq(HashSeq),
    Map(HashKv),
}

impl Object {
    fn apply_with_id(&mut self, id: Id, node: HashNode) {
        match self {
            Object::Seq(seq) => seq.apply_with_id(id, node),
            Object::Map(map) => map.apply_with_id(id, node),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct HashWeb {
    /// Every object this replica knows, by origin id: roots opened
    /// out-of-band and children born by creation ops alike.
    pub(crate) objects: FxHashMap<Id, Object>,
    /// node id -> the origin id of the object it belongs to (the routing
    /// table — replica-local, derived, never on the wire).
    pub(crate) node_home: IdMap<Id>,
    /// Document-wide delivery: refs cross objects at creation bridges, so
    /// parking is global (each object's own buffer stays empty). The gate
    /// here holds ops whose refs determine no single object; kind-vs-object
    /// verdicts quarantine in their routed object instead. Both re-present
    /// on merge.
    pub(crate) delivery: Delivery,
    /// Value-artifact side store shared across objects.
    pub(crate) values: IdMap<Vec<u8>>,
}

impl PartialEq for HashWeb {
    fn eq(&self, other: &Self) -> bool {
        if self.objects.len() != other.objects.len() {
            return false;
        }
        self.objects.iter().all(|(k, v)| {
            other.objects.get(k).is_some_and(|o| match (v, o) {
                (Object::Seq(a), Object::Seq(b)) => a == b,
                (Object::Map(a), Object::Map(b)) => a == b,
                _ => false,
            })
        })
    }
}
impl Eq for HashWeb {}

impl HashWeb {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Open a root object at `origin` with the given kind — both chosen
    /// out-of-band ("each object is free to set its own root"). Returns
    /// false if an object already lives at that origin.
    pub fn create_root(&mut self, origin: Id, kind: Value) -> bool {
        debug_assert!(matches!(kind, Value::NewSeq | Value::NewMap));
        if self.objects.contains_key(&origin) {
            return false;
        }
        let obj = match kind {
            Value::NewSeq => Object::Seq(HashSeq::new(origin)),
            _ => Object::Map(HashKv::new(origin)),
        };
        self.objects.insert(origin, obj);
        // Adopting an origin may wake ops that parked on it.
        let mut queue: Vec<(Id, HashNode)> = Vec::new();
        self.delivery.wake(&origin, &mut queue);
        while let Some((id, node)) = queue.pop() {
            self.park_or_dispatch(id, node, &mut queue);
        }
        true
    }

    pub fn seq(&self, origin: &Id) -> Option<&HashSeq> {
        match self.objects.get(origin)? {
            Object::Seq(s) => Some(s),
            _ => None,
        }
    }

    pub fn map(&self, origin: &Id) -> Option<&HashKv> {
        match self.objects.get(origin)? {
            Object::Map(m) => Some(m),
            _ => None,
        }
    }

    pub fn objects(&self) -> impl Iterator<Item = (&Id, &Object)> {
        self.objects.iter()
    }

    /// An id is present if it is an applied node or a live object origin
    /// (roots adopted out-of-band and creation-derived children alike).
    fn contains(&self, id: &Id) -> bool {
        self.objects.contains_key(id) || self.node_home.contains_key(id)
    }

    pub fn provide_value(&mut self, v: &Value) -> Id {
        let id = v.value_id();
        self.values.entry(id).or_insert_with(|| v.encoded());
        id
    }

    pub fn resolve(&self, value_id: &Id) -> Option<Value> {
        self.values.get(value_id).and_then(|b| Value::decode(b))
    }

    // ---- authoring ----

    /// Write `key = value` in the map object `origin`. Routes through the
    /// composition's apply so creation semantics fire.
    pub fn put(&mut self, origin: &Id, key: Value, value: Value) -> Option<HashNode> {
        let key_id = self.provide_value(&key);
        let value_id = self.provide_value(&value);
        let node = match self.objects.get_mut(origin)? {
            Object::Map(m) => {
                m.provide_value(&key);
                m.provide_value(&value);
                m.make_put(key_id, value_id)
            }
            _ => return None,
        };
        self.apply(node.clone());
        Some(node)
    }

    /// Create a child object under `parent[key]`; returns the child's
    /// origin id. The creating op *is* the object: its identity is
    /// `object_id(creation op id)` — a virtual origin, never an op.
    pub fn create_child(&mut self, parent: &Id, key: Value, kind: Value) -> Option<Id> {
        debug_assert!(matches!(kind, Value::NewSeq | Value::NewMap));
        let node = self.put(parent, key, kind.clone())?;
        let creation = node.id();
        // put() routed the node; creation additionally births the child.
        let child = object_id(&creation);
        debug_assert!(self.objects.contains_key(&child), "creation birthed");
        Some(child)
    }

    pub fn new_map(&mut self, parent: &Id, key: Value) -> Option<Id> {
        self.create_child(parent, key, Value::NewMap)
    }

    pub fn new_seq(&mut self, parent: &Id, key: Value) -> Option<Id> {
        self.create_child(parent, key, Value::NewSeq)
    }

    /// Insert a value commitment into a child seq at `idx` — an artifact,
    /// a link (another object's origin id), or a creation value
    /// (`NewSeq`/`NewMap` births a child object inline). Routes through the
    /// composition's apply so creation semantics fire.
    pub fn seq_insert_value(&mut self, origin: &Id, idx: usize, value: &Value) -> Option<HashNode> {
        let vid = self.provide_value(value);
        let node = match self.objects.get(origin)? {
            Object::Seq(s) => s.make_insert_value(idx, vid)?,
            _ => return None,
        };
        self.apply(node.clone());
        Some(node)
    }

    /// Create a child object as an inline element of a seq (creation-in-seq:
    /// the element IS the creation op; the child's origin id derives from it).
    pub fn seq_new_object(&mut self, parent: &Id, idx: usize, kind: Value) -> Option<Id> {
        debug_assert!(matches!(kind, Value::NewSeq | Value::NewMap));
        let node = self.seq_insert_value(parent, idx, &kind)?;
        let child = object_id(&node.id());
        debug_assert!(self.objects.contains_key(&child), "creation birthed");
        Some(child)
    }

    /// Edit a child text object.
    pub fn text_insert(&mut self, origin: &Id, idx: usize, text: &str) -> bool {
        let Some(Object::Seq(seq)) = self.objects.get_mut(origin) else {
            return false;
        };
        let pre = seq.ids.len();
        seq.insert_batch(idx, text.chars());
        // Register the fresh nodes' home (replica-local routing table).
        let fresh: Vec<Id> = seq.ids[pre..].to_vec();
        for id in fresh {
            self.node_home.insert(id, *origin);
        }
        true
    }

    pub fn text_remove(&mut self, origin: &Id, idx: usize, len: usize) -> bool {
        let Some(Object::Seq(seq)) = self.objects.get_mut(origin) else {
            return false;
        };
        let pre = seq.ids.len();
        let removed = seq.remove_batch(idx, len).is_some();
        let fresh: Vec<Id> = seq.ids[pre..].to_vec();
        for id in fresh {
            self.node_home.insert(id, *origin);
        }
        removed
    }

    pub fn text(&self, origin: &Id) -> Option<String> {
        Some(self.seq(origin)?.iter().collect())
    }

    /// Read `key` in map `origin`, resolving artifacts through the
    /// composition's value store (the store is document-wide; inner
    /// projections keep only what they saw locally). Conflicts and pending
    /// values are not collapsed — `None` (use the map's `read` to surface).
    pub fn get(&self, origin: &Id, key: &Value) -> Option<Value> {
        match self.map(origin)?.read(key) {
            crate::hashkv::Read::One(vid) => self
                .resolve(&vid)
                .or_else(|| self.map(origin)?.resolve(&vid)),
            _ => None,
        }
    }

    // ---- routing + apply ----

    /// Derive the op's object: the single object its refs resolve in —
    /// named refs first, else the pins (a fresh put's frontier is its
    /// object's own, beginning at the origin id). `None` = underdetermined
    /// or contradictory → gate (stable: every ref's home is hash-committed).
    fn route(&self, node: &HashNode) -> Option<Id> {
        let mut home: Option<Id> = None;
        for r in node.iter_refs() {
            let h = if self.objects.contains_key(r) {
                *r // an origin id names its object directly
            } else {
                *self.node_home.get(r)?
            };
            match home {
                None => home = Some(h),
                Some(prev) if prev == h => {}
                Some(_) => return None, // refs span objects
            }
        }
        home
    }

    pub fn apply(&mut self, node: HashNode) {
        let id = node.id();
        self.apply_with_id(id, node);
    }

    /// Apply with a pre-computed id (`id` must be the node's true hash).
    /// Iterative worklist: applying a node wakes exactly the orphans parked
    /// on its id.
    pub fn apply_with_id(&mut self, id: Id, node: HashNode) {
        debug_assert_eq!(id, node.id(), "apply_with_id called with a wrong id");
        if self.contains(&id) || self.delivery.holds(&id) {
            return;
        }
        let mut queue: Vec<(Id, HashNode)> = Vec::new();
        self.park_or_dispatch(id, node, &mut queue);
        while let Some((id, node)) = queue.pop() {
            self.park_or_dispatch(id, node, &mut queue);
        }
    }

    /// One step of the worklist. Parking is document-wide: origin ids count
    /// as present once their object exists (the derive-and-wake in
    /// `interpret` makes that true). A gated node wakes nothing — its
    /// dependents stay parked (the quarantine cascade).
    fn park_or_dispatch(&mut self, id: Id, node: HashNode, queue: &mut Vec<(Id, HashNode)>) {
        let missing = node.iter_refs().find(|d| !self.contains(d)).copied();
        if let Some(missing) = missing {
            self.delivery.park(missing, id, node);
            return;
        }
        self.delivery.unpark(&id);
        match self.interpret(id, node, queue) {
            Ok(()) => self.delivery.wake(&id, queue),
            Err(node) => self.delivery.gate(id, node),
        }
    }

    /// Interpret one node whose refs are all present — routing, creation,
    /// and dispatch to the routed object. `Err` hands the node back for
    /// quarantine (refs spanning objects).
    #[allow(clippy::result_large_err)]
    fn interpret(
        &mut self,
        id: Id,
        node: HashNode,
        queue: &mut Vec<(Id, HashNode)>,
    ) -> Result<(), HashNode> {
        // Routing (stable gate on refs spanning objects).
        let Some(home) = self.route(&node) else {
            return Err(node);
        };

        // Creation: an op whose payload/value is a creation artifact births
        // a child object whose origin is object_id(creation op).
        let creation_kind = match &node.op {
            Op::Put { value, .. } if *value == *NEW_SEQ => Some(true),
            Op::Put { value, .. } if *value == *NEW_MAP => Some(false),
            Op::Insert {
                payload: Payload::Id(v),
                ..
            } if *v == *NEW_SEQ => Some(true),
            Op::Insert {
                payload: Payload::Id(v),
                ..
            } if *v == *NEW_MAP => Some(false),
            _ => None,
        };

        // Dispatch to the routed object; its own edge-table gate handles
        // op-kind-vs-object-type (a Put into a Seq quarantines there).
        self.objects
            .get_mut(&home)
            .expect("routed to a live object")
            .apply_with_id(id, node);
        self.node_home.insert(id, home);

        if let Some(is_seq) = creation_kind {
            let child = object_id(&id);
            self.objects.entry(child).or_insert_with(|| {
                if is_seq {
                    Object::Seq(HashSeq::new(child))
                } else {
                    Object::Map(HashKv::new(child))
                }
            });
            // Derive-and-wake: the origin id just became present; wake any
            // ops parked on it (no inversion needed — we derived it).
            self.delivery.wake(&child, queue);
        }
        Ok(())
    }

    /// Merge = union of knowledge. Unconditional: there is no store
    /// identity to compare. Shared objects merge pairwise (their per-object
    /// commitment domains make cross-object confusion impossible); unknown
    /// origins are adopted — a root object's `(origin, kind)` is knowledge,
    /// not ops, since no creation op exists for it. A kind mis-agreement on
    /// an out-of-band origin degrades gracefully: the other side's ops
    /// quarantine in the local object's own gate, op by op.
    pub fn merge(&mut self, other: Self) {
        for (vid, bytes) in other.values {
            self.values.entry(vid).or_insert(bytes);
        }
        for (origin, obj) in &other.objects {
            if !self.objects.contains_key(origin) {
                let fresh = match obj {
                    Object::Seq(_) => Object::Seq(HashSeq::new(*origin)),
                    Object::Map(_) => Object::Map(HashKv::new(*origin)),
                };
                self.objects.insert(*origin, fresh);
                let mut queue: Vec<(Id, HashNode)> = Vec::new();
                self.delivery.wake(origin, &mut queue);
                while let Some((id, node)) = queue.pop() {
                    self.park_or_dispatch(id, node, &mut queue);
                }
            }
        }
        // Re-apply everything through the global buffer; ordering resolves
        // itself (creation ops wake their children's parked ops).
        for (_, obj) in other.objects.iter() {
            let nodes = match obj {
                Object::Seq(s) => s.all_nodes(),
                Object::Map(m) => {
                    for (vid, bytes) in m.value_store() {
                        self.values.entry(*vid).or_insert_with(|| bytes.clone());
                    }
                    m.all_nodes()
                }
            };
            for (id, node) in nodes {
                self.apply_with_id(id, node);
            }
        }
        for (id, node) in other.delivery.into_held() {
            self.apply_with_id(id, node);
        }
    }

    pub fn orphans(&self) -> impl Iterator<Item = &HashNode> {
        self.delivery.orphans()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> Value {
        Value::String(v.into())
    }

    fn oid(n: u8) -> Id {
        Id([n; 32])
    }

    /// The store is knowledge, not an object: merge is an unconditional
    /// union — unknown origins are adopted (waking anything parked on
    /// them), shared objects merge pairwise, and a kind mis-agreement on
    /// an out-of-band origin degrades to per-op quarantine, never a panic.
    #[test]
    fn store_merge_is_union_of_knowledge() {
        // Two stores with unrelated roots merge unconditionally.
        let mut a = HashWeb::new();
        let mut b = HashWeb::new();
        assert!(a.create_root(oid(1), Value::NewSeq));
        assert!(!a.create_root(oid(1), Value::NewSeq), "already occupied");
        b.create_root(oid(2), Value::NewMap);
        a.text_insert(&oid(1), 0, "hi");
        b.put(&oid(2), s("k"), s("v"));

        a.merge(b.clone());
        assert_eq!(a.objects().count(), 2);
        assert_eq!(a.text(&oid(1)).unwrap(), "hi");
        assert_eq!(a.get(&oid(2), &s("k")), Some(s("v")));

        // Ops arriving before their root is known park; adopting the root
        // wakes them.
        let nodes = a.seq(&oid(1)).unwrap().all_nodes();
        let mut fresh = HashWeb::new();
        for (id, node) in nodes {
            fresh.apply_with_id(id, node);
        }
        assert_eq!(fresh.orphans().count(), 2, "'h' on the origin, 'i' on 'h'");
        fresh.create_root(oid(1), Value::NewSeq);
        assert_eq!(fresh.orphans().count(), 0, "adoption wakes transitively");
        assert_eq!(fresh.text(&oid(1)).unwrap(), "hi");

        // Kind mis-agreement: the same out-of-band origin opened as a Map
        // elsewhere — the seq ops quarantine in the local map's gate.
        let mut confused = HashWeb::new();
        confused.create_root(oid(1), Value::NewMap);
        confused.merge(a.clone());
        assert!(confused.map(&oid(1)).is_some());
        assert!(!confused.map(&oid(1)).unwrap().delivery.gated.is_empty());

        // Roundtrip of a multi-root store.
        let decoded = crate::encoding::decode_hashweb_strict(&crate::encoding::encode_hashweb(
            &a,
        ))
        .expect("strict");
        assert_eq!(decoded, a);
    }

    #[test]
    fn block_document_shape() {
        // A Notion-style block: a map with "content" -> Text child.
        let mut doc = HashWeb::new();
        let root = oid(9);
        doc.create_root(root, Value::NewMap);

        let block = doc.new_map(&root, s("block-1")).unwrap();
        let content = doc.new_seq(&block, s("content")).unwrap();
        doc.put(&block, s("color"), s("blue"));
        doc.text_insert(&content, 0, "hello world");

        assert_eq!(doc.text(&content).unwrap(), "hello world");
        assert_eq!(doc.map(&block).unwrap().get(&s("color")), Some(s("blue")));
        // The root links to the block by... the creation op is the value; the
        // child object id is derived from it.
        assert!(doc.map(&block).is_some());
        assert_eq!(doc.objects().count(), 3); // root + block + content
    }

    #[test]
    fn per_object_frontiers_stay_lean() {
        // Editing one object never enters another's tips (per-object tips).
        let mut doc = HashWeb::new();
        let root = oid(9);
        doc.create_root(root, Value::NewMap);
        let a = doc.new_seq(&root, s("a")).unwrap();
        let b = doc.new_seq(&root, s("b")).unwrap();

        doc.text_insert(&a, 0, "aaa");
        doc.text_insert(&b, 0, "bbb");

        // Each seq's frontier is a single run tail — no cross-contamination.
        assert_eq!(doc.seq(&a).unwrap().tips().len(), 1);
        assert_eq!(doc.seq(&b).unwrap().tips().len(), 1);
    }

    #[test]
    fn concurrent_edits_in_different_objects_merge_cleanly() {
        let mut base = HashWeb::new();
        let root = oid(9);
        base.create_root(root, Value::NewMap);
        let text = base.new_seq(&root, s("text")).unwrap();
        let meta = base.new_map(&root, s("meta")).unwrap();

        let mut r1 = base.clone();
        let mut r2 = base.clone();
        r1.text_insert(&text, 0, "hello");
        r2.put(&meta, s("status"), s("draft"));

        let mut m1 = r1.clone();
        m1.merge(r2.clone());
        let mut m2 = r2;
        m2.merge(r1);

        assert_eq!(m1, m2);
        assert_eq!(m1.text(&text).unwrap(), "hello");
        assert_eq!(m1.get(&meta, &s("status")), Some(s("draft")));
        assert_eq!(m2.get(&meta, &s("status")), Some(s("draft")));
    }

    #[test]
    fn child_ops_park_until_creation_arrives() {
        // Deliver a child's ops before the creation op: they park on the
        // origin id; applying the creation op derives it and wakes them.
        let mut a = HashWeb::new();
        let root = oid(9);
        a.create_root(root, Value::NewMap);
        let child = a.new_seq(&root, s("t")).unwrap();
        a.text_insert(&child, 0, "x");

        // Extract the child's inserts and the creation chain separately
        // (root adoption -> root map's put -> child).
        let child_nodes = a.seq(&child).unwrap().all_nodes();
        let creation_chain: Vec<(Id, HashNode)> = a.map(&root).unwrap().all_nodes();

        let mut fresh = HashWeb::new();
        fresh.create_root(root, Value::NewMap);
        // Child op first: parks (its ref — the origin id — is unknown).
        for (id, node) in &child_nodes {
            fresh.apply_with_id(*id, node.clone());
        }
        assert_eq!(fresh.orphans().count(), 1);
        assert!(fresh.seq(&child).is_none());
        // The creation chain arrives: derive-and-wake, transitively.
        for (id, node) in creation_chain {
            fresh.apply_with_id(id, node);
        }
        assert_eq!(fresh.orphans().count(), 0);
        assert_eq!(fresh.text(&child).unwrap(), "x");
    }

    #[test]
    fn refs_spanning_objects_gate() {
        let mut doc = HashWeb::new();
        let root = oid(9);
        doc.create_root(root, Value::NewMap);
        let a = doc.new_seq(&root, s("a")).unwrap();
        let b = doc.new_seq(&root, s("b")).unwrap();
        doc.text_insert(&a, 0, "a");
        doc.text_insert(&b, 0, "b");
        let a0 = doc.seq(&a).unwrap().id_at(0).unwrap();
        let b0 = doc.seq(&b).unwrap().id_at(0).unwrap();

        // A remove naming elements of two objects: routing underdetermined.
        let node = HashNode {
            pins: Default::default(),
            op: Op::Remove([a0, b0].into_iter().collect()),
        };
        doc.apply(node);
        assert_eq!(doc.delivery.gated.len(), 1);
        assert_eq!(doc.text(&a).unwrap(), "a"); // untouched
        assert_eq!(doc.text(&b).unwrap(), "b");
    }

    /// Creation-in-seq: a child object born as an inline element of a text
    /// object — the transclusion shape. The element renders as the atom
    /// placeholder; the payload id is the creation value, and the child's
    /// origin derives from the creating op.
    #[test]
    fn child_object_born_inline_in_a_seq() {
        let mut doc = HashWeb::new();
        let text = oid(7);
        doc.create_root(text, Value::NewSeq);
        doc.text_insert(&text, 0, "see [] here");

        let inner = doc
            .seq_new_object(&text, 5, Value::NewSeq)
            .expect("inline creation");
        doc.text_insert(&inner, 0, "the embedded doc");

        assert_eq!(doc.text(&inner).unwrap(), "the embedded doc");
        let body = doc.text(&text).unwrap();
        assert_eq!(body, format!("see [{}] here", crate::hashseq::ATOM_CHAR));
        // The atom's payload is the creation value; the child origin
        // derives from the creating op's id.
        let seq = doc.seq(&text).unwrap();
        let atom_id = seq.id_at(5).unwrap();
        assert_eq!(seq.payload_of(&atom_id), Some(*crate::value::NEW_SEQ));
        assert_eq!(crate::value::object_id(&atom_id), inner);

        // Merges carry the whole shape both ways.
        let mut other = HashWeb::new();
        other.merge(doc.clone());
        assert_eq!(other, doc);
        assert_eq!(other.text(&inner).unwrap(), "the embedded doc");
    }

    /// A link atom: a seq element whose payload is another object's origin
    /// id — rendering resolves it by id, nothing embeds.
    #[test]
    fn link_atoms_reference_other_objects()  {
        let mut doc = HashWeb::new();
        let (a, b) = (oid(3), oid(4));
        doc.create_root(a, Value::NewSeq);
        doc.create_root(b, Value::NewSeq);
        doc.text_insert(&a, 0, "link: ");
        doc.text_insert(&b, 0, "the target");

        // Insert a link to b inside a (the origin id as a Ref-like value:
        // carried as raw payload id — no artifact bytes needed).
        let node = {
            let seq = doc.seq(&a).unwrap();
            seq.make_insert_value(6, b).unwrap()
        };
        doc.apply(node.clone());
        let seq = doc.seq(&a).unwrap();
        let atom = seq.id_at(6).unwrap();
        assert_eq!(seq.payload_of(&atom), Some(b), "the link target's origin id");
        assert_eq!(doc.text(&b).unwrap(), "the target");
    }

    #[test]
    fn same_key_concurrent_children_are_both_born() {
        // Two replicas concurrently create a child at the same key: the key
        // register conflicts (MVR), but BOTH child objects exist — creation
        // is never lost, the app resolves the register.
        let mut base = HashWeb::new();
        let root = oid(9);
        base.create_root(root, Value::NewMap);
        let mut r1 = base.clone();
        let mut r2 = base.clone();
        let c1 = r1.new_seq(&root, s("content")).unwrap();
        let c2 = r2.new_map(&root, s("content")).unwrap();
        r1.text_insert(&c1, 0, "one");

        base.merge(r1);
        base.merge(r2);
        assert!(base.seq(&c1).is_some());
        assert!(base.map(&c2).is_some());
        assert!(matches!(
            base.map(&root).unwrap().read(&s("content")),
            crate::hashkv::Read::Conflict(_)
        ));
        assert_eq!(base.text(&c1).unwrap(), "one");
    }
}
