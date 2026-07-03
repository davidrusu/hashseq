//! HashWeb: a flat store of every object this replica knows about.
//!
//! The store has **no identity and no semantics of its own** — it is not a
//! datastructure. All composition lives in the ops: a `Put`/`Insert` whose
//! value is a creation artifact births a child object; a link is an id
//! carried as a value. What the store does is mechanics: instantiate
//! objects, deliver enveloped ops (`obj_id ‖ node` — transport metadata,
//! never hashed), park envelopes for unknown objects, and birth children
//! when creation ops apply.
//!
//! Objects are **holonic**: each is a complete replica rooted at its own
//! origin — a creation op's id, or an arbitrary 32-byte value chosen
//! out-of-band for a root. Ops anchor at the origin; the store indexes by
//! the derived **object id**, `object_id(kind ‖ origin)`, which never
//! appears in a preimage — it is the envelope's address, with the kind
//! inside it, so the same origin opened as a Seq and as a Kv is two
//! objects and kind mis-agreement is unrepresentable. Every op commits
//! transitively to its object's origin, so one object's ops can never
//! merge into another — the commitment domain is per object, and store
//! merge is unconditional: a union of knowledge.

use rustc_hash::FxHashMap;

use crate::hashkv::HashKv;
use crate::hashseq::IdMap;
use crate::value::{NEW_KV, NEW_SEQ, VK_NEW_KV, VK_NEW_SEQ, Value, object_id};
use crate::{HashNode, HashSeq, Id, Op, Payload};

#[derive(Debug, Clone, Default)]
pub struct HashWeb {
    /// Every seq object this replica knows, keyed by derived object id
    /// (`object_id(kind ‖ origin)`): roots opened out-of-band and children
    /// born by creation ops alike.
    pub(crate) seqs: FxHashMap<Id, HashSeq>,
    /// Every map object, likewise.
    pub(crate) kvs: FxHashMap<Id, HashKv>,
    /// Envelopes parked on object ids this store does not know yet; birth
    /// or adoption wakes them. Node-level parking lives inside each
    /// object's own delivery — the store keeps no per-node state at all.
    pub(crate) parked: FxHashMap<Id, Vec<(Id, HashNode)>>,
    /// Creation-valued ops seen enveloped to an object but not yet
    /// observed applied there (they may be parked inside it); `pump`
    /// births their children the moment they apply.
    pending_creations: FxHashMap<Id, Vec<(Id, bool)>>,
    /// Value-artifact side store shared across objects.
    pub(crate) values: IdMap<Vec<u8>>,
}

impl PartialEq for HashWeb {
    fn eq(&self, other: &Self) -> bool {
        self.seqs == other.seqs && self.kvs == other.kvs
    }
}
impl Eq for HashWeb {}

impl HashWeb {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Open a root seq from `origin` — a 32-byte value chosen out-of-band
    /// ("each object is free to set its own root"). The maps are keyed by
    /// the derived object id, `object_id(VK_NEW_SEQ ‖ origin)`, which is
    /// returned as the handle: the kind is inside the store-level identity,
    /// so the same origin opened as a Kv is a different object, not a
    /// clash. The *ops* keep anchoring at the origin itself — the derived
    /// id is a store address (envelope + index) and never appears in a
    /// preimage. Idempotent.
    pub fn create_seq(&mut self, origin: Id) -> Id {
        let obj = object_id(VK_NEW_SEQ, &origin);
        if !self.is_object(&obj) {
            self.seqs.insert(obj, HashSeq::new(origin));
            self.pump(obj);
        }
        obj
    }

    /// Open a root map from `origin` — see [`Self::create_seq`].
    pub fn create_kv(&mut self, origin: Id) -> Id {
        let obj = object_id(VK_NEW_KV, &origin);
        if !self.is_object(&obj) {
            self.kvs.insert(obj, HashKv::new(origin));
            self.pump(obj);
        }
        obj
    }

    pub fn seq(&self, obj: &Id) -> Option<&HashSeq> {
        self.seqs.get(obj)
    }

    pub fn kv(&self, obj: &Id) -> Option<&HashKv> {
        self.kvs.get(obj)
    }

    /// Every known object id (seqs and kvs).
    pub fn objects(&self) -> impl Iterator<Item = &Id> {
        self.seqs.keys().chain(self.kvs.keys())
    }

    pub fn object_count(&self) -> usize {
        self.seqs.len() + self.kvs.len()
    }

    /// Is this id a live object (roots opened from out-of-band seeds and
    /// creation-derived children alike — both keyed by derived object id)?
    fn is_object(&self, id: &Id) -> bool {
        self.seqs.contains_key(id) || self.kvs.contains_key(id)
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
        let node = {
            let m = self.kvs.get_mut(origin)?;
            m.provide_value(&key);
            m.provide_value(&value);
            m.make_put(key_id, value_id)
        };
        self.apply_to(*origin, node.clone());
        Some(node)
    }

    /// Create a child object under `parent[key]`; returns the child's
    /// object id. The creating op *is* the object: its id is the child's
    /// origin anchor, and `object_id(kind ‖ it)` is the store address.
    pub fn create_child(&mut self, parent: &Id, key: Value, kind: Value) -> Option<Id> {
        debug_assert!(matches!(kind, Value::NewSeq | Value::NewKv));
        let tag = match kind {
            Value::NewSeq => VK_NEW_SEQ,
            _ => VK_NEW_KV,
        };
        let node = self.put(parent, key, kind)?;
        // put() delivered the node; creation additionally births the child.
        let child = object_id(tag, &node.id());
        debug_assert!(self.is_object(&child), "creation birthed");
        Some(child)
    }

    pub fn new_kv(&mut self, parent: &Id, key: Value) -> Option<Id> {
        self.create_child(parent, key, Value::NewKv)
    }

    pub fn new_seq(&mut self, parent: &Id, key: Value) -> Option<Id> {
        self.create_child(parent, key, Value::NewSeq)
    }

    /// Insert a value commitment into a child seq at `idx` — an artifact,
    /// a link (another object's origin id), or a creation value
    /// (`NewSeq`/`NewKv` births a child object inline). Routes through the
    /// composition's apply so creation semantics fire.
    pub fn seq_insert_value(&mut self, origin: &Id, idx: usize, value: &Value) -> Option<HashNode> {
        let vid = self.provide_value(value);
        let node = self.seqs.get(origin)?.make_insert_value(idx, vid)?;
        self.apply_to(*origin, node.clone());
        Some(node)
    }

    /// Create a child seq as an inline element of a seq (creation-in-seq:
    /// the element IS the creation op; the child's origin id derives from
    /// it).
    pub fn new_seq_at(&mut self, parent: &Id, idx: usize) -> Option<Id> {
        self.create_child_at(parent, idx, Value::NewSeq)
    }

    /// Create a child kv as an inline element of a seq.
    pub fn new_kv_at(&mut self, parent: &Id, idx: usize) -> Option<Id> {
        self.create_child_at(parent, idx, Value::NewKv)
    }

    fn create_child_at(&mut self, parent: &Id, idx: usize, kind: Value) -> Option<Id> {
        let tag = match kind {
            Value::NewSeq => VK_NEW_SEQ,
            _ => VK_NEW_KV,
        };
        let node = self.seq_insert_value(parent, idx, &kind)?;
        let child = object_id(tag, &node.id());
        debug_assert!(self.is_object(&child), "creation birthed");
        Some(child)
    }

    /// Edit a child text object.
    pub fn text_insert(&mut self, origin: &Id, idx: usize, text: &str) -> bool {
        let Some(seq) = self.seqs.get_mut(origin) else {
            return false;
        };
        seq.insert_batch(idx, text.chars());
        true
    }

    pub fn text_remove(&mut self, origin: &Id, idx: usize, len: usize) -> bool {
        let Some(seq) = self.seqs.get_mut(origin) else {
            return false;
        };
        seq.remove_batch(idx, len).is_some()
    }

    pub fn text(&self, origin: &Id) -> Option<String> {
        Some(self.seq(origin)?.iter().collect())
    }

    /// Read `key` in kv `origin`, resolving artifacts through the store's
    /// value store (store-wide; inner projections keep only what they saw
    /// locally). Conflicts and pending values are not collapsed — `None`
    /// (use the kv's `read` to surface).
    pub fn get(&self, origin: &Id, key: &Value) -> Option<Value> {
        match self.kv(origin)?.read(key) {
            crate::hashkv::Read::One(vid) => self
                .resolve(&vid)
                .or_else(|| self.kv(origin)?.resolve(&vid)),
            _ => None,
        }
    }

    // ---- delivery (the routing envelope: `obj_id ‖ HashNode`) ----

    /// Deliver an enveloped op: `obj ‖ node`. The envelope replaces both a
    /// route field (there is none — GRAMMAR_SPEC.md) and any per-node
    /// routing table; it is transport metadata that needs no trust: an op
    /// enveloped to the wrong object simply never applies there (its refs
    /// never arrive in that object), the same fate as any garbage ref —
    /// bounded, attributable, and correct by construction. Envelopes for
    /// unknown object ids park store-wide and wake on birth or adoption.
    pub fn apply_to(&mut self, obj: Id, node: HashNode) {
        let id = node.id();
        self.apply_to_with_id(obj, id, node);
    }

    pub fn apply_to_with_id(&mut self, obj: Id, id: Id, node: HashNode) {
        debug_assert_eq!(id, node.id(), "apply_to_with_id called with a wrong id");
        if let Some(kind) = Self::creation_kind(&node) {
            self.pending_creations.entry(obj).or_default().push((id, kind));
        }
        if let Some(seq) = self.seqs.get_mut(&obj) {
            seq.apply_with_id(id, node);
        } else if let Some(kv) = self.kvs.get_mut(&obj) {
            kv.apply_with_id(id, node);
        } else {
            self.parked.entry(obj).or_default().push((id, node));
            return;
        }
        self.pump(obj);
    }

    /// Is this op a creation (value/payload = a creation artifact)?
    /// `Some(is_seq)`.
    fn creation_kind(node: &HashNode) -> Option<bool> {
        match &node.op {
            Op::Put { value, .. } if *value == *NEW_SEQ => Some(true),
            Op::Put { value, .. } if *value == *NEW_KV => Some(false),
            Op::Insert {
                payload: Payload::Id(v),
                ..
            } if *v == *NEW_SEQ => Some(true),
            Op::Insert {
                payload: Payload::Id(v),
                ..
            } if *v == *NEW_KV => Some(false),
            _ => None,
        }
    }

    /// Settle an object after delivery: birth the children of creation ops
    /// observed applied in it (derive-and-wake), deliver envelopes parked
    /// on the newly-live ids, and repeat for every object this touches.
    fn pump(&mut self, start: Id) {
        let mut work = vec![start];
        while let Some(o) = work.pop() {
            // Envelopes parked on o deliver now that it exists.
            if self.is_object(&o)
                && let Some(envelopes) = self.parked.remove(&o)
            {
                for (id, node) in envelopes {
                    if let Some(kind) = Self::creation_kind(&node) {
                        self.pending_creations.entry(o).or_default().push((id, kind));
                    }
                    if let Some(seq) = self.seqs.get_mut(&o) {
                        seq.apply_with_id(id, node);
                    } else if let Some(kv) = self.kvs.get_mut(&o) {
                        kv.apply_with_id(id, node);
                    }
                }
            }
            // Birth children of creations that have applied in o.
            let Some(pending) = self.pending_creations.remove(&o) else {
                continue;
            };
            let mut still = Vec::new();
            for (id, is_seq) in pending {
                let applied = self.seqs.get(&o).is_some_and(|x| x.contains_node(&id))
                    || self.kvs.get(&o).is_some_and(|x| x.contains_node(&id));
                if !applied {
                    still.push((id, is_seq));
                    continue;
                }
                let tag = if is_seq { VK_NEW_SEQ } else { VK_NEW_KV };
                let child = object_id(tag, &id);
                if !self.is_object(&child) {
                    // The creation op id is the child's origin anchor; the
                    // derived id is only the store's address for it.
                    if is_seq {
                        self.seqs.insert(child, HashSeq::new(id));
                    } else {
                        self.kvs.insert(child, HashKv::new(id));
                    }
                }
                work.push(child);
            }
            if !still.is_empty() {
                self.pending_creations.insert(o, still);
            }
        }
    }

    /// Merge = union of knowledge. Unconditional: there is no store
    /// identity to compare. Missing origins are adopted with their kind
    /// (for roots, the (seed, kind) agreement is baked into the derived
    /// object id — a kind mis-agreement is a different object, not a
    /// conflict); every op re-delivers in its own envelope.
    pub fn merge(&mut self, other: Self) {
        for (vid, bytes) in other.values {
            self.values.entry(vid).or_insert(bytes);
        }
        let adopt: Vec<(Id, Id, bool)> = other
            .seqs
            .iter()
            .map(|(o, x)| (*o, x.origin(), true))
            .chain(other.kvs.iter().map(|(o, x)| (*o, x.origin(), false)))
            .filter(|(o, _, _)| !self.is_object(o))
            .collect();
        for (obj, origin, is_seq) in adopt {
            if is_seq {
                self.seqs.insert(obj, HashSeq::new(origin));
            } else {
                self.kvs.insert(obj, HashKv::new(origin));
            }
            self.pump(obj);
        }
        for (origin, seq) in &other.seqs {
            for (id, node) in seq.all_nodes() {
                self.apply_to_with_id(*origin, id, node);
            }
            for (id, node) in seq.delivery.held() {
                self.apply_to_with_id(*origin, *id, node.clone());
            }
        }
        for (origin, m) in &other.kvs {
            for (vid, bytes) in m.value_store() {
                self.values.entry(*vid).or_insert_with(|| bytes.clone());
            }
            for (id, node) in m.all_nodes() {
                self.apply_to_with_id(*origin, id, node);
            }
            for (id, node) in m.delivery.held() {
                self.apply_to_with_id(*origin, *id, node.clone());
            }
        }
        for (obj, envelopes) in other.parked {
            for (id, node) in envelopes {
                self.apply_to_with_id(obj, id, node);
            }
        }
    }

    /// Envelopes parked on unknown object ids.
    pub fn orphans(&self) -> impl Iterator<Item = &HashNode> {
        self.parked.values().flatten().map(|(_, node)| node)
    }

    /// Re-register creation-valued ops parked *inside* an object (its own
    /// orphan/gate buffers) as pending births. Decode needs this: the
    /// pending list is apply-time knowledge, and a decoded object carries
    /// its held ops without ever passing through `apply_to`.
    pub(crate) fn pend_held_creations(&mut self, obj: Id) {
        let mut found: Vec<(Id, bool)> = Vec::new();
        let held: Box<dyn Iterator<Item = (&Id, &HashNode)>> =
            if let Some(seq) = self.seqs.get(&obj) {
                Box::new(seq.delivery.held())
            } else if let Some(kv) = self.kvs.get(&obj) {
                Box::new(kv.delivery.held())
            } else {
                return;
            };
        for (id, node) in held {
            if let Some(kind) = Self::creation_kind(node) {
                found.push((*id, kind));
            }
        }
        if !found.is_empty() {
            self.pending_creations.entry(obj).or_default().extend(found);
        }
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
        let s1 = a.create_seq(oid(1));
        assert_eq!(a.create_seq(oid(1)), s1, "idempotent: same seed, same object");
        assert_ne!(s1, oid(1), "the handle is the derived object id, not the seed");
        let k2 = b.create_kv(oid(2));
        a.text_insert(&s1, 0, "hi");
        b.put(&k2, s("k"), s("v"));

        a.merge(b.clone());
        assert_eq!(a.object_count(), 2);
        assert_eq!(a.text(&s1).unwrap(), "hi");
        assert_eq!(a.get(&k2, &s("k")), Some(s("v")));

        // Ops arriving before their root is known park; adopting the root
        // wakes them.
        let nodes = a.seq(&s1).unwrap().all_nodes();
        let mut fresh = HashWeb::new();
        for (id, node) in nodes {
            fresh.apply_to_with_id(s1, id, node);
        }
        assert_eq!(fresh.orphans().count(), 2, "both envelopes park on the unknown object id");
        fresh.create_seq(oid(1));
        assert_eq!(fresh.orphans().count(), 0, "adoption wakes transitively");
        assert_eq!(fresh.text(&s1).unwrap(), "hi");

        // Kind "mis-agreement" is unrepresentable: the same seed opened as
        // a Kv derives a different object id — the two coexist, nothing
        // quarantines.
        let mut confused = HashWeb::new();
        let k1 = confused.create_kv(oid(1));
        assert_ne!(k1, s1);
        confused.merge(a.clone());
        assert_eq!(confused.object_count(), 3);
        assert_eq!(confused.text(&s1).unwrap(), "hi");
        assert!(confused.kv(&k1).unwrap().delivery.gated.is_empty());

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
        let root = doc.create_kv(oid(9));

        let block = doc.new_kv(&root, s("block-1")).unwrap();
        let content = doc.new_seq(&block, s("content")).unwrap();
        doc.put(&block, s("color"), s("blue"));
        doc.text_insert(&content, 0, "hello world");

        assert_eq!(doc.text(&content).unwrap(), "hello world");
        assert_eq!(doc.kv(&block).unwrap().get(&s("color")), Some(s("blue")));
        // The root links to the block by... the creation op is the value; the
        // child object id is derived from it.
        assert!(doc.kv(&block).is_some());
        assert_eq!(doc.object_count(), 3); // root + block + content
    }

    #[test]
    fn per_object_frontiers_stay_lean() {
        // Editing one object never enters another's tips (per-object tips).
        let mut doc = HashWeb::new();
        let root = doc.create_kv(oid(9));
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
        let root = base.create_kv(oid(9));
        let text = base.new_seq(&root, s("text")).unwrap();
        let meta = base.new_kv(&root, s("meta")).unwrap();

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
        let root = a.create_kv(oid(9));
        let child = a.new_seq(&root, s("t")).unwrap();
        a.text_insert(&child, 0, "x");

        // Extract the child's inserts and the creation chain separately
        // (root adoption -> root map's put -> child).
        let child_nodes = a.seq(&child).unwrap().all_nodes();
        let creation_chain: Vec<(Id, HashNode)> = a.kv(&root).unwrap().all_nodes();

        let mut fresh = HashWeb::new();
        assert_eq!(fresh.create_kv(oid(9)), root, "same seed, same object id");
        // Child op first: its envelope names an unknown object — parks.
        for (id, node) in &child_nodes {
            fresh.apply_to_with_id(child, *id, node.clone());
        }
        assert_eq!(fresh.orphans().count(), 1);
        assert!(fresh.seq(&child).is_none());
        // The creation chain arrives: derive-and-wake, transitively.
        for (id, node) in creation_chain {
            fresh.apply_to_with_id(root, id, node);
        }
        assert_eq!(fresh.orphans().count(), 0);
        assert_eq!(fresh.text(&child).unwrap(), "x");
    }

    #[test]
    fn refs_spanning_objects_park_in_their_object() {
        let mut doc = HashWeb::new();
        let root = doc.create_kv(oid(9));
        let a = doc.new_seq(&root, s("a")).unwrap();
        let b = doc.new_seq(&root, s("b")).unwrap();
        doc.text_insert(&a, 0, "a");
        doc.text_insert(&b, 0, "b");
        let a0 = doc.seq(&a).unwrap().id_at(0).unwrap();
        let b0 = doc.seq(&b).unwrap().id_at(0).unwrap();

        // A remove naming elements of two objects, enveloped to `a`: the
        // foreign ref never arrives inside `a`, so the op parks there
        // forever — the same fate as any garbage ref, no verdict needed.
        let node = HashNode {
            pins: Default::default(),
            op: Op::Remove([a0, b0].into_iter().collect()),
        };
        doc.apply_to(a, node);
        assert_eq!(doc.seq(&a).unwrap().delivery.orphans().count(), 1);
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
        let text = doc.create_seq(oid(7));
        doc.text_insert(&text, 0, "see [] here");

        let inner = doc.new_seq_at(&text, 5).expect("inline creation");
        doc.text_insert(&inner, 0, "the embedded doc");

        assert_eq!(doc.text(&inner).unwrap(), "the embedded doc");
        let body = doc.text(&text).unwrap();
        assert_eq!(body, format!("see [{}] here", crate::hashseq::ATOM_CHAR));
        // The atom's payload is the creation value; the child origin
        // derives from the creating op's id.
        let seq = doc.seq(&text).unwrap();
        let atom_id = seq.id_at(5).unwrap();
        assert_eq!(seq.payload_of(&atom_id), Some(*crate::value::NEW_SEQ));
        assert_eq!(
            crate::value::object_id(crate::value::VK_NEW_SEQ, &atom_id),
            inner
        );

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
        let a = doc.create_seq(oid(3));
        let b = doc.create_seq(oid(4));
        doc.text_insert(&a, 0, "link: ");
        doc.text_insert(&b, 0, "the target");

        // Insert a link to b inside a (the origin id as a Ref-like value:
        // carried as raw payload id — no artifact bytes needed).
        let node = {
            let seq = doc.seq(&a).unwrap();
            seq.make_insert_value(6, b).unwrap()
        };
        doc.apply_to(a, node.clone());
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
        let root = base.create_kv(oid(9));
        let mut r1 = base.clone();
        let mut r2 = base.clone();
        let c1 = r1.new_seq(&root, s("content")).unwrap();
        let c2 = r2.new_kv(&root, s("content")).unwrap();
        r1.text_insert(&c1, 0, "one");

        base.merge(r1);
        base.merge(r2);
        assert!(base.seq(&c1).is_some());
        assert!(base.kv(&c2).is_some());
        assert!(matches!(
            base.kv(&root).unwrap().read(&s("content")),
            crate::hashkv::Read::Conflict(_)
        ));
        assert_eq!(base.text(&c1).unwrap(), "one");
    }
}
