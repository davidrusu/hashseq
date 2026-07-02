//! HashWeb: the composition — one op DAG hosting many objects
//! (HASHWEB_SPEC.md). It adds no new conflict type: every op routes to a
//! per-object projection (`HashSeq` or `HashKv`) that resolves per its own
//! spec. What lives here is objects and creation, routing, per-object
//! frontiers, the origin-id derive-and-wake, and the schema gate's
//! op-kind-vs-object-type row.
//!
//! Objects are **holonic**: identified by origin ids (`object_id` of the
//! creation op; the root's origin is the document id itself, supplied
//! out-of-band), and any object is a complete replica root from its own
//! perspective.

use std::collections::HashMap;

use rustc_hash::FxHashMap;

use crate::hashkv::HashKv;
use crate::hashseq::{IdMap, IdSet};
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

#[derive(Debug, Clone)]
pub struct HashWeb {
    /// The document identity — the root object's origin id.
    root: Id,
    /// Objects by origin id. The root is a `Map` (the block-document
    /// default); children are created by ops whose payload/value is a
    /// creation artifact.
    objects: FxHashMap<Id, Object>,
    /// node id -> the origin id of the object it belongs to (the routing
    /// table — replica-local, derived, never on the wire).
    node_home: IdMap<Id>,
    /// Global orphan buffering: refs cross objects at creation bridges, so
    /// parking is document-wide; each object's own buffer stays empty.
    orphaned: HashMap<Id, Vec<(Id, HashNode)>>,
    orphan_ids: IdSet,
    /// The composition-level gate: ops whose refs determine no single
    /// object, or whose kind the routed object does not admit, quarantine
    /// here or in their object (both re-presented on merge).
    pub gated: IdMap<HashNode>,
    /// Value-artifact side store shared across objects.
    values: IdMap<Vec<u8>>,
}

impl PartialEq for HashWeb {
    fn eq(&self, other: &Self) -> bool {
        if self.root != other.root || self.objects.len() != other.objects.len() {
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

impl Default for HashWeb {
    fn default() -> Self {
        Self::new(Id::default())
    }
}

impl HashWeb {
    /// Open the document identified by `doc_id`. The root object is a map;
    /// its origin id is the doc id — chosen out-of-band, no creation op,
    /// present axiomatically (GRAMMAR_SPEC.md `ref_count ≥ 1`).
    pub fn new(doc_id: Id) -> Self {
        let mut objects = FxHashMap::default();
        objects.insert(doc_id, Object::Map(HashKv::new(doc_id)));
        Self {
            root: doc_id,
            objects,
            node_home: IdMap::default(),
            orphaned: HashMap::new(),
            orphan_ids: IdSet::default(),
            gated: IdMap::default(),
            values: IdMap::default(),
        }
    }

    pub fn root_id(&self) -> Id {
        self.root
    }

    pub fn root(&self) -> &HashKv {
        match &self.objects[&self.root] {
            Object::Map(m) => m,
            _ => unreachable!("root is a map"),
        }
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

    /// An id is present if it is an applied node or a live object origin.
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

    pub fn apply_with_id(&mut self, id: Id, node: HashNode) {
        debug_assert_eq!(id, node.id());
        if self.contains(&id) {
            return;
        }
        if !self.orphan_ids.is_empty() && self.orphan_ids.contains(&id) {
            return;
        }
        if !self.gated.is_empty() && self.gated.contains_key(&id) {
            return;
        }
        let mut queue: Vec<(Id, HashNode)> = Vec::new();
        self.park_or_dispatch(id, node, &mut queue);
        while let Some((id, node)) = queue.pop() {
            self.park_or_dispatch(id, node, &mut queue);
        }
    }

    fn park_or_dispatch(&mut self, id: Id, node: HashNode, queue: &mut Vec<(Id, HashNode)>) {
        // Global buffering: park on the first ref not present anywhere in
        // the document (origin ids count as present once their object
        // exists — the derive-and-wake below makes that true).
        let missing = node.iter_refs().find(|d| !self.contains(d)).copied();
        if let Some(missing) = missing {
            self.orphan_ids.insert(id);
            self.orphaned.entry(missing).or_default().push((id, node));
            return;
        }
        if !self.orphan_ids.is_empty() {
            self.orphan_ids.remove(&id);
        }

        // Routing (stable gate on refs spanning objects).
        let Some(home) = self.route(&node) else {
            self.gated.insert(id, node);
            return;
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
            if !self.orphaned.is_empty()
                && let Some(waiting) = self.orphaned.remove(&child)
            {
                queue.extend(waiting);
            }
        }

        if !self.orphaned.is_empty()
            && let Some(waiting) = self.orphaned.remove(&id)
        {
            queue.extend(waiting);
        }
    }

    pub fn merge(&mut self, other: Self) {
        assert_eq!(self.root, other.root, "different documents never merge");
        for (vid, bytes) in other.values {
            self.values.entry(vid).or_insert(bytes);
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
        for (id, node) in other.orphaned.into_values().flatten() {
            self.apply_with_id(id, node);
        }
        for (id, node) in other.gated {
            self.apply_with_id(id, node);
        }
    }

    pub fn orphans(&self) -> impl Iterator<Item = &HashNode> {
        self.orphaned.values().flatten().map(|(_, node)| node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> Value {
        Value::String(v.into())
    }

    #[test]
    fn block_document_shape() {
        // A Notion-style block: a map with "content" -> Text child.
        let mut doc = HashWeb::default();
        let root = doc.root_id();

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
        let mut doc = HashWeb::default();
        let root = doc.root_id();
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
        let mut base = HashWeb::default();
        let root = base.root_id();
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
        let mut a = HashWeb::default();
        let root = a.root_id();
        let child = a.new_seq(&root, s("t")).unwrap();
        a.text_insert(&child, 0, "x");

        // Extract the child's insert and the creation put separately.
        let child_nodes = a.seq(&child).unwrap().all_nodes();
        let root_nodes = a.root().all_nodes();

        let mut fresh = HashWeb::new(root);
        // Child op first: parks (its ref — the origin id — is unknown).
        for (id, node) in &child_nodes {
            fresh.apply_with_id(*id, node.clone());
        }
        assert_eq!(fresh.orphans().count(), 1);
        assert!(fresh.seq(&child).is_none());
        // Creation op arrives: derive-and-wake.
        for (id, node) in root_nodes {
            fresh.apply_with_id(id, node);
        }
        assert_eq!(fresh.orphans().count(), 0);
        assert_eq!(fresh.text(&child).unwrap(), "x");
    }

    #[test]
    fn refs_spanning_objects_gate() {
        let mut doc = HashWeb::default();
        let root = doc.root_id();
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
        assert_eq!(doc.gated.len(), 1);
        assert_eq!(doc.text(&a).unwrap(), "a"); // untouched
        assert_eq!(doc.text(&b).unwrap(), "b");
    }

    #[test]
    fn same_key_concurrent_children_are_both_born() {
        // Two replicas concurrently create a child at the same key: the key
        // register conflicts (MVR), but BOTH child objects exist — creation
        // is never lost, the app resolves the register.
        let mut base = HashWeb::default();
        let root = base.root_id();
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
            base.root().read(&s("content")),
            crate::hashkv::Read::Conflict(_)
        ));
        assert_eq!(base.text(&c1).unwrap(), "one");
    }
}
