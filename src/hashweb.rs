//! HashWeb: a flat store of every object this replica knows about.
//!
//! The store has **no identity and no semantics of its own** — it is not a
//! datastructure, and it defines no composition. What it does is
//! mechanics: open objects, deliver enveloped ops (`obj_id ‖ node` —
//! transport metadata, never hashed), park envelopes for unknown objects,
//! and merge as a union of knowledge. Composition is the *user's*
//! convention: a link is an object id carried as a value; ownership-style
//! nesting is recreated by opening a child at one of your own op ids
//! (`create_seq(node.id())`) — the derived identity and the causal weld
//! come for free, with no creation semantics at this layer.
//!
//! Objects are **holonic**: each is a complete replica rooted at its own
//! origin — an arbitrary 32-byte value its creator chose (often another
//! op's id, per the convention above). Ops anchor at the origin; the
//! store indexes by the derived **object id**, `object_id(kind ‖ origin)`,
//! which never appears in a preimage — it is the envelope's address, with
//! the kind inside it, so the same origin opened as a Seq and as a Kv is
//! two objects and kind mis-agreement is unrepresentable. Every op
//! commits transitively to its object's origin, so one object's ops can
//! never merge into another — the commitment domain is per object.

use rustc_hash::FxHashMap;

use crate::hashkv::HashKv;
use crate::hashseq::IdMap;
use crate::value::{KIND_KV, KIND_SEQ, Value, object_id};
use crate::{HashNode, HashSeq, Id};

#[derive(Debug, Clone, Default)]
pub struct HashWeb {
    /// Every seq object this replica knows, keyed by derived object id
    /// (`object_id(kind ‖ origin)`).
    pub(crate) seqs: FxHashMap<Id, HashSeq>,
    /// Every map object, likewise.
    pub(crate) kvs: FxHashMap<Id, HashKv>,
    /// Envelopes parked on object ids this store does not know yet;
    /// opening or adopting the object wakes them. Node-level parking
    /// lives inside each object's own delivery — the store keeps no
    /// per-node state at all.
    pub(crate) parked: FxHashMap<Id, Vec<(Id, HashNode)>>,
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
    /// the derived object id, `object_id(KIND_SEQ ‖ origin)`, which is
    /// returned as the handle: the kind is inside the store-level identity,
    /// so the same origin opened as a Kv is a different object, not a
    /// clash. The *ops* keep anchoring at the origin itself — the derived
    /// id is a store address (envelope + index) and never appears in a
    /// preimage. Idempotent.
    pub fn create_seq(&mut self, origin: Id) -> Id {
        let obj = object_id(KIND_SEQ, &origin);
        if !self.is_object(&obj) {
            self.seqs.insert(obj, HashSeq::new(origin));
            self.wake(obj);
        }
        obj
    }

    /// Open a root map from `origin` — see [`Self::create_seq`].
    pub fn create_kv(&mut self, origin: Id) -> Id {
        let obj = object_id(KIND_KV, &origin);
        if !self.is_object(&obj) {
            self.kvs.insert(obj, HashKv::new(origin));
            self.wake(obj);
        }
        obj
    }

    /// The full object, for reading. The store has no read API of its
    /// own — iterate, index, and resolve on the object directly.
    pub fn seq(&self, obj: &Id) -> Option<&HashSeq> {
        self.seqs.get(obj)
    }

    /// See [`Self::seq`].
    pub fn kv(&self, obj: &Id) -> Option<&HashKv> {
        self.kvs.get(obj)
    }

    /// The full object, for authoring. The store has no authoring API of
    /// its own either: edit the object with its own mutating surface
    /// (`insert_batch`, `remove_batch`, `insert_value`, `put`, `del`, the
    /// cursor…), which builds *and applies* each op in one step — an edit
    /// can never be built and then forgotten, and consecutive edits
    /// anchor on each other instead of mis-anchoring at a stale snapshot.
    /// Build-without-apply (`make_*`) is the *wire* vocabulary: a remote
    /// node arrives already built and is delivered with
    /// [`Self::apply_to`].
    pub fn seq_mut(&mut self, obj: &Id) -> Option<&mut HashSeq> {
        self.seqs.get_mut(obj)
    }

    /// See [`Self::seq_mut`].
    pub fn kv_mut(&mut self, obj: &Id) -> Option<&mut HashKv> {
        self.kvs.get_mut(obj)
    }

    /// Every known object id (seqs and kvs).
    pub fn objects(&self) -> impl Iterator<Item = &Id> {
        self.seqs.keys().chain(self.kvs.keys())
    }

    pub fn object_count(&self) -> usize {
        self.seqs.len() + self.kvs.len()
    }

    /// Is this id a live object (keyed by derived object id)?
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

    // ---- delivery (the routing envelope: `obj_id ‖ HashNode`) ----

    /// Deliver an enveloped op: `obj ‖ node`. The envelope replaces both a
    /// route field (there is none — GRAMMAR_SPEC.md) and any per-node
    /// routing table; it is transport metadata that needs no trust: an op
    /// enveloped to the wrong object simply never applies there (its refs
    /// never arrive in that object), the same fate as any garbage ref —
    /// bounded, attributable, and correct by construction. Envelopes for
    /// unknown object ids park store-wide and wake when the object is
    /// opened or adopted.
    pub fn apply_to(&mut self, obj: Id, node: HashNode) {
        let id = node.id();
        self.apply_to_with_id(obj, id, node);
    }

    pub fn apply_to_with_id(&mut self, obj: Id, id: Id, node: HashNode) {
        debug_assert_eq!(id, node.id(), "apply_to_with_id called with a wrong id");
        if let Some(seq) = self.seqs.get_mut(&obj) {
            seq.apply_with_id(id, node);
        } else if let Some(kv) = self.kvs.get_mut(&obj) {
            kv.apply_with_id(id, node);
        } else {
            self.parked.entry(obj).or_default().push((id, node));
        }
    }

    /// Deliver envelopes parked on a newly opened object.
    fn wake(&mut self, obj: Id) {
        let Some(envelopes) = self.parked.remove(&obj) else {
            return;
        };
        for (id, node) in envelopes {
            self.apply_to_with_id(obj, id, node);
        }
    }

    /// Merge = union of knowledge. Unconditional: there is no store
    /// identity to compare. Missing objects are adopted with their kind
    /// (committed inside their derived object id); every op re-delivers
    /// in its own envelope.
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
            self.wake(obj);
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

}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::Op;

    fn s(v: &str) -> Value {
        Value::String(v.into())
    }

    fn oid(n: u8) -> Id {
        Id([n; 32])
    }

    // App-level helpers over the public surface: author on the object via
    // `seq_mut`/`kv_mut` (each edit applies as it is built), read on the
    // object via `seq`/`kv`, resolve values through both the store-wide
    // and the object's own artifact store.
    pub(crate) fn put(web: &mut HashWeb, obj: &Id, key: Value, value: Value) -> HashNode {
        web.provide_value(&key);
        web.provide_value(&value);
        web.kv_mut(obj).unwrap().put(key, value)
    }

    pub(crate) fn type_text(web: &mut HashWeb, obj: &Id, idx: usize, text: &str) {
        web.seq_mut(obj).unwrap().insert_batch(idx, text.chars());
    }

    pub(crate) fn insert_value(web: &mut HashWeb, obj: &Id, idx: usize, value: &Value) -> HashNode {
        let vid = web.provide_value(value);
        web.seq_mut(obj).unwrap().insert_value(idx, vid)
    }

    pub(crate) fn read_text(web: &HashWeb, obj: &Id) -> String {
        web.seq(obj).unwrap().iter().collect()
    }

    pub(crate) fn get(web: &HashWeb, obj: &Id, key: &Value) -> Option<Value> {
        match web.kv(obj)?.read(key) {
            crate::hashkv::Read::One(vid) => {
                web.resolve(&vid).or_else(|| web.kv(obj)?.resolve(&vid))
            }
            _ => None,
        }
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
        type_text(&mut a, &s1, 0, "hi");
        put(&mut b, &k2, s("k"), s("v"));

        a.merge(b.clone());
        assert_eq!(a.object_count(), 2);
        assert_eq!(read_text(&a, &s1), "hi");
        assert_eq!(get(&a, &k2, &s("k")), Some(s("v")));

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
        assert_eq!(read_text(&fresh, &s1), "hi");

        // Kind "mis-agreement" is unrepresentable: the same seed opened as
        // a Kv derives a different object id — the two coexist, nothing
        // quarantines.
        let mut confused = HashWeb::new();
        let k1 = confused.create_kv(oid(1));
        assert_ne!(k1, s1);
        confused.merge(a.clone());
        assert_eq!(confused.object_count(), 3);
        assert_eq!(read_text(&confused, &s1), "hi");
        assert!(confused.kv(&k1).unwrap().delivery.gated.is_empty());

        // Roundtrip of a multi-root store.
        let decoded = crate::encoding::decode_hashweb_strict(&crate::encoding::encode_hashweb(
            &a,
        ))
        .expect("strict");
        assert_eq!(decoded, a);
    }

    /// PLACEMENT_SPEC.md end to end: cross-container move is Insert (order
    /// claim, in the destination) + Place (membership claim, in the moved
    /// object's own DAG). Concurrent moves freeze at the last agreed
    /// placement — no duplication is representable; the next Place naming
    /// both heads resolves; Place ops survive the canonical snapshot.
    #[test]
    fn cross_container_move_via_placement_register() {
        let mut a = HashWeb::new();
        let child_origin = oid(0x10);
        let child = a.create_seq(child_origin);
        let p = a.create_seq(oid(0x20));
        let q = a.create_seq(oid(0x30));
        let r = a.create_seq(oid(0x40));
        type_text(&mut a, &child, 0, "content");

        // Birth: a containment link in P (payload = the child's ORIGIN —
        // the containment type per the spec's typing rule).
        let birth = a.seq_mut(&p).unwrap().insert_value(0, child_origin);
        // Agreed initial placement (upgrades the child out of the legacy
        // presence rule).
        a.seq_mut(&child).unwrap().place(birth.id());
        assert_eq!(a.seq(&child).unwrap().placement().chain(), vec![birth.id()]);

        // Fork two replicas.
        let mut b = a.clone();

        // a moves the child into Q; b concurrently moves it into R.
        let link_q = a.seq_mut(&q).unwrap().insert_value(0, child_origin);
        a.seq_mut(&child).unwrap().place(link_q.id());
        let link_r = b.seq_mut(&r).unwrap().insert_value(0, child_origin);
        b.seq_mut(&child).unwrap().place(link_r.id());

        // Merge both ways.
        let mut ab = a.clone();
        ab.merge(b.clone());
        let mut ba = b.clone();
        ba.merge(a.clone());

        for web in [&ab, &ba] {
            let reg = web.seq(&child).unwrap().placement();
            assert!(reg.conflicted(), "two heads — surfaced, never silently won");
            // Freeze: the chain starts at the last AGREED placement (the
            // birth atom) — neither contender's destination renders, and
            // exactly one placement candidate exists: no duplication.
            assert_eq!(reg.chain(), vec![birth.id()],
                "the chain starts below the contenders, at the agreed birth placement");
        }
        // Identical bytes on both merge orders (canonical encoding is the
        // convergence test).
        let bytes_ab = crate::encoding::encode_hashweb(&ab);
        let bytes_ba = crate::encoding::encode_hashweb(&ba);
        assert_eq!(bytes_ab, bytes_ba);

        // A resolving Place naming both heads dominates.
        let link_q2 = ab.seq_mut(&q).unwrap().insert_value(0, child_origin);
        ab.seq_mut(&child).unwrap().place(link_q2.id());
        let reg = ab.seq(&child).unwrap().placement();
        assert!(!reg.conflicted());
        assert_eq!(reg.chain()[0], link_q2.id());

        // Place ops round-trip the canonical snapshot exactly.
        let decoded = crate::encoding::decode_hashweb_strict(
            &crate::encoding::encode_hashweb(&ab),
        )
        .expect("strict");
        assert_eq!(decoded, ab);
        assert_eq!(
            decoded.seq(&child).unwrap().placement().chain(),
            ab.seq(&child).unwrap().placement().chain()
        );

        // Kv objects carry the same register (a page is placeable too).
        let page = ab.create_kv(oid(0x50));
        let link_pg = ab.seq_mut(&p).unwrap().insert_value(0, oid(0x50));
        ab.kv_mut(&page).unwrap().place(link_pg.id());
        assert_eq!(ab.kv(&page).unwrap().placement().chain(), vec![link_pg.id()]);
        let decoded = crate::encoding::decode_hashweb_strict(
            &crate::encoding::encode_hashweb(&ab),
        )
        .expect("strict");
        assert_eq!(
            decoded.kv(&page).unwrap().placement().chain(),
            vec![link_pg.id()]
        );
    }

    /// Delivery-order independence: Place ops arriving before their
    /// overwritten predecessors park on refs and converge identically.
    #[test]
    fn place_ops_converge_under_any_delivery_order() {
        let mut a = HashWeb::new();
        let child_origin = oid(0x11);
        let child = a.create_seq(child_origin);
        let p = a.create_seq(oid(0x21));
        let l1 = a.seq_mut(&p).unwrap().insert_value(0, child_origin);
        let pl1 = a.seq_mut(&child).unwrap().place(l1.id());
        let l2 = a.seq_mut(&p).unwrap().insert_value(0, child_origin);
        let pl2 = a.seq_mut(&child).unwrap().place(l2.id());
        let expected = a.seq(&child).unwrap().placement().chain();
        assert_eq!(expected, vec![l2.id(), l1.id()]);

        // Reverse delivery: the superseder first — it parks on its
        // overwritten ref, then wakes when the predecessor lands.
        let mut fresh = HashWeb::new();
        fresh.create_seq(child_origin);
        let child2 = object_id(KIND_SEQ, &child_origin);
        fresh.apply_to_with_id(child2, pl2.id(), pl2.clone());
        assert_eq!(fresh.seq(&child2).unwrap().placement().heads().len(), 0);
        fresh.apply_to_with_id(child2, pl1.id(), pl1.clone());
        assert_eq!(
            fresh.seq(&child2).unwrap().placement().chain(),
            expected,
            "late-delivered supersession converges"
        );
    }

    #[test]
    fn block_document_shape() {
        // A Notion-style block, composed in user space: creation is not a
        // store concept — the app commits an op in the parent, then opens
        // the child at that op's id. Same derived identity, same causal
        // weld, no magic.
        let mut doc = HashWeb::new();
        let root = doc.create_kv(oid(9));

        let p1 = put(&mut doc, &root, s("block-1"), s("kv"));
        let block = doc.create_kv(p1.id());
        let p2 = put(&mut doc, &block, s("content"), s("seq"));
        let content = doc.create_seq(p2.id());
        put(&mut doc, &block, s("color"), s("blue"));
        type_text(&mut doc, &content, 0, "hello world");

        assert_eq!(read_text(&doc, &content), "hello world");
        assert_eq!(doc.kv(&block).unwrap().get(&s("color")), Some(s("blue")));
        assert_eq!(doc.object_count(), 3); // root + block + content
        // The convention is reproducible from the ops: anyone holding the
        // parent's op can re-derive the child's address.
        assert_eq!(object_id(KIND_KV, &p1.id()), block);
    }

    #[test]
    fn per_object_frontiers_stay_lean() {
        // Editing one object never enters another's tips (per-object tips).
        let mut doc = HashWeb::new();
        let root = doc.create_kv(oid(9));
        let pa = put(&mut doc, &root, s("a"), s("seq"));
        let a = doc.create_seq(pa.id());
        let pb = put(&mut doc, &root, s("b"), s("seq"));
        let b = doc.create_seq(pb.id());

        type_text(&mut doc, &a, 0, "aaa");
        type_text(&mut doc, &b, 0, "bbb");

        // Each seq's frontier is a single run tail — no cross-contamination.
        assert_eq!(doc.seq(&a).unwrap().tips().len(), 1);
        assert_eq!(doc.seq(&b).unwrap().tips().len(), 1);
    }

    #[test]
    fn concurrent_edits_in_different_objects_merge_cleanly() {
        let mut base = HashWeb::new();
        let root = base.create_kv(oid(9));
        let pt = put(&mut base, &root, s("text"), s("seq"));
        let text = base.create_seq(pt.id());
        let pm = put(&mut base, &root, s("meta"), s("kv"));
        let meta = base.create_kv(pm.id());

        let mut r1 = base.clone();
        let mut r2 = base.clone();
        type_text(&mut r1, &text, 0, "hello");
        put(&mut r2, &meta, s("status"), s("draft"));

        let mut m1 = r1.clone();
        m1.merge(r2.clone());
        let mut m2 = r2;
        m2.merge(r1);

        assert_eq!(m1, m2);
        assert_eq!(read_text(&m1, &text), "hello");
        assert_eq!(get(&m1, &meta, &s("status")), Some(s("draft")));
        assert_eq!(get(&m2, &meta, &s("status")), Some(s("draft")));
    }

    #[test]
    fn envelopes_park_until_object_is_opened() {
        // Deliver an object's ops before this replica knows the object:
        // the envelopes park store-wide; opening the object wakes them.
        let mut a = HashWeb::new();
        let root = a.create_kv(oid(9));
        let p = put(&mut a, &root, s("t"), s("seq"));
        let child = a.create_seq(p.id());
        type_text(&mut a, &child, 0, "x");
        let child_nodes = a.seq(&child).unwrap().all_nodes();

        let mut fresh = HashWeb::new();
        for (id, node) in &child_nodes {
            fresh.apply_to_with_id(child, *id, node.clone());
        }
        assert_eq!(fresh.orphans().count(), 1);
        assert!(fresh.seq(&child).is_none());
        // The app learns the convention's input (the parent op's id) and
        // opens the child: parked envelopes deliver.
        assert_eq!(fresh.create_seq(p.id()), child);
        assert_eq!(fresh.orphans().count(), 0);
        assert_eq!(read_text(&fresh, &child), "x");
    }

    #[test]
    fn refs_spanning_objects_park_in_their_object() {
        let mut doc = HashWeb::new();
        let a = doc.create_seq(oid(1));
        let b = doc.create_seq(oid(2));
        type_text(&mut doc, &a, 0, "a");
        type_text(&mut doc, &b, 0, "b");
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
        assert_eq!(read_text(&doc, &a), "a"); // untouched
        assert_eq!(read_text(&doc, &b), "b");
    }

    /// Child-in-seq, user-space: an inline element op is the child's
    /// origin — the app inserts a marker value, then opens the child at
    /// the element's id. The element renders as the atom placeholder.
    #[test]
    fn child_object_opened_at_an_inline_element() {
        let mut doc = HashWeb::new();
        let text = doc.create_seq(oid(7));
        type_text(&mut doc, &text, 0, "see [] here");

        let node = insert_value(&mut doc, &text, 5, &s("embed"));
        let inner = doc.create_seq(node.id());
        type_text(&mut doc, &inner, 0, "the embedded doc");

        assert_eq!(read_text(&doc, &inner), "the embedded doc");
        let body = read_text(&doc, &text);
        assert_eq!(body, format!("see [{}] here", crate::hashseq::ATOM_CHAR));
        // The atom's payload is the app's marker; the child's address
        // derives from the element op's id.
        let seq = doc.seq(&text).unwrap();
        let atom_id = seq.id_at(5).unwrap();
        assert_eq!(seq.payload_of(&atom_id), Some(s("embed").value_id()));
        assert_eq!(object_id(KIND_SEQ, &atom_id), inner);

        // Merges carry the whole shape both ways.
        let mut other = HashWeb::new();
        other.merge(doc.clone());
        assert_eq!(other, doc);
        assert_eq!(read_text(&other, &inner), "the embedded doc");
    }

    /// A link atom: a seq element whose payload is another object's origin
    /// id — rendering resolves it by id, nothing embeds.
    #[test]
    fn link_atoms_reference_other_objects()  {
        let mut doc = HashWeb::new();
        let a = doc.create_seq(oid(3));
        let b = doc.create_seq(oid(4));
        type_text(&mut doc, &a, 0, "link: ");
        type_text(&mut doc, &b, 0, "the target");

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
        assert_eq!(read_text(&doc, &b), "the target");
    }

    #[test]
    fn same_key_concurrent_children_both_survive_merge() {
        // Two replicas concurrently attach a child at the same key: the
        // key register conflicts (MVR), but BOTH child objects exist after
        // merge (adoption is unconditional) — the app resolves the
        // register.
        let mut base = HashWeb::new();
        let root = base.create_kv(oid(9));
        let mut r1 = base.clone();
        let mut r2 = base.clone();
        let p1 = put(&mut r1, &root, s("content"), s("seq"));
        let c1 = r1.create_seq(p1.id());
        let p2 = put(&mut r2, &root, s("content"), s("kv"));
        let c2 = r2.create_kv(p2.id());
        type_text(&mut r1, &c1, 0, "one");

        base.merge(r1);
        base.merge(r2);
        assert!(base.seq(&c1).is_some());
        assert!(base.kv(&c2).is_some());
        assert!(matches!(
            base.kv(&root).unwrap().read(&s("content")),
            crate::hashkv::Read::Conflict(_)
        ));
        assert_eq!(read_text(&base, &c1), "one");
    }
}
