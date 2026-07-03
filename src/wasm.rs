//! WebAssembly bindings for hashseq.
//!
//! Mirrors the design of the emacs dynamic-module FFI (`hashseq-emacs`): the same
//! cursor → in-flight Run → op model, per-op (delta) sync, and structural
//! introspection. IDs cross the boundary as 64-char lowercase hex strings;
//! encoded ops cross as `Uint8Array`.
//!
//! Local-mutation contract (matching the emacs FFI): every local change yields a
//! serializable op you can broadcast, and every op from a peer can be applied:
//!   - insert burst:  `WasmRun.newAfter`/`newBefore` + `extend`, then
//!     `seq.applyRun(run)`; broadcast `run.encodeOp()`.
//!   - delete:        `seq.removeAndEncodeOp(idx, n)` mutates and returns the op.
//!   - remote op:     `seq.applyOp(bytes)`.

use wasm_bindgen::prelude::*;

use crate::encoding::{
    decode_hashseq, decode_hashweb, decode_op, encode_hashseq, encode_hashweb, encode_op,
};
use crate::hashseq::{Cursor, Loc};
use crate::hashweb::HashWeb;
use crate::value::{KIND_KV, KIND_SEQ, TOMBSTONE, Value, object_id};
use crate::{Anchor, EncodableOp, HashSeq, Id, Run};

fn id_to_hex(id: &Id) -> String {
    hex::encode(id.0)
}

fn hex_to_id(s: &str) -> Result<Id, JsValue> {
    let bytes = hex::decode(s).map_err(|e| JsValue::from_str(&format!("bad id hex: {e}")))?;
    if bytes.len() != 32 {
        return Err(JsValue::from_str("id must be 32 bytes (64 hex chars)"));
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(Id(out))
}

fn first_char(s: &str) -> Result<char, JsValue> {
    s.chars()
        .next()
        .ok_or_else(|| JsValue::from_str("expected non-empty string"))
}

/// One box in the `structure_json` dump; field names are the wire contract
/// with the visualizer.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct StructureNode {
    id: String,
    kind: &'static str,
    text: String,
    parent: Option<String>,
    parent_offset: Option<usize>,
    rel: Option<&'static str>,
    removed: bool,
    removed_mask: String,
    deps: Vec<StructureDep>,
}

#[derive(serde::Serialize)]
struct StructureDep {
    r#box: String,
    off: usize,
}

#[derive(serde::Serialize)]
struct Structure {
    tips: Vec<String>,
    nodes: Vec<StructureNode>,
}

fn collect_deps(hex_deps: Vec<String>) -> Result<std::collections::BTreeSet<Id>, JsValue> {
    hex_deps.iter().map(|s| hex_to_id(s)).collect()
}

/// A causal insertion point, returned by `WasmHashSeq.cursorAt`. `op` is the
/// string `"after"` or `"before"` — telling the caller which `WasmRun`
/// constructor to use.
#[wasm_bindgen]
pub struct WasmCursor {
    op: String,
    anchor: String,
    extra_deps: Vec<String>,
}

#[wasm_bindgen]
impl WasmCursor {
    #[wasm_bindgen(getter)]
    pub fn op(&self) -> String {
        self.op.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn anchor(&self) -> String {
        self.anchor.clone()
    }

    #[wasm_bindgen(getter, js_name = extraDeps)]
    pub fn extra_deps(&self) -> Vec<String> {
        self.extra_deps.clone()
    }
}

/// An in-flight Run: a typing burst coalesced into a single op.
///
/// Build one from a cursor (`newAfter`/`newBefore`), `extend` it as the user
/// types, then `seq.applyRun(run)`. Broadcast `run.encodeOp()` to peers.
#[wasm_bindgen]
pub struct WasmRun {
    inner: Run,
}

#[wasm_bindgen]
impl WasmRun {
    /// Build an InsertAfter-rooted Run (first char lands immediately after `anchor`).
    #[wasm_bindgen(js_name = newAfter)]
    pub fn new_after(
        anchor_hex: &str,
        extra_deps: Vec<String>,
        first: &str,
    ) -> Result<WasmRun, JsValue> {
        let anchor = hex_to_id(anchor_hex)?;
        let deps = collect_deps(extra_deps)?;
        Ok(WasmRun {
            inner: Run::new(anchor, deps, first_char(first)?),
        })
    }

    /// Build an InsertBefore-rooted Run (first char is constrained to land
    /// immediately before `anchor`).
    #[wasm_bindgen(js_name = newBefore)]
    pub fn new_before(
        anchor_hex: &str,
        extra_deps: Vec<String>,
        first: &str,
    ) -> Result<WasmRun, JsValue> {
        let anchor = hex_to_id(anchor_hex)?;
        let deps = collect_deps(extra_deps)?;
        Ok(WasmRun {
            inner: Run::new_before(anchor, deps, first_char(first)?),
        })
    }

    /// Append a character to the run (continuation of the typing burst).
    pub fn extend(&mut self, ch: &str) -> Result<(), JsValue> {
        self.inner.extend(first_char(ch)?);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[wasm_bindgen(js_name = isEmpty)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn text(&self) -> String {
        self.inner.run.clone()
    }

    /// Serialize this run as an `EncodableOp::Run` for broadcast to peers.
    #[wasm_bindgen(js_name = encodeOp)]
    pub fn encode_op(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_op(&EncodableOp::Run(self.inner.clone()), &mut buf);
        buf
    }
}

#[wasm_bindgen]
#[derive(Default)]
pub struct WasmHashSeq {
    inner: HashSeq,
}

#[wasm_bindgen]
impl WasmHashSeq {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self::default()
    }

    // --- high-level convenience (full local edits) ---

    pub fn insert(&mut self, idx: usize, text: &str) {
        self.inner.insert_batch(idx, text.chars());
    }

    pub fn remove(&mut self, idx: usize, len: usize) {
        self.inner.remove_batch(idx, len);
    }

    pub fn text(&self) -> String {
        self.inner.iter().collect()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[wasm_bindgen(js_name = isEmpty)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    // --- identity / introspection ---

    /// Hex ID of the node at visible position `idx`, or undefined.
    #[wasm_bindgen(js_name = idAt)]
    pub fn id_at(&self, idx: usize) -> Option<String> {
        self.inner.id_at(idx).as_ref().map(id_to_hex)
    }

    /// Current visible position of the node `id_hex`, or undefined if absent/removed.
    #[wasm_bindgen(js_name = positionOf)]
    pub fn position_of(&self, id_hex: &str) -> Result<Option<usize>, JsValue> {
        Ok(self.inner.position_of(&hex_to_id(id_hex)?))
    }

    /// The current causal tips (heads of the DAG), as hex IDs.
    pub fn tips(&self) -> Vec<String> {
        self.inner.tips().iter().map(id_to_hex).collect()
    }

    /// Dump the structure as JSON for visualization, at run/node granularity.
    ///
    /// Each "box" is a root char, an After-run, or a Before-run (a burst whose
    /// first char is constrained before its anchor). Every non-root box resolves
    /// to one parent box via its anchor — which may sit mid-box, since befores can
    /// anchor at interior run elements — with a `rel` of `"after"` (right child)
    /// or `"before"` (left child) — i.e. the generalized binary tree of FIG. 01.
    ///
    /// Shape: `{ "tips": [hex...], "nodes": [
    ///   { "id": hex, "kind": "root"|"run"|"before", "text": str,
    ///     "parent": hex|null, "parentOffset": int|null,
    ///     "rel": "after"|"before"|null, "removed": bool,
    ///     "removedMask": str,
    ///     "deps": [{ "box": hex, "off": int }] } ] }`
    ///
    /// `deps` are extra causal dependencies beyond the anchor, each resolved to
    /// the box it lands in and the element offset within it (so a mid-run insert
    /// depending on the run's tip points at that tip char, not just the box).
    ///
    /// `removed` is true only when *every* element of the run is tombstoned (so
    /// the box reads as fully deleted); `removedMask` is one `'0'`/`'1'` per
    /// character (per run element) giving the per-char tombstone state, so a run
    /// with only some chars deleted can be rendered char-by-char.
    ///
    /// `parentOffset` is the anchor's element index within the parent box: a
    /// `rel:"before"` child sits immediately before that char, a `rel:"after"`
    /// child immediately after it.
    #[wasm_bindgen(js_name = structureJson)]
    pub fn structure_json(&self) -> String {
        let s = &self.inner;
        // Resolve any element id to (its box id, element offset within the
        // box): anchors may point at interior run elements (befores don't
        // split runs), and so may extra-deps (a mid-run insert depends on the
        // run's *tip*, a different element of the same box than its anchor).
        // Keeping the offset lets each edge attach to the right character.
        let resolve = |id: &Id| -> (Id, usize) {
            match s.idx_of(id).map(|i| s.loc_of(i)) {
                Some(Loc::Run { run, pos }) => (s.id_of(run), pos as usize),
                _ => (*id, 0),
            }
        };

        let mut nodes = Vec::new();
        for (head, run) in &s.runs {
            // Origin-anchored runs are the document's top level — emitted
            // with no anchor, like the old standalone root nodes.
            let is_top_level = run.anchor == s.origin();
            let (kind, rel) = match run.first_op {
                _ if is_top_level => ("root", "after"),
                crate::run::FirstOp::After => ("run", "after"),
                crate::run::FirstOp::Before => ("before", "before"),
            };
            let (parent, parent_offset) = if is_top_level {
                (None, None)
            } else {
                let (box_id, off) = resolve(&run.anchor);
                (Some(id_to_hex(&box_id)), Some(off))
            };
            // Per-element tombstone state — one '0'/'1' per char, aligned
            // with `run.text` (one element per char). `removed` (box-level)
            // is true only when the whole run is gone.
            let mut removed_mask = String::with_capacity(run.elements.len());
            let mut all_removed = true;
            for e in &run.elements {
                let r = s.is_removed(*e);
                removed_mask.push(if r { '1' } else { '0' });
                all_removed &= r;
            }
            let mut seen = std::collections::BTreeSet::new();
            let deps = run
                .first_extra_deps
                .iter_ids(&s.ids)
                .map(|id| resolve(&id))
                .filter(|bo| seen.insert(*bo)) // dedup by (box, offset)
                .map(|(box_id, off)| StructureDep {
                    r#box: id_to_hex(&box_id),
                    off,
                })
                .collect();
            nodes.push(StructureNode {
                id: id_to_hex(&s.id_of(*head)),
                kind,
                text: run.text.clone(),
                parent,
                parent_offset,
                rel: (!is_top_level).then_some(rel),
                removed: all_removed,
                removed_mask,
                deps,
            });
        }
        let structure = Structure {
            tips: s.tips().iter().map(id_to_hex).collect(),
            nodes,
        };
        serde_json::to_string(&structure).expect("plain data serializes")
    }

    /// Build a `WasmCursor` for inserting at position `idx`, or undefined when
    /// `idx` is out of bounds. In an empty document the cursor is
    /// After(origin), so it is always run-buildable.
    #[wasm_bindgen(js_name = cursorAt)]
    pub fn cursor_at(&self, idx: usize) -> Option<WasmCursor> {
        self.inner.cursor_at(idx).map(|cursor| {
            let (op, anchor, extra_deps) = match cursor {
                Cursor::After { anchor, extra_deps } => ("after", anchor, extra_deps),
                Cursor::Before { anchor, extra_deps } => ("before", anchor, extra_deps),
            };
            WasmCursor {
                op: op.to_string(),
                anchor: id_to_hex(&anchor),
                extra_deps: extra_deps.iter().map(id_to_hex).collect(),
            }
        })
    }

    // --- per-op (delta) sync ---

    /// Apply an in-flight run to the sequence.
    #[wasm_bindgen(js_name = applyRun)]
    pub fn apply_run(&mut self, run: &WasmRun) {
        self.inner.apply_op(EncodableOp::Run(run.inner.clone()));
    }

    /// Remove `n` chars at visible position `idx` and return the resulting op for
    /// broadcast, or undefined if nothing was removed.
    #[wasm_bindgen(js_name = removeAndEncodeOp)]
    pub fn remove_and_encode_op(&mut self, idx: usize, n: usize) -> Option<Vec<u8>> {
        self.inner.remove_batch(idx, n).map(|node| {
            let mut buf = Vec::new();
            encode_op(&EncodableOp::Node(node), &mut buf);
            buf
        })
    }

    /// Apply a single encoded op (`EncodableOp`) from a peer.
    #[wasm_bindgen(js_name = applyOp)]
    pub fn apply_op(&mut self, bytes: &[u8]) -> Result<(), JsValue> {
        let (op, _) =
            decode_op(bytes).map_err(|e| JsValue::from_str(&format!("decode op: {e}")))?;
        self.inner.apply_op(op);
        Ok(())
    }

    // --- full-state sync (kept for convenience / initial bootstrap) ---

    pub fn encode(&self) -> Vec<u8> {
        encode_hashseq(&self.inner)
    }

    #[wasm_bindgen(js_name = mergeEncoded)]
    pub fn merge_encoded(&mut self, bytes: &[u8]) -> Result<(), JsValue> {
        let other =
            decode_hashseq(bytes).map_err(|e| JsValue::from_str(&format!("decode error: {e}")))?;
        self.inner.merge(other);
        Ok(())
    }
}

/// Anchors for a visible range `[start, end)`: expanding ends
/// (`Before(next)`), with the document tail falling back to `After(last)`.
fn anchor_range(seq: &HashSeq, start: usize, end: usize) -> Result<(Anchor, Anchor), JsValue> {
    if start >= end || end > seq.len() {
        return Err(app_err("mark range out of bounds"));
    }
    let s = Anchor::Before(seq.id_at(start).expect("start < len"));
    let e = if end < seq.len() {
        Anchor::Before(seq.id_at(end).expect("end < len"))
    } else {
        Anchor::After(seq.id_at(end - 1).expect("end-1 < len"))
    };
    Ok((s, e))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The knowledge-base app's whole object model, end to end: a
    /// workspace kv opened at a well-known origin; pages as kv objects at
    /// app-chosen origins carried as ref values; bodies as seq children;
    /// snapshot persistence and two-store merge.
    #[test]
    fn kb_app_conventions_end_to_end() {
        let ws_origin = "11".repeat(32);
        let page_origin = "22".repeat(32);
        let body_origin = "33".repeat(32);

        let mut web = WasmHashWeb::new();
        let ws = web.create_kv(&ws_origin).unwrap();

        // New page: register under the workspace, open, title it, give it
        // a body.
        web.put_ref(&ws, "page:abc", &page_origin).unwrap();
        let page = web.create_kv(&page_origin).unwrap();
        web.put_string(&page, "title", "My Page").unwrap();
        web.put_ref(&page, "body", &body_origin).unwrap();
        let body = web.create_seq(&body_origin).unwrap();
        web.text_insert(&body, 0, "hello world").unwrap();
        web.text_insert(&body, 5, ",").unwrap();
        web.text_remove(&body, 0, 1).unwrap();
        assert_eq!(web.text(&body).unwrap(), "ello, world");

        // Discovery walk: keys -> refs -> derived object ids.
        let keys: Vec<String> = serde_json::from_str(&web.keys(&ws).unwrap()).unwrap();
        assert_eq!(keys, vec!["page:abc"]);
        let read: serde_json::Value =
            serde_json::from_str(&web.read_key(&ws, "page:abc").unwrap()).unwrap();
        assert_eq!(read["kind"], "one");
        assert_eq!(read["values"][0]["type"], "ref");
        assert_eq!(read["values"][0]["id"], page_origin);
        assert_eq!(WasmHashWeb::kv_id(&page_origin).unwrap(), page);
        let title: serde_json::Value =
            serde_json::from_str(&web.read_key(&page, "title").unwrap()).unwrap();
        assert_eq!(title["values"][0]["value"], "My Page");

        // Snapshot roundtrip carries everything (objects, values, text).
        let snap = web.encode();
        let restored = WasmHashWeb::decode(&snap).unwrap();
        assert_eq!(restored.object_count(), 3);
        assert_eq!(restored.text(&body).unwrap(), "ello, world");
        let title: serde_json::Value =
            serde_json::from_str(&restored.read_key(&page, "title").unwrap()).unwrap();
        assert_eq!(title["values"][0]["value"], "My Page");

        // A second store merges to the same state; concurrent title edits
        // surface as a conflict, never a silent winner.
        let mut other = WasmHashWeb::decode(&snap).unwrap();
        other.put_string(&page, "title", "Renamed").unwrap();
        web.put_string(&page, "title", "Also Renamed").unwrap();
        web.merge_encoded(&other.encode()).unwrap();
        let title: serde_json::Value =
            serde_json::from_str(&web.read_key(&page, "title").unwrap()).unwrap();
        assert_eq!(title["kind"], "conflict");
        assert_eq!(title["values"].as_array().unwrap().len(), 2);

        // Formatting is marks, not markup: code-mark "ello", then check
        // the span read; typing inside the region inherits the mark
        // (regional semantics), and unmark suppresses.
        web.mark_range(&body, 0, 4, "code", "on").unwrap();
        let spans: serde_json::Value =
            serde_json::from_str(&web.marked_spans(&body).unwrap()).unwrap();
        assert_eq!(spans[0]["text"], "ello");
        assert_eq!(spans[0]["marks"][0]["kind"], "code");
        assert_eq!(spans[1]["text"], ", world");
        assert_eq!(spans[1]["marks"].as_array().unwrap().len(), 0);
        web.text_insert(&body, 2, "XX").unwrap(); // inside the marked region
        let spans: serde_json::Value =
            serde_json::from_str(&web.marked_spans(&body).unwrap()).unwrap();
        assert_eq!(spans[0]["text"], "elXXlo", "insert inherits the region's mark");
        web.unmark_range(&body, 0, 6, "code").unwrap();
        let spans: serde_json::Value =
            serde_json::from_str(&web.marked_spans(&body).unwrap()).unwrap();
        for span in spans.as_array().unwrap() {
            assert_eq!(
                span["marks"].as_array().unwrap().len(),
                0,
                "tombstone suppresses the code mark everywhere"
            );
        }

        // Table shape: body embeds a table seq (link atom); the table's
        // elements are atoms referencing row seqs; rows reference cell
        // seqs (a hashseq of hashseqs).
        let table = web.create_seq(&"55".repeat(32)).unwrap();
        let row = web.create_seq(&"66".repeat(32)).unwrap();
        let cell = web.create_seq(&"77".repeat(32)).unwrap();
        web.text_insert(&cell, 0, "cell text").unwrap();
        web.seq_insert_ref(&row, 0, &"77".repeat(32)).unwrap();
        web.seq_insert_ref(&table, 0, &"66".repeat(32)).unwrap();
        web.seq_insert_ref(&body, 0, &"55".repeat(32)).unwrap();
        assert_eq!(web.payload_at(&body, 0).unwrap().unwrap(), "55".repeat(32));
        assert_eq!(web.payload_at(&table, 0).unwrap().unwrap(), "66".repeat(32));
        assert_eq!(web.payload_at(&row, 0).unwrap().unwrap(), "77".repeat(32));
        assert_eq!(web.payload_at(&body, 1).unwrap(), None, "plain char");
        assert!(web.text(&body).unwrap().starts_with('\u{FFFC}'));

        // Drag-reorder is a Move op: block-shaped atoms reorder without
        // losing identity, and the move survives a merge round-trip.
        let list = web.create_seq(&"99".repeat(32)).unwrap();
        web.text_insert(&list, 0, "abc").unwrap();
        web.seq_move(&list, 0, 3).unwrap(); // a → end
        assert_eq!(web.text(&list).unwrap(), "bca");
        web.seq_move(&list, 2, 0).unwrap(); // a → front
        assert_eq!(web.text(&list).unwrap(), "abc");
        let peer = WasmHashWeb::decode(&web.encode()).unwrap();
        assert_eq!(peer.text(&list).unwrap(), "abc");

        // Deletion reads as absent; the key drops from the live key list.
        web.del(&ws, "page:abc").unwrap();
        let read: serde_json::Value =
            serde_json::from_str(&web.read_key(&ws, "page:abc").unwrap()).unwrap();
        assert_eq!(read["kind"], "absent");
        let keys: Vec<String> = serde_json::from_str(&web.keys(&ws).unwrap()).unwrap();
        assert!(keys.is_empty());
    }

    /// A mid-text burst becomes one Before-run box anchored at an interior
    /// element of its (unsplit) parent run — structureJson must report the
    /// burst text and the anchor's offset within the parent box.
    #[test]
    fn structure_json_reports_interior_anchor_offsets() {
        let mut seq = WasmHashSeq::new();
        seq.insert(0, "hello world");
        seq.insert(5, " brave");
        assert_eq!(seq.text(), "hello brave world");

        let v: serde_json::Value = serde_json::from_str(&seq.structure_json()).unwrap();
        let nodes = v["nodes"].as_array().unwrap();

        let before = nodes.iter().find(|n| n["kind"] == "before").unwrap();
        assert_eq!(before["text"], " brave");
        // parent run is the whole origin-anchored "hello world"; the anchor
        // ' ' sits at element offset 5
        assert_eq!(before["parentOffset"], 5);
        assert_eq!(before["rel"], "before");

        // The top-level run is reported as a root box: whole, unanchored.
        let root = nodes.iter().find(|n| n["kind"] == "root").unwrap();
        assert_eq!(root["text"], "hello world", "parent run must not be split");
        assert_eq!(before["parent"], root["id"]);
        assert_eq!(root["parent"], serde_json::Value::Null);
    }

    /// Deleting some characters of a run must surface per-element tombstone
    /// state in `removedMask` (aligned with `text`), and `removed` (box-level)
    /// must stay false until the whole run is gone.
    #[test]
    fn structure_json_reports_per_char_tombstones() {
        let mut seq = WasmHashSeq::new();
        seq.insert(0, "abcdef");
        seq.remove(1, 2); // delete 'b','c' -> visible "adef"
        assert_eq!(seq.text(), "adef");

        let v: serde_json::Value = serde_json::from_str(&seq.structure_json()).unwrap();
        let nodes = v["nodes"].as_array().unwrap();
        // One run still holding all six elements (tombstones don't compact it).
        let run = nodes.iter().find(|n| n["text"] == "abcdef").unwrap();
        assert_eq!(run["removedMask"], "011000", "b,c tombstoned; rest live");
        assert_eq!(run["removed"], false, "run is only partially deleted");

        // Delete the rest -> the box reads as fully removed.
        seq.remove(0, 4);
        assert_eq!(seq.text(), "");
        let v: serde_json::Value = serde_json::from_str(&seq.structure_json()).unwrap();
        let run = v["nodes"].as_array().unwrap()[0].clone();
        assert_eq!(run["removedMask"], "111111");
        assert_eq!(run["removed"], true);
    }

    /// A concurrent after-fork splits a run: "ab" + a sibling "c" forked after
    /// 'a' breaks [a,b] into a left run "a" and a right run "b" re-anchored
    /// (after) 'a'. structureJson must report that right run's after-edge.
    #[test]
    fn structure_json_reports_split_continuation_edge() {
        let mut a = WasmHashSeq::new();
        a.insert(0, "a");
        let mut b = WasmHashSeq::new();
        b.merge_encoded(&a.encode()).unwrap();
        a.insert(1, "b"); // "ab"
        b.insert(1, "c"); // "ac" — concurrent fork after 'a'
        a.merge_encoded(&b.encode()).unwrap();

        let v: serde_json::Value = serde_json::from_str(&a.structure_json()).unwrap();
        let nodes = v["nodes"].as_array().unwrap();
        eprintln!("SPLIT STRUCTURE: {}", serde_json::to_string_pretty(&nodes).unwrap());

        let left = nodes.iter().find(|n| n["text"] == "a").unwrap();
        // Both forked continuations re-anchor (after) 'a'.
        let b_run = nodes.iter().find(|n| n["text"] == "b").unwrap();
        let c_run = nodes.iter().find(|n| n["text"] == "c").unwrap();
        assert_eq!(b_run["parent"], left["id"], "b must anchor at the left run");
        assert_eq!(b_run["rel"], "after");
        assert_eq!(c_run["parent"], left["id"]);
        assert_eq!(c_run["rel"], "after");
    }

    /// A mid-run insert depends on the run's *tip* (a different element of the
    /// anchor's box than its anchor); structureJson must report that dep with
    /// its element offset so the visualizer can draw the edge to the tip char.
    #[test]
    fn structure_json_reports_dep_offset_within_anchor_box() {
        let mut seq = WasmHashSeq::new();
        seq.insert(0, "hello");
        seq.insert(2, "X"); // before 'l' (offset 2); tip is 'o' (offset 4)
        assert_eq!(seq.text(), "heXllo");

        let v: serde_json::Value = serde_json::from_str(&seq.structure_json()).unwrap();
        let nodes = v["nodes"].as_array().unwrap();
        let run = nodes.iter().find(|n| n["text"] == "hello").unwrap();
        let x = nodes.iter().find(|n| n["text"] == "X").unwrap();
        assert_eq!(x["parentOffset"], 2, "anchored before 'l'");
        let deps = x["deps"].as_array().unwrap();
        assert_eq!(deps.len(), 1);
        assert_eq!(deps[0]["box"], run["id"], "dep lands in the run's box");
        assert_eq!(deps[0]["off"], 4, "...at the tip 'o', not the anchor offset");
    }
}

// ---- HashWeb: the app-facing store binding ----
//
// The Rust store deliberately has no authoring or read wrappers (objects
// carry their own APIs). This binding is an *app-side adapter*: it owns the
// store, reaches objects through `seq_mut`/`kv_mut`, and speaks the app's
// value vocabulary (string keys; string or raw-id values). Ids cross the
// boundary as 64-char hex.

fn app_err(msg: &str) -> JsValue {
    JsValue::from_str(msg)
}

#[wasm_bindgen]
#[derive(Default)]
pub struct WasmHashWeb {
    inner: HashWeb,
}

#[wasm_bindgen]
impl WasmHashWeb {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self::default()
    }

    // --- derivation (pure; usable before anything is open) ---

    /// `object_id(KIND_SEQ ‖ origin)` as hex.
    #[wasm_bindgen(js_name = seqId)]
    pub fn seq_id(origin_hex: &str) -> Result<String, JsValue> {
        Ok(id_to_hex(&object_id(KIND_SEQ, &hex_to_id(origin_hex)?)))
    }

    /// `object_id(KIND_KV ‖ origin)` as hex.
    #[wasm_bindgen(js_name = kvId)]
    pub fn kv_id(origin_hex: &str) -> Result<String, JsValue> {
        Ok(id_to_hex(&object_id(KIND_KV, &hex_to_id(origin_hex)?)))
    }

    // --- opening ---

    /// Open (idempotently) the seq at `origin`; returns the object id.
    #[wasm_bindgen(js_name = createSeq)]
    pub fn create_seq(&mut self, origin_hex: &str) -> Result<String, JsValue> {
        Ok(id_to_hex(&self.inner.create_seq(hex_to_id(origin_hex)?)))
    }

    /// Open (idempotently) the kv at `origin`; returns the object id.
    #[wasm_bindgen(js_name = createKv)]
    pub fn create_kv(&mut self, origin_hex: &str) -> Result<String, JsValue> {
        Ok(id_to_hex(&self.inner.create_kv(hex_to_id(origin_hex)?)))
    }

    #[wasm_bindgen(js_name = isSeq)]
    pub fn is_seq(&self, obj_hex: &str) -> Result<bool, JsValue> {
        Ok(self.inner.seq(&hex_to_id(obj_hex)?).is_some())
    }

    #[wasm_bindgen(js_name = isKv)]
    pub fn is_kv(&self, obj_hex: &str) -> Result<bool, JsValue> {
        Ok(self.inner.kv(&hex_to_id(obj_hex)?).is_some())
    }

    #[wasm_bindgen(js_name = objectCount)]
    pub fn object_count(&self) -> usize {
        self.inner.object_count()
    }

    /// Envelopes parked on unknown object ids (waiting for an open).
    #[wasm_bindgen(js_name = orphanCount)]
    pub fn orphan_count(&self) -> usize {
        self.inner.orphans().count()
    }

    // --- seq objects (text) ---

    pub fn text(&self, obj_hex: &str) -> Result<String, JsValue> {
        let seq = self
            .inner
            .seq(&hex_to_id(obj_hex)?)
            .ok_or_else(|| app_err("no such seq object"))?;
        Ok(seq.iter().collect())
    }

    #[wasm_bindgen(js_name = textLen)]
    pub fn text_len(&self, obj_hex: &str) -> Result<usize, JsValue> {
        let seq = self
            .inner
            .seq(&hex_to_id(obj_hex)?)
            .ok_or_else(|| app_err("no such seq object"))?;
        Ok(seq.len())
    }

    #[wasm_bindgen(js_name = textInsert)]
    pub fn text_insert(&mut self, obj_hex: &str, idx: usize, text: &str) -> Result<(), JsValue> {
        let seq = self
            .inner
            .seq_mut(&hex_to_id(obj_hex)?)
            .ok_or_else(|| app_err("no such seq object"))?;
        seq.insert_batch(idx, text.chars());
        Ok(())
    }

    #[wasm_bindgen(js_name = textRemove)]
    pub fn text_remove(&mut self, obj_hex: &str, idx: usize, len: usize) -> Result<(), JsValue> {
        let seq = self
            .inner
            .seq_mut(&hex_to_id(obj_hex)?)
            .ok_or_else(|| app_err("no such seq object"))?;
        seq.remove_batch(idx, len);
        Ok(())
    }

    // --- atoms (embedded object refs in a seq) ---

    /// Insert an atom at visible position `idx` whose payload is a raw id
    /// (an embed: another object's origin, by app convention). Renders as
    /// U+FFFC in `text()`; read back with `payloadAt`.
    #[wasm_bindgen(js_name = seqInsertRef)]
    pub fn seq_insert_ref(
        &mut self,
        obj_hex: &str,
        idx: usize,
        ref_hex: &str,
    ) -> Result<String, JsValue> {
        let payload = hex_to_id(ref_hex)?;
        let seq = self
            .inner
            .seq_mut(&hex_to_id(obj_hex)?)
            .ok_or_else(|| app_err("no such seq object"))?;
        Ok(id_to_hex(&seq.insert_value(idx, payload).id()))
    }

    /// The payload id (hex) of the atom at visible position `idx`, or
    /// undefined for plain characters / out of range.
    #[wasm_bindgen(js_name = payloadAt)]
    pub fn payload_at(&self, obj_hex: &str, idx: usize) -> Result<Option<String>, JsValue> {
        let seq = self
            .inner
            .seq(&hex_to_id(obj_hex)?)
            .ok_or_else(|| app_err("no such seq object"))?;
        Ok(seq
            .id_at(idx)
            .and_then(|id| seq.payload_of(&id))
            .as_ref()
            .map(id_to_hex))
    }

    /// Move the element at visible `from` to sit at visible slot
    /// `to_slot` (0..=len, interpreted on the current rendering): a Move
    /// op superseding the placement heads this replica sees.
    #[wasm_bindgen(js_name = seqMove)]
    pub fn seq_move(&mut self, obj_hex: &str, from: usize, to_slot: usize) -> Result<(), JsValue> {
        let seq = self
            .inner
            .seq_mut(&hex_to_id(obj_hex)?)
            .ok_or_else(|| app_err("no such seq object"))?;
        let len = seq.len();
        if from >= len || to_slot > len {
            return Err(app_err("move out of bounds"));
        }
        let target = seq.id_at(from).expect("from < len");
        let to = if to_slot == len {
            Anchor::After(seq.id_at(len - 1).expect("len > 0"))
        } else {
            Anchor::Before(seq.id_at(to_slot).expect("to_slot < len"))
        };
        if *to.id() == target {
            return Ok(()); // dropping onto itself
        }
        seq.move_element(target, to);
        Ok(())
    }

    // --- marks (formatting as ops, not markup) ---

    /// Mark visible range `[start, end)` with `kind`/`value` (both strings;
    /// the artifacts register store-wide). Expanding-end anchors: typing at
    /// the edges grows the region (MARKS.md anchor table). Returns the mark
    /// op id.
    #[wasm_bindgen(js_name = markRange)]
    pub fn mark_range(
        &mut self,
        obj_hex: &str,
        start: usize,
        end: usize,
        kind: &str,
        value: &str,
    ) -> Result<String, JsValue> {
        let k = Value::String(kind.to_owned());
        let v = Value::String(value.to_owned());
        let kind_id = self.inner.provide_value(&k);
        let value_id = self.inner.provide_value(&v);
        let seq = self
            .inner
            .seq_mut(&hex_to_id(obj_hex)?)
            .ok_or_else(|| app_err("no such seq object"))?;
        let (s, e) = anchor_range(seq, start, end)?;
        Ok(id_to_hex(&seq.mark_range(s, e, kind_id, value_id).id()))
    }

    /// Like `markRange`, but with a CLOSED end: the end anchor is
    /// `After(last covered element)`, so text typed immediately after the
    /// region deterministically lands OUTSIDE it. This is what typed-markup
    /// conversions want (`Before(next)` ends would race the tombstoned
    /// delimiter's ghost in sibling id order — nondeterministic growth).
    #[wasm_bindgen(js_name = markRangeClosed)]
    pub fn mark_range_closed(
        &mut self,
        obj_hex: &str,
        start: usize,
        end: usize,
        kind: &str,
        value: &str,
    ) -> Result<String, JsValue> {
        let k = Value::String(kind.to_owned());
        let v = Value::String(value.to_owned());
        let kind_id = self.inner.provide_value(&k);
        let value_id = self.inner.provide_value(&v);
        let seq = self
            .inner
            .seq_mut(&hex_to_id(obj_hex)?)
            .ok_or_else(|| app_err("no such seq object"))?;
        if start >= end || end > seq.len() {
            return Err(app_err("mark range out of bounds"));
        }
        let s = Anchor::Before(seq.id_at(start).expect("start < len"));
        let e = Anchor::After(seq.id_at(end - 1).expect("end-1 < len"));
        Ok(id_to_hex(&seq.mark_range(s, e, kind_id, value_id).id()))
    }

    /// Remove `kind` formatting over `[start, end)` — a tombstone-valued
    /// mark (partial unmark works: the overwritten mark keeps applying
    /// outside the range).
    #[wasm_bindgen(js_name = unmarkRange)]
    pub fn unmark_range(
        &mut self,
        obj_hex: &str,
        start: usize,
        end: usize,
        kind: &str,
    ) -> Result<String, JsValue> {
        let k = Value::String(kind.to_owned());
        let kind_id = self.inner.provide_value(&k);
        let seq = self
            .inner
            .seq_mut(&hex_to_id(obj_hex)?)
            .ok_or_else(|| app_err("no such seq object"))?;
        let (s, e) = anchor_range(seq, start, end)?;
        Ok(id_to_hex(&seq.unmark_range(s, e, kind_id).id()))
    }

    /// The rendered document as coalesced marked spans. JSON:
    /// `[{"text": s, "marks": [{"kind": s, "values": [s...]}]}]` — marks
    /// whose live values are all unmark tombstones are omitted; multiple
    /// values on one kind = an MVR conflict, surfaced whole.
    #[wasm_bindgen(js_name = markedSpans)]
    pub fn marked_spans(&self, obj_hex: &str) -> Result<String, JsValue> {
        let obj = hex_to_id(obj_hex)?;
        let seq = self
            .inner
            .seq(&obj)
            .ok_or_else(|| app_err("no such seq object"))?;
        let resolve_str = |id: &Id| match self.inner.resolve(id) {
            Some(Value::String(s)) => s,
            Some(other) => format!("{other:?}"),
            None => id_to_hex(id),
        };
        let spans: Vec<serde_json::Value> = seq
            .marked_spans()
            .iter()
            .map(|(text, set)| {
                let marks: Vec<serde_json::Value> = set
                    .iter()
                    .filter_map(|(kind, lives)| {
                        let values: Vec<String> = lives
                            .iter()
                            .filter(|(_, v)| *v != *TOMBSTONE)
                            .map(|(_, v)| resolve_str(v))
                            .collect();
                        if values.is_empty() {
                            return None; // fully unmarked
                        }
                        Some(serde_json::json!({
                            "kind": resolve_str(kind),
                            "values": values,
                        }))
                    })
                    .collect();
                serde_json::json!({ "text": text, "marks": marks })
            })
            .collect();
        Ok(serde_json::to_string(&spans).expect("plain data serializes"))
    }

    // --- kv objects (string keys; string or raw-id values) ---

    /// `obj[key] = value` (both strings). Returns the put op id (hex) — the
    /// composition convention's input when a child anchors at this op.
    #[wasm_bindgen(js_name = putString)]
    pub fn put_string(&mut self, obj_hex: &str, key: &str, value: &str) -> Result<String, JsValue> {
        let k = Value::String(key.to_owned());
        let v = Value::String(value.to_owned());
        self.inner.provide_value(&k);
        self.inner.provide_value(&v);
        let kv = self
            .inner
            .kv_mut(&hex_to_id(obj_hex)?)
            .ok_or_else(|| app_err("no such kv object"))?;
        Ok(id_to_hex(&kv.put(k, v).id()))
    }

    /// `obj[key] = <raw id>` — a link / origin reference; the value is an id,
    /// not an artifact. Returns the put op id (hex).
    #[wasm_bindgen(js_name = putRef)]
    pub fn put_ref(&mut self, obj_hex: &str, key: &str, ref_hex: &str) -> Result<String, JsValue> {
        let k = Value::String(key.to_owned());
        let vid = hex_to_id(ref_hex)?;
        self.inner.provide_value(&k);
        let kv = self
            .inner
            .kv_mut(&hex_to_id(obj_hex)?)
            .ok_or_else(|| app_err("no such kv object"))?;
        let key_id = kv.provide_value(&k);
        Ok(id_to_hex(&kv.put_ids(key_id, vid).id()))
    }

    pub fn del(&mut self, obj_hex: &str, key: &str) -> Result<(), JsValue> {
        let kv = self
            .inner
            .kv_mut(&hex_to_id(obj_hex)?)
            .ok_or_else(|| app_err("no such kv object"))?;
        kv.del(Value::String(key.to_owned()));
        Ok(())
    }

    /// Read a key. JSON:
    /// `{"kind":"absent"}` |
    /// `{"kind":"one","values":[v]}` |
    /// `{"kind":"conflict","values":[v...]}` — every head surfaced (MVR).
    /// Each v: `{"type":"string","value":s}` | `{"type":"ref","id":hex}` |
    /// `{"type":"pending","id":hex}` (artifact bytes not yet known) |
    /// `{"type":"deleted"}`.
    #[wasm_bindgen(js_name = readKey)]
    pub fn read_key(&self, obj_hex: &str, key: &str) -> Result<String, JsValue> {
        let obj = hex_to_id(obj_hex)?;
        let kv = self
            .inner
            .kv(&obj)
            .ok_or_else(|| app_err("no such kv object"))?;
        let describe = |vid: &Id| -> serde_json::Value {
            if *vid == *TOMBSTONE {
                return serde_json::json!({"type": "deleted"});
            }
            match self.inner.resolve(vid).or_else(|| kv.resolve(vid)) {
                Some(Value::String(s)) => serde_json::json!({"type": "string", "value": s}),
                Some(other) => serde_json::json!({
                    "type": "string",
                    "value": format!("{other:?}")
                }),
                // Unresolvable: either a raw-id value (a link/origin — by
                // construction never a provided artifact) or an artifact
                // whose bytes have not arrived. The app's key conventions
                // distinguish them; we surface both faces.
                None => serde_json::json!({"type": "ref", "id": id_to_hex(vid)}),
            }
        };
        let read = kv.read(&Value::String(key.to_owned()));
        let json = match read {
            crate::hashkv::Read::Absent => serde_json::json!({"kind": "absent"}),
            crate::hashkv::Read::One(vid) => {
                serde_json::json!({"kind": "one", "values": [describe(&vid)]})
            }
            crate::hashkv::Read::Conflict(vids) => serde_json::json!({
                "kind": "conflict",
                "values": vids.iter().map(describe).collect::<Vec<_>>()
            }),
        };
        Ok(json.to_string())
    }

    /// Live string keys of a kv, as a JSON array (id-ordered; display
    /// ordering is the app's concern). Keys whose artifact bytes are
    /// unknown are skipped.
    pub fn keys(&self, obj_hex: &str) -> Result<String, JsValue> {
        let obj = hex_to_id(obj_hex)?;
        let kv = self
            .inner
            .kv(&obj)
            .ok_or_else(|| app_err("no such kv object"))?;
        let mut out: Vec<String> = Vec::new();
        for kid in kv.keys() {
            if let Some(Value::String(s)) = self.inner.resolve(kid).or_else(|| kv.resolve(kid)) {
                out.push(s);
            }
        }
        Ok(serde_json::to_string(&out).expect("plain data serializes"))
    }

    /// The live put-op ids for a key (hex, id-ordered) — the anchoring
    /// input for op-id-welded composition.
    #[wasm_bindgen(js_name = headIds)]
    pub fn head_ids(&self, obj_hex: &str, key: &str) -> Result<Vec<String>, JsValue> {
        let obj = hex_to_id(obj_hex)?;
        let kv = self
            .inner
            .kv(&obj)
            .ok_or_else(|| app_err("no such kv object"))?;
        let key_id = Value::String(key.to_owned()).value_id();
        Ok(kv.heads(&key_id).iter().map(id_to_hex).collect())
    }

    // --- sync ---

    /// Deliver a remote enveloped op: `obj_id ‖ encoded node`.
    #[wasm_bindgen(js_name = applyTo)]
    pub fn apply_to(&mut self, obj_hex: &str, op_bytes: &[u8]) -> Result<(), JsValue> {
        let obj = hex_to_id(obj_hex)?;
        let (op, _) =
            decode_op(op_bytes).map_err(|e| app_err(&format!("decode op: {e}")))?;
        match op {
            EncodableOp::Node(node) => self.inner.apply_to(obj, node),
            EncodableOp::Run(run) => {
                for (id, node) in run.decompress_with_ids() {
                    self.inner.apply_to_with_id(obj, id, node);
                }
            }
        }
        Ok(())
    }

    /// Canonical whole-store snapshot.
    pub fn encode(&self) -> Vec<u8> {
        encode_hashweb(&self.inner)
    }

    pub fn decode(bytes: &[u8]) -> Result<WasmHashWeb, JsValue> {
        let inner =
            decode_hashweb(bytes).map_err(|e| app_err(&format!("decode error: {e}")))?;
        Ok(WasmHashWeb { inner })
    }

    /// Merge a peer snapshot: union of knowledge.
    #[wasm_bindgen(js_name = mergeEncoded)]
    pub fn merge_encoded(&mut self, bytes: &[u8]) -> Result<(), JsValue> {
        let other =
            decode_hashweb(bytes).map_err(|e| app_err(&format!("decode error: {e}")))?;
        self.inner.merge(other);
        Ok(())
    }
}
