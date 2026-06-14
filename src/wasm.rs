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

use crate::encoding::{decode_hashseq, decode_op, encode_hashseq, encode_op};
use crate::hashseq::{Cursor, Loc};
use crate::{EncodableOp, HashSeq, Id, Run};

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

/// Minimal JSON string escaping (we hand-roll the structure dump to avoid pulling
/// serde_json into the wasm build).
fn push_json_str(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0C}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
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
    ///     "rel": "after"|"before"|null, "removed": bool } ] }`
    ///
    /// `parentOffset` is the anchor's element index within the parent box: a
    /// `rel:"before"` child sits immediately before that char, a `rel:"after"`
    /// child immediately after it.
    #[wasm_bindgen(js_name = structureJson)]
    pub fn structure_json(&self) -> String {
        let s = &self.inner;
        // Map any element id to the id of the box (run head or root) it belongs
        // to; anchors may point at interior run elements.
        let resolve = |id: &Id| -> Id {
            match s.idx_of(id).map(|i| s.loc_of(i)) {
                Some(Loc::Run { run, .. }) => s.id_of(run),
                _ => *id,
            }
        };

        let mut out = String::from("{\"tips\":[");
        for (i, t) in s.tips().iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            push_json_str(&mut out, &id_to_hex(t));
        }
        out.push_str("],\"nodes\":[");

        // Resolve a set of dependency ids to the boxes (run heads / roots /
        // before-nodes) they belong to, deduped.
        let resolve_deps = |set: &std::collections::BTreeSet<Id>| -> Vec<Id> {
            let mut seen = std::collections::BTreeSet::new();
            set.iter()
                .map(&resolve)
                .filter(|b| seen.insert(*b))
                .collect()
        };

        let mut first = true;
        {
            let mut emit = |id: &Id,
                            kind: &str,
                            text: &str,
                            parent: Option<(Id, usize)>,
                            rel: Option<&str>,
                            removed: bool,
                            deps: &[Id]| {
                if !first {
                    out.push(',');
                }
                first = false;
                out.push_str("{\"id\":");
                push_json_str(&mut out, &id_to_hex(id));
                out.push_str(",\"kind\":\"");
                out.push_str(kind);
                out.push_str("\",\"text\":");
                push_json_str(&mut out, text);
                out.push_str(",\"parent\":");
                match parent {
                    Some((p, _)) => push_json_str(&mut out, &id_to_hex(&p)),
                    None => out.push_str("null"),
                }
                out.push_str(",\"parentOffset\":");
                match parent {
                    Some((_, off)) => out.push_str(&off.to_string()),
                    None => out.push_str("null"),
                }
                out.push_str(",\"rel\":");
                match rel {
                    Some(r) => {
                        out.push('"');
                        out.push_str(r);
                        out.push('"');
                    }
                    None => out.push_str("null"),
                }
                out.push_str(",\"removed\":");
                out.push_str(if removed { "true" } else { "false" });
                out.push_str(",\"deps\":[");
                for (i, d) in deps.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    push_json_str(&mut out, &id_to_hex(d));
                }
                out.push_str("]}");
            };

            for (head, run) in &s.runs {
                let head_id = s.id_of(*head);
                // Origin-anchored runs are the document's top level — emitted
                // with no anchor, like the old standalone root nodes.
                let is_top_level = run.anchor == s.origin();
                let (kind, rel) = match run.first_op {
                    _ if is_top_level => ("root", "after"),
                    crate::run::FirstOp::After => ("run", "after"),
                    crate::run::FirstOp::Before => ("before", "before"),
                };
                // The anchor may be an interior element of the parent box (befores
                // don't split runs); report its offset so the layout can attach there.
                let anchor_offset = match s.idx_of(&run.anchor).map(|i| s.loc_of(i)) {
                    Some(Loc::Run { pos, .. }) => pos as usize,
                    _ => 0,
                };
                let anchor = if is_top_level {
                    None
                } else {
                    Some((resolve(&run.anchor), anchor_offset))
                };
                emit(
                    &head_id,
                    kind,
                    &run.text,
                    anchor,
                    (!is_top_level).then_some(rel),
                    s.is_removed(*head),
                    &resolve_deps(&run.first_extra_deps.to_id_set(&s.ids)),
                );
            }
        }

        out.push_str("]}");
        out
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
