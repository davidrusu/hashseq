# Recovered code-review findings (session 68868adf, /code-review high on src/hashseq.rs src/run.rs src/run_index.rs src/placement.rs src/hashseq_iter.rs src/hash_node.rs src/encoding.rs src/bitset.rs)

Recovered 2026-09-02 from ~/.claude/projects/-Users-davidrusu-src-hashseq/68868adf-b48d-48a8-934b-350f12ba2f9f/subagents/.
First pass: 9 finder agents (all completed). Second pass: 15 verifiers (4 completed, 11 killed).

## WARNING: why the machine ran out of RAM
Verifier scratch tests (tests/zz_scratch_verify.rs) fed decode_hashseq a BLK_REMOVE span with end = 2^40.
`(start..=end).collect::<Vec<usize>>()` runs before any bounds check → 8 TiB reservation that macOS grants
and then fills. Never run allocation-bomb repros with mid-range sizes (2^30..2^50). Use 1<<60 (immediate
"capacity overflow" panic) or verify by reading only. The three leftover scratch tests were moved to
scratchpad/quarantined_scratch_tests/ — do not move them back.

## Verification status

Legend: RESOLVED = fix landed in working tree (see git diff). Each row is updated as fixes land.

| # | Finding | Status | Evidence |
|---|---------|--------|----------|
| 1 | move_ancestors/resolve_decider contamination (hashseq.rs ~1263-1325) | **RESOLVED** 2026-09-02 — filter overwrites before `seen.insert` in move_ancestors; regression tests `foreign_overwrites_never_become_the_decider`, `other_register_overwrites_never_become_the_decider`. Gate still accepts foreign overwrite ids (by design, 'ignored, never errors'). Was: CONFIRMED | Scenario A panics hashseq.rs:1371 'no entry found for key'; Scenario B panics run_index.rs:598 'old rendering must be cleared first'. interpret gate (2135-2149) never validates `overwrites`. Honest move_element cannot produce it → remote-peer crash vector. Fix: in move_ancestors filter each overwrite (`move_nodes.get(o).is_some_and(|m| m.target == target)`) BEFORE `seen.insert`; trial-patched, both scenarios then complete (A: 'abc' frozen at creation; B: 'acb'). |
| 2 | decode_hashseq with_capacity(num_ids+1) (encoding.rs:1590) | **RESOLVED** 2026-09-02 — new `Cursor::count(min_bytes)` bounds the varint by remaining bytes (32 per id) → UnexpectedEof; test `hashseq_num_ids_bounded_by_input`. Was: CONFIRMED | verifier repro: 34 bytes → capacity overflow panic; reachable via wasm merge_encoded, decode_hashweb OBJ_SEQ |
| 3 | decode_hashkv_v with_capacity(n) (encoding.rs:1867) | **RESOLVED** 2026-09-02 — `c.count(1)`; test `hashkv_entry_count_bounded_by_input`. Was: CONFIRMED | verifier repro; reachable via decode_hashkv, decode_hashweb OBJ_KV |
| 4 | BLK_REMOVE_FWD/BWD span collect before bounds (encoding.rs:1637) | **RESOLVED** 2026-09-02 — span ends validated against target run length before any allocation; walk by index, no intermediate Vec; inverted span stays empty (as before); test `remove_span_bounds_checked_before_walk`. Was: CONFIRMED | collect() precedes run_elem() check |
| 5 | Payload::Id(char_value_id(c)) not canonicalized (encoding.rs:310) | **RESOLVED (interim)** 2026-09-02 — by-id form stays legal (GRAMMAR_SPEC allows it when the sender lacks the artifact). `Payload::resolved()` turns a by-id payload into `Char` when this replica knows the value; applied in decode_payload, make_insert_value and interpret, so one node id has one stored form. 'Known' today = the ASCII char table (pure, replica-independent). Full fix queued: a value store (Id → Value) consulted on read — see 'Queued follow-ups'. Tests `by_id_payload_for_a_known_char_resolves_on_decode`, `insert_value_of_a_char_id_is_the_char`. Was: CONFIRMED | verifier repro: same node id, A renders 'a', B renders U+FFFC, both pass strict decode |
| 6 | off+count overflow in BLK_REMOVE_OTHER (encoding.rs:1684) | **RESOLVED** 2026-09-02 — `off.checked_add(count)` bounded by the run length → InvalidIdIndex; the four `c.pos + len` slices now go through `Cursor::take` (checked add). decode_payload's `pos + len` left alone (len ≤ 32). Tests `remove_other_range_bounded_by_run`, `cursor_take_rejects_wrapping_length`. Was: CONFIRMED | debug: 'attempt to add with overflow'; release: decodes OK with text "ab" while original was "" — removes silently dropped, re-encode != input |
| 7 | pins normalization not enforced on decode (hash_node.rs:260) | **RESOLVED** 2026-09-02 — REJECTED at the wire boundary (David's call: one wire form per op). `HashNode::is_normalized`; `DecodeError::RedundantPin` from decode_node_with, decode_hashkv_v, the three BLK_REMOVE arms; `Run::from_text` now returns `Result<_, RunError>` and refuses deps that repeat the chain anchor. Honest builders untouched (debug_assert in `id()` still guards them). Tests `wire_pins_naming_a_named_id_are_rejected`, `run_deps_naming_the_chain_anchor_are_rejected`. Was: CONFIRMED | debug: panic 'pins must be normalized' from remote input; release: same id, 45 vs 47 bytes, both pass strict |
| 8 | marks_at panics on non-insert id (hashseq.rs:1688/801) | **RESOLVED** 2026-09-02 — marks_at returns an empty set for any id whose loc is not `Loc::Run` (origin, remove/move/mark/place op ids), mirroring position_of. Test `marks_at_non_element_ids_is_empty_not_a_panic`. Was: CONFIRMED | verifier repro; pub Rust API only, not exposed via wasm |
| 9 | record_authored before gate; gated op still shipped (hashseq.rs:1919) | **RESOLVED** 2026-09-02 — new `HashSeq::author(node)` seam: apply first, record to the outbox only if admitted, else hand the node back. `mark_range`/`unmark_range`/`move_element` now return `Result<HashNode, HashNode>` (Err = gated); `place`/`insert_value`/`remove_batch` route through `author` too (always admitted, signatures unchanged); `insert_batch` keeps record-before-apply on the hot path with a comment (cursor-derived, always admitted). wasm markRange/markRangeClosed/unmarkRange/seqMove now return a JS error instead of a fake op id. HashKv untouched (its authoring ops are map ops, never gated). Test `gated_authoring_is_reported_and_never_queued_for_peers`. Was: CONFIRMED | applied=false, shipped=true, peer_contains_mark=false |
| 10 | wasm seq_move anchors at visible id, not render_anchor (wasm.rs:949) | **RESOLVED** 2026-09-02 — new `HashSeq::anchor_id_at(pos)` (the id `cursor_at` would anchor on: the element, or its deciding move op when moved in). `seq_move` uses it for the destination anchor (target stays the element id); self-drop check is now slot-based (`to == from || to == from + 1`). Test `seq_move_chain_tracks_the_visible_order` (8-move drag chain vs a Vec model). Was: CONFIRMED | 3/3 scenarios fail: "bca"→ expected "cba"; drag chain diverges at move(1,3) |
| 11 | wasm anchor_range/mark_range_closed on moved-in elements (wasm.rs:389, 1017) | **RESOLVED** 2026-09-02 — `anchor_range` and `mark_range_closed` build anchors via `anchor_id_at`; MARKS.md's op-anchored endpoint rule does the rest. Tests `anchor_id_at_maps_moved_in_elements_to_their_move_op` (Rust: spans [b][cd*][a], closed range covers the moved-in element), `mark_range_over_moved_in_text_applies` (wasm). Was: CONFIRMED | Base-slot mark points are deliberate per MARKS.md 59-65/140-143, so bug locus is wasm. Repro on 'abcd'→'bcda': markRange(1,3) gated (contains_node=false) yet op id returned because HashSeq::mark_range (~1918) discards apply's Err; markRangeClosed(3,4) applies but marks_at(a)=0, spans [("bcda",[])]. Fix: anchor at deciding move op (Before(c)..Before(mv) renders correctly); needs a pub render_anchor_id since decider_of/rendered_elsewhere are pub(crate). |
| 12 | wasm merge_encoded panics on origin mismatch (wasm.rs:379, 1263) | **RESOLVED** 2026-09-02 — `WasmHashSeq::merge_encoded` compares origins before `merge` and returns a JsValue error. `WasmHashWeb::merge_encoded` needs nothing: `HashWeb::merge` adopts objects by id, never asserting. Test `merge_encoded_of_another_document_is_an_error_not_a_trap`. Was: CONFIRMED | no origin check before HashSeq::merge assert_eq at hashseq.rs:2289 |
| 13 | Run::from_text silently drops interior deps at offset 0 / >= len (run.rs:70) | **RESOLVED** 2026-09-02 (with #7) — any interior offset not consumed by the chain walk → `RunError::DepOffsetOutOfRange` → `DecodeError::InvalidDepOffset`. Test `run_interior_dep_offsets_must_address_an_element`. Was: CONFIRMED (code read) | decode_run_with inserts any offset; from_text only removes keys 1..len-1; non-strict decode_hashseq is the wasm path |
| 14 | Latent/low batch C1-C6 | DONE (2026-09-02) | C1 Frag::rank k==len: REFUTED/latent (both callers guarantee k < len). C2 frag_containing release guard: REFUTED/latent (every ElemRef computed fresh from loc table; split_run_at rewrites locs + rekeys moved). C3 iterator afters-implies-tail: REFUTED (all 3 creation sites split first; unregister_sibling drops empty entries at 1440-1442). C4 placement chain: REFUTED/by-design (PLACEMENT_SPEC 'Freeze' 131-138; falling back to A2/A1 would pick a side of the 2/3 conflict; tests assert it). C5 sorted_subset_indices: REFUTED (named_set covers exactly the 5 sets hashed). **C6 RESOLVED** 2026-09-02: registers are now `MoveRegister { heads, decider }`; `apply_move` (the only writer) resolves once when the head set changes and caches it, so `decider_of` is a field read on every hot path. Within one resolution `move_ancestors` is memoized (the maximal-element filter asked O(m²) times). Residual: the write-time resolution itself is still O(k·n) per admitted fork op — bounded by the ops the attacker sends, i.e. the spec's linear bound now holds for reads; a persistent ancestor cache would be the next step if fork-spam apply cost ever matters. Test `contested_register_reads_never_re_resolve`. |
| 15 | Cleanup batch R1-R3, S1-S6, E1-E5 | DONE (2026-09-02) **R1–R3 RESOLVED** 2026-09-02: encode_node_preimage uses hash_node::varint_len / sorted_subset_indices; one `value::char_artifact` serves Value::encode, char_value_id and encode_payload. **S3 RESOLVED**: `Run::{split_at, first_node, run_id, find_position}` and their four tests deleted (StoredRun::split_at is the live split; `Arbitrary for Run` kept for encoding's prop_run_roundtrip). **S1, S2, S4 RESOLVED**: `merge` iterates `all_nodes()`; `mark_anchored_ops` removed (`mark_events.contains_key` is the same predicate); interpret's Mark arm borrows for the gate and moves once. | All CONFIRMED except S5 PLAUSIBLE (IndexTarget refactor collapses the 4 Moved/Splice arms cleanly; the 2 Elem arms carry different split logic). E5 wording fix: decompress_with_ids takes &self so `run` is dropped not consumed, but the move is still valid. S1 note: merge's mark/place loop order differs from all_nodes but both go through the orphan machinery. |

Dropped by orchestrator before verification: `first_ge` style nit (pure style); HASHSEQ_SPEC.md vs Fugue-rule divergence (spec doc, out of scope for code review — but worth a look: spec still says the old "causally-before → Before(right)" rule while cursor_at implements Fugue).

## Full finder reports

### Angle A1: line scan hashseq.rs
1. hashseq.rs:1274 — move_ancestors adds every overwritten id to `seen` before the same-target/move filter, so resolve_decider can pick a non-move node as decider and panic (`move_nodes[&op]`) on remote input. Scenario: doc 'abc'; remote m2 = Move{target:a, to:After(c), overwrites:{b}} then concurrent m3 = Move{target:a, to:Before(b), overwrites:{b}} → panic at hashseq.rs:1371 'no entry found for key'. Confirmed with scratch test by finder. Same root cause panics in placement_of/move_anchor. With common chain {m1, e} where e sorts below m1, `move_nodes[&many[0]]` at 1287 panics.
2. hashseq.rs:1287 — resolve_decider can return a move op of a DIFFERENT target as decider; rerender then renders the wrong element and corrupts RunIndex.moved. Scenario: honest q = move_element(b, After(c)); remote m2 = Move{a, After(c), overwrites:{q}} and concurrent m3 = Move{a, Before(c), overwrites:{q}} → decider(a)=q → register_op_fragment(q) → place_moved_at(b) while b already moved-rendered: debug panic run_index.rs:597 'old rendering must be cleared first'; release silently overwrites moved[b], b rendered twice, a rendered nowhere.
3. hashseq.rs:1688 — marks_at panics for any applied id that is not an insert element (origin, move/mark/place/remove op ids) via elem_ref's panic. position_of guards with `_ => return None`; marks_at does not.
4. hashseq.rs:1275 — even without panic, contaminated `seen` changes last-agreed result: foreign id in `common` makes maximal set >1, recursion returns None, placement freezes at creation instead of the real common ancestor.

### Angle A2: line scan encoding.rs
1. encoding.rs:1637 — BLK_REMOVE_FWD/BWD materializes `start..=end` into a Vec before bounds check → capacity overflow panic / multi-TiB alloc from ~45 untrusted bytes.
2. encoding.rs:1590 — `Vec::with_capacity(num_ids + 1)` from untrusted varint → capacity overflow / huge alloc; `num_ids + 1` overflows in debug for usize::MAX.
3. encoding.rs:1867 — decode_hashkv_v `Vec::with_capacity(n)` same class; reachable from decode_hashkv and every nested map in decode_hashweb.
4. encoding.rs:310 — payload elision not enforced on decode: 0x01 by-id form accepted for char value id → Payload::Id(char_value_id(c)) renders as atom U+FFFC though same node id as Payload::Char(c). Two canonical byte strings for one op set; strict decode accepts both. Fix: normalize in decode_payload's 0x01 arm (or interpret/insert_value), or reject by-id for ≤32-byte artifacts.
5. encoding.rs:1684 — BLK_REMOVE_OTHER `off + count` unchecked: debug panic; release wraps to empty range → Remove applied with fewer targets than declared. Same unchecked `c.pos + len` at 1936, 2082, 2095, 2953.
6. encoding.rs:1664 — decoder builds HashNodes whose pins may contain a named id; `HashNode::id()` debug_assert fires on untrusted bytes; release: two wire forms map to one id with non-canonical stored state.

### Angle A3: line scan small modules
1. run.rs:70 — Run::from_text silently discards interior_extra_deps at offset 0 or >= char count; wire decoder accepts malformed run, derives element ids omitting those deps; re-encode doesn't round-trip.
2. run_index.rs:105 — Frag::rank on Bits::Large indexes ws[k/64] unconditionally; OOB when k == len and len % 64 == 0. Latent (all callers pass k < len).
3. run_index.rs:822 — frag_containing only debug_asserts coverage; release: stale/foreign ElemRef with off >= len resolves to last fragment, Small-bitmap shifts by k >= 64 wrap → bogus Some(position). Reachable only via stale/foreign ElemRef.
4. hashseq_iter.rs:61 — run-rest push skipped whenever run head has ANY explicit afters entry; relies on invariant "afters entry ⇒ run tail". Violation silently drops elements 1..len from causal iteration.
5. placement.rs:138 — chain(): when a single head's `below` has >1 members, switches to multi-head intersection and never emits those members' placed_at; apply(1,A1,{}); apply(2,A2,{1}); apply(3,A3,{}); apply(4,A4,{2,3}) → chain == [A4] only; if A4 unresolvable, renders unplaced rather than falling back to A2/A1. Spec question (PLACEMENT_SPEC).
6. hash_node.rs:413 — sorted_subset_indices unbounded `while refs[i] != *want` panics OOB if subset has id not in refs. Unreachable today; fragility at hash-preimage boundary.

### Angle B: invariant auditor
1. hashseq.rs:1274 — same move_ancestors contamination (Variant A: overwrites:{a} the target itself → panic; Variant B: overwrites:{my} a move of d → debug_assert / release infinite loop in RunIndex::settle, reproduced via quickcheck fuzz of delivery orders).
2. encoding.rs:1590 — with_capacity(num_ids+1) (dup of A2.2).
3. encoding.rs:1637 — span collect (dup of A2.1).
4. hash_node.rs:260 — pin normalization only debug_assert (dup of A2.6). Repro: encode_hashseq(X)=49 bytes vs (Y)=84 bytes, both pass strict; breaks ENCODING_SPEC 'equal op sets encode identically'.
5. hashseq.rs:1306 — resolve_decider recomputes move_ancestors per pair of common ancestors on every decider_of read while register frozen → O(n²): 0.54ms (n=100), 2.1ms (n=200), 8.6ms (n=400). HASHSEQ_SPEC/MOVE.md claim fork-spam is linear in attacker ops.
6. HASHSEQ_SPEC.md:186 — spec still states old anchoring rule ('left causally-before right → Before(right), else After(left)') while cursor_at (hashseq.rs:2449-2470) implements the Fugue rule after commit eccaa1c; MODEL.md C5 wording also stale.

### Angle C: cross-file tracer
1. wasm.rs:949 — WasmHashWeb::seq_move anchors at visible element id without render_anchor; moved-in neighbour → move renders at ghost. seq 'abc'; seq_move(0,3) → 'bca'; seq_move(0,2) → stays 'bca' (expected 'cba'). kb.js drag-reorder calls seqMove repeatedly on the same list.
2. wasm.rs:393 — anchor_range (markRange/unmarkRange) and mark_range_closed build mark anchors from visible-position ids; mark points sit at base slot → inverted span gated (but op id returned as success) or covers none of the text. seq 'abcd', move a to end → 'bcda'; markRange(1,3) gated; markRangeClosed(3,4) applies but marked_spans == [("bcda", [])].
3. hashseq.rs:1919 — authoring helpers (mark_range, move_element, place, insert_value) call record_authored BEFORE apply and return HashNode unconditionally; gate verdict dropped; outbox ships an op this replica quarantined.
4. wasm.rs:382 — merge_encoded decodes untrusted bytes then HashSeq::merge assert_eq!s origins → wasm trap instead of JsValue error. nool's sidecar checks origin first.

### Angle Reuse: duplicated helpers
- R1 encoding.rs:342 — local `varint_len` closure in encode_node_preimage duplicates hash_node::varint_len (hash_node.rs:224) byte-for-byte.
- R2 encoding.rs:330 — local `subset_idxs` closure duplicates hash_node::sorted_subset_indices (hash_node.rs:409).
- R3 encoding.rs:278 — encode_payload hand-assembles char artifact (VK_CHAR ‖ utf8), duplicating Value::encode (value.rs:102-106) and char_value_id (value.rs:244-246).
- (dropped) hashseq.rs:447 — SortedIdVec::first_ge `match Ok(p)|Err(p)` vs `.unwrap_or_else(|p| p)` idiom at hashkv.rs:309.
- (low) encoding.rs:2140 — six private test-id constructors with two incompatible shapes (test_id byte-0 only vs tid/oid all bytes).

### Angle Simplification: complexity
- S1 hashseq.rs:2283 — `merge` re-implements `all_nodes` inline (seven loops, 2297-2346 vs 2244-2281); new node kind must be added twice.
- S2 hashseq.rs:525 — `mark_anchored_ops` is derivable from `mark_events.contains_key(&op)`; replace in op_has_children (1362), delete insert at 1562.
- S3 run.rs:222 — Dead Run API: split_at (222-251), first_node (152), run_id (178), find_position (183) have no non-test callers; StoredRun::split_at (hashseq.rs:336) is the live one.
- S4 hashseq.rs:2161 — interpret's Mark arm destructures node.op by value then rebuilds identical HashNode to return Err; borrow instead.
- S5 run_index.rs:201 — IndexTarget six paired Before*/After* variants → (slot kind, before: bool); attach_at (672-726) six arms differ only by attach_pred/attach_succ; index_target (hashseq.rs:996-1030) three if/else blocks.
- S6 run_index.rs:30 — `Bits` Small/Large hand-rolled small-vector; every op written twice; `Frag::visible` count maintained by hand at 95, 156-158, 559, 576, 648, 664.

### Angle Efficiency: wasted work
- E1 encoding.rs:910 + hashseq.rs:1850 — canonical-run encoder walk and marked_spans call char_at per element; StoredRun::char_at is `text.chars().nth(pos)` (hashseq.rs:313) → O(len²) per stored run (69k-element run → ~2.4e9 char steps per encode).
- E2 hashseq.rs:2113 — each applied insert resolves anchor id via IdIndex three times (park_or_dispatch contains_node 2085, interpret idx_of 2113, insert_after/before idx_of_known 1034/2010). ~10% of apply time.
- E3 run_index.rs:418 — cmp_sweep calls root_path twice (each allocates + reverses a Vec) per comparison; called from mark_covers 2× per mark per marks_at. Allocation-free LCA instead.
- E4 run_index.rs:112 — Frag::select / rank on Bits::Large scan words linearly; RunIndex::get called twice per cursor_at → ~2.2k words scanned per keystroke at end of 69k run. Bound fragment length or superblock prefix counts.
- E5 encoding.rs:1620 — decode_hashseq clones `run.elements` into ranks.runs while `run` is consumed right after; move instead (~32 MB transient for 1M chars).

### Angle Altitude: bandaids (design-level, not verified individually)
1. hashseq.rs:1006 — glue-point resolution split across three re-derivations (admission whitelist at interpret:2114/2137/glue_point:1479; rendered-vs-splice-ghost predicate at index_target:1010/ensure_op_fragment:1421/op_point_pos:1509/hashseq_iter.rs:87; `ensure_op_fragment` pre-call at 5 sites). Proposed: single `resolve_glue(anchor) -> Option<IndexTarget>`.
2. hashseq.rs:106 — per-op-kind node representation grows by enumeration (PackedLoc 3-bit kind, 7 of 8 used, `_ => Loc::MultiRemove` catch-all at 131, no debug_assert on kind range in pack); each kind has its own side table + hand-written arm in interpret/all_nodes/merge/encoder depth walk/encoder trailing section. Next-but-one op kind silently corrupts the handle.
3. hashseq.rs:1369 — sibling attachment implemented twice (register_op_fragment mirrors insert_after:1061-1110); after_sibling_target vs before_sibling_target have different shapes. Proposed: one `attach_sibling(anchor, side, id)`.
4. encoding.rs:642 — block encoding hardcodes two chain shapes; Move/Mark/Place/atom inserts all go to the 'orphans' trailing section as individually tagged nodes re-applied via seq.apply (full rehash). KB workload (moves, marks, embeds) loses run compression.
5. encoding.rs:1987 — artifact inclusion in wire snapshots decided by byte size (WIRE_ARTIFACT_MAX = 1024) not by role; a Put value >1KB is stripped from hello snapshot and never lazily fetched → register missing forever on fresh peers.
6. hashseq.rs:658 — delta outbox populated only by hand-instrumented authoring helpers (record_authored at 914, 925, 943, 959, 1468, 1661, 1919); public `apply` records nothing though Cursor::first_node/payload_node are documented as the canonical insertion path → locally applied nodes never leave via 0xDE frames.

## Verifier prompts
Saved verbatim in scratchpad/verifier_prompts.md (15 prompts). Re-run one at a time; add the allocation-bomb warning to any that touch decode_hashseq.

## Queued follow-ups

### Q1. Value store: resolve by-id atoms through `HashWeb.values` (from #5)

Decision (David, 2026-09-02): never refuse the by-id payload form; resolve the id on read when the replica knows the value. The interim fix resolves via the ASCII char table only (`Payload::resolved()`, hash_node.rs). Scoping (read-only pass, 2026-09-02):

**What exists.** `HashWeb.values: IdMap<Vec<u8>>` (hashweb.rs:43) is already the store — raw canonical artifact bytes keyed by value id, decoded lazily by `HashWeb::resolve` (:221); fed by `provide_artifact_bytes` (:182), `provide_value` (:202), `merge` (:281/:316), `decode_hashweb` (encoding.rs:2172-2178). `HashKv.values` (hashkv.rs:56) is the per-object twin. Mark kind/value ids and kv values are already resolved at read time by callers against this store (wasm.rs:1073, hashkv.rs:208). Only text atoms are fixed at apply: `apply` maps `Payload::Id(v)` → `(ATOM_CHAR, Some(v))` (hashseq.rs:2195) into `StoredRun.text` + `elem_payloads`, and every reader (`iter`, `char_at`, `marked_spans`, wasm `text`, nool) reads run text.

**Ownership: keep the store on HashWeb, resolve at apply + re-resolve on artifact arrival.** No new struct, no signature changes to `iter`/`char_at` (≈140 call sites incl. tests), nothing on the per-char hot path. Rejected: (a) a store per HashSeq — duplicates HashWeb.values and must be excluded from `encode_hashseq`; (b) readers take `&ValueStore` — touches every read API and puts an `elem_payloads` probe on `iter`. Do NOT use `CHAR_MEMO` as the store: thread-local, history-dependent → rendering would differ per thread.

**Steps.**
1. hashweb.rs: `resolve_char(&self, id) -> Option<char>` (ASCII table, else `values` → `Value::decode` → `Char`); `resolve_node(node) -> node` rewriting `Op::Insert{Payload::Id(v)}` when it hits (id unchanged; debug_assert).
2. hashweb.rs `apply_to_with_id` (:240): route seq deliveries through `resolve_node` (parked nodes resolve on wake, :272; `merge` :307 already goes through it).
3. hashseq.rs: `pub(crate) fn resolve_atoms(&mut self, vid: &Id, c: char) -> usize`: for each `elem_payloads[e] == vid`, set the single-element run's text to `c`, drop the `elem_payloads` entry. Run length unchanged → RunIndex untouched. Add a strict round-trip test proving a resolved 1-char run encodes identically to a typed one.
4. Call `resolve_atoms` on every seq whenever new bytes decode to a non-ASCII `Value::Char`: `provide_artifact_bytes`, `provide_value`, `merge`, and `decode_hashweb` after objects decode.
5. encoding.rs: `decode_payload`'s 0x00 non-char arm drops the inline artifact bytes (:363); thread a sink (`decode_*_into(bytes, &mut Vec<Vec<u8>>)` variants, existing names as wrappers) so `decode_hashweb` / wasm op ingestion capture them into `web.values`. (Our own encoder only inlines `Payload::Char`; inline non-char small artifacts come from foreign encoders.)
6. Tests: by-id non-ASCII char applied before and after its artifact arrives renders as the char; ids equal; `payload_of` → None after resolution; strict web round-trip of resolved state.
7. Docs: HASHSEQ_SPEC.md "Payload" (line ~99) and the `ATOM_CHAR` doc (hashseq.rs:219-222): placeholder means "until resolved".

**Canonical-bytes note.** Node ids never change. Seq stream bytes DO change once an atom resolves (`0x01 id` → `0x00 len bytes`), which GRAMMAR_SPEC.md:241-244 says is the mandatory form when present-and-small. So `decode_hashweb_strict` of an old snapshot that holds a by-id char atom together with its artifact will report `NotCanonical` after resolution — correct per spec, but a visible behaviour change. `decode_hashseq_strict` alone stays ASCII-only (no store).

### Q2. Derive the delta outbox from an arena watermark + provenance (from #9 / Altitude #6)

Decision (David, 2026-09-02): replace the hand-fed outbox with a derived delta.

**Watermark.** The node arena (`HashSeq.ids`, `HashKv` equivalent) is append-only in apply order: `intern` pushes and returns `NodeIdx(ids.len())`, only admitted paths intern (parked/gated nodes never do), nothing is removed or reordered. So `w = ids.len()` taken when a delta is drained to peer P names exactly "everything applied at that moment" = the causal closure of the tips at that moment. Delta for P = nodes at `ids[w..]`; then `w = ids.len()`. Late-arriving concurrent nodes get high indices, which is correct (P lacked them too). The number is local per replica and per object — keep one per (peer, object) on the sender's side; a peer cannot name it.

**Provenance.** The watermark says "since when", not "whose". Merge / snapshot decode / `apply_delta` all intern, so a pure watermark delta echoes remote nodes back (idempotent, but wasteful in a mesh; today's outbox avoids echo by construction). Add a per-node local/remote bit set at the apply seam (`author` = local; `apply`/`apply_with_id` from merge, decode, delta = remote) and filter the delta to local. This also closes Altitude #6: a node applied through public `apply` by a local author is currently never shipped.

**Steps.**
1. hashseq.rs / hashkv.rs: `pub(crate) authored: BitSet` (or `Vec<bool>`) parallel to `ids`, set by `author`; make `apply` the remote path (or add `apply_local`). `record_authored` and the `outbox` Vec go away; `author` collapses to "apply + mark local".
2. `nodes_since(w: usize) -> impl Iterator<(Id, HashNode)>`: per-index rebuild via `loc_of` (runs: element node from the stored run; removes/moves/marks/places from their side tables — the same reconstructions `all_nodes` does, restricted to `idx >= w`).
3. hashweb.rs: `take_deltas(peer)` = for each object, `nodes_since(w[peer][obj]).filter(authored)`, then advance the watermark. Keep the peer-agnostic `take_deltas()` as the single-upstream case (one watermark).
4. Snapshot resync resets that peer's watermarks to the arena length after the merge.
5. wasm.rs: `take_deltas` unchanged on the outside.
6. Tests: local apply of a cursor-built node ships; remote nodes never echo; a gated authored op never ships; watermark after merge excludes the merged history.

Invariant to keep: the delta must never include a node this replica quarantined — automatic, since gated nodes are not interned.
