# Queue

- check if self-moves are actually problematic, currently we guard against it.
- afters_of | befores_of, can they return sortedidvec? seems that way.
## Core

### 44. Encoder block layer in handle space — OPEN

Where: `src/encoding.rs` block derivation and emit (`CanonRun`, `Block`, `Payload`, `visit_refs`, `producer`, `resolve` / `encode_ref`); `encode_hashkv_with_store`.

Done so far: `InternedHashNode` (`src/interned_hash_node.rs`) is the `HashNode` / `Op` shape with every ref a `NodeIdx`. Both projections resolve a node once at the apply seam (`orphan_or_dispatch`; the `Err` is the orphan key) and `interpret` takes the interned form; the stored forms are built from it by move (`StoredRun.at`, `StoredMove.to`, `StoredMark.start/end` are `InternedAnchor`s); `HashSeq::interned_at` is the one stored-form → node seam and `node_at` is `interned_at(..).to_node(&ids)`; the clock walk and the encoder's depth pass take edges from `HashSeq::for_each_ref` with no probes; `HashKv::nodes` is a `Vec<InternedHashNode>` parallel to `order` (origin at slot 0). The encoder no longer calls `idx_of`. Perf-gated: `target/perf/item44-baseline-d203db5.txt` vs `item44-after.txt`, encoded bytes unchanged.

Problem: the block layer still works in id space. `CanonRun.first_pins` / `interior`, `Payload::*::extra_deps` / `targets` and `Block.exposed` are rebuilt as `BTreeSet<Id>` / `Vec<Id>` from handles, `producer` is an `FxHashMap<Id, _>` over every exposed id, and `encode_ref` hashes a 32-byte id per ref to find its block. The kv encoder likewise sorts `all_nodes()` and ranks through an `index_of: FxHashMap<Id, usize>`.

Fix: keep block bodies as handles (`SortedIdVec` pins, `NodeIdx` targets), make `producer` a `Vec<(u32, u32)>` indexed by handle, and go to ids only for the dictionary and for orphans (whose refs may be unknown). Byte-identical output is the gate: the strict round-trip and determinism tests, plus the size tables of `sequential_traces`.

### 3. Delta drain is O(n²) for non-ASCII runs — OPEN

Where: `src/hashseq.rs` `node_at`, `StoredRun::char_at`.

Problem: `char_at(pos)` falls back to `text.chars().nth(pos)` whenever `text.len() != elements.len()`, and `node_at` calls it once per element. Delta emission and the clock walk's closure marking both go through `node_at`, so a drain or a hello over a non-ASCII run is quadratic. Release: 100k ASCII = 3.3 ms; "é" + 99,999 'a' = 376 ms. The wasm client drains every 200 ms and basecamp drains synchronously after every mutating call.

Fix: give `StoredRun` an O(1) char accessor for the mixed case, or reconstruct per run with one `text.chars().zip(elements)` pass as `all_nodes` does.

### 4. `deltas_for` and `frontier` have duplicated seq/kv loops — OPEN

Where: `src/hashweb.rs` `deltas_for`, `frontier`.

Problem: each has a seq loop and a kv loop differing only in `KIND_SEQ`/`KIND_KV`. `deltas_for` also runs a walk per object per drain, cheap but unconditional.

Fix: share the loop body (hoist `encode_hashweb_with`'s `ObjRef { Map, Seq }`). If per-drain cost shows, a per-object dirty flag skips objects untouched since the clock last advanced.

### 5. The run pin/anchor convention is derived in three places — OPEN

Where: `src/hashseq.rs` `interned_at` / `for_each_ref` (Run arm), `src/encoding.rs` `elem_pins` / `elem_anchor`, `src/run.rs` `Run::decompress_with_ids`.

Problem: "element 0 carries `at` + `first_pins`, element `i > 0` is `After(elements[i-1])` + `interior_pins[i]`" is written out in each. (The per-kind reconstructors are gone: `node_at` is `interned_at(..).to_node(&ids)`, and `all_nodes` uses it for everything but char runs, which stay batched per run so item 3's cost does not leak into merge.)

Fix: one `StoredRun::elem(pos) -> (InternedAnchor, Option<&SortedIdVec>)` that the three share.

### 6. Stale doc comments from the FirstOp→Anchor rename — OPEN

Where: `src/hashseq.rs:276` (broken rustdoc link `` [`Run::interior_extra_deps`] ``, `cargo doc` warns), `src/run.rs:19,23,58`, `src/hashseq.rs:155,488`.

Fix: replace `first_op` / `FirstOp::Before` / `extra_deps` with `at` / `Anchor::Before` / `interior_pins` / `pins`.

### 7. Dead guard in `IdIndex::insert` — OPEN

Where: `src/hashseq.rs:52`.

Problem: `if *e.get() != idx { spill }` cannot be false: the only caller `intern` always passes the arena tail, which no existing prefix entry can hold.

Fix: `Occupied(_) => { self.spill.insert(id, idx); }` with a note that each id is interned once.

### 8. Stray TODO in `cursor_at` — OPEN

Where: `src/hashseq.rs` ~:2609.

Problem: `// TODO: why not the visible right neighbor?` sits above the comment that answers it (Fugue rule; `region_first` guarantees no before-children).

Fix: delete it or fold the question into the existing comment.

### 9. Run-op anchor polarity is inverted relative to `Anchor::side_bit` — OPEN

Where: `src/encoding.rs:296` and the `BLK_RUN_*` emit ~:1499.

Problem: run frames encode After=0x00, Before=0x01; `Anchor::side_bit` in the node preimage is Before=0, After=1. The op-stream form is live on the wire, so flipping is not viable.

Fix: one-line comment on the `RUN_OP_*` / `BLK_RUN_*` constants saying the polarity is deliberately opposite to `side_bit`.

### 10. Value store: resolve by-id atoms through `HashWeb.values` — OPEN

Where: `src/hashweb.rs`, the `src/hashseq.rs` apply path, `src/encoding.rs` `decode_payload`.

Problem: the by-id payload form is never refused; the id is resolved on read when the replica knows the value. Small values are done (2026-09-24): artifacts of ≤ 15 bytes are their own value id (GRAMMAR_SPEC.md "Identity-form value ids"), so a char — or any small value — sent as `0x01 id` resolves from the id on every replica with no artifact and no table; steps 1–4 below remain only for hashed (16–32 B) artifacts. Still open: the spec says inline `0x00 len bytes` is mandatory for any artifact ≤ 32 B, but the encoder inlines only `Payload::Char` and the decoder drops the bytes of an inline non-char artifact.

Plan: keep the store on HashWeb; resolve at apply and re-resolve on artifact arrival; no signature change to `iter`/`char_at`; never use `CHAR_MEMO` (thread-local, history-dependent).

1. `HashWeb::resolve_char(id) -> Option<char>` (ASCII table, else `values` → `Value::decode` → `Char`) and `resolve_node(node)` rewriting `Op::Insert{Payload::Id(v)}` on hit, id unchanged.
2. Route seq deliveries in `apply_to_with_id` through `resolve_node`; orphaned nodes resolve on wake; `merge` already passes through it.
3. `HashSeq::resolve_atoms(vid, c) -> usize`: for each `elem_payloads[e] == vid` set the single-element run's text to `c`, drop the entry. Run length unchanged so RunIndex is untouched. Strict round-trip test that a resolved 1-char run encodes identically to a typed one.
4. Call `resolve_atoms` on every seq when new bytes decode to a non-ASCII `Value::Char`: `provide_artifact_bytes`, `provide_value`, `merge`, `decode_hashweb`.
5. Thread a value sink through `decode_*_into(bytes, &mut Vec<Vec<u8>>)` so `decode_hashweb` / wasm ingestion capture inline non-char artifacts into `web.values`.
6. Tests: by-id non-ASCII char applied before and after its artifact arrives renders as the char; ids equal; `payload_of` → None after resolution; strict web round-trip of resolved state.
7. Docs: HASHSEQ_SPEC.md "Payload" (~:99) and the `ATOM_CHAR` doc: placeholder means "until resolved".

Note: seq stream bytes change once an atom resolves (`0x01 id` → `0x00 len bytes`, mandatory per GRAMMAR_SPEC "Stream references and value elision"), so `decode_hashweb_strict` of an old snapshot holding a by-id atom plus its artifact reports `NotCanonical` after resolution. Correct per spec; a visible behaviour change.

### 11. Bounded select/rank on large fragments — OPEN

Where: `src/run_index.rs:112` `Frag::select` / `rank` on `Bits::Large`.

Problem: linear word scan; `RunIndex::get` is called twice per `cursor_at`, so ~2.2k words scanned per keystroke at the tail of a 69k-element run. Constraint: `Bits::Small(u64)` stays exactly as is; nothing may cost the measured gains.

Fix: inside `Bits::Large` only, a visible count per superblock of 8 words (512 elements). `select` skips whole blocks then scans ≤ 8 words; `rank` sums blocks below `k` then ≤ 8 words. `push_visible`/`set_bit`/`clear_bit` adjust one block count; `split_bits` rebuilds. Gate: `cargo run --release --example sequential_traces` vs `target/perf/review-after-e2.txt`, no regression on clownschool / friendsforever / json-crdt or it does not land. Also `debug_assert_eq!(frag.visible, popcount(bits))` at the six mutation sites.

### 12. `IndexTarget` as (slot kind, before: bool) — DEFERRED

Where: `src/run_index.rs:201` `IndexTarget`, `attach_at` (~672-726), `index_target` (hashseq.rs ~996-1030).

Problem: six paired Before*/After* variants. Collapsing to (slot kind, before: bool) folds the four Moved/Splice arms of `attach_at`; the two Elem arms carry different split logic and stay.

Fix: only alongside the next change that touches `attach_at`, and measure.

### 13. Glue-point resolution is split across three re-derivations — OPEN

Where: `src/hashseq.rs` ~:1006.

Problem: the admission whitelist (now one predicate, `is_glue_point`), the rendered-vs-splice-ghost predicate (`index_target` ~:1010, `ensure_op_fragment` ~:1421, `op_point_pos` ~:1509, `hashseq_iter.rs:87`), and the `ensure_op_fragment` pre-call at 5 sites.

Fix: one `resolve_glue(anchor) -> Option<IndexTarget>`.

### 14. Node representation grows by enumeration — OPEN

Where: `src/hashseq.rs` ~:106 `PackedLoc` and the per-kind side tables.

Problem: 3-bit kind with 7 of 8 used, `_ => Loc::MultiRemove` catch-all, no debug_assert on kind range in `pack`. The next-but-one op kind silently corrupts the handle. Every kind still has its own side table, but the hand-written arms are down to three: `interpret`, `interned_at` (the reconstruction seam every walker shares) and `for_each_ref` (its allocation-free edge twin, kept equal by `check_for_each_ref_matches_interned_refs`).

Fix: a debug_assert on the kind range and a comment on the budget.

### 15. Sibling attachment implemented twice — OPEN

Where: `src/hashseq.rs` ~:1369 `register_op_fragment` vs `insert_after` (~1061-1110).

Problem: `register_op_fragment` mirrors `insert_after`; `after_sibling_target` vs `before_sibling_target` have different shapes.

Fix: one `attach_sibling(anchor, side, id)`.

### 16. Block encoder hardcodes two chain shapes — OPEN

Where: `src/encoding.rs` ~:642.

Problem: Move / Mark / Place / atom inserts all go to the trailing section as individually tagged nodes re-applied via `seq.apply` (full rehash). A KB workload (moves, marks, embeds) loses run compression.

Fix: block forms for the op kinds the KB emits, or at least a batched re-apply that skips the rehash.

### 17. Artifact inclusion in wire snapshots decided by byte size, not role — OPEN

Where: `src/encoding.rs:2109` `WIRE_ARTIFACT_MAX = 1024`, `src/hashkv.rs:136`.

Problem: a Put value > 1 KiB is stripped from the hello snapshot and never lazily fetched, so the register is missing forever on fresh peers. Overlaps item 34.

Fix: decide by role (register values always ride; large blobs are fetched by id), or add a lazy fetch path on `Read::One(id)` misses.

### 18. HASHSEQ_SPEC states the pre-Fugue anchor rule — OPEN

Where: `HASHSEQ_SPEC.md:186-189`.

Problem: the spec says "left causally-before right → `Before(right)`, else `After(left)`" while `cursor_at` implements the Fugue rule.

Fix: rewrite the paragraph to describe the rule `cursor_at` implements, and say what "block non-interleaving" claim survives.

### 19. `PackedLoc::pack` 29-bit position only debug-asserted — OPEN

Where: `src/hashseq.rs:114`.

Problem: needs 512 MB of run text to overflow, then silently corrupts the handle in release.

Fix: split the run before the limit, or return an error from the insert path.

### 20. Six private test-id constructors with two incompatible shapes — OPEN

Where: `src/encoding.rs:2260` `test_id`, `:3470` `oid`, and siblings.

Fix: one shared test helper module.

### 45. `HashSeq::apply` admits nodes the wire decoders reject — OPEN

Where: `src/hashseq.rs` `apply_with_id` (and `HashKv::apply_with_id`); `src/hash_node.rs` `HashNode::id` only `debug_assert!`s normalization.

Problem: a node with a redundant pin (pin = its anchor) hashes to the same id as its normalized twin and is accepted; so is a ref-less `Remove({})`. The replica then stores a form its own snapshot cannot decode (`RedundantPin` / `NoRefs`) — every peer rejects its snapshots and deltas; debug builds panic in `apply`. Reachable through the public API and `merge` from an untrusted `HashSeq`; every network path goes through `validate_node`. (2026-09-24 review.)

Fix: run the `validate_node` rules (`is_normalized`, non-empty refs) in `apply_with_id` and refuse, or normalize before interning.

### 46. Decode amplification: remove blocks expand superlinearly — OPEN

Where: `src/encoding.rs` decode of `BLK_REMOVE_FWD | BLK_REMOVE_BWD` (~1651) and `BLK_REMOVE_OTHER` segments (~1712).

Problem: each ~8-byte remove-span block mints a whole chain over an existing run, so N blocks with distinct first pins mint N × run-length nodes (measured 4.9 KB → 250k nodes; ~1 MB of input is an OOM). `REMOVE_OTHER` segments need not be sorted or disjoint: S overlapping segments cost S × L. Strict mode does not help — it decodes before comparing. ENCODING_SPEC claims decode is never superlinear. (2026-09-24 review.)

Fix: require segments strictly ascending and non-adjacent, and enforce a global expansion budget (expanded targets + minted nodes ≤ c × input bytes).

### 47. `apply_delta` opens objects before validating them — OPEN

Where: `src/encoding.rs` `apply_delta` (~3370): `create_seq` / `create_kv` per group before any node is checked.

Problem: ~34 bytes per junk object, even for empty groups and for deltas later rejected; the junk persists in every snapshot and ships to peers, while `delivered` reports 0. (2026-09-24 review.)

Fix: open an object only once one of its nodes applies (or orphans), and never on a rejected delta.

### 48. Unknown op kinds are rejected, not carried — DECISION

Where: `src/encoding.rs` `decode_node_with` / block tags (`InvalidOpTag`); GRAMMAR_SPEC.md "Grammar-level validation".

Problem: the spec says unknown kinds are not malformed and must be carried opaquely, but every decoder rejects them, and the standalone/trailing node form (`tag ‖ pins ‖ per-kind fields`) has no length envelope, so an unknown kind cannot even be skipped. One future-kind op aborts a whole delta or snapshot: adding a kind is a hard fork. Related: clocks ignore refusals, so replicas that disagree on a verdict (a "loosening" upgrade) re-ship the same suffix on every sync. (2026-09-24 review.)

Fix: decide the extension story — a length-framed node envelope on the wire plus carry-opaque storage — or amend the spec to "unknown kinds are a hard fork".

### 49. Contested move-register resolution is super-cubic — OPEN

Where: `src/hashseq.rs` `resolve_decider_with` (~1108), called on every `apply_move`.

Problem: a "ladder" history (a_k, b_k each overwriting {a_{k-1}, b_{k-1}} — two synced users repeatedly moving the same element) re-resolves the whole ladder per apply, with an O(n²) ancestor memo: 1,600 ops take 14 s. Honest-reachable, and decode replays it. (2026-09-24 review.)

Fix: a generation number per move op and an iterative descent of the maximal common set, or cache the last-agreed op per antichain incrementally.

### 50. Treap priorities are a fixed, public LCG — OPEN

Where: `src/run_index.rs` `rng: 0x9E3779B97F4A7C15` (~273), `next_prio` (~950).

Problem: every insert creates one fragment with a predictable priority, so a peer can order its inserts to make the treap a path: depth 39,998 at 40k inserts, and every later `get` / `seek` / `position_of` / `cmp_sweep` on that replica is O(n). (2026-09-24 review.)

Fix: seed per replica from a random source, or derive priorities from a keyed hash of the slot. Perf-gated (item 11's run-index rule).

### 51. Forking through a long run is quadratic — OPEN

Where: `src/hashseq.rs` `split_run_at` (~943) relocates every element of the right part; `src/run_index.rs` `Frag::split_bits` (~159) copies `Bits::Large` one bit at a time and keeps the left piece in place.

Problem: forking left-to-right through a long run is O(n²) (40k chars: 6.4 s); `Before` inserts marching forward through a large fragment likewise (40k: 5.5 s vs 0.1 s in reverse). Honest-reachable, peer-triggerable. (2026-09-24 review.)

Fix: relocate the smaller side; shift bitmap words, not bits.

### 52. Encoder is O(k²) / O(F²) on some shapes — OPEN

Where: `src/encoding.rs` force-emit victim scan over `blocked` (~1282); the fork-extender minimum recomputed per child (~852) and per remove link (`heads_pinning`, ~971-987).

Problem: k soft cycles cost O(k²) (8k cycles: 0.6 s); F fork children of one element cost O(F²) (16k: 2.5 s to encode a 144 KB snapshot). Strict decode re-encodes, so it inherits both. (2026-09-24 review.)

Fix: keep hard-ready blocked blocks in an ordered set keyed by `(depth, idx)`; memoize the extender per parent.

### 53. `live_set` scans overwrites linearly per member pair — OPEN

Where: `src/hashseq.rs` `live_set` (~1557), under `marks_at` / `marked_spans`.

Problem: `overwrites.iter().any(..)` makes each read O(k² · W) for k marks naming W ids (k = W = 1000: 326 ms per `marks_at`). Related: honest mark toggling grows overwrites quadratically on the wire (MARKS.md open problem 1). (2026-09-24 review.)

Fix: `SortedIdVec::contains` (binary search), or one suppressed-id set per kind group.

### 54. Orphans are never evicted — DECISION

Where: `src/delivery.rs` `orphaned`; `src/hashweb.rs` store-level `orphaned`.

Problem: nodes waiting on a ref that never arrives (or was refused) are kept forever, re-applied on `merge`, and dropped only by encode. Memory is linear in attacker input, with no bound. (2026-09-24 review.)

Fix: decide a policy — per-object / per-peer caps, age-out, or drop-on-encode made explicit.

### 55. Non-strict decoders accept trailing bytes and duplicate sections — OPEN

Where: `src/encoding.rs` `decode_hashseq` / `decode_hashkv` / `decode_hashweb` (non-strict); `decode_hashweb` object sections (`insert`, not merge); wasm `applyTo` ignores consumed length.

Problem: trailing garbage is accepted silently, and a duplicated object section replaces the earlier one (a full section followed by an empty one decodes to empty text). Strict mode catches both. (2026-09-24 review.)

Fix: reject trailing bytes everywhere; merge or reject duplicate object sections.

## HashKv / HashWeb / value

### 21. Locally minted value stays pending in the local kv view — OPEN

Where: `src/hashkv.rs` `put_ids`, `src/hashweb.rs` `provide_value`.

Problem: a `kv_mut().put_ids(k, vid)` whose value was minted only via `web.provide_value` stays pending in the authoring replica's own kv view: `web.resolve` sees it, every decoding/merging peer hydrates it, but the local `kv.get` does not. The wasm and test `or_else(web.resolve)` fallbacks exist only for this case.

Fix: hydrate the kv view from `web.values` on `put_ids` (or give the kv a handle to the shared store), then delete the `or_else` fallbacks.

### 22. Spurious kv conflicts between identical heads — DECISION

Where: `src/hashkv.rs` `read_id` (~183-197).

Problem: two concurrent `del(k)` → `Conflict([TOMBSTONE, TOMBSTONE])`, `get` None, `keys()` lists the key as live. Spec-conformant but useless to the app.

Fix: dedup value ids in `read_id`. Needs a HASHKV_SPEC decision first.

### 23. Store-orphaned dedup and `knows()` are O(N²) — OPEN

Where: `src/hashweb.rs:269` `knows`, the store-orphaned dedup.

Problem: both scan the orphaned Vec linearly, so N orphaned envelopes on one unopened object cost O(N²).

Fix: key `orphaned` by node id, or keep a side set of ids per object.

### 24. `HashKv::merge` panics on origin mismatch — OPEN

Where: `src/hashkv.rs:413`.

Problem: `assert_eq!` on origin. `HashSeq::merge` has the same assert but wasm guards it (`merge_encoded`); the kv path is Rust-API only.

Fix: return an error, or document the precondition and keep the assert.

### 25. `Value::decode` conflates unknown kind with pending — OPEN

Where: `src/value.rs:123`.

Problem: returns None for unknown kinds, so `resolve` cannot tell "unknown value kind" from "artifact not yet received".

Fix: a distinct `Unknown(kind)` variant or a `Result`.

### 26. Link payload doc/test drift — OPEN

Where: `src/hashweb.rs:8` vs tests.

Problem: disagreement on whether a link payload is the origin or the object id.

Fix: pick one, fix the other.

### 56. Parent/child op replay across the composition convention — DECISION

Where: `src/hashweb.rs` `apply_to_with_id` (~313) and the module docs ("one object's ops can never merge into another"); HASHWEB_SPEC.md "the envelope needs no trust".

Problem: a child opened at parent op X has origin X, which is also an ordinary element of the parent, so any op whose refs are ⊆ {X} ∪ child ops is valid in both objects; the untrusted envelope picks which. Confirmed: child text re-enveloped into the parent (`"abEVIL"`), a child put into the parent kv, a parent put into the child, and the child's birth `Place` into the parent's placement register (can manufacture containment cycles or freeze a register). Per-object convergence holds; integrity and attribution do not. (2026-09-24 review.)

Fix: decide — a per-object domain separator in the preimage (commit each op to its object), or state in the spec that parent and child share one commitment domain at X.

### 57. `HashKv::get` depends on artifact vs op delivery order — OPEN

Where: `src/hashkv.rs` `hydrate` (~162); `src/hashweb.rs` `provide_artifact_bytes` (~239), `merge` (~352).

Problem: store artifacts are mirrored into a kv only when a node is delivered; bytes that arrive later (0xAF frames, `HashWeb::merge`) never reach kvs that already exist. Replica A (artifact then op) reads `Some`, replica B (op then artifact) reads `None`, yet `A == B`. A snapshot round-trip heals it; wasm and nool read the web store first. Mostly moot for values ≤ 15 bytes (identity-form ids resolve without the store). (2026-09-24 review.)

Fix: resolve kv reads through the web store, or re-hydrate kvs when new artifacts arrive.

### 58. D4 cycle detachment is not in the crate — OPEN

Where: `src/placement.rs` (register + `chain()` only); `web/kb.js` approximates it.

Problem: HASHWEB_SPEC / CYCLE_REVERT.md specify cyclic SCCs detach, flagged; the core has no implementation, and kb.js checks only `chain[0]` instead of falling back along the chain. `chain()` itself is O(n³–n⁴) (`closure` recomputed inside `maximal`'s double loop: 400 Place ops → 4.4 s per read). Item 56 lets a peer manufacture the cycles. (2026-09-24 review.)

Fix: implement D4 in the crate; memoize closures in `chain()` (or compute maximal elements in one reverse-topological pass).

## wasm / web

### 27. kb.js paths never run in a browser — OPEN

Where: `web/kb.js`, `web/index.js`.

Problem: unverified: `offsetOfPoint` / `locateOffset` DOM mapping after the code-point conversion layer, IME event ordering with the deferred render, sidebar drag resolving by origin at drop, the CodeMirror listener in `web/index.js`, and the title debounce capturing its page.

Fix: drive the KB in a browser and check each path: astral chars ("😀a￼b" Backspace-merge, replacing 😀 with 😁), IME composition during a remote delta, drag during a remote render, title edit then page switch within 350 ms.

### 28. wasm mark/unmark mint artifacts before validating the range — OPEN

Where: `src/wasm.rs` ~1050-1140.

Problem: an invalid call still pushes kind/value artifacts. Harmless.

Fix: validate first.

### 29. Full-snapshot upload on connect and reconnect — OPEN

Where: `web/kb.js` `connectSync`, `sync-server/src/main.rs` `client_loop`.

Problem: every connect uploads a full snapshot and the server answers every join with one.

Fix: item 39.

### 39. Hello by frontier: replace the snapshot handshake with 0xC1 frames — OPEN

Where: `web/kb.js` `connectSync` / `handleSyncMessage`, `sync-server/src/main.rs` `client_loop`.

Problem: the library has everything for a snapshot-free handshake (`encode_frontier` / `decode_frontier`, per-peer `HashWebClock`, `deltas_for`, `HashWeb::clock`) but the wire protocol still opens with full snapshots both ways: the server sends `fresh_bytes()` on join, the client sends `snapshotBytes()` on open, and the lagged path resends a snapshot. Clocks are values that change only when the peer states its clock or when we send, so a delta the client receives from the relay is not in the relay's clock and ships back on the next drain (the relay dedups, then re-broadcasts it): steady state needs the peer's clock to travel with its deltas, or the relay to answer each delta with its clock.

Fix: on open the client sends its 0xC1 frontier; the server keeps a `HashWebClock` per connection from the frame, replies with `deltas_for` (a delta, or nothing) plus its own 0xC1 so the client can answer with its delta; each 0xDE frame carries the sender's frontier (or is followed by a 0xC1) so the receiver replaces the sender's clock on every delta; after sending, each side keeps its own `clock()` as the peer's; lagged clients get a fresh frontier exchange instead of a snapshot. Keep the snapshot path for legacy clients (first frame `HWB2`). The sync-server's `wire_stale` snapshot cache is then only needed for the compat path.

## nool

### 30. `nool status | head` panics on broken pipe — OPEN

Where: `src/bin/nool/main.rs` `println!` sites.

Fix: write through a locked `stdout` and ignore `ErrorKind::BrokenPipe`, or reset SIGPIPE at startup.

### 31. Top-level USAGE omits `--force` — OPEN

Where: `src/bin/nool/main.rs:28` `USAGE`.

Problem: the per-command usage strings in `repo.rs:525,610` mention `--force` on `rm` and `merge`; the top-level text does not.

Fix: add the flags to the top-level text.

### 32. `init` ordering and stray `.nool` entry — OPEN

Where: `src/bin/nool/repo.rs` `init`, `tracked_in_cwd`.

Problem: `init` writes `store` before `root`; `tracked_in_cwd` turns a stray `.nool` entry into file name `""`. Unverified whether the atomic-save and `safe_key` changes covered these.

Fix: check; fix or drop.

### 59. No lock on `.nool/store` or sidecars — OPEN

Where: `src/bin/nool/repo.rs` `Repo::load` … `save`; `src/bin/nool/sidecar.rs` `load_seq` … `store_seq`.

Problem: two concurrent invocations both load, and the last save wins. A merge/apply whose save is lost has already rewritten the working tree, so the next commit records the merged text as local inserts — duplicated on the next merge with that peer. (2026-09-29 nool review.)

Fix: an exclusive lock file (`.nool/lock`, `<file>.nool.lock`) held from load to save.

### 60. `status` / bare `commit` scan the whole tree with no ignore list — OPEN

Where: `src/bin/nool/repo.rs` `scan_untracked`, `stage`.

Problem: the scan descends into `target/`, `node_modules/` and the like; with any tracked file missing, `stage` diffs it against every untracked file for move detection. A code checkout is read in full on every `status`. (2026-09-29 nool review.)

Fix: an ignore file (`.noolignore`), or at least skip common build directories.

## Basecamp

### 33. Concurrent editing of one focused block corrupts positions — OPEN

Where: `basecamp/ui/src/HashwebView.qml` ~1936-1952, `basecamp/rust-lib/src/cursor.rs`.

Problem: the QML ignores authority while `localEdits > 0`, which never resets while a peer is also editing. "hello": A types at the end while B inserts "XYZ" at 0 → `text_insert(5,"!")` → "XYZhe!llo". `cursor.rs` exists for this and is unused.

Fix: route typing through `cursor_insert` / `backspace` / `delete`, or apply authority with id-based caret restore.

### 34. Artifact and snapshot delivery gaps — OPEN

Where: `basecamp/rust-lib/src/module.rs` ~343-351, `bridge.rs` ~171-198, `space.rs` ~195-209.

Problem: a lost mid-size artifact frame is never retransmitted; once a space's ops snapshot exceeds the 120 KB frame cap, anti-entropy stops and a fresh peer never bootstraps (visible in status, not fixed). Overlaps item 17.

Fix: chunk snapshots; retransmit or lazily fetch artifacts on `Read::One(id)` misses.

### 35. Persist worker holds the global lock across encode + disk I/O — OPEN

Where: `basecamp/rust-lib/src/space.rs` ~148-154.

Fix: encode under the lock, write outside it.

### 36. hashseq pin is far behind HEAD — OPEN

Where: `basecamp/rust-lib/Cargo.toml:25`.

Problem: pinned to `5d3bf5b`. When the pin moves: `mark_range` / `move_element` / `unmark_range` return `Result<HashNode, HashNode>` (breaks `lib.rs:611,631,649,466`), `doc.rs:38-56` should use `anchor_id_at`, module-side artifact tracking should switch to `web.take_new_artifacts()`, and `enable_outbox` / `take_deltas` are gone: `SpaceState` must own a `hashseq::HashWebClock` for the topic: `deltas_for(&clock)` then `clock = web.clock()` in `drain_deltas`, `clock = other.clock()` before merging a snapshot in `merge_frame`; the `recent_sent` echo filter and the "outbox" comments in `space.rs` / `cursor.rs` go with it. The local `file://` git source must become the github URL once master is pushed (comment at `:22`).

Fix: bump the pin in one change with those adaptations; rebuild via the lgx-portable variant and restart the app (factory dylib).

## sync-server

### 37. No auth on `/sync` and `/artifact` — DECISION

Where: `sync-server/src/main.rs`.

Problem: anyone reaching the port can write. Caps exist (4 MiB ws message, 2 MiB artifact, `HASHWEB_MAX_ARTIFACT_BYTES` 512 MiB, `HASHWEB_MAX_OBJECTS` 200k, 256 new objects per delta). Prod is on a public IP.

Fix: shared token, origin allowlist, or accept open-write for the devnet.
