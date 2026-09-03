# Bug scan findings, 2026-09-03

Full-tree scan, 8 reviewer agents in parallel, one per area. Yesterday's review
(REVIEW_FINDINGS.md) covered hashseq/encoding only; this pass re-read those and
added hashkv/hashweb/value, wasm + web JS, the nool CLI, the Basecamp module + UI,
and sync-server. No code was changed. Scratch tests were deleted after each run.

Status legend: OPEN / RESOLVED (fill in as fixes land). Confidence: CONFIRMED
(test or repro run), HIGH (code read end to end), PLAUSIBLE.

## Fix pass, 2026-09-03 (same day)

Five fixer agents, one per area, working on disjoint files. Verified after the
pass: `cargo test` green (lib 230, nool 13, grammar_vectors 2), `cargo build
--release` green, sync-server builds, `wasm-pack build` regenerated web/pkg and
the kb.js wasm-backed check script passes, `cargo check --offline` in
basecamp/rust-lib passes. Basecamp C++/QML edits are reviewed by reading only
(no Qt toolchain here).

Still OPEN after the pass, by decision or scope:

- sync-server auth on `/sync` and `/artifact` (top-10 #3): caps landed, auth
  is David's call. Caps: 4 MiB ws message, 2 MiB artifact, `HASHWEB_MAX_ARTIFACT_BYTES`
  (512 MiB), `HASHWEB_MAX_OBJECTS` (200k), 256 new objects per delta.
- hashkv #6 spurious tombstone conflicts: spec decision, untouched.
- Basecamp #6 (concurrent editing of one focused block should use the cursor
  API), #11 (persist worker holds the global lock), #13 (hashseq pin at 5d3bf5b;
  when it moves, `mark_range`/`move_element` Results and the module-side
  artifact tracking should switch to `web.take_new_artifacts()`).
- HashKv-in-HashWeb residual: a `kv_mut().put_ids(k, vid)` whose value was
  minted only via `web.provide_value` stays pending in the authoring replica's
  own kv view (`web.resolve` sees it; every decoding/merging peer hydrates it).
  The wasm/test `or_else(web.resolve)` fallbacks stay for that case.
- Perf note, not a bug: store-parked envelope dedup and `knows()` scan the
  parked Vec linearly, so N parked envelopes on one unopened object cost O(N²).
- nool: `nool status | head` shows a pre-existing broken-pipe panic from
  `println!`; top-level USAGE text does not mention the new `--force` flags.
- kb.js paths not verifiable without a browser: `offsetOfPoint`/`locateOffset`
  DOM mapping, IME event ordering, sidebar drag, CodeMirror listener, the
  title-switch race.

What landed per area (details in the sections below, marked RESOLVED):

- Library: parked dedup + std HashMap; kv view hydration on apply/decode/merge
  plus kv-level `new_artifacts` drained by `take_new_artifacts`; zigzag and
  varint 10th-byte/non-minimal rejection; `DecodeError::NoRefs` via
  `validate_node` at all five sites; `WasmRun.newAfter/newBefore` strip the
  anchor; `mark_range` returns Err on non-glue anchors; `saturating_add` in
  `make_remove_batch`. 9 new tests.
- web: `mergeEncoded` fix; code-point conversion layer (`cpLen/cpIndex/utf16Index/cpSlice`)
  applied at every wasm boundary in kb.js and index.js; title debounce captures
  its page; drag resolves by origin at drop; sync try/catch; IME-deferred render.
- nool: `safe_key` choke point + status report; untracked-clobber refusal with
  `--force`; sidecar apply replays orphans; `rm` refuses dirty; delta varint
  checked_add; symlink-aware `rel`; move threshold 16 shared chars;
  `require_clean` propagates errors + revert hint; diff trailing newline;
  symlinks skipped in scan; atomic pid-suffixed fsync'd saves. 3 new tests.
- sync-server: caps above; corrupt file set aside; persist Result re-dirties;
  SIGTERM/ctrl-c flush; artifacts flushed immediately; panic hook aborts;
  fsync chain; broadcast 1024 + cached wire snapshot via `wire_stale`.
- Basecamp: artifact frames published before deltas; `provide_bytes` refuses
  over-cap artifacts and UI cap lowered to match; drop/failure counters in
  status; UTF-16↔scalar conversion in HashwebBackend at 13 sites; forced
  `blockUpdated` resets the delegate; presence bounded; Instant-based hello;
  echo filter for redundant emits; corrupt state set aside; init lock narrowed.

## Top 10 by impact

| # | Finding | Where | Conf | Status |
|---|---------|-------|------|--------|
| 1 | nool: path traversal from merged/applied registry keys (write/delete outside repo, `.nool/` clobber) | `src/bin/nool/repo.rs:162`, `:673-699`, `:726-732` | CONFIRMED | RESOLVED |
| 2 | UTF-16 code-unit offsets handed to char-indexed APIs (kb.js, index.js, Basecamp UI) | `web/kb.js` many sites, `web/index.js:29-41`, `basecamp/ui/src/HashwebBackend.cpp:616-641`, `HashwebView.qml:2159-2196` | CONFIRMED | RESOLVED |
| 3 | sync-server: unauthenticated, uncapped writes (64 MiB frames, artifacts stored forever, full rewrite every 3s) | `sync-server/src/main.rs:164-231` | CONFIRMED | PARTIAL (caps, no auth) |
| 4 | HashKv inside HashWeb: split artifact store; `get` returns None after merge/decode; `kv_mut().put` never ships values | `src/hashkv.rs:111-119`, `src/hashweb.rs:315-318`, `src/encoding.rs:2088-2103` | CONFIRMED | RESOLVED (residual noted) |
| 5 | HashWeb store-parked envelopes never deduped (merge not idempotent, unbounded growth, strict accepts doubled snapshot) | `src/hashweb.rs:247`, `:326-330` | CONFIRMED | RESOLVED |
| 6 | Basecamp module never pushes small value artifacts; >120KB frames silently dropped (images, snapshots) | `basecamp/rust-lib/src/lib.rs:512-647`, `module.rs:343-351`, `bridge.rs:171-198` | CONFIRMED / HIGH | RESOLVED |
| 7 | Basecamp UI: rejected `applyBlockEdit` leaves editor permanently desynced; concurrent edits of one block corrupt positions | `HashwebView.qml:2193-2196`, `HashwebBackend.cpp:637-641` | HIGH | RESOLVED |
| 8 | Decoders accept zero-ref nodes as canonical; varint / zigzag 10th-byte truncation and non-minimal forms accepted | `src/encoding.rs:161`, `:627`, `:1783`, `:2005`; `src/value.rs:187-206` | CONFIRMED | RESOLVED |
| 9 | wasm `WasmRun.newAfter/newBefore` accept the anchor in extraDeps: applies locally, rejected by every peer | `src/wasm.rs:118-144` | CONFIRMED | RESOLVED |
| 10 | web/index.js calls `merge_encoded` (export is `mergeEncoded`): demo SYNC never merges | `web/index.js:96,104` | CONFIRMED | RESOLVED |

## Core CRDT (hashseq.rs, run_index.rs, run.rs, bitset.rs, placement.rs, hashseq_iter.rs, hash_node.rs)

No new correctness bugs. Both reviewers read the full non-test ranges and fuzzed:
700 three-replica worlds (inserts, removes incl. of moved elements, honest and
adversarial raw moves with junk overwrites/op anchors, splice-anchored inserts,
marks on elements/ops, unmarks, atoms, places), each replayed under 12 random
delivery orders, checking text, `id_at`, `position_of`, `marked_spans`, per-element
`marks_at` vs sweep, canonical encoding, zero orphans; debug and release. Plus a
6-seed two-replica fuzz over long bursts (Large bitmaps), `remove_batch`,
`move_element`, `mark_range`, bidirectional merge, checking treap/position
invariants per step.

Low items only:

- `mark_range` / `unmark_range` `expect("anchor must be applied")` panics on a
  non-glue anchor id (remove/place/mark op id, or unknown id). Rust API only;
  wasm builds anchors via `anchor_id_at`. Fix: return Err. `src/hashseq.rs:1966-1967`
- `make_remove_batch` `idx + amount` unchecked (debug overflow for huge amount).
  Fix: `saturating_add`. `src/hashseq.rs:1008`
- `PackedLoc::pack` 29-bit pos only debug-asserted; needs 512MB of run text.
  `src/hashseq.rs:116-117`

Candidates traced and refuted (for the record): remove-moved then register
churn re-rendering a tombstone; splice/moved fragment kind vs `index_target`
predicate; stale `ElemRef` after `split_run`; `after_sibling_target` before
split; Bits Small/Large push guard and 64-shift; iterator run-rest drop (C3);
`cmp_sweep` LCA branches; `index_target(ORIGIN, before=true)`; `HashNode::id`
fast path vs general path; `Run::from_text` empty/duplicate interior dep set
(strict re-encode rejects).

## encoding.rs

1. **Zero-ref nodes pass strict decode.** GRAMMAR_SPEC Part A says `ref_count = 0`
   is malformed; no decoder arm checks it. BLK_REMOVE_OTHER (`:1783`), kv
   Put/Place (`:2005`), `decode_node_with` for trailing/orphan sections (`:627`).
   Repro: 35-byte empty-seq snapshot + block `05 00 00` → `decode_hashseq_strict`
   Ok with an applied `Remove({})` that becomes a permanent tip and re-encodes
   byte-identically, so it ships to peers. CONFIRMED. Fix: extend
   `reject_redundant_pins` into `validate_node` returning `DecodeError::NoRefs`
   when `iter_refs()` is empty (covers all five call sites; run blocks are immune).
2. **HashWeb trailing section not deduped** (same root as HashWeb #5 below):
   patch a snapshot's parked count to 2 and repeat the envelope → strict Ok,
   `orphans().count() == 2`. CONFIRMED. `:2198` → `src/hashweb.rs:247`.
3. **`decode_varint` truncates the 10th byte** (`:161-167`): the `shift >= 64`
   check runs after OR-ing the byte at shift 63, so `[0x80×9, 0x7f]` decodes as
   `1<<63`; non-minimal forms (`80 00` = 0) accepted everywhere. No unchecked
   allocation is fed by it any more, and strict re-encode rejects, but every
   transport-mode decoder accepts many byte strings per value. CONFIRMED.
   Fix: reject `shift == 63 && byte > 1` and a terminating `0x00` at `pos > 1`.
4. **Inline payload rule narrower than spec** (`:268-306`, PLAUSIBLE): spec says
   inline `0x00 len bytes` is mandatory for any artifact ≤ 32 B; encoder inlines
   only `Payload::Char`; decoder drops the bytes of an inline non-char artifact.
   Not reachable from this crate's encoder. Amend spec or thread a value sink.

Residual from yesterday (#5 interim): by-id char payloads for non-ASCII chars
still have two strict-canonical forms (`'é'` as `0x01 id` renders U+FFFC).

Clean: every wire-driven allocation bounded by `Cursor::count`/`take` or an
existing run length; remove-span and Other-range bounds before iteration;
rank-space fill order matches encoder; dict entry 0 = origin both sides;
round-trips strict-clean for non-ASCII, interior deps across a delete,
Before-anchored head, atoms mid-run, move, mark, place, multi-target remove,
nested HashWeb with all Value kinds and a parked envelope; Kahn order cannot
deadlock; UTF-8 rejects surrogates/overlongs.

## hashkv.rs, hashweb.rs, value.rs, delivery.rs

1. **HashWeb::merge / apply_to duplicate store-parked envelopes.** `parked.entry(obj).or_default().push(..)`
   has no dedup (`src/hashweb.rs:247`); merge re-delivers `other.parked` through
   it (`:326-330`). `m.merge(m.clone())` doubles orphans; encoded bytes change;
   an echoing relay grows `parked` forever for any never-opened object. wasm
   `apply_to` (`src/wasm.rs:1309`) and `decode_hashweb` do not pre-check `knows`;
   only `encoding::apply_delta` does. CONFIRMED. Fix: skip when
   `self.knows(obj, &id)`, or key `parked` values by node id.
2. **Two artifact stores per kv.** `HashWeb::merge` (`:315-318`) and
   `decode_hashweb` (`src/encoding.rs:2088-2103`, `2164-2170`) union kv-local
   artifacts into `web.values` only; `HashKv::get/resolve` (`src/hashkv.rs:119,208`)
   read `kv.values` only. `decode_hashweb(encode_hashweb(&doc)).kv(&root).get("color")`
   → None while `web.resolve(vid)` → Some. `HashKv::merge` (`:322`) does union
   into the kv store, so the two merge paths disagree. Test helper and wasm paper
   over it with `or_else`. CONFIRMED.
3. **`kv_mut().put`/`del` mint artifacts that never ship.** `src/hashkv.rs:111-115`
   writes `kv.values`, never `web.new_artifacts` (`src/hashweb.rs:202-212`).
   Peer applying the delta sees `Read::One(id)` with no resolvable value until a
   full snapshot. wasm `put_string` works around it via `provide_value`. CONFIRMED.
   Fix for 2+3: give the kv a handle to the shared store, or add
   `HashWeb::put(obj, key, value)` and mark `HashKv::put` standalone-only.
4. **`Value::decode` accepts non-canonical/overflowing zigzag varint**
   (`src/value.rs:187-206`): `[VK_INT, 0xFF×9, 0x7F]` decodes to `Int(i64::MIN)`
   whose canonical bytes differ → one Value, two artifact ids;
   `provide_value(resolve(vid))` mints a second id. CONFIRMED.
5. **`HashWeb::parked` is `FxHashMap` keyed by attacker-chosen ids** (`:41`);
   `delivery.rs:22-24` deliberately uses std HashMap for the same threat. HIGH.
6. **Spurious kv conflicts between identical heads** (`src/hashkv.rs:183-197`):
   two concurrent `del(k)` → `Conflict([TOMBSTONE, TOMBSTONE])`, `get` None,
   `keys()` lists the key as live. Spec-conformant but useless to the app.
   PLAUSIBLE / design decision. Fix: dedup value ids in `read_id`.
7. Low: `HashKv::merge` panics on origin mismatch (`:317`); `Value::decode`
   returns None for unknown kinds so `resolve` conflates unknown with pending;
   doc/test drift on whether a link payload is the origin or the object id
   (`hashweb.rs:8` vs tests).

Clean: kv register semantics (supersession, MVR, out-of-order park/wake,
foreign overwrites ignored, Place in kv, tips-only PartialEq); delivery
park/unpark/wake/gate/quarantine/`into_held`; HashWeb object-id derivation,
idempotent create, adoption-then-wake order; Value char/bool/tombstone/f64/string
decoders canonical; `char_artifact` sizing and cache.

## wasm.rs and web/ JS

1. **`web/index.js:96,104` calls `peer.merge_encoded()`**; the export is
   `mergeEncoded`. SYNC → TypeError → "Merge into A failed". CONFIRMED.
2. **kb.js hands UTF-16 offsets to char-indexed wasm methods.** Sites:
   `applyDiffAt`/`applyDiff` (888-1015), `offsetOfPoint` (1561, feeds every
   caret/selection offset), `flagsAt` (3028), `regionExtent` (1672),
   `mergeBlockInto` (2156), `repaintEmbedsOf` (576), `embedSig` (2553),
   `captureEditState` (2729), `collectComments` (2782), `replaceThreadMessage`
   (2864), Enter-split (2330-2345). `renderBody`/`renderEditableInto` use
   `[...text].length` (code points), so the file is internally inconsistent.
   Scenarios: Backspace-merge of "😀a￼b" replaces the embed with ' ' then throws
   "mark range out of bounds" uncaught, leaving the source block in place (second
   Backspace duplicates); replacing 😀 with 😁 splits the pair → seq holds "😀�";
   any typing right of an astral char lands one slot too far. CONFIRMED.
   Fix: one conversion layer at the boundary (`[...s.slice(0,i)].length`, diff
   over `[...s]` as app.js:60-72 already does), or make the wasm API take UTF-16.
3. **`web/index.js:29-41` same mismatch with CodeMirror positions.** HIGH.
4. **`web/kb.js:3167-3173` title debounce writes to whichever page is `current`
   when the timer fires** (edit title, click another page within 350 ms →
   renames the wrong page). HIGH. Fix: capture the page in the handler; flush on
   page switch.
5. **`src/wasm.rs:118-144` `newAfter/newBefore` accept extraDeps containing the
   anchor**; `Run::new` does not normalize. Release: applied locally, `encodeOp()`
   rejected by every peer (`RunError::RedundantDep`) → silent divergence; debug
   traps at `hash_node.rs:292`. CONFIRMED (release test). Fix: strip the anchor or
   return a JS error.
6. `web/kb.js:1363-1372` sidebar `treeDrag` captures `meta.idx`; a remote render
   mid-drag rebuilds the tree, `dragend` may never fire → next drop uses a stale
   index. PLAUSIBLE. Fix: resolve source by origin at drop time; clear drag state
   in `render()`.
7. `web/kb.js:605-615, 642`: `mergeEncoded(theirs)` on the snapshot path is
   unguarded; a delta failing midway has applied some nodes but `fresh` stays 0 →
   no render/persist. PLAUSIBLE.
8. `web/kb.js:2496` IME composition discarded by a remote-delta render. PLAUSIBLE.
9. Nit: `src/wasm.rs:1050-1140` mark/unmark mint kind/value artifacts before
   validating the range, so an invalid call still pushes artifacts. Harmless.

Clean: every index-taking wasm method clamps or returns undefined; hex parsing;
`seq_move` slot arithmetic matches kb.js drop logic; `apply_delta` rejects Run
frames, dedups via `knows`, parks out-of-order; outboxes attached on every
create/adopt/decode path; `take_new_artifacts` records only local mints; HWB2
magic cannot collide with 0xDE/0xAF; delta echo skip; artifacts sent before the
ops naming them; reconnect snapshot covers dropped deltas; caret restore by
element id; handles freed in app.js.

## nool CLI (src/bin/nool)

1. **Path traversal via registry keys** (`repo.rs:162` `abs = root.join(rel)`,
   used by `sync_working_tree` `:673-699` and `write_working` `:726-732`). Only
   CLI args go through `rel()`; merged/applied keys are unvalidated. Repro
   (scratchpad `craft` tool planted keys into a `.nool/store`): keys
   `../outside/rel_pwned.txt` and an absolute path were written outside the repo
   via both `nool merge ../b` and `nool apply evil.delta`; a `.nool/root` key
   overwrote nool's own state ("Odd number of digits" on next load); a tracked
   absolute key was `remove_file`d on the next merge. CONFIRMED. Fix: in
   `tracked_files` reject/skip empty, absolute, `..`/`.`, or `.nool`-prefixed
   keys; flag rejected entries in status.
2. **`merge`/`apply` silently overwrite untracked working files** (`:688-694`;
   `require_clean` `:648-668` checks tracked paths only). CONFIRMED. Fix: refuse
   when a path in `after − before` exists on disk with different content.
3. **Sidecar `apply` drops parked ops but reports them applied**
   (`sidecar.rs:293-295` rebuilds from `all_nodes()`, which excludes orphans).
   Applying d2 (depends on d1) printed "applied 4 new op(s)", content unchanged;
   d1 afterwards gave `base\none` with d2 gone; re-applying d2 delivered 4 again.
   Repo mode is correct. CONFIRMED. Fix: also replay `seq.orphans()`; distinguish
   parked from applied.
4. **`nool rm` deletes uncommitted edits with no warning** (`repo.rs:490-506`).
   CONFIRMED. Fix: refuse when dirty unless `--force`.
5. **Overflow panic on a crafted delta** (`delta.rs:104` `pos + len`, `:130`).
   Debug: "attempt to add with overflow". CONFIRMED. Fix: `checked_add` / `get`.
6. **Absolute paths through symlinks rejected** (`repo.rs:129-160` lexical
   normalize vs physical cwd): in `/tmp/nool_t5`, `nool track /tmp/nool_t5/f.md`
   fails "outside the repo at /private/tmp/nool_t5". CONFIRMED (macOS).
7. **Move detection fires on trivially similar short files** (`repo.rs:94,362-396`;
   `similarity("x\n","y\n") = 0.5`); bare `commit` records the move. CONFIRMED.
8. **Store saved before the tree is synced; read errors count as clean**
   (`repo.rs:553-555, 636-643`; `require_clean` `.ok()?` at `:653-655`). With a
   tracked file replaced by a directory, merge saved the store, updated one file,
   failed on the other; after restoring, status shows the reversal as a local
   modification. CONFIRMED.
9. **`nool diff` empty for trailing-newline / CRLF-only changes**
   (`sidecar.rs:176-178`, `repo.rs:482-485` use `.lines()`). CONFIRMED.
10. **`scan_untracked` follows directory symlinks** (`repo.rs:416`): `ln -s . loop`
    → ~32 nested spurious untracked lines; a symlink to `~` scans the home tree.
    CONFIRMED.
11. Lower: `store_seq`/`save` rename without fsync and use a fixed `.tmp` name
    (concurrent invocations clobber); `init` writes `store` before `root`;
    `tracked_in_cwd` turns a stray `.nool` entry into file name `""`.

Clean: Myers forward/backtrack and `coalesce`, prefix/suffix trimming, coarse
fallback; char-level indices match HashSeq; `apply_edits` bookkeeping incl.
non-ASCII/empty/CRLF; `tracked_files` heads/read_id zip; origin checks in
sidecar merge/delta/apply; repo-mode replay idempotent and unparks; doc ids are
random (not content-derived, contrary to the memory note).

## Basecamp module (basecamp/rust-lib) and UI (basecamp/ui)

1. **UTF-16 vs scalar index mismatch across the UI↔hub seam.** Hub indexes by
   chars (`lib.rs:306,332,610,630,648`); `HashwebBackend.cpp:616-641`
   (applyBlockEdit), `:740`, `:1590` (mark offsets), `HashwebView.qml:2159-2196`
   use UTF-16. Type "😀" then "a": second insert is `text_insert(idx=2)` on a
   len-1 seq → Err, every later keystroke in the block rejected or shifted.
   CONFIRMED. Fix: convert in HashwebBackend (`toUcs4().size()` for every
   pos/len; map hub offsets back), or make hub methods UTF-16.
2. **Rejected/partial `applyBlockEdit` desyncs the focused editor permanently.**
   QML sets `shadow = newText; localEdits++` before the call (`:2193-2196`); on
   failure `rebuildBlocks()` emits `blockUpdated` only when hub text differs
   (`:376-384`), which it doesn't, so `localEdits` never resets. HIGH. Fix: emit a
   forced `blockUpdated` on failure; delegate resets shadow/localEdits.
3. **Small value artifacts never ride 0xAF frames.** `lib.rs:512-513,531,607-608,
   627-628,647` call `provide_value`; nothing drains `take_new_artifacts` in
   `module::author`/`publish_authored` (wasm.rs:1360-1375 and kb.js:478 do). Peer
   shows "Untitled", no list markers, hex mark kinds until the 20s snapshot hello;
   `new_artifacts` grows forever. CONFIRMED. Fix: drain and publish
   `[ARTIFACT_TAG] ‖ bytes` before the delta.
4. **Artifact sync gaps**: `module.rs:49,343-351` drops frames >120KB ("kept
   local"); UI accepts up to 1.5MB (`HashwebBackend.cpp:865`); hello snapshots
   exclude artifacts >1KB. A 300KB PNG renders `pendingEmbed` forever on peers
   (polling every 1.5s); a lost 50KB frame is never retransmitted. HIGH.
5. **Silent sync cliff once a space's ops snapshot exceeds 120KB**
   (`bridge.rs:171-186,189-198`, `space.rs:195-209` via `send_to_topic`, eprintln
   only): anti-entropy stops, a fresh peer never bootstraps, `status()` silent.
   HIGH (threshold PLAUSIBLE). Fix: surface in status; chunk snapshots.
6. **Concurrent editing of one block corrupts positions** (`HashwebView.qml:1936-1952`
   ignores authority while `localEdits > 0`, which never resets while a peer is
   also editing). "hello": A types at end while B inserts "XYZ" at 0 →
   `text_insert(5,"!")` → "XYZhe!llo". `cursor.rs` exists for this and is unused.
   HIGH. Fix: route typing through `cursor_insert/backspace/delete`, or apply
   authority with id-based caret restore.
7. `record_presence` (`module.rs:392-406`) unbounded, unauthenticated growth keyed
   by attacker `sid`; pruning only in `presence_json` for the queried space.
   CONFIRMED, low.
8. Hello cadence counts loop iterations not time (`bridge.rs:57-67`): under
   bursts full-snapshot hellos fire every 400 messages. PLAUSIBLE.
9. Redundant `frame` emits for no-op echoes (`space.rs:181-185`, `bridge.rs:126`):
   two UI rebuilds per keystroke. LOW.
10. Undecodable state file silently overwritten on next persist (`space.rs:88-96`). LOW.
11. Persist worker holds the global lock across encode + disk I/O (`space.rs:148-154`). LOW.
12. `init` holds the state mutex during sync SDK calls (`module.rs:105-154`); a
    stale bootstrap callback firing inline would deadlock. PLAUSIBLE, low.
13. `Cargo.toml` pins hashseq to `5d3bf5b` (17 commits behind HEAD). HEAD's
    `Result`-returning `mark_range`/`move_element` break `lib.rs:611,631,649,466`
    when the pin moves; `doc.rs:38-56` should use `anchor_id_at`.

By design but worth stating: frames are acked before the 3s persist and the UI
never calls `shutdown`, so a hub kill loses ≤3s of edits.

Clean: `cargo check --offline` passes; lock discipline (publish/emit outside
`with_state`, shutdown takes state out before joining, no lock across
`send_async`); no network/UI-reachable panics found; replays idempotent;
snapshot quiesce terminates; `leave_space` flush has no race with the persist
worker; structural mutations anchor correctly on winning atoms.

## sync-server

1. **Unauthenticated, unlimited write path** (`main.rs:164-166` no auth on
   `/sync`, `:224-231` artifact ingest, `:205-222` delta ingest). tungstenite
   default 64 MiB/message; every artifact stored forever in `web.values` (no
   cap, no GC; the 1.5MB cap is client-only at `kb.js:3316`), broadcast to all
   peers, and the whole store rewritten every 3s (`:122-126`). `apply_delta`
   calls `create_seq(origin)` for any (kind, origin) → unlimited objects. Prod is
   on a public IP. CONFIRMED. Fix: `ws.max_message_size`, cap artifact and store
   size, reject unknown-object creation or rate-limit, token/origin check.
2. **Undecodable state file silently clobbered on next flush** (`:91-94`,
   `:122-126`). HIGH. Fix: rename to `.corrupt-<ts>` or refuse to start.
3. **Persist failure clears `dirty`** (`:123` vs `:313-318`): disk full → nothing
   retries until the next delta. HIGH. Fix: re-set dirty on error.
4. **No graceful shutdown** (`:161`; tokio `signal` feature enabled, unused).
   SIGTERM drops ≤3s of deltas and any artifact uploaded in that window (artifact
   bytes are pushed once at upload, `kb.js:495-510`) → other clients 404 forever.
   HIGH. Fix: SIGTERM → flush; persist artifacts immediately.
5. **Partial `apply_delta` failure leaves state mutated but not dirty**
   (`:208-216`, `src/encoding.rs:3223-3255`). CONFIRMED by reading. Fix: dirty on
   Err too.
6. Persist/`fresh_bytes` panic kills the persist task permanently and silently
   (`:113-128`; encode `expect`/`unreachable!` at `src/encoding.rs:887-963, 1301,
   1322`); process keeps serving with persistence off. PLAUSIBLE. Fix:
   `catch_unwind` or abort so systemd restarts.
7. `persist` never fsyncs (`:311-320`). PLAUSIBLE.
8. Full-snapshot amplification (`:189-194` Lagged → full re-encode per client;
   `:246-267` each snapshot = decode + merge + 2 encodes under the lock;
   `kb.js:694` every reconnect uploads a full snapshot; `kb.js:641-647` every
   client with undrained edits replies with a full snapshot). Broadcast capacity
   64 (`:102`). PLAUSIBLE. Fix: bigger channel; cache encoded snapshot per
   generation; delta-only reconnect when hello matches.

Clean: hello/subscribe ordering race-free; delta ids recomputed server-side so
ops cannot be spoofed into another object; 1-byte and empty frames safe;
decoder allocation class closed; `/artifact/{id}` validates hex + length;
static files via `ServeDir`; per-connection state dropped on return; client
reconnect backoff single-timer; artifact id verified on GET; snapshot quiesce
terminates.
