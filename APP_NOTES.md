# App notes — building on HashWeb

A running journal of friction, gaps, and confirmations encountered while
building real applications on the system. Each entry names the app-layer
problem, what the system made easy or hard, and what (if anything) that
suggests for the design. This is deliberately a *feedback* document: entries
here graduate into spec changes or get closed with a "working as intended."

App #1: **kb** — a Notion-shaped knowledge base (`web/kb.html` + `web/kb.js`,
bindings in `src/wasm.rs::WasmHashWeb`). Object model: a workspace kv at a
well-known origin; pages as kv objects; bodies as text seqs; nesting and
bodies wired by ref-valued keys. Sync: whole-store canonical snapshots over
BroadcastChannel/localStorage.

---

## 1. You cannot open an object from its object id (2026-07-03)

`object_id(kind ‖ origin)` is one-way: a link that carries an *object id*
names an object no replica can ever instantiate without separately learning
its origin. The app therefore stores **origins, not object ids**, in every
ref-valued key (`page:*`, `body`), and derives the object id at open time.

Consequence for conventions: "link to an existing object" (transclusion) and
"here is an object you can open" (composition) are *different value types*
even though both are 32-byte ids. The kb only needs the second. A transclusion
feature would need to decide whether links carry origins (openable, but then
any reader can also mint puts into the object — no capability distinction) or
object ids (pure names, unopenable without a side channel).

**Feedback**: possibly fine — this asymmetry may even be a feature (an object
id is a *reference*, an origin is a *capability to instantiate*). Worth one
line in HASHWEB_SPEC so app authors don't discover it by surprise.

## 2. The op-id weld vs. overwritable heads (2026-07-03)

The spec's composition convention ("open the child at your parent op's id")
welds the child into the parent's closure. Building the app surfaced the
hazard: if the child's origin is *the current head* of a key, any later put
on that key changes the head — and the convention's input with it. The weld
wants the op id of a *specific historical put*, but `read`/`heads` surface
only live heads; an overwrite (buggy or malicious) hides the creating op
from the read path entirely, making the child undiscoverable-by-walk while
its ops still verify.

The kb sidesteps this: refs carry **freshly minted random origins as
values** (stable under overwrite, conflict = two refs surfaced by MVR), at
the cost of the weld — kb children are causally disconnected roots.

**Feedback**: the two halves of the old creation semantics pulled apart
cleanly (identity+weld vs. discovery), but the *robust* version of the weld
convention needs one of:
  - a way to enumerate all puts ever made on a key (not just heads) — the
    data is in the DAG; the read surface hides it;
  - or a "write-once key" idiom apps can rely on;
  - or blessing the value-carried-origin convention as the normative one and
    accepting that welds are opt-in for apps that never overwrite.

## 3. Open-on-discovery is real app code, and it's fine (2026-07-03)

The app-level birth walk (`openPagesUnder`: read keys → refs → `createSeq`/
`createKv`, idempotent, cycle-guarded) replaced the deleted store-level
creation semantics in ~20 lines, runs on every render, and needed no new
system surface. Snapshot sync makes it almost vestigial — decoded snapshots
arrive with objects materialized — but it is load-bearing for any partial
sync, and it is exactly where a future "which objects does this document
comprise" protocol hook would land.

**Feedback**: confirms the deletion. The store never interprets values and
nothing was missed in practice.

## 4. Canonical bytes give sync a termination test for free (2026-07-03)

Tab-to-tab sync is: merge theirs, re-encode, re-broadcast **only if my bytes
differ from what I received**. "Equal op sets ⟺ identical snapshots" turns
convergence detection into `memcmp` — no version vectors, no dirty flags, no
op counting. The anti-entropy ping-pong provably quiesces at byte equality.

**Feedback**: working as intended, and better than expected — canonical
encoding is a *sync primitive*, not just a storage nicety. Worth noting in
ENCODING_SPEC's motivation list.

## 5. Every app will re-write the same five wrappers (2026-07-03)

Keeping HashWeb free of authoring/read wrappers is right for the Rust core,
but the FFI boundary immediately re-grew them: `WasmHashWeb` is precisely
`putString`/`putRef`/`readKey`/`text`/`textInsert`+`textRemove` over
`seq_mut`/`kv_mut`, plus a value vocabulary (string keys, string-or-ref
values). The Rust tests carry the same five as local helpers.

**Feedback**: no store change wanted. But an official *app-layer* crate (or
a blessed `helpers` module) would stop N apps from re-deriving the same
conventions with N subtle disagreements — especially `readKey`'s
string/ref/pending/deleted classification, which encodes real semantics
(tombstone filtering, unresolvable-artifact vs. raw-id ambiguity).

## 6. A raw-id value and a missing artifact are indistinguishable (2026-07-03)

`readKey` cannot tell "this value is a link/origin (raw id, never had
bytes)" from "this value is an artifact whose bytes haven't arrived" — both
are unresolvable ids. The kb disambiguates by key convention (`body` and
`page:*` are always refs), which works but is invisible to generic tooling
(a debugger/inspector cannot classify values without the app's schema).

**Feedback**: known and accepted at the spec level (values are just ids;
HASHWEB_SPEC's pending/unavailable state). If it starts hurting, the cheap
fix is an app-level tag inside the value artifact, not a system change.
Revisit if a generic inspector gets built.

## 7. MVR conflicts surfaced trivially in the UI (2026-07-03)

Concurrent title edits → `readKey` returns `conflict` with both values → the
UI shows both and lets the user click one; the resolving put names all heads
and dominates. Zero app-side merge logic, no winner ever silently picked.
Body-creation races (two replicas mint different body origins) surface the
same way; the kb renders the smallest origin deterministically and keeps the
other reachable.

**Feedback**: the freeze/MVR design translates directly into honest UI.
Confirms Law II's shape end to end.

## 8. Deltas need the envelope on the wire (2026-07-03)

v1 syncs whole snapshots (fine at kb scale — hundreds of KB before it
matters). The moment we want per-op sync, the transport unit must be the
**envelope** `obj_id ‖ op`, not the bare op — `applyTo` already demands it.
So the local-edit API for delta-sync apps has to return *(object, op bytes)*
pairs; today's mutating calls (`insert_batch` etc.) return nothing usable
for broadcast. The seq-only wasm API solved this with the Run/`encodeOp`
model; the store-level equivalent ("give me everything I authored since the
last flush, enveloped") does not exist yet.

**Feedback**: next system-facing work item if the app grows real-time sync:
an authored-ops outbox on HashWeb (or per object), yielding enveloped bytes
in apply order. This also re-raises APP layering: the outbox is exactly the
"delta" concept ENCODING_SPEC scopes out of snapshots.

## 9. Formatting wanted to be marks, not markup (2026-07-03)

First cut implemented code blocks, inline code, math, equation blocks, and
tables as in-band markup (fences, backticks, `$…$`). Redirected: formatting
belongs to the **marks projection**. The rewrite (mark kinds `code`, `math`,
`codeblock` — value carries the language — and `eqblock`; wasm surface
`markRange`/`unmarkRange`/`markedSpans`) confirmed why:

- **Fences shatter; marks cannot.** A concurrent edit that crosses a fence
  boundary silently reflows the whole document's rendering. Mark regions are
  anchored to elements: verified through the wasm layer that a concurrent
  insert *inside* a code-block region merges into the block
  (`let x = 5;` + fork inserting `yz` → `let xyz = 5;`, still marked, still
  `rust`).
- **Regional semantics did the right thing unprompted**: typing inside a
  marked region inherits the mark; expanding-end anchors (`Before(next)`)
  make edge-typing grow code blocks naturally; unmark is a tombstone-valued
  mark and partial unmark just works.
- **Mark values carry structure**: the code block's language is the mark
  value — no syntax, MVR-surfaced on conflict like everything else.

What stayed markup: **tables and headings** — structure, not formatting;
marks have nothing to say about rows and cells. (The structural answer would
be table-as-object — a seq of row objects — which is the blocks-as-objects
iteration, not this one.)

Friction found:
- A `<textarea>` cannot *display* marks, so editing is blind to formatting
  until the VIEW toggle. A real editor needs decorations driven by
  `markedSpans` — the data is there; the widget isn't.
- `marked_spans` coalesces on raw live sets, so a tombstone-suppressed
  region still splits spans (renders identically; cosmetic).
- Block-shaped marks (codeblock/eqblock) want line granularity; the app
  expands selections to line boundaries before marking. The system is
  character-ranged and agnostic — right call, the granularity policy is
  the app's.

**Feedback**: marks carried all four formatting features with zero system
changes — the projection earned its complexity budget. The missing piece is
editor-side: a decorations-capable text widget over `markedSpans`.

## 10. Tables as a hashseq of hashseqs (2026-07-03)

Pipe-markup tables replaced by structure: a table is a seq of row refs,
each row a seq of cell refs, each cell a text seq — embedded in the body as
a link atom (payload = table origin, rendered from U+FFFC via `payloadAt`).
Rows insert/order like any seq elements; cells edit like any text; all of
it merges per the existing seq semantics with zero new system surface.

Two real findings:

- **Column identity does not exist.** A "column" is only an index into each
  independent row seq, so two replicas concurrently adding columns can
  interleave differently per row after merge — a structurally misaligned
  table, each row individually correct. The honest fix is a column-list
  object plus cells keyed by (row id, column id) — i.e. the database model,
  where a kv per row keyed by column origin replaces positional cells. This
  is the app-schema layer of the same lesson as marks-vs-markup: *alignment
  is a constraint, and constraints need identity to attach to*.
- **Embeds need the atom's visible index.** Rendering resolves U+FFFC
  placeholders via `payloadAt(body, idx)`, which means the renderer must
  thread absolute positions through span chunking. Worked fine; an
  `atomsOf(obj) -> [(idx, payload)]` read would be marginally nicer.

**Feedback**: no system change needed for doc-tables; the column-identity
problem is worth a paragraph in HETEROGENEITY.md as the canonical example
of when positional composition must graduate to keyed composition.

## 11. Comments are marks with per-comment kinds (2026-07-03)

"Highlight a span and comment on it" is exactly a mark — value = the
comment text, region = the highlight, regional semantics keep it attached
under concurrent edits, tombstone = resolve. One catch: `mark_range`'s
overwrites hygiene names every intersecting same-kind mark, which is the
right policy for formatting (bold over bold replaces) and the wrong one for
annotations (a new comment must not suppress an overlapping older one). The
app mints a **fresh kind per comment** (`comment:<tag>`), giving each its
own register: overlapping comments coexist, the overlap region carries
both, resolving one leaves the other (verified through the wasm layer).

**Feedback**: the *model* already supports coexisting same-kind marks (MVR
per (element, kind)); only the authoring helper bakes in replace-on-overlap.
Worth a line in MARKS.md naming the two idioms: formatting = shared kind
(overwrite hygiene), annotation = minted kind (coexistence). Slight wrinkle:
kind-per-comment means "all comments" is a prefix scan over kind strings —
fine for an app, invisible to generic tooling.
