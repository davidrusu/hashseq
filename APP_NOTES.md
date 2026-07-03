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

## 12. Blocks + drag-reorder: the Move op earns its keep (2026-07-03)

Pages moved to a block model: the body seq now holds only block refs
(atoms); each block is its own text seq carrying its own marks, comments,
and embeds. Dragging a block emits exactly one op — `seqMove` →
`move_element`, a placement-register Move superseding the heads this
replica sees. Observations:

- **Reorder-vs-edit concurrency is the whole point**: one replica drags a
  block while another types inside it — the text ops target the block
  object, the move targets the body atom; they cannot conflict, and the
  moved block arrives intact. This decomposition (identity-carrying atom +
  content object) is what the Move op's same-container rule was designed
  around, and it composes with zero app-side merge logic.
- **Block granularity is a schema decision with system consequences**:
  per-block seqs mean marks and comments are block-scoped (a comment
  cannot span two blocks), per-block frontiers stay lean, and body-level
  ops are tiny. The costs surface honestly (cross-block selection needs
  app-level composition later).
- **Schema evolution is itself a convergence problem.** v1 bodies were one
  text seq; the app migrates on open (wrap plain runs into blocks). Two
  replicas migrating the same legacy page concurrently would double the
  blocks — deterministic per replica, divergent jointly. Real apps need
  either migration ops with identity (same origin derivation on every
  replica — derive block origins from the content ids instead of random!)
  or version-gated conventions. This deserves ledger status: "how do
  app conventions version?" is the app-layer twin of the system's
  cross-version parking story.

**Feedback**: Move via drag needed one wasm method and ~40 lines of DOM.
The deterministic-migration idea (derive new origins from existing op ids,
so concurrent migrations converge) falls out of the identity design for
free and should be the blessed idiom — worth an APP-conventions section
somewhere once there are two apps.

## 13. Links: names vs capabilities, now visible in the UI (2026-07-03)

Page links are link atoms whose payload is the target page's **object id**
— a pure name. Embeds (tables) carry **origins** — instantiation
capabilities. Note #1's asymmetry became a live UI distinction:

- A link to a page that exists renders as navigation; a link to a page
  that was unlinked from the tree renders as a struck-through name (the
  object may still exist — the link is honest about reachability, and the
  ops behind it still verify). Dangling links are a *rendering state*, not
  an error.
- The renderer must classify a raw 32-byte payload with heuristics: known
  page object id → link; id whose derived seq object exists → table embed;
  otherwise an inert chip. Crucially the unknown case must NOT auto-open —
  `createSeq(payload)` on an arbitrary id is a write that mints a bogus
  object (the first embed renderer did exactly this bug). Classification
  by probing is workable but smells; the clean fix remains a tagged value
  artifact (`link:<id>` vs `embed:<origin>`) — an app-vocabulary change,
  no system change.

**Feedback**: reinforces #1 and #6. If a second app appears, the tagged
value vocabulary should be shared, not re-invented.

## 14. WYSIWYG: the DOM as a view of the seq (2026-07-03)

The textarea editor is gone; blocks are contenteditable divs rendered from
`markedSpans` — formatting visible while typing, closing note #9's gap.
The architecture that made it tractable:

- **The seq stays the source of truth; the DOM is a view.** Every input
  event extracts plain text from the DOM (atomic widgets contribute their
  `data-text`; math widgets carry their TeX source, embeds/links carry
  U+FFFC) and diffs it into seq ops. The browser's own edit is left in
  place during typing; a debounced normalize re-renders from spans and
  restores the caret by text offset, reconciling any styling drift at mark
  edges.
- **Atomic widgets solve the render-vs-source problem.** KaTeX output is
  not text — so math renders as a `contenteditable=false` island whose
  extraction value is the source. Double-click replaces the region's chars
  in place, and the mark's regional points survive the replacement (the
  ghosts hold the region), so re-entered source stays math. Regional mark
  semantics did the heavy lifting again.
- **Newlines as text, never markup**: Enter is intercepted to insert a
  literal `\n` (pre-wrap rendering), so extraction never has to interpret
  `<br>`/`<div>` soup and offsets stay exact.
- Small tricks that mattered: toolbar `mousedown` preventDefault keeps the
  editor's selection alive through button clicks; table cells
  stopPropagation so embed edits never double-apply as block edits; text
  drops into the editor are blocked (they'd splice DOM the extractor
  doesn't own).

Honest gaps: IME composition is best-effort; a comment can still not span
blocks; typing at a code-span edge shows correct CRDT behavior only after
the normalize pass catches up (~1s).

**Feedback**: no system changes needed — `markedSpans` + `payloadAt` were
sufficient to drive a real WYSIWYG. The offset-mapping layer (DOM point ↔
visible index) is app code any editor will need; it belongs in the shared
app-layer kit alongside note #5's wrappers.

## 15. The page tree as nested seqs; derived children origins (2026-07-03)

The sidebar graduated from kv keys (`page:<rand>` → ref) to the block
model: every object implicitly owns an ordered **children seq** whose
origin derives deterministically from the object's id
(`seqId(parentObj)` reused as a plain hash). Consequences:

- **No pointer, nothing to race.** Two replicas "creating" a page's child
  list converge on the same object by construction — verified: independent
  stores derived identical list objects and a merge unioned their entries
  with no pointer op ever existing. This is note #12's deterministic-
  origin idiom, now load-bearing; it should be the default convention for
  any per-object auxiliary structure (children, comments-threads,
  backlinks…).
- **Sidebar drag is one Move op** within a parent — same machinery, same
  guarantees as block reorder. **Reparenting is not a Move**: the
  same-container rule means cross-parent drag is remove + insert — a new
  atom with a new identity. For bare refs that costs nothing (no marks or
  ops hang off the atom), but it is a visible asymmetry: "move within" and
  "move across" are different operations with different concurrency
  stories (a concurrent edit races an eviction+recreation, not a move).
  If cross-container moves ever matter semantically, that is a system
  question, and the same-container rule was chosen deliberately — the app
  must own the composite.
- **The same-key create conflict class vanished** (it was an artifact of
  random key slots); concurrent creates now simply both appear, ordered by
  the seq's arbitration. Tree conflicts shift to the placement-register
  class (freeze, surfaced).
- Ancestor-cycle guard moved into the app's drop handler (dropping a page
  into its own subtree would orphan it) — reachability constraints are
  render/app concerns, consistent with the store never validating shape.

State was discarded rather than migrated (active development; storage key
bumped) — the migration story from #12 remains unexercised beyond design.

The app's shape is now uniform: **seqs for everything ordered** (blocks,
table rows/cells, the tree), **kv for named registers** (title, body),
**derived origins for implicit structure**, Move for reorder everywhere.

## 16. Composite comments: identity = a derived thread (2026-07-03)

Cross-block comments landed as pure app composition, and the identity
trick collapsed three problems into one 32-byte value: a comment's tag is
(a) the suffix of its mark kind (`comment:<tag>` on every fragment), (b)
the ORIGIN of its discussion thread (the seq opened at the tag — replies
are appends; concurrent replies from two replicas merged cleanly in the
smoke test), and (c) the grouping key that reassembles fragments across
blocks for display, hover-highlighting, and resolve. No pointer, no
registry, no new system surface.

- Fragments ride their blocks' regional semantics independently — a
  dragged block carries its fragment along; resolve tombstones each
  fragment while the thread object survives as history (reachability, not
  erasure — consistent with the deletion story).
- Threads as newline-delimited text seqs are honest mini-CRDTs: whole-
  message appends anchor at the tail, and run non-interleaving keeps
  concurrent replies intact as units.
- The mark VALUE became vestigial ('on') — identity moved into the kind
  and content into the thread. Marks-as-annotations really only need the
  region + a name; the value slot mattered for formatting (language
  labels), not for anchored discussions.

**Feedback**: the derived-origin idiom (#15) plus minted-kind idiom (#11)
compose. An app-conventions document should present them as one pattern:
"mint a 32-byte identity; hang marks, objects, and grouping off it."

## 17. First live-browser session: what real DOM editing taught us (2026-07-03)

First session driving the app in an actual Chrome (via automation). Five
findings, all app-layer; zero system changes needed:

- **`prompt()` froze the tab** (and any automation with it). All dialogs
  removed: math/equations edit inline (click or arrow-in exposes the
  source; caret-out re-renders), delete is arm-then-confirm.
- **Text diffs need the caret.** A prefix/suffix diff is ambiguous when
  typed text borders equal text (` \cdot k` before ` \cdot 42` shares
  ` \cdot `) and can slide an insertion across a mark boundary. The caret
  position disambiguates; fall back to the plain diff only when the caret
  story doesn't check out.
- **Anchor side is a live UX lever, not just a schema choice.** Typed-
  markup conversions author CLOSED ends (After(last)) so typing after a
  rendered widget stays plain — the expanding end would race the
  tombstoned delimiter's ghost in sibling id order (observed live:
  " which is neat" swallowed into a math region). But while a region's
  source is EXPOSED for editing it must accept appends, so expose
  re-authors the mark open-ended and collapse re-closes it: two ops,
  same-kind overwrite hygiene does the rest. MARKS.md's anchor table
  turned out to be an interaction-design table.
- **Trailing newlines need a sentinel line box** (contenteditable can't
  put a caret after a trailing `\n`; Chrome types *before* it, reordering
  the text). A `<br data-sentinel>` the extractor ignores fixes it.
- **Async re-renders eat keystrokes**: KaTeX's late arrival rebuilt all
  rows under the user's first click. Any full rebuild must yield to an
  in-flight edit (defer to focusout).

Also: Enter = new block after (plain tails split into it; tails carrying
marks/embeds stay — their anchors live in the old block's elements);
cmd+Enter = literal newline. And `window.__kb` now exposes the store to
the console — indispensable for verifying seq truth vs DOM appearance.

## 18. Caret affinity is the intent (2026-07-03)

The user's report — "typing at a code span's edge extends it, then arrow
keys make the typed text jump out" — was the WYSIWYG view and the seq
disagreeing at boundaries and reconciling a second later. Resolution, in
three layers, all app-side:

1. **The browser's rendering of a boundary keystroke is the user's
   intent.** If Chrome drew the char inside the span, the mark extends
   (markRangeClosed over the union); if outside, the mark stays put
   (partial unmark if the seq disagreed). Nothing ever retroactively
   moves.
2. **Chrome normalizes boundary inserts into the previous inline
   element**, even when the caret was programmatically placed outside
   (verified: range in the next text node at offset 0 still inserted into
   the preceding <code>). DOM positioning cannot express "outside" at an
   element boundary — hence ZWSP filler text nodes after every styled
   span (the ProseMirror cursor-wrapper trick), stripped by extraction
   and skipped by offset mapping. With a filler, the post-conversion
   caret is mid-text-node and typing continues plain.
3. **Escape is native**: ArrowRight from a span's inside-end crosses the
   filler; typing right after a fresh `x` conversion is plain by default
   (preferAfter placement); clicking mid-span joins, clicking the edge
   doesn't. Boundary policy became: conversion exits, click follows the
   pixel, arrows move affinity.

Also from this session: the extension's synthesized input needs a real
click after a page reload before keystrokes route (automation quirk, not
app); coordinates go stale as KaTeX/layout settle — element-rect lookups
immediately before clicking are mandatory.

## 19. Images: the value side store earns its keep (2026-07-03)

An image is nothing new: a `Value::Bytes` artifact, embedded as an atom
whose payload is the artifact's value id. Everything the side store
promised showed up in practice: identical images dedupe to one artifact;
content-addressing makes the blob-URL cache sound forever (bytes can
never change under an id); artifacts ride the existing snapshot artifact
section, so images sync through the relay server with zero new protocol;
and the erasability story (drop bytes, keep id — ops still verify)
applies to images exactly as HASHWEB_SPEC describes.

The cost is also exactly where the spec said it would be: **snapshots
carry every artifact**, so each sync exchange now hauls all images. The
app mitigates (client-side downscale to ≤1400px WebP), but this is the
"large values sync lazily" gap made concrete — the wire has no
have/want negotiation for artifacts. Together with #8 (delta outbox),
that's the sync-protocol work the live-collab deployment will motivate
first.

**Feedback**: the pending/unavailable value state (HASHWEB_SPEC) now has
an obvious UI: an image whose bytes were erased or haven't arrived
should render as a placeholder chip with its id — worth adding when
lazy artifact sync exists.

## 20. Concurrent edits must not steal the caret (2026-07-03)

Live collaboration surfaced the classic editor-CRDT integration bug: a
peer editing the block you're in caused a full re-render — focus lost,
selection gone mid-comment. Two-layer fix, and the second layer is only
possible because of the CRDT:

- **Render incrementally.** Block rows persist keyed by object id with a
  markedSpans signature; a merge that didn't change a block leaves its
  DOM alone — the browser's own caret and selection survive untouched.
  Same-text-different-marks defers repaint to the normalize pass rather
  than stealing the caret.
- **Anchor selections to element ids, not offsets.** Offsets shift when
  a peer inserts before your caret; ids never do. Before merging, capture
  the id of the character left of each selection endpoint; after
  re-render, re-derive positions via positionOf. The selection re-attaches
  to the same characters wherever they moved — verified live: selection
  held across a same-block prepend. This is the id-addressing story
  (stable identity under concurrency) paying off at the UI layer; a
  plain-text collaborative editor has to invent OT-style transform for
  exactly this, and here it is two 10-line functions over seqIdAt /
  seqPositionOf.

**Feedback**: id-anchored cursors are what the emacs/wasm FFI's cursor
concept should expose natively for any future editor integration; worth
promoting into the shared app kit (#5, #14).

## 21. A big image broke page load — and the CRDT saved the data (2026-07-03)

An iPhone HEIC photo, undecodable by Chrome's createImageBitmap, fell
back to raw (~6MB) and pushed the snapshot past localStorage's ~5MB
quota. The uncaught QuotaExceededError fired inside the sync onmessage
handler BEFORE render(), so a fresh browser received the full state from
the server yet showed the empty initial page — "can't load."

Fixes: (a) localStorage is a disposable cache — a quota failure disables
it and continues, since the server and op DAG are the real source of
truth; (b) hard 1.5MB image cap, refusing undecodable oversized images
(HEIC) with guidance to export as JPEG/PNG.

Two design observations that outlast the bug:

- **The CRDT made data loss impossible even when I tried.** Believing it
  was test data, I `rm`'d the server state file. A still-connected client
  immediately re-merged its copy back (union of knowledge), restoring
  everything — a real conversation and photos. Whole-store merge means
  any live replica is a full backup; there is no single point of deletion.
  This is the deletion/GC story (HASHWEB_SPEC) as lived experience:
  "deleted" only means "no honest peer forwards it," and a peer that has
  it re-presents it harmlessly.
- **Whole-snapshot sync + large artifacts is the wall.** A 6MB image made
  every sync haul 6MB and blew a browser storage limit. This is APP_NOTES
  #8 (delta outbox) and #19 (lazy artifact have/want) converging into the
  single most load-bearing gap: the sync protocol must not ship the whole
  store, and must not ship artifact bytes a peer already has. Until then,
  images are a cap-and-downscale liability, not a feature that scales.

**Feedback**: the artifact side store needs out-of-band, content-addressed
transfer (fetch-by-id, cache-forever) separate from the op-snapshot wire —
exactly what content addressing was designed to enable. First real
system-facing work item the app has forced.

## 22. Writing-flow pass: the chrome was in the way (2026-07-03)

Wrote a real document (a floating-point explainer) in the editor and the
friction was exactly where intuition said: an 11-button toolbar sat
between title and body, permanently, and I never touched it while writing
prose — yet to format I'd have to leave the text, travel up to a fixed
bar (which scrolls off in a long doc), and travel back. Redesigned around
"tools appear where the work is":

- **Floating selection toolbar.** Selecting text pops a small toolbar
  just above the selection with the inline ops (code, math, comment,
  clear). It follows the selection, flips below when there's no room
  above, and vanishes on collapse. This is the whole formatting surface
  now — no fixed chrome.
- **Slash menu for blocks.** Typing `/` in an empty block opens a
  filterable menu (heading, code block, equation, table, image, page
  link) with arrow/Enter nav; picking removes the `/query` and inserts.
  Block insertion is a keyboard flow, not a button hunt.
- **The top toolbar is gone**, replaced by a one-line hint. EDIT/SPLIT/
  VIEW (view modes, not formatting) stay.
- **Headings render live in EDIT** (a leading `#` styles the whole
  block); tighter equation padding.

Bug the writing pass surfaced (had been mis-attributed to "CDN slow" all
session): **math never repainted after KaTeX finished loading.** KaTeX
loads async and post-render; the incremental renderer then skips the math
blocks because their *span signature* is unchanged — only KaTeX
availability changed. Fix: on KaTeX load, force-rerender blocks that
carry math/eqblock marks (not by scanning for `$` — eqblock text has the
delimiters stripped). Math now reliably renders.

**Feedback**: none of this touched the store — it's all editor-surface
work over the same marks/atoms model. Confirms the projection API is the
right shape; the app layer is where the product lives. Still owed: vendor
KaTeX so first-paint math needs no network (the load race is now handled,
but offline still shows source).
