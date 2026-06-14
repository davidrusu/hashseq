# Marks: Peritext-style span annotations on hashseq (design sketch)

Status: design sketch, 2026-06-11. Nothing here is implemented. Companion to
HASHDOC.md (marks are a natural third projection alongside seq and map).

## Goal

Rich-text formatting and span annotations (bold, links, comments) over a
hashseq text object, with Peritext's merge behavior — anchored to characters,
not indices; sane concurrent mark/unmark semantics; surviving deletion of the
underlying text — but with BFT identities instead of actor/counter opIds and
causal supersession instead of timestamp LWW.

## Why hashseq is already most of Peritext

Peritext's substrate requirements are: (1) every character has a stable id,
(2) deleted characters leave tombstones so anchors keep resolving, (3) anchors
can attach to either *side* of a character. Hashseq has all three natively —
ids are the BLAKE3 node ids, removes tombstone rather than drop, and the
insert tree already distinguishes before-children from after-children. The
annotation layer adds no new requirements to the text CRDT.

## Anchors

```rust
enum Anchor {
    Before(Id), // glued to the char's left edge
    After(Id),  // glued to the char's right edge
    DocEnd,     // virtual point after everything (see below)
}
```

An anchor is a virtual point in the linearization, **glued tight** to its
character:

- `Before(c)` is crossed immediately before emitting `c` — after all of `c`'s
  before-descendants. Anything later inserted in the gap to `c`'s left
  (whether `InsertBefore(c)` or `InsertAfter(prev)`) lands *before* the point.
- `After(c)` is crossed immediately after emitting `c` — before any of `c`'s
  after-descendants. Anything later inserted in the gap to `c`'s right lands
  *after* the point.

Both insertion flavors that can target a gap land on the same side of a glued
anchor, so span membership is unambiguous: a character is in the span iff it
is emitted strictly between the two anchor points. Since hashseq never
reorders two existing characters as new ops arrive, anchor points are stable.

### Expansion is anchor choice, not a flag

Peritext's grow-at-edges behavior ("typing at the end of bold text continues
bold; typing at the end of a link does not") falls out of *which char and
which side* the editor anchors to, with glued semantics doing the rest. For a
span whose first/last chars are `s`/`e`, with `p` preceding and `n` following:

| edge behavior        | anchor          | why                                  |
|----------------------|-----------------|--------------------------------------|
| start, non-expanding | `Before(s)`     | gap inserts land before the point    |
| start, expanding     | `After(p)`      | gap inserts land after the point     |
| end, non-expanding   | `After(e)`      | gap inserts land after the point     |
| end, expanding       | `Before(n)`     | gap inserts land before the point    |

Bold = `Before(s) .. Before(n)`; link = `Before(s) .. After(e)`. The policy
is chosen by the editor at op-creation time and committed in the artifact —
the CRDT layer never needs a registry of mark kinds to converge.

Edge cases of the table:

- expanding start at document start: `After(origin)` — the origin is a real
  interned node, so this needs nothing new;
- expanding end at document end: there is no `n`, hence the `DocEnd` sentinel;
- anchors to deleted chars: tombstones keep resolving, the point sits where
  the tombstone sits. This is exactly why Peritext keeps tombstones; hashseq
  gets it for free.

## The op: one shape for mark, unmark, and re-style

The HashKv observation applies verbatim: changing formatting is superseding
prior formatting, which is the `Remove`/`Put` pattern — name what you saw and
replace it, causally anchored.

```rust
struct MarkOp {
    start: Anchor,
    end: Anchor,
    kind: Box<str>,            // "bold", "link", "comment", ...
    value: Value,              // Bool(true), Bytes(url), Ref(comment_obj), ...
                               // Tombstone = pure unmark
    /// Mark ops this op saw and supersedes within [start, end] — the
    /// analog of Remove's targets / Put's overwrites (Peritext has no
    /// equivalent; it uses opId LWW instead).
    overwrites: BTreeSet<Id>,
}

struct MarkNode {
    extra_dependencies: BTreeSet<Id>, // mark-layer tips − overwrites − anchors
    op: MarkOp,
}
// id = BLAKE3 derive_key, own tag in the shared tag space — same convention
```

Dependencies: both anchor ids and all `overwrites` are deps (a mark is
uninterpretable without its anchor chars). Global orphan buffering parks
marks that arrive before their text.

This one shape covers:

- **mark**: `value = Bool(true)` (or payload), `overwrites = {}` — or the
  currently-visible same-kind marks it intends to replace;
- **unmark**, including *partial* unmark (unbolding the middle of a bold
  span): `value = Tombstone`, range = the sub-span, `overwrites = {bold op}`.
  The bold op keeps applying outside the unmark's range;
- **re-style** (change a link's url): new value, `overwrites = {old}`.

### Read semantics: per-(char, kind) MVR, no LWW

At character `x`, for kind `k`: the live set is every `k`-mark covering `x`
that is not named in the `overwrites` of some op also covering `x`. Suppression
is *range-scoped*: an overwrite only erases its targets where the superseding
op's span overlaps them.

- The API is MVR-first: expose the live set. There is no LWW (timestamps
  are forgeable), and per the locality invariant (HASHDOC.md), max-`Id` is
  only a *display* tiebreak for cosmetic ambiguity (e.g. which of two
  concurrent identical bolds to attribute). Anything semantics-bearing
  renders the conflict state instead — the sharp case is a link's URL:
  hash order is grindable, and a ground hash must not silently win a
  phishing target. Conflicted links render disabled with both targets
  surfaced.
- Comments want the full set anyway (concurrent comments must all survive);
  they are just kind="comment" marks whose values never get arbitrated.
- A comment *thread* is a child object in the HashDoc sense:
  `value = Ref(creation_op)` — replies are seq inserts in the child.

Checking against Peritext's hard cases:

1. *Concurrent bold vs unbold, overlapping ranges.* The unbold only kills
   what it names. A concurrent bold it never saw survives in the overlap —
   "add wins", and the conflict is visible as a multi-head, not silently
   timestamp-flattened.
2. *Insert into a gap inside a concurrently-unbolded sub-span.* The new char
   falls between the unmark's anchors, the unmark covers it, the bold it
   targets is suppressed there → not bold. Matches Peritext.
3. *Span text deleted, new text inserted between the tombstones.* New text is
   between the anchors → inherits the mark. Matches Peritext (documented,
   slightly surprising, correct).
4. *Adversarial inverted span* (end point before start point): rejected at
   apply time and permanently quarantined — see "Inverted spans" below.

## Marks are a separate layer with their own tips

Mark ops must not enter the text object's tips: a mark at the tips becomes
`extra_deps` on the next insert and fragments runs — the per-object-tips
argument from HASHDOC.md, applied here. So the mark layer is its own object:

- own tips, own (trivial) projection state;
- causally **downstream-only**: marks depend on text ops (anchors), text ops
  never depend on marks. Text run formation is untouched;
- shares the substrate: IdIndex/interning, orphan buffering, merge = op-set
  union, the same merge-law quickcheck props.

In HashDoc terms it is a per-text-object sibling projection; standalone it is
a companion struct alongside `HashSeq` sharing the same `doc_id`.

## State and rendering

```rust
struct MarkLayer {
    marks: Vec<MarkRecord>,                              // by mark NodeIdx
    anchor_events: FxHashMap<NodeIdx, SmallVec<[MarkIdx; 2]>>, // char → start/end events
    // tips, orphans: substrate
}
```

Rendering is a sweep that piggybacks on the existing linear iterator: walk
the sequence, toggle an active set at anchor events (an unmark/overwrite op
is itself an interval, so suppression is interval-vs-interval inside the
sweep, not per-char set algebra), emit coalesced `(text run, FormatSet)`
spans — a rich-text iterator. Cost is O(text + anchor events); marks attach
by id, so text edits never reposition marks — only a span cache (if we add
one) invalidates locally.

Anchor → position lookups for point queries go through the existing
`position_of` / run index; no new index needed for v1.

Wire format: mark volume is orders of magnitude below char volume, so v1
encodes ops individually — dict header + positional refs for anchor and
overwrite ids (they overwhelmingly point at recently-encoded runs, exactly
like remove targets). A "format painter" session chain could run-compress
later if profiles ever say so.

## Inverted spans: validate at apply, O(log F) per mark

A malicious peer can author a well-formed, well-hashed mark whose end anchor
precedes its start anchor. This must be cheap to reject — and naively it is
dangerous: a sweep that just toggles at anchor events would see the end event
first (no-op), then the start event, activate the mark, and never deactivate
it — formatting leaks to end of document.

Resolution: this is an instance of HASHDOC.md's **validate before apply**
rule, and the check is total, convergent, and permanent:

1. **The check can always run at apply time.** Anchor ids are dependencies,
   so a mark op only applies once both anchor chars are in the sequence (it
   parks as a normal orphan until then). At that moment both anchor points
   are resolvable.
2. **The check is O(log F) with no new state.** Anchor order is lexicographic
   `(char order, Before < After)` thanks to glued semantics, so it reduces to
   a tombstone-inclusive order comparison between two chars. The run index
   already supports this: it is an order-statistics treap of fragments *with
   parent pointers*, and tombstoned elements stay in their fragments (only
   the visibility bit clears). Compare `(run, element offset)` within a
   fragment directly, otherwise walk both fragments' root paths and compare
   at the fork — a `cmp_order(a, b) -> Ordering` primitive next to
   `position_of`, O(log F), touching nothing convergence-relevant.
   (`After(origin)` is before everything; `DocEnd` after everything.)
3. **A failed check is permanent.** Hashseq never reorders two existing
   elements, so an inverted span can never become valid — quarantine is
   forever, exactly like a failed type check in HASHDOC.md. And the relative
   order of two chars is convergent across replicas, so honest replicas
   agree on every verdict.
4. **No honest op ever depends on one.** An honest editor authors a mark from
   chars it can see, in their (convergent) order — it cannot produce an
   inverted span. Because every honest replica validates before applying,
   inverted marks never enter honest mark-layer tips; anything that depends
   on one was authored by a faulty peer and correctly orphans forever.

The same apply-time gate also handles the neighboring ill-typed case: an
anchor naming a non-char node (a Remove or another mark op). Anchor must
resolve to `Loc::Run`/`Origin`; anything else fails validation identically.

Cost under attack is one O(log F) comparison plus one quarantine entry per
malicious op — linear in attacker effort, no amplification. Note that
rejecting inverted spans does not shrink the spam surface (valid empty marks
like `Before(c)..Before(c)` are always authorable); the goals are bounded
cost and no rendering leak, which this gives.

Defense in depth: the sweep stays activation-guarded anyway — a mark
activates only at its start event, and an end event for a never-started mark
flags it inert rather than perturbing the active set. O(1) per event, and a
future relaxation of the apply gate can't reintroduce the leak.

## BFT properties

Nothing new: ids self-certifying, deps unforgeable, worst-case malice is
concurrent mark forks surfaced as MVR conflicts. Two notes:

- `kind` strings are adversary-chosen bytes: in-memory keying by kind hashes
  with SipHash or a BLAKE3-derived KindHash (the HashKv `KeyHash` rule);
- mark spam over huge ranges costs the renderer O(anchor events), not
  O(range), and MVR set growth is the application-surfaced symptom — same
  posture as sibling-fork spam in the text layer.

## Open problems (decide before building)

1. **DocEnd encoding.** A reserved sentinel id vs a tagged anchor variant on
   the wire. Leaning tagged variant (no magic ids in `Id` space).
2. **Overwrites hygiene.** Should a new bold over an existing bold *have* to
   name it (keeping live sets minimal), or may it stack? Leaning: editors
   should name what they see (HashKv discipline), but stacking must still
   converge since a malicious peer can always stack.
3. **Cross-object spans** (a mark spanning multiple HashDoc text objects):
   punt — marks are per-object; the editor splits the gesture into one op per
   object.
4. **History retention.** Superseded marks: keep (time travel) vs ids only.
   Same question and likely same answer as HashKv open problem 4.

## Test strategy

Merge-law props (commutative/associative/reflexive) port directly; the
Peritext paper's worked examples become a fixture suite (each is a small op
DAG with an expected rendered span list); plus a quickcheck invariant: render
is identical across all delivery orders, including marks delivered before
their anchor text.
