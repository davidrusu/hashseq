# Marks op spec

Framework: FRAMEWORK.md (one reference set + honest frontier rule; Law I/II;
resource → conflict → resolution; locality dividing line; stability
requirement). Lineage: Peritext (PRIOR_ART.md §5) — its anchoring and span
model, with hash-committed ids and causal supersession in place of Lamport
ids and timestamp LWW. Glued points, base order, and move splice points are
defined in HASHSEQ_SPEC.md; this document is the normative home of the
`Mark` op.

Formatting and span annotations (bold, links, comments) over a seq object:
anchored to elements, not indices; concurrent mark/unmark converges; spans
survive deletion of the underlying text.

## Op

```rust
Mark {
    start: Anchor,             // glued points (HASHSEQ_SPEC.md)
    end: Anchor,
    kind: Id,                  // value commitment, e.g. value_id("bold")
    value: Id,                 // value commitment: flag, payload, or an
                               //   object link (comment thread);
                               //   TOMBSTONE = unmark
    overwrites: BTreeSet<Id>,  // mark ops this op saw and supersedes
                               //   within [start, end]
}
// a node like every other op: { refs, op }, id under the family's single
// NODE_CONTEXT (HASHSEQ_SPEC.md "Op")
```

One shape covers **mark** (`value = value_id(true)` or a payload,
`overwrites` = the same-kind marks it replaces), **unmark, including
partial unmark** (`value = TOMBSTONE` over the sub-range; the overwritten
mark keeps applying outside it), and **re-style** (a link's new URL,
`overwrites = {old}`).

## Refs

```
named(u) = { anchor_id(start), anchor_id(end) } ∪ overwrites
refs(u)  = named(u) ∪ frontier pins   // kind and value are values, not refs
```

A mark enters and pins the object's one frontier like every other op.
Marks reference content; content never references marks. Anchor ids are
refs, so a mark arriving before its text waits as an ordinary orphan.
`kind` and `value` are value commitments: never waited on, `pending` when
unresolvable.

## Anchors and expansion

**Marks are regional.** The two points are fixed for life at their
anchors' *base* slots (origin ghosts): a `Move` never relocates a point, so
no placement op can reshape or drag a span's region. An element is in the
span iff its **rendered** crossing falls strictly between the points: a
moved-out element sheds the region's marks, a moved-in element acquires
them. Formatting that should travel with moved content is editor policy
(the move gesture authors a re-mark).

The one exception is an endpoint anchored at a **move op**: it brackets
*wherever that op's target renders* ("cover that moved-in word"). Its
position is the op's rank in its anchor's fork order — permanent — and an
op anchored by a mark keeps its fragment for life (HASHSEQ_SPEC.md
"Apply").

Grow-at-edges behavior ("typing at the end of bold continues bold; at the
end of a link it does not") is **anchor choice, not a flag**. For a span
with first/last elements `s`/`e`, `p` preceding and `n` following:

| edge behavior        | anchor      | why                               |
|----------------------|-------------|-----------------------------------|
| start, non-expanding | `Before(s)` | gap inserts land before the point |
| start, expanding     | `After(p)`  | gap inserts land after the point  |
| end, non-expanding   | `After(e)`  | gap inserts land after the point  |
| end, expanding       | `Before(n)` | gap inserts land before the point |

Bold = `Before(s) .. Before(n)`; link = `Before(s) .. After(e)`. The editor
chooses at op creation and the choice is committed in the op, so the CRDT
needs no registry of mark kinds to converge.

Edges: an expanding start at the document start is `After(origin)`. There
is no end sentinel: to expand at the document end, the editor extends the
span with an overwrite mark as typing continues, or the app keeps its own
terminal element to anchor `Before`. Anchors on tombstoned elements keep
resolving — tombstones keep their slot.

## Resource

One **register per (element, kind)**: the `k`-formatting of element `x`,
claimed by every `k`-mark covering `x`.

## Conflict

The live set at `(x, k)` is every `k`-mark covering `x` not named in the
`overwrites` of another `k`-mark that also covers `x`. Suppression is
**range-scoped** (an overwrite erases its targets only where the
superseding span covers) and **kind-scoped** (cross-kind entries in
`overwrites` are ignored by the filter, never refused — HASHWEB_SPEC.md
"Admission vs filter"). A conflict is a live set with more than one member.

## Resolution (read time)

The read API (`marks_at`, `marked_spans`) exposes the live set (MVR);
there is **no LWW** — no timestamp input exists. Renderer policy on top:

- *Cosmetic ambiguity* (two identical concurrent bolds): `max-Id` display
  tiebreak, never semantics.
- *Semantics-bearing values* (a link's URL): **freeze** — render the
  conflict (link disabled, all targets surfaced). Ids are grindable; a
  ground id must not silently win a phishing target.
- **Comments never arbitrate**: concurrent comments all survive. A comment
  is `value = <the thread object's origin id>`; replies are inserts in that
  object (HASHWEB_SPEC.md).
- Unmark (`TOMBSTONE`) values suppress but never display.

Classic hard cases:

1. *Bold ∥ overlapping unbold*: the unbold kills only what it names; the
   concurrent bold survives in the overlap — add wins, surfaced as a
   multi-member live set.
2. *Insert into a gap inside a concurrently unbolded sub-span*: the element
   falls between the unmark's points, so it is not bold.
3. *Span text deleted, new text inserted between the tombstones*: between
   the points, so it inherits the mark (surprising, but correct).
4. *Inverted span*: refused (Validation).

## Apply

O(1) bookkeeping: intern; record start/end events on the anchor nodes
(`mark_events: anchor → events`); update the tips as for any op.
Suppression is computed at read, never at apply.

## Rendering

One treap-order sweep: anchor events toggle the active set as their base
slots are crossed — tombstoned and moved-out slots included — and each
element samples the active set where it *renders* (its base slot, or its
destination fragment when moved), emitting coalesced `(text, marks)` spans.
The sweep is activation-guarded: an end event for a never-started mark is
inert. Cost O(text + anchor events); marks attach by id, so text edits
never reposition them. A point query compares sweep positions (rendered
element vs base-fixed points) in O(log F). Mark ops ride the wire as
individual nodes (ENCODING_SPEC.md).

## Validation

Admission, once both anchors have applied (HASHSEQ_SPEC.md "Validation"):

- **Anchor kind**: each anchor names a glue point — an element, the
  origin, or a move op — else `NotAGluePoint`.
- **Inverted span** (end point before start point) → `InvertedSpan`. One
  base-order comparison, O(log F), through origin ghosts, so no concurrent
  or later `Move` can flip it. The verdict is permanent (base order is
  immutable) and convergent, and no honest op depends on a refused one.
  The sweep's activation guard is defense in depth, so a future relaxation
  cannot reintroduce formatting leaking to the end of the document.
- `kind` and `value` are author-chosen value ids (small ones are identity
  form, GRAMMAR_SPEC.md), so any in-memory keying by them must use a
  seeded hasher, never Fx. Marks group by kind in ordered maps.
- **Amplification**: one O(log F) comparison per malicious op. A refused
  mark is not stored, though checking one may materialize a zero-width
  splice slot for a move op it names (derived index state, at most one per
  move op). Mark spam over huge ranges costs the renderer O(anchor events),
  not O(range); a growing live set is the app-visible symptom. Refusal does
  not shrink the spam surface (empty spans are always authorable); its
  goals are bounded cost and no rendering leak.

## Open problems

1. **Overwrites hygiene.** `mark_range` names every same-kind mark whose
   span intersects the new one, keeping live sets minimal for honest
   editors (the HashKv discipline). Stacking must still converge, since a
   Byzantine author can always stack. Cost: that set includes
   already-superseded marks, so honest toggling grows `overwrites`
   quadratically on the wire, and live-set reads are O(k² · W) (QUEUE.md).
2. **Cross-object spans**: marks are per-object; the editor splits a
   multi-object gesture into one op per object.
3. **History retention.** Superseded marks: keep (time travel) or ids
   only. Same question as the placement-register spine; likely the same
   answer.

## Test strategy

Merge-law props (commutative, associative, idempotent); the Peritext
worked examples as a fixture suite (op DAG → expected span list); and the
invariant that rendering is identical across all delivery orders,
including marks delivered before their anchor text and interleaved `Move`s
of anchored elements.
