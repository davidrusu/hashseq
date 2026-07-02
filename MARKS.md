# Marks op spec

Framework: FRAMEWORK.md (one reference set + honest frontier rule; Law I/II;
resource → conflict → resolution; locality dividing line; stability
requirement). Design lineage: Peritext (PRIOR_ART.md §5) — the anchoring and
span model, with hash-committed ids and causal supersession in place of
Lamport ids and timestamp LWW.

Rich-text formatting and span annotations (bold, links, comments) over a seq
object: anchored to elements, not indices; concurrent mark/unmark converges;
spans survive deletion of the underlying text. The substrate already
provides everything the model requires — stable element ids, tombstones that
keep resolving, and both-sided glued anchors.

## Op

```rust
struct MarkOp {
    start: Anchor,             // glued points — HASHSEQ_SPEC.md
    end: Anchor,
    kind: Id,                  // value commitment, e.g. value_id("bold")
    value: Id,                 // value commitment: flag, payload, or an
                               //   object link (comment thread);
                               //   TOMBSTONE = unmark
    overwrites: BTreeSet<Id>,  // mark ops this op saw and supersedes
                               //   within [start, end]
}
struct MarkNode { refs: BTreeSet<Id>, op: MarkOp }
// id = BLAKE3::derive_key(NODE_CONTEXT, canonical_encoding) — the family's
// single context; op kinds are tags in the encoding (HETEROGENEITY.md)
```

One shape covers **mark** (`value = value_id(true)` or a payload,
`overwrites` = the visible same-kind marks it replaces), **unmark including
partial unmark** (`value = TOMBSTONE` over the sub-range; the overwritten
bold keeps applying outside it), and **re-style** (a link's new URL,
`overwrites = {old}`).

## Refs

```
named(u) = { anchor_id(start), anchor_id(end) } ∪ overwrites
refs(u)  = named(u) ∪ frontier pins   // kind and value are values, not references
```

The pinned frontier is the mark layer's own: marks are **downstream-only**
(marks reference content; content never references marks), so mark ops never
enter the text object's tips or touch its runs — the frontier-granularity
choice per LAYERING.md. Anchor ids are refs, so a mark arriving before its
text parks as a normal orphan. `kind` and `value` are value commitments —
never buffered on, `pending` when unresolvable.

## Anchors and expansion

An anchor is a glued point (HASHSEQ_SPEC.md): `Before(c)` is crossed
immediately before emitting `c`, after all of `c`'s before-descendants;
`After(c)` immediately after `c`, before any of its after-descendants.
Anything later inserted into the adjacent gap lands on the same side of the
point, so span membership is unambiguous: an element is in the span iff it
is emitted strictly between the two points — **in the base order**
(FRAMEWORK "Stability"). Rendered relocation of elements (`Move`) never
changes which elements a span covers.

Grow-at-edges behavior ("typing at the end of bold text continues bold; at
the end of a link does not") is **anchor choice, not a flag**. For a span
with first/last elements `s`/`e`, `p` preceding and `n` following:

| edge behavior        | anchor      | why                               |
|----------------------|-------------|-----------------------------------|
| start, non-expanding | `Before(s)` | gap inserts land before the point |
| start, expanding     | `After(p)`  | gap inserts land after the point  |
| end, non-expanding   | `After(e)`  | gap inserts land after the point  |
| end, expanding       | `Before(n)` | gap inserts land before the point |

Bold = `Before(s) .. Before(n)`; link = `Before(s) .. After(e)`. The policy
is chosen by the editor at op-creation time and committed in the artifact —
the CRDT layer needs no registry of mark kinds to converge.

Edge cases: expanding start at document start = `After(origin)` (a real
interned node). Expanding end at document end — deliberately **no
sentinel** (HASHSEQ_SPEC.md): the editor extends the span with an overwrite
mark as typing continues at the boundary, or the app maintains its own
terminal element to anchor `Before` of. Anchors to tombstoned elements keep
resolving — tombstones keep their slot; that is why they exist.

## Resource

One **register per (element, kind)**: the `k`-formatting of element `x`,
claimed by every `k`-mark covering `x`.

## Conflict

The live set at `(x, k)` is every `k`-mark covering `x` not named in the
`overwrites` of a **same-kind** op that also covers `x`. Suppression is
**range-scoped**: an overwrite erases its targets only where the
superseding op's span overlaps them — and **kind-scoped**: cross-kind
entries in `overwrites` are ignored by the definitional filter (mirroring
HashKv's same-key rule; never gated — HASHWEB_SPEC.md "Gate vs filter"). A conflict is a multi-head live set — non-supersession, per
FRAMEWORK; the honest-author lemma connects it to concurrency exactly as
everywhere else.

## Resolution (read time)

- **MVR-first**: expose the live set. **No LWW** — there is no timestamp
  input.
- *Cosmetic ambiguity* (which of two identical concurrent bolds to
  attribute) → `max-Id` display tiebreak, never semantics.
- *Semantics-bearing values* — the sharp case is a link's URL — **freeze**:
  render the conflict state (link disabled, all targets surfaced). Hash
  order is grindable; a ground id must not silently win a phishing target
  (locality dividing line).
- **Comments never arbitrate**: concurrent comments all survive. A comment
  thread is `value = <the thread object's origin id>` — replies are seq
  inserts in the child object (HASHWEB_SPEC.md).

The classic hard cases, as they converge here:

1. *Concurrent bold vs overlapping unbold*: the unbold kills only what it
   names; a concurrent bold it never saw survives in the overlap — add
   wins, surfaced as a multi-head, never timestamp-flattened.
2. *Insert into a gap inside a concurrently-unbolded sub-span*: the new
   element falls between the unmark's points → covered by it → not bold.
3. *Span text deleted, new text inserted between the tombstones*: between
   the points → inherits the mark (documented, slightly surprising,
   correct).
4. *Adversarial inverted span*: gated at apply — Validation below.

## Apply

O(1) bookkeeping: intern; attach start/end events to the anchor elements
(`anchor_events: element → mark events`); update mark-layer tips.
Suppression is computed at read, never at apply.

## Rendering

A sweep piggybacking the linear iterator: walk the sequence, toggle an
active set at anchor events (an unmark/overwrite op is itself an interval,
so suppression is interval-vs-interval inside the sweep), emit coalesced
`(run, FormatSet)` spans. Cost O(text + anchor events); marks attach by id,
so text edits never reposition marks. Point queries ride `position_of` /
`cmp_order`. Wire: mark volume is orders of magnitude below element volume —
individual ops, dict + positional refs (ENCODING_SPEC.md); a "format
painter" session chain can run-compress later if profiles say so.

## Validation

- **Inverted span** (end point before start point) — an apply-time gate:
  1. the check can always run at apply: anchor ids are refs, so both
     elements are present when the op leaves the orphan buffer;
  2. it is one `cmp_order` comparison over the **base order** — O(log F),
     resolved through origin ghosts (HASHSEQ_SPEC.md), so a concurrent or
     later `Move` can never flip a verdict;
  3. a failed check is permanent (base order is immutable) and convergent
     (all replicas agree on every verdict);
  4. no honest op ever depends on one: honest replicas gate before apply,
     so inverted marks never enter honest mark-layer tips.
  Defense in depth: the sweep stays activation-guarded — an end event for a
  never-started mark is inert rather than perturbing the active set — so a
  future relaxation of the gate cannot reintroduce the
  formatting-leaks-to-end-of-document failure.
- **Anchor kind**: anchors must name elements or the origin, in one `Seq` —
  a row of the edge table (HASHWEB_SPEC.md). Move-op splice points are
  gated for now; admitting them later is a loosening-class upgrade
  (HASHWEB_SPEC.md "Tighten never, loosen carefully").
- `kind` and `value` are ids (BLAKE3 outputs), so in-memory keying by kind
  id is fast-hash-safe — no SipHash needed (the HASHKV_SPEC.md key rule;
  adversarial kind bytes cost their author indirection, never a table).
- **Amplification**: one O(log F) comparison plus one quarantine entry per
  malicious op — linear in attacker effort. Mark spam over huge ranges
  costs the renderer O(anchor events), not O(range); MVR set growth is the
  application-surfaced symptom. Note the gate does not shrink the spam
  surface (valid empty spans are always authorable); its goals are bounded
  cost and no rendering leak.

## Open problems

1. **Overwrites hygiene.** Should a new bold be required to name the bold
   it covers (keeping live sets minimal)? Leaning yes for honest editors —
   the HashKv discipline — but stacking must converge regardless, since a
   Byzantine author can always stack.
2. **Cross-object spans** (a mark spanning multiple text objects): punt —
   marks are per-object; the editor splits the gesture into one op per
   object.
3. **History retention.** Superseded marks: keep (time travel) vs ids only.
   Same question as the seq placement-register spine; likely the same
   answer.

## Test strategy

Merge-law props (commutative/associative/idempotent) port directly; the
Peritext worked examples become a fixture suite (each a small op DAG with an
expected rendered span list); plus the quickcheck invariant: render is
identical across all delivery orders — including marks delivered before
their anchor text, and now including interleaved `Move`s of anchored
elements.
