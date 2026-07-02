# HashSeq op spec

Framework: FRAMEWORK.md (one reference set + honest frontier rule; Law I/II;
resource → conflict → resolution; locality dividing line; stability
requirement). Design rationale: MOVE.md (move), MARKS.md (anchors). Specifies
the sequence CRDT — three ops on one object: `Insert` claims a gap, `Remove`
claims liveness, `Move` claims placement.

## Op

```rust
enum Anchor { Before(Id), After(Id) }   // THE glued point — the same primitive
                                        // marks address (MARKS.md)

enum Op {
    Insert { at: Anchor, payload: Id },
    Remove(BTreeSet<Id>),
    Move { target: Id, to: Anchor, overwrites: BTreeSet<Id> },
}
struct HashNode { refs: BTreeSet<Id>, op: Op }
// id = BLAKE3::derive_key(NODE_CONTEXT, canonical_encoding)
// NODE_CONTEXT = "hashweb v1 node id" — ONE string for every op kind in the
// family; kinds are tags inside the encoding, never separate contexts
// (HETEROGENEITY.md, GRAMMAR_SPEC.md).
```

The anchor's side is data, not op kind — matching the resource definition
below (a gap *is* an `(anchor, side)` pair). There is
deliberately **no end sentinel**: the origin node anchors the document start
(`After(origin)`), and end-of-document behavior is a higher-level concern —
an editor extends a boundary span with an overwrite mark, or an app wanting
a stable end point inserts its own terminal element and anchors `Before` it.
Sentinels are user-space objects, not protocol.

`Move` is **same-container only**: the destination anchor must resolve
within `target`'s own object — a stable gate, both sides hash-committed.
Placement registers therefore never change parent edges, containment stays
the creation forest, and placement cycles are unrepresentable
(CYCLE_REVERT.md records the problem this dissolves). Cross-container
relocation is deliberately *not* an op: it is remove + insert of the
object's link, with concurrent relocations surfacing as detectable
multi-link duplication (MOVE.md "Reparenting").

## Refs

```
named(u) = { anchor_id(at) }                          Insert
named(u) = targets                                    Remove
named(u) = { target, anchor_id(to) } ∪ overwrites     Move
refs(u)  = named(u) ∪ frontier pins    // the payload is NOT a ref — see Payload
```

Honest construction: `refs = observed frontier ∪ named` (FRAMEWORK honest
frontier rule). Anchors are "attach-to"; targets and overwrites are
"replaces". The typing fast path is `refs = {anchor}` — the anchor *is* the
frontier, one id. The encoding stores each id of `refs(u)` once (the sorted
refs table, GRAMMAR_SPEC.md); indices are an encoding concern.

Which frontier a `Move` pins — the object's own, or a separate
downstream-only move frontier that never enters the object's tips — is the
LAYERING.md granularity parameter; nothing below depends on the choice. If
moves keep a separate frontier, a move may additionally pin its container's
observed frontier (the commitment vector), making move-vs-remove concurrency
decidable; nothing requires it and v1 does not. Orphan buffering covers
moves arriving before `target` or `to`.

*Code today* (aligned 2026-07-02): the preimage and id derivation implement
this spec exactly (GRAMMAR_SPEC.md Part A; `tests/grammar_vectors.rs` locks
the vectors). Storage keeps the normalized split — `HashNode { pins, op }`
with `pins = refs ∖ named` and `iter_refs()` yielding `refs(u)` — a
replica-local layout choice, not a wire or identity one. Known gaps, gated
(quarantined pending re-evaluation, the loosening path): non-char insert
payloads (the value-column generalization), and inserts anchored on move-op
splice points. Move's rendered index relocation is wired per "Apply" below:
origin ghosts (base slots live forever), one relocation per rendered-
placement change, glued blocks id-ordered and skipped by the insert/extend
paths; `prop_index_matches_iterator_with_moves` pins the index to the
definitional iterator. Cursors still anchor at base slots — inserting at a
rendered position adjacent to a moved-in element lands at the element's
ghost until splice-point anchors are admitted.

## Payload

The payload is an **id, not a raw value**: the id of a content-addressed
value artifact (a kind-tagged canonical value encoding — char, int, bytes,
…; `value_id = BLAKE3::derive_key(VALUE_CONTEXT, encoding)`, one context for
all value kinds; well-known artifacts like `TOMBSTONE` and the creation
artifacts are ordinary derived value ids — computed constants, never magic
ids — creation artifacts included), of an object's origin id (a link;
transclusion when the object lives elsewhere — HASHWEB_SPEC.md), or of an
op node.
Element identity is unchanged: the *element* is the insert node id (what
anchors, marks, removes, and moves name); the payload id is the element's
**content commitment**.

- **Payloads are value commitments, not references.** The payload is not in
  `refs(u)`: buffering never waits on it (a text insert must not orphan on
  its own char), and an unresolvable payload is the app-visible `pending`
  state (HASHWEB_SPEC.md blobs), not a delivery condition.
- **Transport is identity-neutral.** Values at or below the hash size
  encode inline (chars ride the run columns exactly as today; value ids are
  derived at decode); larger values live in the content-addressed side
  store. Inline vs indirect is pure encoding — the node id is identical
  either way, so the same logical value can never yield two op identities.
- **For text this is identity-only.** State, rendering, run storage, and
  wire bytes are unchanged; the preimage hashes `value_id(char)` (a fixed,
  cacheable universe) in place of the char. Cost is a wider chain-hash
  input — benchmark against the sequential_traces discipline.

Any payload kind is insertable; what a renderer does with a payload it
cannot interpret is the placeholder semantics of HETEROGENEITY.md.

## Resource

Three:

- **a gap** — an `(anchor, side)` pair, i.e. an `Anchor`, claimed by
  `Insert`;
- **a target's liveness** — claimed by `Remove`;
- **a placement register per element** — "where is `target`" *within its
  container* — claimed by `Move`. Initial value = the creation placement
  (the original insert); every value in the register's history is a glued
  point in the same object.

```
heads(x) = { m ∈ Moves(x) : ∄ n ∈ Moves(x). m.id ∈ overwrites(n) }
```

## Conflict

| op                                                    | resource           | concurrency on it means                           | true conflict?             |
|-------------------------------------------------------|--------------------|---------------------------------------------------|----------------------------|
| `Insert`                                              | a gap              | ≥2 concurrent inserts in the same gap             | **yes — order arbitrated** |
| `Remove`                                              | target liveness    | ≥2 concurrent removes of the same target          | no — idempotent union      |
| `Move`                                                | placement register | `\|heads(x)\| > 1` — none superseded              | **yes — freeze**           |
| `Remove` vs the insert it targets                     | —                  | impossible: target ∈ refs, so remove *follows* it | no — never concurrent      |
| `Remove` vs an insert anchored on the removed element | the element's gap  | child saw the element; tombstoning ⊥ placement    | no — they compose          |
| `Move` vs `Remove` of target                          | —                  | remove wins; element tombstoned, register moot    | no — absorption            |
| `Move` vs edits inside target's child object          | —                  | orthogonal: child edits target the object by id   | no                         |

The "no" rows:

- concurrent removes are a join-semilattice — an idempotent tombstone bit,
  converging by union;
- a remove can never race the creation of its target (`target → remove`
  always); a remove naming a non-insert node is inert (no index entry, dead
  bit);
- tombstoning `X` clears only `X`'s visibility; characters `After(X)` saw
  `X`, are causally later, and stay live in their own gap — a delete never
  drags content typed into it;
- remove-beats-move is an absorption rule, not a causality test — dead is
  absorbing regardless of what the register says.

## Resolution (read time)

### Insert — total order on contending siblings by id

Concurrent inserts in one gap render in **ascending `Id` order** among
themselves (`first_ge`: a new node lands before the smallest present sibling
whose id exceeds it; `afters` / `befores_by_anchor` are id-ordered). Sound
under the locality dividing line: id-order arranges only the contenders' own
content — existing elements never reorder (Stability, below), so grinding an
id only moves *your own* element within a gap you are already writing to.

*Non-interleaving (decided — single anchor, convention-scoped guarantee):*
the *k*-th element of a burst anchors `After` the (*k*−1)-th — not the
gap — so a forward run orders as a contiguous *block* (its head's id
decides), and prepend chains of `Before`-children keep backward bursts
contiguous the same way. The guarantee is **convention-scoped**: honest
clients following the anchor rule (left causally-before right →
`Before(right)`, else `After(left)`) get block non-interleaving on both
suites; the format does not enforce it, and maximal non-interleaving in
the Fugue sense
([Weidner & Kleppmann 2023](https://arxiv.org/abs/2305.00583)) is not
claimed. Dual left/right origins were **rejected, not overlooked**: a
committed interval hands every malicious peer an inverted `(right, left)`
pair — and crossing intervals from several peers form constraint sets with
no consistent order at all — forcing per-insert interval validation and a
fresh arbitration surface onto the system's hottest op (marks pay their
inverted-span gate only at mark volume), on top of a second ref in every
insert preimage and an absent-right sentinel. The single anchor has no
interval to invert — structurally valid by form — and an adversary's
anchor games order only their own content (locality invariant). Accepted
residuals: gap-pinning lives in client discipline rather than the
artifact, and concurrent append-vs-insert intents encode identically
(GRAMMAR_SPEC.md "Op kinds").

### Remove — union into the tombstone lattice

No arbitration: set the bit, drop the element from the position index, leave
its anchor in place for descendants/marks. Concurrent removes commute and
are idempotent; a remove concurrent with gap inserts tombstones its targets
while the inserts order among themselves — independent functions of the op
set, no tiebreak. ("Remove wins, removed is moot": dead is absorbing,
reachable only causally-after life.)

### Move — freeze, do not flip

A register's history is a DAG (nodes = move ops, edges = `overwrites`, root
= creation placement, which every op implicitly overwrites). Rendered
placement:

- `|heads| = 1` → that head's anchor.
- `|heads| > 1` → **last agreed placement**: recurse on the maximal ops that
  *every* head transitively overwrites; bottoms out at the creation
  placement (unique root). **Freeze, do not flip to max-id** — a placement
  is other people's content, so a grindable id must not decide it, and any
  winner-picking rule would let a freely-fabricated conflict re-decide
  settled state (locality dividing line, FRAMEWORK). Conflict surfaced
  either way; the next move naming both heads in `overwrites` dominates and
  resolves it.

### Self-move

`to` resolving into `target`'s own move chain is syntactically checkable and
stable → apply-time quarantine (same gate class as inverted spans,
MARKS.md).

## Apply

O(1) bookkeeping, no replay (nothing is order-dependent):

- intern the id; attach to the named ids by role; update tips;
- insert → fast-path run extension when `tips = {anchor}` and the anchor is
  a run tail, else fork: split the run and start a new one tracked in
  `afters` / `befores_by_anchor`;
- remove → set tombstone bits; single-target removes coalesce into
  RemoveRuns (the delete analog of a typing burst);
- move → `heads(x) = heads(x) − overwrites(u) ∪ {u}` (O(1)); if the
  *rendered* placement changed, one index relocation — excise at origin
  (clear the visibility bit, **keeping the origin ghost: the base slot is
  never removed**), insert as a singleton fragment at the newly rendered
  point (the arriving head's destination on the single-head path; the
  last-agreed placement when the arrival creates a conflict). A re-move
  deletes the stale destination fragment unless content anchored to that
  move op's splice point — splice ghosts materialize lazily, only when
  anchored-to, bounding index growth to live placements plus anchored
  splice points. **Moved-in elements sit exactly at the glued point**:
  after `u`, before all of `u`'s after-children including later inserts at
  `After(u)` (glued semantics, MARKS.md); multiple elements at the same
  point order among themselves by move-op `Id` — the sound id-order use
  (arranges the contenders' own content; displaces nothing). Descendants of
  a moved element (elements chained off it by run formation) stay at the
  origin — run chaining is a causality artifact, not user intent.

The `run_index` treap maintains the rendered order incrementally; per
FRAMEWORK Law II it is a cache pinned equal to the definitional iterator
(`iter_idxs` / `HashSeqIter`) for all delivery orders.

## Validation

- A `Remove` naming a non-insert node is inert, not rejected (no leak).
- Self-moves → permanent quarantine (stable check).
- A `Move`'s anchor must resolve to a valid glued point **in `target`'s own
  object** (an element, the origin, or a move op's own splice point for
  `After(move_op)`); a cross-container destination fails the apply-time gate
  like a malformed anchor — the check is stable, both objects
  hash-committed (HASHWEB_SPEC.md edge table).
- **Stability requirement (realized).** Two already-emitted elements never
  reorder in the **base order** — new inserts only subdivide gaps,
  tombstones keep their run/fragment slot — so `cmp_order(a, b)` over
  element ids is a convergent, permanent total order, computed O(log F) via
  the index's fragment root paths **resolved through origin ghosts only**:
  a moved element's base slot stays in the index for life, and rendered
  relocation must never change a base-order verdict. (Reading the rendered
  slot here is a convergence bug — replicas that have and haven't seen a
  move would disagree on permanent gate verdicts.) Anchor points
  (`Before(c)` / `After(c)`, MARKS.md) are glued and stable for life.
  **Rendered placement is not base order**: a move relocates where an
  element renders, never the base order that anchors, marks, and gate
  verdicts depend on (FRAMEWORK "Stability"). Every higher layer (marks' permanent inverted-span check,
  the move splice-point anchor) borrows the base order's immutability.
- **Retention.** The supersession spine of each placement register
  (superseded ops' ids, `overwrites` edges, and one `Anchor` each) is
  retained — the last-agreed computation walks it. Payloads of superseded
  moves are otherwise droppable (MOVE.md open problem 1).
- **Amplification.** Late delivery costs the same as on-time apply (no
  replay); ground ids decide no placement; fork-spam pins at last-agreed,
  flagged, linear in attacker ops; mass moves degrade the index linearly in
  the attacker's op count (singleton fragments). Full audit table: MOVE.md.

## Open threads

1. **`cmp_order` as a public primitive.** Internal to the index today; marks
   and move both need it — promote during substrate extraction.
2. **Move frontier + commitment vector.** Decide the move ops' frontier
   (LAYERING.md parameter) and whether moves pin their container's frontier
   (the commitment vector — makes move-vs-remove causally decidable).
