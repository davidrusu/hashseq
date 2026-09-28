# HashSeq op spec

Framework: FRAMEWORK.md (one reference set + honest frontier rule; Law I/II;
resource → conflict → resolution; locality dividing line; stability
requirement). Rationale: MOVE.md (move), MARKS.md (marks). Preimage and
wire: GRAMMAR_SPEC.md, ENCODING_SPEC.md.

A seq object is one op DAG. The ops it admits:

| op       | claims                                 | rules                              |
|----------|----------------------------------------|------------------------------------|
| `Insert` | a gap                                  | this spec                          |
| `Remove` | a target's liveness                    | this spec                          |
| `Move`   | an element's placement register        | this spec (rationale: MOVE.md)     |
| `Mark`   | an (element, kind) formatting register | MARKS.md                           |
| `Place`  | the object's containment register      | PLACEMENT_SPEC.md                  |

`Put` is a map op and is refused in a seq (`WrongObjectKind`).

## Op

```rust
enum Anchor { Before(Id), After(Id) }   // a glued point

enum Op {
    Insert { at: Anchor, payload: Id },
    Remove(BTreeSet<Id>),
    Move   { target: Id, to: Anchor, overwrites: BTreeSet<Id> },
    Mark   { start: Anchor, end: Anchor, kind: Id, value: Id,
             overwrites: BTreeSet<Id> },                    // MARKS.md
    Place  { placed_at: Id, overwrites: BTreeSet<Id> },     // PLACEMENT_SPEC.md
}
struct HashNode { refs: BTreeSet<Id>, op: Op }
// id = BLAKE3::derive_key(NODE_CONTEXT, canonical_encoding)
// NODE_CONTEXT = "hashweb v1 node id": one context for every op kind in the
// family; kinds are tags inside the encoding (GRAMMAR_SPEC.md).
```

- **Glue points.** An anchor names an element (an insert node), the origin,
  or a move op (its splice point — Apply, below). `Before(c)` is crossed
  immediately before `c`, after all of `c`'s before-descendants; `After(c)`
  immediately after `c`, before any of its after-descendants. Anything
  later inserted into the adjacent gap lands on the same side of the point.
- **The side is data, not op kind**: a gap *is* an `(anchor, side)` pair.
- **No end sentinel.** The origin anchors the document start
  (`After(origin)`). End-of-document behavior is the app's: extend a
  boundary span with an overwrite mark, or insert a terminal element and
  anchor `Before` it. Sentinels are user-space objects, not protocol.
- **`Move` is same-container only**: `target` and `to` resolve in the same
  object (Validation). Placement registers therefore never change parent
  edges, and intra-object placement cycles are unrepresentable.
  Cross-container relocation is `Insert` of the object's link in the
  destination plus `Place` in the moved object's own DAG
  (PLACEMENT_SPEC.md); containment cycles across registers are resolved at
  read time by detaching the SCC (D4, CYCLE_REVERT.md).

Storage keeps a normalized split, `HashNode { pins, op }` with
`pins = refs ∖ named` and `iter_refs()` yielding `refs(u)`; the in-memory
payload is `Payload::Char(c)` for text and `Payload::Id` otherwise. Both are
replica-local layout, not wire or identity.

## Refs

```
named(u) = { anchor_id(at) }                                 Insert
named(u) = targets                                           Remove
named(u) = { target, anchor_id(to) } ∪ overwrites            Move
named(u) = { anchor_id(start), anchor_id(end) } ∪ overwrites Mark
named(u) = overwrites                                        Place
refs(u)  = named(u) ∪ frontier pins   // payload, kind, value, placed_at are NOT refs — see Payload
```

- Honest construction: `refs = observed frontier ∪ named` (FRAMEWORK honest
  frontier rule). Anchors are "attach-to"; targets and overwrites are
  "replaces". The typing fast path is `refs = {anchor}`: the anchor *is* the
  frontier.
- Every op, of every kind, enters and pins the object's one frontier
  (LAYERING.md granularity, settled at per-object).
- A node applies only once all its refs have applied; until then it is
  buffered as an orphan. So `overwrites` always name ops applied earlier,
  and a ref that never arrives in this object (e.g. a node of another
  object) keeps the node orphaned forever.
- A move may additionally pin its container's observed frontier (the
  commitment vector), making move-vs-remove concurrency decidable. Nothing
  requires it; v1 does not (open thread 2).
- The encoding stores each id of `refs(u)` once (the sorted refs table,
  GRAMMAR_SPEC.md).

## Payload

The payload is an **id, not a raw value**: a value artifact id (a
kind-tagged canonical value encoding — char, int, bytes, …; `value_id` is
the artifact itself when ≤ 15 bytes, the identity form, else
`BLAKE3::derive_key(VALUE_CONTEXT, encoding)` — GRAMMAR_SPEC.md), an object
id (a link; transclusion when the object lives elsewhere — HASHWEB_SPEC.md),
or an op node id. Well-known artifacts such as `TOMBSTONE` are ordinary
derived value ids, never magic ids.

The *element* is the insert node id — what anchors, marks, removes, and
moves name. The payload is the element's **content commitment**.

- **Value fields are commitments, not references.** They are not in
  `refs(u)`: delivery never waits on them (a text insert must not orphan on
  its own char), and an unresolvable payload is the app-visible `pending`
  state (HASHWEB_SPEC.md), not a delivery condition.
- **Transport is identity-neutral.** Small values encode inline (chars ride
  the run columns); larger ones live in the content-addressed side store.
  The node id is identical either way, so one logical value never yields two
  op identities.
- **Text is unchanged except for identity**: the preimage hashes
  `value_id(char)` (its identity form — the char's artifact bytes, no hash)
  in place of the char.
- **Atoms.** A payload that is not a char renders as an atom: a
  single-element run holding the U+FFFC placeholder, its commitment id in
  the value column (`elem_payloads`). A by-id payload whose value id is a
  char's identity form stores as that char. Atoms never chain (an atom
  neither extends a run nor is extended), ride the wire as individual
  nodes, and are ordinary elements to every projection (movable, markable,
  removable). What a renderer does with a payload it cannot interpret is
  HETEROGENEITY.md's placeholder semantics.

## Resource

- **a gap** — an `Anchor` — claimed by `Insert`;
- **a target's liveness** — claimed by `Remove`;
- **a placement register per element** — "where is `target` within its
  container" — claimed by `Move`. Its initial value is the creation
  placement (the original insert); every value is a glued point in the same
  object.

```
heads(x) = { m ∈ Moves(x) : ∄ n ∈ Moves(x). m.id ∈ overwrites(n) }
```

## Conflict

| op                                                    | resource           | concurrency on it means                           | true conflict?             |
|-------------------------------------------------------|--------------------|---------------------------------------------------|----------------------------|
| `Insert`                                              | a gap              | ≥2 concurrent inserts in the same gap             | **yes — order arbitrated** |
| `Remove`                                              | target liveness    | ≥2 concurrent removes of the same target          | no — idempotent union      |
| `Move`                                                | placement register | `\|heads(x)\| > 1`                                | **yes — freeze**           |
| `Remove` vs the insert it targets                     | —                  | impossible: target ∈ refs, so remove *follows* it | no — never concurrent      |
| `Remove` vs an insert anchored on the removed element | the element's gap  | the child saw the element; tombstoning ⊥ placement | no — they compose         |
| `Move` vs `Remove` of target                          | —                  | remove wins; element tombstoned, register moot    | no — absorption            |
| `Move` vs edits inside target's child object          | —                  | child edits address the object by id              | no — orthogonal            |

Tombstoning `X` clears only `X`'s visibility: content anchored `After(X)`
saw `X` and stays live in its own gap — a delete never drags content typed
into it. Remove-beats-move is an absorption rule, not a causality test:
dead is absorbing whatever the register says.

## Resolution (read time)

### Insert — total order on contending siblings by id

Document order is a traversal from the origin: for each node, the regions
of its before-children in ascending id, then the node, then the regions of
its after-children in ascending id (a run's continuation is an ordinary
after-child). So concurrent inserts in one gap render in **ascending `Id`
order** among themselves. This is sound under the locality dividing line:
existing elements never reorder (Stability), so grinding an id only moves
*your own* element within a gap you are already writing to. The causal
iterator (`iter_idxs_causal`) is the definition; the run index is a cache
of it.

**Anchor rule (honest clients, `cursor_at`)** — the Fugue rule
([Weidner & Kleppmann 2023](https://arxiv.org/abs/2305.00583)) over a
single anchor. For an insert at visible position `i`, with `L` the element
at `i − 1` (a moved-in element is represented by its deciding move op, so
typing next to moved content lands where the user sees it):

1. no `L`: `Before(first visible element)`, or `After(origin)` if empty;
2. something visible lies right of `L` and `L` has after-children
   (tombstones included): `Before(region_first(c))`, where `c` is `L`'s
   first after-child and `region_first` descends first before-children.
   That node is `L`'s traversal successor and has no before-children, so
   the insert lands directly after `L` with no sibling race. (Anchoring
   `Before` the *visible* right neighbor is wrong: it races that
   neighbor's existing, possibly tombstoned, before-siblings by id.)
3. otherwise `After(L)`.

Consequence (convention-scoped — the format does not enforce it): a burst
is a single chain — forward typing an `After`-chain, backward typing a
`Before`-chain — so concurrent bursts at one gap render as contiguous
blocks ordered by their heads' ids. Maximal non-interleaving (FugueMax) is
not claimed.

*Why not dual left/right origins*: a committed interval hands every
malicious peer an inverted `(right, left)` pair, and crossing intervals
from several peers can have no consistent order at all — forcing
per-insert interval validation and a new arbitration surface onto the
hottest op, plus a second ref per insert and an absent-right sentinel. A
single anchor has no interval to invert; an adversary's anchor choice
orders only their own content. Accepted residuals: gap-pinning is client
discipline rather than artifact, and concurrent append-vs-insert intents
encode identically (GRAMMAR_SPEC.md "Op kinds").

### Remove — union into the tombstone lattice

No arbitration: set the bit, drop the element from the rendered index, and
keep its slot for descendants and mark points. Concurrent removes commute
and are idempotent; a remove concurrent with gap inserts tombstones its
targets while the inserts order among themselves.

### Move — freeze, do not flip

A register's history is a DAG: nodes are move ops, edges are `overwrites`
(filtered to moves of the same target), and the root is the creation
placement, which every op implicitly overwrites. Rendered placement:

- no heads → the creation placement;
- `|heads| = 1` → that head's destination;
- `|heads| > 1` → the **last agreed placement**: recurse on the maximal ops
  that *every* head transitively overwrites; it bottoms out at the creation
  placement. **Never flip to a max-id winner**: a grindable id must not
  decide placement of others' content, and any winner-picking rule lets a
  freely fabricated conflict re-decide settled state (MOVE.md). The
  conflict is surfaced (`placement_conflicted`, `move_heads`); the next move
  naming both heads in `overwrites` dominates and resolves it.

## Apply

No replay; state is a function of the op set.

- **all ops**: intern the id; attach to named ids by role; remove `refs(u)`
  from the tips and add the op.
- **insert**: extend the run in place when the anchor is a run tail with no
  after-children and neither the anchor nor the payload is an atom;
  otherwise fork — split the run at a mid-run `After` anchor and start a
  single-element run tracked in `afters` / `befores_by_anchor`, id-ordered.
- **remove**: set tombstone bits. Single-target removes coalesce into
  remove chains (the delete analog of a typing burst).
- **move**: `heads(x) = heads(x) − overwrites(u) ∪ {u}`, then recompute and
  cache the register's decider (single head: O(1); contested: the
  last-agreed walk over that register's own history). If the decider
  changed and `x` is live, one index relocation:
  - **excise** the rendered copy. The base slot is never removed: it stays
    as the **origin ghost**.
  - **render at the deciding op as an insert sibling**: the deciding move op
    joins its anchor's fork order exactly as an insert child at that anchor
    would — keyed by its own id, a leaf holding the moved element. Later
    inserts, run continuations, and other moved-ins interleave with it by
    the one sibling rule; there is no second ordering concept. A mid-run
    `After` anchor splits the run, as for an insert.
  - **only the element moves.** Elements chained off it by run formation
    stay at the origin: run chaining is a causality artifact, not intent.
  - **splice points.** Anything anchored at a move op — inserts
    (`Before/After(move_op)`), mark endpoints, other moves' destinations —
    positions at that op's rank in its anchor's fork order, which is
    permanent. While the op decides, that is the moved element's fragment;
    when it stops deciding (superseded, or the target removed) and something
    is anchored at it, its fragment **demotes in place to a zero-width
    splice ghost** (and promotes back if the register re-agrees on it);
    otherwise it is deleted. Ghosts are lazy: a non-deciding op gets one
    only when something first anchors at it. A splice child never inherits
    the target's conflicts and never follows the element elsewhere.
- **mark**: MARKS.md "Apply". **place**: PLACEMENT_SPEC.md.

The `run_index` treap maintains the rendered order incrementally. Per
FRAMEWORK Law II it is a cache, pinned equal to the definitional iterator
for all delivery orders (`prop_index_matches_iterator`,
`prop_index_matches_iterator_with_moves`).

## Validation

Admission rules run once, when a node's refs have all applied. Each reads
only immutable facts, so every verdict is stable and convergent. A refused
node is dropped: it never enters the tips, and nodes depending on it stay
orphaned.

| op       | refused when                                        | `Refused`          |
|----------|-----------------------------------------------------|--------------------|
| `Insert` | `at` is not a glue point                            | `NotAGluePoint`    |
| `Move`   | `target` is not an element                          | `NotAnElement`     |
| `Move`   | `to` is not a glue point                            | `NotAGluePoint`    |
| `Move`   | `to` names `target` itself (contentless)            | `SelfMove`         |
| `Mark`   | an anchor is not a glue point; the span is inverted | MARKS.md           |
| `Put`    | always (map op)                                     | `WrongObjectKind`  |

Admitted, not refused:

- A `Remove` naming a non-insert node is inert (no index entry, dead bit).
- `overwrites` naming ops of another register or kind are ignored by the
  definitional filter.
- A `Move` or `Remove` of a tombstoned element applies; the element renders
  nowhere.
- A `Move` destination at any move op's splice point, including ops of
  `target`'s own chain: "put x where that op placed it" is well-defined
  because excision precedes placement and op ranks are permanent.
- A cross-container `Move` is never admitted: an id of another object never
  resolves in this one, so the node stays orphaned (HASHWEB_SPEC.md
  admission table).

**Stability.** Two emitted elements never reorder in the **base order**:
new inserts only subdivide gaps, and tombstones keep their slot. The
base-order comparison of two points is O(log F) over the index's fragment
paths, **resolved through origin ghosts only**. A moved element's base
slot stays in the index for life, and rendered relocation never changes a
base-order verdict — reading the rendered slot here would be a convergence
bug (replicas that have and haven't seen a move would disagree on
permanent verdicts). Anchor points are glued and stable for life.
**Rendered placement is not base order**: a move changes where an element
renders, never the base order that anchors, mark *points*, and admission
verdicts depend on. (Mark *membership* samples the rendered crossing
against base-fixed points — MARKS.md "regional".) Higher layers — marks'
inverted-span check, splice-point anchors — rely on this immutability.

**Retention.** Each placement register's supersession spine (superseded
ops' ids, `overwrites` edges, one `Anchor` each) is retained: the
last-agreed walk reads it. Anything else about superseded moves is
droppable (MOVE.md open problem 1).

**Amplification.** Late delivery costs the same as on-time apply (no
replay); ids decide no placement; fork-spam pins an element at last-agreed,
flagged; mass moves grow the index by one singleton fragment each. Audit
table and bounds: MOVE.md "Amplification audit".

## Open threads

1. **Base-order comparison as a public primitive.** Internal today
   (`cmp_points` over `RunIndex::cmp_sweep`); marks and move both use it.
2. **Move commitment vector.** Frontier granularity is settled
   (per-object); open is whether moves also pin their container's frontier,
   making move-vs-remove causally decidable.
