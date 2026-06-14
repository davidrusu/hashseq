# Move: reordering and reparenting without oplog replay (design sketch)

Status: design sketch, 2026-06-12. Nothing here is implemented. Companion to
HASHDOC.md (which punts on move — this is the un-punt) and MARKS.md (whose
anchor machinery is reused for destinations).

## Goal

Move semantics for HashDoc — reordering elements within a list, and
reparenting objects across containers — with preserved identity (marks,
comments, and concurrent child-object edits follow the element), no
duplication under concurrent moves, and **bounded, local cost under
adversarial delivery**. The last requirement is the design driver.

## Why Kleppmann's tree move cannot be imported

Kleppmann et al.'s tree-move CRDT (2021) is correct and elegant in a
crash-fault setting, but its safety mechanism is a **total order over ops**:
moves apply in timestamp order; a move that would create a cycle *at its
position in the order* is skipped (kept in the log); an op arriving
out of order triggers undo of every later op, apply, redo. The cost model
assumes lateness is rare and shallow.

In a hash DAG, lateness is a free choice. An adversary forks from an ancient
tip — or simply authors an op whose hash sorts early in whatever
DAG-derived total order we'd use — and delivers it now. Honest replicas must
undo/redo the entire suffix: one cheap op buys an O(oplog) replay,
repeatable, i.e. quadratic amplification. No tweak fixes this, because the
vulnerability *is* the order-dependence. The same reasoning that rejected
LWW in HASHDOC.md rejects undo/redo here: any semantics that is a function
of application order is an amplification machine under BFT.

The design rule that follows: **state must remain a function of the op set**
(hashseq's merge law — union, commutative, associative), and every
order-sensitive decision moves to read time, where it is arbitration over a
set, not replay of a sequence.

## List reorder: a position register per element

Kleppmann's own list-move design (PaPoC 2020) is already order-free: move =
assignment to a per-element position register, concurrent moves collapse by
register semantics. Loro ships this with LWW registers. We swap LWW for the
HashKv Put pattern — explicit supersession + MVR + hash arbitration — making
this the third instance of the same op shape:

```rust
struct MoveOp {
    target: Id,               // the element being moved (its creation op id)
    to: Anchor,               // destination: a glued point, as in MARKS.md
    /// Prior move ops on this target that this op saw and replaces —
    /// the Put/Remove/Mark `overwrites` pattern, fourth verse.
    overwrites: BTreeSet<Id>,
}
// deps = {target, to's id} ∪ overwrites (+ extra_deps from the move layer's
// own tips). Orphan buffering covers moves arriving before their element.
```

Read semantics, per element:

- live heads = move ops on `target` not named in another's `overwrites`;
  no moves → the element renders at its insertion position, as today;
- one head (the overwhelmingly common case) → element renders at that
  anchor point; sequential moves by one writer chain into a tidy register
  history;
- multiple heads = true concurrency → MVR: surface the conflict, and
  **freeze — don't flip** (next section).

### Contested registers freeze, they don't flip

The obvious arbitration — render at the max-`Id` head — violates the
locality invariant (HASHDOC.md): hash order is grindable, so an adversary
mints a concurrent head with a winning id and *relocates honest content* —
one cheap op per block reshuffles a document, and keeps beating honest
counter-moves until each is superseded. Placement of honest content must
never be decided by anything an adversary can grind.

Rule: a register's history is a DAG — ops are nodes, `overwrites` are
edges, rooted at the creation placement. The rendered placement is:

- the head, if there is exactly one;
- otherwise the **last agreed placement**: recurse on the maximal ops that
  every head transitively overwrites. Terminates at the creation placement
  (the unique root). The conflict is flagged to the app either way.

Consequences:

- *honest ‖ honest* (two users drag the same block): the block stays where
  it was, both clients flag it, either user's next drag — which names both
  heads in `overwrites` and so dominates — resolves it. No silent teleport;
  arguably better UX than last-write-wins;
- *grinding buys nothing*: ids decide no placement, ever. The entire attack
  class max-`Id` would have enabled is gone;
- *the residual attack*: an adversarial fork can **pin** a block at an
  ancestor placement (their choice of fork point selects which ancestor —
  forking off the creation placement pins it home). They cannot select a
  fresh destination by forking; that requires a dominating op, which is the
  plain, attributable vandalism class (same as Remove of honest text) that
  permissionless write always grants. Fork-pinning is strictly weaker than
  the vandalism the adversary already has, and every instance raises an
  explicit conflict flag — the policy hook for rate-limiting or quarantining
  the author.

Cost: the rule needs the supersession spine of each register retained
(superseded ops' ids and edges, not necessarily payloads) — this revises
open problem 3 below. Spines grow linearly in genuine moves plus
attributable adversarial ops.

Concurrent move + remove: remove wins — the element is tombstoned and the
register is moot (matches the PaPoC design and automerge's posture).
Concurrent moves of the same element: exactly one winner per replica, same
winner on every replica — **no duplication**, the failure mode of naive
remove+reinsert.

And the other naive-move failure — identity loss — evaporates for free:
the element keeps its creation id, so marks anchored to it (MARKS.md) and
concurrent edits inside a child object (HashDoc `Ref`s) are completely
untouched by where the parent renders it. For object lists, which is the
real use case, move composes with everything else by doing nothing.

### Apply cost, and why there is no replay

Applying a move — early, late, adversarial, whatever — is:
`heads = heads − overwrites ∪ {new}` (O(1)), then if the winning head
changed, one index relocation (below). The rendered state is a function of
the op set; delivery order literally cannot matter. The undo/redo machinery
has nothing to undo because nothing was order-dependent in the first place.

### Index integration

A relocated element is excised at its origin (visibility bit clears — the
treap already does this for removes) and enters the index as a singleton
fragment at the destination point. Multiple elements relocated to the same
glued point order by move-op `Id` — a legitimate use of hash order under
the locality invariant: it arranges the contending ops' own content
relative to each other and displaces nothing. Fragment count grows by O(moved
elements) — an adversary mass-moving elements degrades the index linearly
in *their* op count, never superlinearly.

The insert tree is untouched: anchoring is convergence-relevant, the index
is projection. Descendants of a moved element (chars/items chained off it
by run formation) stay at the origin — run chaining is a causality
artifact, not user intent, so a moved list item must not drag the items
that happened to be batch-inserted after it.

### Anchoring next to a moved element

The wart to resolve: `InsertAfter(x)` where `x` has moved lands at `x`'s
*origin* ghost (tree semantics are static), but a user inserting below a
moved item means "below its new home". Proposed resolution: **anchor to the
move op itself**. The move op is a node with a well-defined place in the
linearization — the splice point — so `InsertAfter(move_op_id)` is stable,
intent-preserving ("after x where I saw it"), and needs no new machinery:
runs can anchor at a move op like any node. If a newer move relocates `x`
again, items anchored to the old move op stay at the old splice point — the
same defensible semantics as anchoring to a tombstone. Cursor logic chooses:
`After(move_op)` = relative to x's new home; `After(u)`/`Before(v)` (the
destination neighbors) = relative to the gap.

Self-moves (`to` resolving to `target` itself or its own move chain) are
syntactically checkable and stable → apply-time quarantine, same as
inverted spans in MARKS.md.

## Tree reparenting: a parent register per object

Moving an object between containers is the same op — `target` is the
object's creation id, `to` is a glued point in the destination container —
but introduces the cycle problem: concurrent "A under B" and "B under A"
are individually fine and jointly a cycle.

The crucial structural difference from MARKS.md's inverted-span check:
**cycle-ness is not stable.** Whether a move creates a cycle depends on
which other moves are present, and that changes as ops arrive. An
apply-time verdict would have to be revised — which is precisely why
Kleppmann's design replays. So cycles are resolved at **read time**,
where re-evaluation is natural:

- each object has a rendered parent edge (per the freeze rule above; note a
  cycle can arise even among *uncontested* registers — "A under B" and
  "B under A" are two clean single-head registers that jointly cycle);
- on the honest fast path, accepting a move costs one ancestor walk —
  O(depth) — and finds no cycle; done;
- if the rendered edges contain a cycle, **all members of the cycle revert**
  to their previous agreed placement (recursing through register history as
  in the freeze rule; bottoming out at creation containment, which is
  acyclic by construction, so this always terminates in a forest). No
  member wins — picking a winner by op `Id` would reopen the grinding
  surface the freeze rule just closed, and symmetry keeps the rule
  convergent without favoritism;
- a broken cycle **is a conflict and is surfaced** (MVR philosophy): the
  app sees "these concurrent moves contended", not a silent reshuffle. An
  adversary who entangles an honest move in a crafted cycle reverts that
  one named move — bounded, flagged, attributable.

Convergent because it is a pure function of the register sets; order-free
because nothing depends on when ops arrived; local because recomputation
touches only the affected component, never the oplog.

## Amplification audit

The question this design exists to answer: what does one adversarial op
cost honest replicas?

| adversarial action | honest cost | bound |
|---|---|---|
| late-delivered move (any fork depth) | same as on-time apply | O(1) + index update — no replay, by construction |
| hash-ground move op (id fighting) | none — ids never decide placement | the attack class the freeze rule exists to kill |
| fork-spam on one element's register | head-set growth; block pins at last agreed placement, flagged | linear in attacker ops; destination never attacker-chosen |
| dominating-op vandalism (move honest block to garbage) | one relocation | the permissionless-write baseline (same class as Remove); attributable, revertible |
| mass element moves | singleton fragments in the treap | linear in attacker ops, treap stays O(log F) |
| deep-nesting then move (tree) | ancestor walk per move | O(depth); depth costs the attacker one op per level |
| cycle bombs over honest subtrees | cycle members revert + recompute | bounded by affected component, never the oplog; flagged below |

No reshuffle row exists because none is reachable: relocation of honest
content requires a dominating op per block (linear, attributable), frozen
conflicts move nothing, and reverts only walk a register's own history.

Nothing here is order-dependent, so nothing replays. The one honestly
sub-par cell is cycle-break recomputation: an attacker repeatedly minting
new max-`Id` cyclic moves over a large honest subtree forces
component-sized recomputes per op — linear, not quadratic, but the constant
is the component. If profiles ever show it matters, the mitigations are
policy-level (rate/reputation gating on move ops per author), not
protocol-level.

## Instantiation: a Notion-style block editor

The block document model is the motivating consumer, and it is the *easy*
case — every hard sub-problem above either vanishes or hits its cheap path.

A block is a map object: `"content" → Text`, `"children" → List<Ref>`,
plus properties. Two consequences do most of the work:

1. **Nesting is by reference, so subtree drag is one op.** A block's
   descendants live in its own children list, keyed by the block's id —
   the subtree was never positionally encoded, so it follows the block by
   doing nothing. Dragging a toggle with 200 nested blocks = one `MoveOp`.
2. **Reorder and reparent are the same op.** The destination anchor is a
   glued point in *some* children list, and the anchor id implies which
   list — so a drag (same list), an indent/outdent (previous sibling's
   children / grandparent's list), and a cross-page move are all
   `MoveOp { target, to, overwrites }`. One register per block: "where am
   I". Its initial value is the creation placement (the original Ref
   insert); moves supersede it.

The drag-and-drop concurrency matrix, all falling out of the register
semantics:

- *drag + concurrent typing inside the block*: edits target the block's
  content object by id — fully orthogonal, never conflicts. The headline
  win over remove+reinsert, where typing into the tombstoned copy loses;
- *two users drag different blocks to the same gap*: both splice at the
  same glued point, ordered by move-op `Id` — clean interleave;
- *two users drag the same block*: the block stays put (freeze rule), both
  UIs badge it as contested, and either user's next drag dominates both
  heads and resolves it (Notion silently last-writes here; we can do
  better);
- *drag A into B's children ‖ drag B into A's children*: the cycle case —
  read-time break per above; block documents are shallow, so the O(depth)
  ancestor walk is trivially cheap;
- *drag to a gap whose neighbor was concurrently dragged away*: the
  destination anchor resolves at the neighbor's origin ghost — the block
  lands in the intended list at the intended slot (the user pointed at the
  gap, not at the departed neighbor);
- *drag into a concurrently deleted container*: the block goes with the
  deleted subtree — consistent with Notion's trash semantics; the app can
  surface "moved into deleted content" since the condition is detectable;
- *Enter to create a block right below a dragged one*: the
  anchor-to-move-op mechanism — `InsertAfter(move_op)` — was designed for
  exactly this gesture;
- *multi-block drag*: one move op per selected block, each block's
  destination anchored after the previous block's move op — the ops chain
  like an insert run and can session-compress the same way.

Indent/outdent deserve a note: they are *high-frequency* moves (Tab/Shift-
Tab while organizing), each superseding the block's previous placement —
register histories grow linearly in real gestures, with superseded ops
droppable per open problem 3.

Projection per list: native elements, minus moved-away blocks, plus a
splice table (glued point → moved-in blocks, ordered by op `Id`)
maintained O(1) per move. Page-scale block counts make all of this noise.

Worth contrasting with the industry default for this exact UI: fractional
indexing (Figma-style order keys). Fractional keys are LWW writes to a
shared keyspace — forgeable order, interleaving anomalies, and an
adversary can force unbounded key growth by repeatedly splitting the same
gap; the usual fix is server arbitration, which is the thing a BFT setting
doesn't have. Anchored registers sidestep all three: order comes from ids,
splice points don't grow, and contested placements freeze visibly instead
of going to whoever ground the better key.

## What v1 does not do: range moves

Moving a *text range* (cut/paste of a paragraph with concurrent edits
inside it) is the open research problem — Kleppmann flags it as unsolved,
and it stays unsolved here. The tempting direction: a range move is a pair
of glued anchors (exactly a mark) plus a destination, with chars between
the points — including concurrent arrivals — rendering at the splice. That
gives deterministic, convergent semantics, but concurrent *overlapping*
range moves contend per-char and the splice algebra gets genuinely hairy.
v1: element moves only; cut/paste of text is remove + reinsert (fresh
identity, marks reattached by the editor), documented as such.

## Open problems

1. **Cycle-break incrementality.** The greedy max-`Id` edge acceptance is
   convergent but the incremental algorithm (what to recompute when one
   register changes) needs working out — union-find-ish, dirty-component
   tracking.
2. **Move-layer tips.** Same argument as marks: move ops must not enter the
   container's text/list tips or they fragment runs. Own layer,
   downstream-only. Needs the same per-object-tips treatment when the
   substrate lands.
3. **Register history retention.** Revised by the freeze rule: the
   supersession spine (superseded ops' ids and `overwrites` edges) must be
   retained per register — both the last-agreed computation and the cycle
   revert walk it. Payloads of superseded ops are still droppable except
   for placements that a revert can land on; the cheap conservative answer
   is to keep placement anchors (they are one `Anchor` each) and drop
   nothing else.
4. **Anchor-to-move-op interactions.** `InsertAfter(move_op)` is proposed
   above; the encoding (move ops in the dict/positional ref space) and the
   cursor-selection heuristics need a pass once marks land.
