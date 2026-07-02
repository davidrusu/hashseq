# Move: reordering and reparenting without oplog replay (design rationale)

Status: 2026-07-01. The rationale companion to HASHSEQ_SPEC.md's `Move` op —
the normative rules live there; this document records *why* they are what
they are, the amplification analysis that drives them, and the worked
block-editor instantiation.

## Goal

Move semantics for seq objects — reordering elements **within their
container** — with preserved identity (marks, comments, and concurrent
child-object edits follow the element), no duplication under concurrent
moves, and **bounded, local cost under adversarial delivery**. The last
requirement is the design driver. Cross-container relocation is
deliberately *not* an op (§Reparenting below): the same-container
restriction is what makes placement cycles unrepresentable
(CYCLE_REVERT.md).

## Why a total-order move cannot be imported

Kleppmann et al.'s tree-move CRDT (PRIOR_ART.md §5) is correct and elegant
in a crash-fault setting, but its safety mechanism is a **total order over
ops**: moves apply in timestamp order; a move that would create a cycle *at
its position in the order* is skipped; an op arriving out of order triggers
undo of every later op, apply, redo. The cost model assumes lateness is rare
and shallow.

In a hash DAG, lateness is a free choice. An adversary forks from an ancient
tip — or authors an op whose hash sorts early in whatever DAG-derived total
order we'd use — and delivers it now. Honest replicas must undo/redo the
entire suffix: one cheap op buys an O(oplog) replay, repeatable — quadratic
amplification. No tweak fixes this, because the vulnerability *is* the
order-dependence. The same reasoning that rejects LWW rejects undo/redo:
**any semantics that is a function of application order is an amplification
machine under BFT.** Hence FRAMEWORK Law I/II: state is a function of the op
set; every order-sensitive decision moves to read time, where it is
arbitration over a set, never replay of a sequence.

## The register design

Move is the third instance of the supersession pattern (`Remove` targets,
`Put.overwrites`, `Mark.overwrites`):

```rust
Move { target: Id, to: Anchor, overwrites: BTreeSet<Id> }
```

One **placement register per element** — "where is `target`" — whose initial
value is the creation placement and whose history is the `overwrites` DAG.
Read semantics per HASHSEQ_SPEC.md: no moves → creation placement; one head
(the overwhelmingly common case) → that head's anchor; multiple heads →
MVR, surface the conflict, and **freeze**.

### Contested registers freeze, they don't flip

The obvious arbitration — render at the max-`Id` head — violates the
locality dividing line (FRAMEWORK): hash order is grindable, so an adversary
mints a concurrent head with a winning id and *relocates honest content* —
one cheap op per block reshuffles a document, and keeps beating honest
counter-moves until each is superseded. And the withholding lever compounds
it: a "conflict" is freely fabricatable by omission, so **any**
winner-picking rule lets a fabricated conflict re-decide settled state.
Placement of honest content must never be decided by anything an adversary
can grind or fabricate.

So: `|heads| > 1` renders at the **last agreed placement** — recurse on the
maximal ops every head transitively overwrites; the creation placement is
the implicit root each op overwrites, so the recursion is total and bottoms
out. Consequences:

- *honest ∥ honest* (two users drag the same block): the block stays where
  it was, both clients flag it, and either user's next drag — naming both
  heads in `overwrites` — dominates and resolves. No silent teleport;
  arguably better UX than last-write-wins;
- *grinding buys nothing*: ids decide no placement, ever;
- *the residual*: a forking adversary can **pin** a block at an ancestor
  placement (fork-point choice selects which ancestor; forking off the
  creation placement pins it home). They cannot select a *fresh* destination
  by forking — that requires a dominating op, the plain, attributable
  vandalism class permissionless write always grants. Fork-pinning is
  strictly weaker than the vandalism the adversary already has, and every
  instance raises a conflict flag — the policy hook for rate-limiting or
  quarantining the author.

Concurrent move + remove: remove wins — the element is tombstoned and the
register moot (an absorption rule, not a causality test; cross-layer
concurrency is decidable only if the move pins the container frontier — the
optional commitment vector, HASHSEQ_SPEC.md Refs). Concurrent moves of one
element: exactly one rendering per replica, the same on every replica —
**no duplication**, the failure mode of naive remove+reinsert. And identity
loss evaporates for free: the element keeps its creation id, so marks
anchored to it and concurrent edits inside a child object are untouched by
where the parent renders it.

### Apply cost, and why there is no replay

Applying a move — early, late, adversarial — is
`heads = heads − overwrites ∪ {u}` (O(1)), then if the rendered placement
changed, one index relocation. The rendered state is a function of the op
set; delivery order cannot matter. The undo/redo machinery has nothing to
undo because nothing was order-dependent in the first place.

## Reparenting: deliberately not a move

A cross-container `Move` would turn placement registers into mutable parent
edges, and concurrent mutable parenthood has an irreducible failure mode:
**islands** — "A under B" ∥ "B under A" are individually fine and jointly
make both unreachable from every root. Surfacing islands requires a
read-time cycle-break rule, and CYCLE_REVERT.md shows the natural rule is
not schedule-independent (two legal revert schedules reach different
fixpoints) — closing that gap costs either a confluence proof with
stage-faithful incremental maintenance, or a detach-the-SCC rule. The
same-container restriction removes the problem's input instead: parent
edges never change, containment stays the creation forest, and the gate
("destination resolves in `target`'s object") is stable and convergent.

Cross-container relocation is **remove + insert of the object's link**.
Identity is unaffected — the object keeps its id; content, children, marks,
and comment threads live in the object and never notice. What is given up
relative to a register is atomicity under concurrency: two users
concurrently relocating the same block produce two live links — the object
appears in both destinations. This residue is **detectable and flaggable**
(two live link-elements naming one object is a deterministic render-time
condition), so the failure is surfaced duplication resolved by deleting a
link — visible, attributable, and structurally identical to the aliasing
question transclusion raises anyway (HETEROGENEITY.md).

Two related non-problems, for the record: *link cycles* (A links B links A)
need only a deterministic render guard — embed each object at most once per
root-to-leaf path, degrade to a navigation link on repetition — and
*unreachability via removes* is deletion semantics working as intended. If
cross-container moves ever return (a versioned extension through the
envelope path), CYCLE_REVERT.md records D4 (detach the SCC) as the leaning.

## Index integration

The run-index treap holds **both orders** after Move lands, and the
discipline that keeps that sound (HASHSEQ_SPEC.md Apply/Validation):

- a moved element keeps its **origin ghost** — the base slot is never
  removed; `cmp_order` and every permanent verdict resolve through ghosts
  only (reading the rendered slot is a convergence bug: replicas that
  have/haven't seen a move would disagree on permanent gate verdicts);
- the rendered copy is a singleton fragment placed by **insert-sibling
  semantics**: the deciding move op joins its anchor's fork order like an
  insert child, keyed by its own id — later inserts, run continuations,
  and other moved-ins interleave with it by the one sibling rule the tree
  already has (moves ground ids exactly as inserts can; no second
  ordering concept, no privileged adjacency);
- re-moves delete the stale destination fragment; **splice ghosts**
  materialize lazily, only where content actually anchored to a move op's
  splice point — bounding index growth to live placements plus anchored
  splice points rather than total move churn;
- descendants of a moved element (elements chained off it by run formation)
  stay at the origin — run chaining is a causality artifact, not user
  intent: a moved list item must not drag the items that happened to be
  batch-inserted after it;
- the insert tree is untouched: anchoring is convergence-relevant, the
  index is projection.

### Anchoring next to a moved element

`Insert` at `After(x)` where `x` has moved lands at `x`'s *origin* ghost
(base order is static) — but a user inserting below a moved item means
"below its new home." Resolution: **anchor to the move op itself**. The move
op has a well-defined place in the linearization — its splice point — so
`After(move_op)` is stable, intent-preserving, and needs no new machinery —
the splice point derives from the move op's `to`. If a newer
move relocates `x` again, content anchored to the old move op stays at the
old splice point — the same defensible semantics as anchoring to a
tombstone. Cursor logic chooses: `After(move_op)` = relative to x's new
home; `After(u)`/`Before(v)` (destination neighbors) = relative to the gap.

Self-moves (`to` resolving into `target`'s own move chain) are syntactically
checkable and stable → apply-time quarantine (the shared gate class).

## Amplification audit

The question this design exists to answer: what does one adversarial op cost
honest replicas?

| adversarial action | honest cost | bound |
|---|---|---|
| late-delivered move (any fork depth) | same as on-time apply | O(1) + index update — no replay, by construction |
| hash-ground move op (id fighting) | none — ids never decide placement | the attack class the freeze rule kills |
| fork-spam on one element's register | head-set growth; block pins at last agreed placement, flagged | linear in attacker ops; a fresh destination is never attacker-chosen |
| dominating-op vandalism (move honest block to garbage) | one relocation | the permissionless-write baseline (same class as Remove); attributable, revertible |
| mass element moves | singleton fragments in the treap | linear in attacker ops; treap stays O(log F) |
| move-churn ghost spam | none beyond live placements | splice ghosts are lazy — only anchored-to splice points persist |
| cross-container relocation games | n/a — not an op | the gate quarantines cross-container destinations; re-link duplication is flagged, per-op bounded, resolved by one delete |

No reshuffle row exists because none is reachable: relocation of honest
content requires a dominating op per block (linear, attributable), frozen
conflicts move nothing, and the last-agreed walk touches only a register's
own history. (The former worst cells — cycle bombs and deep-nesting walks —
left the table with cross-container moves; CYCLE_REVERT.md.)

## Instantiation: a block editor

The block-document model is the motivating consumer, and it is the *easy*
case. A block is a `Map` object: `"content" → <seq>`, `"children" → <seq of
object links>`, plus properties (all values are ids — HASHKV_SPEC.md). Two
consequences do most of the work:

1. **Nesting is by reference, so subtree drag is one op.** A block's
   descendants live in its own children seq; the subtree was never
   positionally encoded, so it follows the block by doing nothing. Dragging
   a toggle with 200 nested blocks = one `Move`.
2. **Reorder is the op; reparent is a re-link.** A drag within the same
   seq is `Move { target, to, overwrites }` — one register per block. An
   indent/outdent or cross-page move targets a *different* children seq, so
   it is remove + insert of the block's link: two ops, chained, still one
   gesture, with concurrent same-block re-links surfacing as flagged
   duplication rather than a frozen conflict.

The drag-and-drop concurrency matrix, all falling out of the register
semantics:

- *drag + concurrent typing inside the block*: edits target the block's
  content object by id — fully orthogonal. The headline win over
  remove+reinsert, where typing into the tombstoned copy loses;
- *two users drag different blocks to the same gap*: both join the same
  sibling fork order, ordered by move-op id — clean interleave;
- *two users drag the same block within one list*: the block stays put
  (freeze), both UIs badge it, either user's next drag dominates and
  resolves (the industry default silently last-writes here; this is
  better);
- *two users drag the same block to different pages*: two re-links — the
  block appears in both, flagged as multi-linked; either user deletes one.
  Visible and attributable, never a silent loss;
- *drag A into B ∥ drag B into A* (cross-page): two re-links forming a link
  cycle — harmless: the embed guard renders the inner occurrence as a
  navigation link; no content is lost, nothing reverts;
- *drag to a gap whose neighbor was concurrently dragged away*: the
  destination anchor resolves at the neighbor's origin ghost — the block
  lands in the intended seq at the intended slot (the user pointed at the
  gap, not at the departed neighbor);
- *drag into a concurrently deleted container*: the block goes with the
  deleted subtree — detectable, surfaceable ("moved into deleted content");
- *Enter to create a block right below a dragged one*: `After(move_op)` —
  the splice-point anchor was designed for exactly this gesture;
- *multi-block drag*: one move per selected block, each destination anchored
  after the previous block's move op — the ops chain and can
  session-compress like a run.

Indent/outdent are *high-frequency* gestures (Tab/Shift-Tab while
organizing) and are re-links: remove + insert pairs that chain and
session-compress like any edit run. Same-list drags grow register histories
linearly in real gestures, superseded placements droppable per open
problem 1, and the lazy splice-ghost rule keeps the index from paying for
the churn.

Contrast with the industry default for this exact UI — fractional indexing
(order keys): forgeable LWW writes to a shared keyspace, interleaving
anomalies, and adversarially unbounded key growth from repeated gap
splitting; the usual fix is server arbitration, the thing a BFT setting
doesn't have. Anchored registers sidestep all three: order comes from ids,
splice points don't grow, and contested placements freeze visibly instead of
going to whoever ground the better key.

## What v1 does not do: range moves

Moving a *text range* (cut/paste of a paragraph with concurrent edits inside
it) is the open research problem, and it stays unsolved here. The tempting
direction — a range move as a pair of glued anchors (exactly a mark) plus a
destination — gives deterministic semantics, but concurrent *overlapping*
range moves contend per-element and the splice algebra gets genuinely hairy.
v1: element moves only; cut/paste of text is remove + reinsert (fresh
identity, marks reattached by the editor), documented as such.

## Open problems

1. **Register history retention.** The supersession spine (superseded ops'
   ids, `overwrites` edges, one `Anchor` each) is retained per register —
   the last-agreed computation walks it. Payloads of superseded moves are
   otherwise droppable; the cheap conservative answer is keep placement
   anchors, drop nothing else.
2. **Anchor-to-move-op ergonomics.** Encoding (move ops in the ref spaces —
   ENCODING_SPEC.md block kinds) and the cursor-selection heuristics
   (`After(move_op)` vs gap neighbors) need a pass alongside marks.
3. **Move frontier + commitment vector.** Which frontier a move pins
   (LAYERING.md parameter) and whether moves pin their container's frontier
   — tracked as HASHSEQ_SPEC.md open thread 2.
4. **Multi-link surfacing.** Whether the renderer flags "one object,
   several live links" generically (the re-link duplication residue and
   transclusion aliasing are the same condition) — shared with
   HETEROGENEITY.md's transclusion open problem.
