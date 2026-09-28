# Move: reordering without oplog replay (design rationale)

The rationale for HASHSEQ_SPEC.md's `Move` op. The rules live there
(Resolution "Move", Apply, Validation); this document says why they are
what they are, gives the amplification analysis that drives them, and
works through a block editor.

## Goal

Reorder elements **within their container** with preserved identity
(marks, comments, and concurrent child-object edits follow the element),
no duplication under concurrent moves, and **bounded, local cost under
adversarial delivery**. The last requirement drives the design.
Cross-container relocation is not a `Move` (§Reparenting).

## Why a total-order move cannot be imported

Kleppmann et al.'s tree-move CRDT (PRIOR_ART.md §5) is correct in a
crash-fault setting, but its safety mechanism is a **total order over
ops**: moves apply in timestamp order, a move that would create a cycle at
its position is skipped, and a late op triggers undo of every later op,
apply, redo. That assumes lateness is rare and shallow.

In a hash DAG lateness is free: an adversary forks from an ancient tip, or
grinds an op that sorts early, and delivers it now. Every honest replica
undoes and redoes the suffix — one cheap op buys an O(oplog) replay,
repeatably. The vulnerability *is* the order-dependence: **any semantics
that is a function of application order is an amplification machine under
BFT.** Hence FRAMEWORK Law I/II: state is a function of the op set, and
every order-sensitive decision is read-time arbitration over a set.

## The register design

`Move { target, to, overwrites }` is an instance of the supersession
pattern (`Remove` targets, `Put.overwrites`, `Mark.overwrites`,
`Place.overwrites`): one **placement register per element**, initial value
the creation placement, history the `overwrites` DAG. One head (the common
case) renders at its destination; several heads freeze.

### Contested registers freeze, they don't flip

The obvious arbitration — render at the max-`Id` head — violates the
locality dividing line (FRAMEWORK): ids are grindable, so an adversary
mints a concurrent head with a winning id and relocates honest content,
one cheap op per block, beating each honest counter-move until it is
superseded. Worse, a conflict can be fabricated by omission, so **any**
winner-picking rule lets a fabricated conflict re-decide settled state.

So `|heads| > 1` renders at the **last agreed placement**. Consequences:

- *honest ∥ honest* (two users drag one block): the block stays put, both
  clients flag it, and either user's next drag — naming both heads —
  resolves it. No silent teleport.
- *grinding buys nothing*: ids never decide placement.
- *residual*: a forking adversary can **pin** a block at an ancestor
  placement (forking off the creation placement pins it home). Choosing a
  *fresh* destination requires a dominating op — plain, attributable
  vandalism that permissionless write grants anyway. Every instance raises
  a conflict flag, the hook for rate-limiting or quarantining the author.

Concurrent moves of one element render exactly once, identically on every
replica — **no duplication**, the failure of naive remove+reinsert. The
element keeps its creation id, so marks and child-object edits are
untouched by where it renders. Move vs remove: remove wins by absorption
(concurrency is decidable only with the optional commitment vector,
HASHSEQ_SPEC.md "Refs").

### Apply cost, and why there is no replay

Applying a move — early, late, adversarial — updates the head set, re-derives
the register's decider from that register's own history, and relocates at
most one element in the index. Rendered state is a function of the op set,
so there is nothing to undo.

## Reparenting: deliberately not a move

A cross-container `Move` would make placement registers mutable parent
edges, and concurrent mutable parenthood has an irreducible failure mode:
**islands** — "A under B" ∥ "B under A" are individually fine and jointly
unreachable from every root. The natural read-time cycle-break rule is not
schedule-independent (CYCLE_REVERT.md). Restricting `Move` to one
container removes the input: parent edges never change and admission
("destination resolves in `target`'s object") is stable.

Cross-container relocation is `Insert` of the object's link in the
destination plus `Place` in the moved object (PLACEMENT_SPEC.md). An
earlier design used remove + insert of the link; its residue — concurrent
relocations leaving two live links — occurred in practice (APP_NOTES.md
#29), which is why membership is decided by a register single-homed in the
moved object, while order stays with the container's link atoms. Cycles
across containment registers are resolved by D4, detach the SCC
(CYCLE_REVERT.md).

## Index integration

The run-index treap holds both the **base order** and the **rendered
order**. Why each rule in HASHSEQ_SPEC.md "Apply" is there:

- **Origin ghosts.** A moved element's base slot is never removed, and
  base-order verdicts read ghosts only. Reading the rendered slot would let
  replicas that have and haven't seen a move disagree on permanent
  admission verdicts.
- **A rendered move is an insert sibling.** The deciding op joins its
  anchor's fork order keyed by its own id, so later inserts, run
  continuations, and other moved-ins interleave with it by the one sibling
  rule. Moves can grind ids exactly as inserts can — only within their own
  gap — and no second ordering concept or privileged adjacency is needed.
- **Only the element moves.** Run chaining is a causality artifact: a
  moved list item must not drag the items batch-inserted after it.
- **Lazy splice ghosts.** A superseded op keeps a zero-width slot only if
  something anchored at it, bounding index growth by live placements plus
  anchored splice points rather than by move churn.
- The insert tree is untouched: anchoring is convergence-relevant, the
  index is projection.

### Anchoring next to a moved element

`Insert` at `After(x)` for a moved `x` lands at `x`'s origin ghost (base
order is static), but a user inserting below a moved item means "below its
new home". So anchor to the **move op**: its splice point derives from its
`to`, is stable, and needs no new machinery. If a newer move relocates `x`
again, content anchored to the old op stays at the old splice point — the
same semantics as anchoring to a tombstone. `cursor_at` does this
automatically: a moved-in neighbor is represented by its deciding op. An
explicit `After(u)` / `Before(v)` on destination neighbors anchors to the
gap instead.

## Amplification audit

What does one adversarial op cost honest replicas? (`h` = heads, `n` =
history of the one register involved.)

| adversarial action | honest cost | bound |
|---|---|---|
| late-delivered move (any fork depth) | same as on-time apply | no replay; O(1) + one relocation on an uncontested register |
| hash-ground move op | none — ids never decide placement | killed by the freeze rule |
| fork-spam on one register | head-set growth; block pins at last agreed, flagged | a fresh destination is never attacker-chosen; each contested arrival re-walks the register, O(h·n + n²) — see open problem 5 |
| dominating-op vandalism | one relocation | the permissionless-write baseline (as `Remove`); attributable, revertible |
| mass element moves | one singleton fragment each | linear in attacker ops; treap stays O(log F) |
| move-churn ghost spam | none beyond live placements | splice ghosts exist only where something anchored |
| cross-container move | none | never admitted: the foreign id never resolves, the node stays orphaned |

No reshuffle row exists: relocating honest content needs a dominating op
per block, frozen conflicts move nothing, and the last-agreed walk touches
only one register's history.

## Instantiation: a block editor

A block is a `Map` object: `"content" → <seq>`, `"children" → <seq of
object links>`, plus properties (values are ids — HASHKV_SPEC.md).

1. **Nesting is by reference, so subtree drag is one op.** Descendants live
   in the block's own children seq and follow it by doing nothing:
   dragging a toggle with 200 nested blocks is one `Move`.
2. **Reorder is a `Move`; reparent is `Insert` + `Place`.** A drag within
   one seq is `Move { target, to, overwrites }`. Indent/outdent or a
   cross-page move targets a different children seq: insert a link there
   and `Place` the block at it — two ops, one gesture.

Concurrency, all from the register semantics:

- *drag + typing inside the block*: edits address the content object by
  id — orthogonal. (With remove+reinsert, typing into the tombstoned copy
  would be lost.)
- *two users drag different blocks to one gap*: both join the gap's
  sibling order, ordered by move-op id.
- *two users drag one block within a list*: it stays put (freeze), both UIs
  badge it, the next drag resolves. (The industry default silently
  last-writes.)
- *two users move one block to different pages*: concurrent `Place`s
  freeze the containment register at last agreed, flagged
  (PLACEMENT_SPEC.md).
- *move A into B ∥ move B into A*: a containment cycle, detached as a
  flagged cluster (D4).
- *drag to a gap whose neighbor was concurrently dragged away*: the anchor
  resolves at the neighbor's origin ghost — the user pointed at the gap,
  not the departed neighbor.
- *drag into a concurrently deleted container*: the block goes with the
  deleted subtree — detectable, surfaceable.
- *Enter right below a dragged block*: `After(move_op)`.
- *multi-block drag*: one move per block, each anchored after the previous
  block's move op; the ops chain.

Same-list drags grow register histories linearly in real gestures, and
lazy splice ghosts keep the index from paying for the churn.

*Why not fractional indexing* (order keys, the usual choice for this UI):
forgeable LWW writes to a shared keyspace, interleaving anomalies, and
adversarially unbounded key growth; the usual fix is server arbitration,
which a BFT setting lacks. Anchored registers order by ids, splice points
don't grow, and contested placements freeze visibly.

## What v1 does not do: range moves

Moving a *text range* with concurrent edits inside it stays open. A range
move as two glued anchors (a mark) plus a destination is deterministic, but
concurrent overlapping range moves contend per element and the splice
algebra gets hard. v1: element moves only; text cut/paste is remove +
reinsert (fresh identity, marks reattached by the editor).

## Open problems

1. **Register history retention.** The supersession spine (superseded ops'
   ids, `overwrites` edges, one `Anchor` each) is retained — the
   last-agreed walk reads it. Everything else about superseded moves is
   droppable; the conservative answer is keep placement anchors, drop
   nothing else.
2. **Move-op encoding.** Moves ride the wire as individually tagged nodes;
   no block form yet (QUEUE.md).
3. **Commitment vector.** Whether moves pin their container's frontier —
   HASHSEQ_SPEC.md open thread 2.
4. **Multi-link surfacing.** Answered by PLACEMENT_SPEC.md: an object has
   at most one containment placement and any number of references, which
   the render guard handles.
5. **Contested-register resolve cost.** Each arrival on a contested
   register recomputes last-agreed from scratch (per-head ancestor walks,
   then a pairwise maximal filter), so n adversarial ops on one register
   cost superlinearly in n. The walk stays local to that register; an
   incremental resolve would restore a linear bound.
