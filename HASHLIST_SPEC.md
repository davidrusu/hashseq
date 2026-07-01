

# HashList (move) op spec

Framework: FRAMEWORK.md (deps = observed-version commitment; Law I/II; resource →
conflict → resolution). Design rationale: MOVE.md. Anchors: MARKS.md.

Covers the **move op** — list reorder (position register) and tree reparent
(parent register) are the same op; the destination anchor's container implies
which.

## Op

```rust
enum Anchor { Before(Id), After(Id), DocEnd }   // glued point, MARKS.md

struct MoveOp {
    target: Id,                 // element being moved (its creation op id)
    to: Anchor,                 // destination: a glued point
    overwrites: BTreeSet<Id>,   // prior move ops on `target` this op saw and replaces
}

struct MoveNode { extra_dependencies: BTreeSet<Id>, op: MoveOp }
// id = BLAKE3::derive_key("hashlist v1 node id", canonical_encoding)
```

## Deps

```
named(u)            = {target} ∪ {anchor_id(to)} ∪ overwrites
extra_dependencies  = move_layer_tips − overwrites
deps(u)             = extra_dependencies ∪ named
```

The move layer is **its own object, downstream-only**: move ops depend on
target/anchor (text/list ops), never the reverse, so they never enter the
container's tips or fragment its runs (MOVE.md open problem 2). Orphan buffering
covers moves arriving before `target` or `to`.

## Resource

One **register per element**: "where is `target`". Initial value = the creation
placement (the original insert). For tree objects the register holds the parent
edge; for lists it holds the sibling slot. Same register, different read.

```
heads(target) = { m ∈ Moves(target) : ∄ n ∈ Moves(target). m.id ∈ overwrites(n) }
```

## Conflict

Two kinds:

1. **Register conflict** (list & tree): `|heads(target)| > 1` — concurrent moves
   of the same element, neither superseding the other.
2. **Cycle conflict** (tree only, cross-register): the rendered parent edges
   (one head per register) contain a cycle — e.g. clean single-head "A under B"
   and "B under A". **Cycle-ness is not stable** (depends on which moves are
   present), so it is not an apply-time predicate.

## Resolution (read time)

### Register (the freeze rule — NOT max-id)

A register's history is a DAG (nodes = move ops, edges = `overwrites`, root =
creation placement). Rendered placement:

- `|heads| = 1` → that head's anchor.
- `|heads| > 1` → **last agreed placement**: recurse on the maximal ops that
  *every* head transitively overwrites; bottoms out at the creation placement
  (unique root). **Freeze, do not flip to max-id** — a placement is "other
  people's content," so a grindable id must not decide it (locality invariant).
  Conflict surfaced either way; the next drag naming both heads in `overwrites`
  dominates and resolves it.

### Cycle (tree)

- Honest fast path: accepting a move = one O(depth) ancestor walk, no cycle.
- If rendered edges cycle: **all members of the cycle revert** to their previous
  agreed placement (recurse through register history as above; bottoms out at
  creation containment, acyclic by construction → always terminates in a
  forest). **No member wins** — picking by id reopens grinding. Surface the
  broken cycle as a conflict.

### Cross-op

- **Move vs Remove of target**: remove wins; element tombstoned, register moot.
- **Move vs concurrent edits inside target's child object**: orthogonal — child
  edits target the object by id, untouched by where the parent renders.
- **Self-move** (`to` resolves into `target`'s own move chain): syntactically
  checkable, stable → apply-time quarantine (MARKS.md inverted-span class).

## Apply

```
heads(target) = heads(target) − overwrites(u) ∪ {u}     // O(1)
```

If the winning head changed: one index relocation — excise at origin (clear
visibility bit, as for removes), insert as a singleton fragment at the
destination point. **No replay** (nothing is order-dependent). Multiple elements
relocated to the same glued point order among themselves by move-op `Id` — the
sound id-order use (arranges the contenders' own content; HASHSEQ_SPEC.md).

## Validation

- Self-moves → permanent quarantine (stable check).
- Anchor must resolve to a valid glued point (a `Loc::Run`/`Origin`, or the
  move op's own splice point for `After(move_op)`); else fails the apply-time
  gate like a malformed anchor (MARKS.md).
- Retention: the supersession spine (superseded ops' ids, `overwrites` edges,
  and one `Anchor` each) is **retained** per register — both last-agreed and the
  cycle-revert walk it (MOVE.md open problem 3). Payloads otherwise droppable.
