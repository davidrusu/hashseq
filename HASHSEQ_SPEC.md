# HashSeq op spec

Framework: FRAMEWORK.md (deps = observed-version commitment; Law I/II; resource
→ conflict → resolution; locality dividing line; stability requirement).
Specifies the existing `src/hashseq.rs` sequence CRDT — the base case, where the
contended resource is a gap and id-order is a sound resolution.

## Op

```rust
enum Op { InsertAfter(Id, char), InsertBefore(Id, char), Remove(BTreeSet<Id>) }
struct HashNode { extra_dependencies: BTreeSet<Id>, op: Op }
// id = BLAKE3::derive_key("hashseq v1 node id", canonical_encoding)
```

## Deps

```
named(u)            = { anchor }          for InsertAfter / InsertBefore
named(u)            = targets            for Remove
extra_dependencies  = tips − named        // tips_minus(anchor); tips ∖ targets
deps(u)             = extra_dependencies ∪ named = tips
```

(`HashNode::iter_dependencies` is exactly this union.) The anchor is "attach-to";
the targets are "replaces". An insert with `tips = {anchor}` carries empty
extra-deps — the typing fast path.

## Resource

Two:

- **a gap** — an `(anchor, side)` pair, claimed by `InsertAfter` / `InsertBefore`;
- **a target's liveness** — claimed by `Remove`.

## Conflict

| op                                                    | resource          | concurrency on it means                           | true conflict?             |
|-------------------------------------------------------|-------------------|---------------------------------------------------|----------------------------|
| `InsertAfter` / `InsertBefore`                        | a gap             | ≥2 concurrent inserts in the same gap             | **yes — order arbitrated** |
| `Remove`                                              | target liveness   | ≥2 concurrent removes of the same target          | no — idempotent union      |
| `Remove` vs the insert it targets                     | —                 | impossible: target ∈ deps, so remove *follows* it | no — never concurrent      |
| `Remove` vs an insert anchored on the removed element | the element's gap | child saw the element; tombstoning ⊥ placement    | no — they compose          |

The sole arbitrated case is **intra-gap insert order**. The three "no" rows:

- concurrent removes are a join-semilattice — `apply_remove` sets an idempotent
  tombstone bit (`self.removed.set(..)`), converging by union;
- a remove can never race the creation of its target (`target → remove` always);
  a remove naming a non-insert node is inert (no index entry, dead bit);
- tombstoning `X` clears only `X`'s visibility; characters `After(X)` saw `X`,
  are causally later, and stay live in their own gap — a delete never drags
  content typed into it.

## Resolution (read time)

### Insert — total order on contending siblings by id

Concurrent inserts in one gap render in **ascending `Id` order** among
themselves (`first_ge`: a new node lands before the smallest present sibling
whose id exceeds it; `afters` / `befores_by_anchor` are id-ordered). Sound under
the locality dividing line: id-order arranges only the contenders' own bytes —
existing elements never reorder (Stability, below), so grinding an id only moves
*your own* char within a gap you are already writing to.

*Non-interleaving (honest scope):* the *k*-th char of a burst anchors `After`
the (*k*−1)-th — not the gap — so a writer's run orders as a contiguous *block*
(its head's id decides), YATA/Yjs-style, not Logoot char-interleave. Not proven
maximally non-interleaving in the Fugue sense
([Weidner & Kleppmann 2023](https://arxiv.org/abs/2305.00583)); adversarial
anchors can still interleave (open thread 1).

### Remove — union into the tombstone lattice

No arbitration: set the bit, drop the element from the position index, leave its
anchor in place for descendants/marks. Concurrent removes commute and are
idempotent; a remove concurrent with gap inserts tombstones its targets while
the inserts order among themselves — independent functions of the op set, no
tiebreak. ("Remove wins, removed is moot": dead is absorbing, reachable only
causally-after life.)

## Apply

O(1) bookkeeping, no replay (nothing is order-dependent):

- intern the id; attach to the named anchor/targets; update tips;
- insert → fast-path run extension when `tips = {anchor}` and the anchor is a
  run tail, else fork: split the run and start a new one tracked in `afters` /
  `befores_by_anchor`;
- remove → set tombstone bits; single-target removes coalesce into RemoveRuns
  (the delete analog of a typing burst).

The `run_index` treap maintains the rendered order incrementally; per FRAMEWORK
Law II it is a cache pinned equal to the definitional iterator (`iter_idxs` /
`HashSeqIter`) for all delivery orders.

## Validation

- A `Remove` naming a non-insert node is inert, not rejected (no leak).
- **Stability requirement (realized).** Two already-emitted elements never
  reorder — new inserts only subdivide gaps, tombstones keep their run/fragment
  slot — so `cmp_order(a, b)` over element ids is a convergent, permanent total
  order, computed O(log F) via the treap's fragment root paths. Anchor points
  (`Before(c)` / `After(c)`, MARKS.md) are glued and stable for life. Every
  higher layer (marks' permanent inverted-span check, move's splice-point
  anchor) borrows this.

## Open threads

1. **Maximal non-interleaving.** Block ordering is not proven FugueMax; decide
   whether to adopt Fugue leftOrigin/rightOrigin at substrate extraction or
   document the weaker guarantee.
2. **`cmp_order` as a public primitive.** Internal to the index today; marks and
   move both need it — promote during substrate extraction.
3. **Shared validate-before-apply gate.** Fold the inert-remove check into the
   shared apply-time gate (MARKS.md inverted-span rule) once typed anchors land,
   rather than per-layer.
