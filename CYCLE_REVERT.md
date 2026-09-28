# Cycle revert: containment cycles

Why containment cycles are resolved by **D4 — detach the SCC**, the rule
stated normatively in PLACEMENT_SPEC.md "Cycles — D4, detach the SCC".
This document holds the problem, the counterexample that rules out
iterate-and-revert, and the acceptance criteria an implementation must
meet. D4 is **not yet implemented** in the crate (QUEUE.md 58).

## Findings that stand outside the placement register

1. **Link cycles need only a render guard.** An embedding renderer embeds
   each object at most once per root-to-leaf path and degrades to a
   navigation link on repetition: deterministic, local, convergent. For
   links, unreachability is deletion (intended), so no protocol mechanism
   is needed.
2. **Containment cycles are an island problem.** Cyclic placements make
   content unreachable from every root; the question is where orphaned
   content surfaces, not renderer termination.

## Setup

Each object has one containment register (PLACEMENT_SPEC.md); only its
parent edge matters here (sibling order cannot create cycles). The op
set determines a **fallback chain** per register:

```
L(x) = [ p₀(x), p₁(x), …, root(x) ]
```

`p₀` is the rendered placement (the single head, or last-agreed under
freeze); each `pᵢ₊₁` is the next agreed placement strictly below `pᵢ` in
the register's overwrites DAG, ending at the register root. `L(x)` is
finite, non-empty, and a pure function of the op set.

- **All-creation is acyclic**: to create X inside Y, Y must exist first,
  so creation edges follow causal order — the guaranteed floor.
- **Cycles need no conflicts**: "A under B" and "B under A" can be two
  clean single-head registers, so cycle handling runs even when no
  register is contested.

Scope: one containment register per object. Transclusion's multi-slot
references are handled by the render guard (finding 1).

## Requirements

`Render : op set → (x ↦ a position in L(x), or detached)` must be:

- **R1 acyclic** — the rendered placements form a forest;
- **R2 pure** — a function of the op set alone (Law I);
- **R3 no winner** — no cycle member keeps its placement by id (grindable)
  or by any rule a fabricated conflict could exploit;
- **R4 bounded blast radius** — registers not implicated by a cycle render
  at `p₀`;
- **R5 component-local cost** — recompute after one op is bounded by the
  affected component, never the oplog;
- **R6 deterministic flagging** — the flagged set is a pure function of the
  op set;
- and (Law II) admit an **incremental cache** provably equal to batch
  recompute under every delivery order.

## Why iterate-and-revert fails

The natural rule — "members of a cycle advance down their chains until
acyclic" — has a non-monotone step: advancing one register can dissolve
one cycle and create another, so fixed-point confluence arguments do not
apply. It does not even define a function:

```
L(A) = [ under B, under C, under R ]
L(B) = [ under A, under R ]
L(C) = [ under A, under R ]
p₀ edges: A→B, B→A, C→A        cycle {A, B}; C hangs off A
```

Synchronous (advance every member of every cycle):

```
step 1: A → C, B → R     A→C, C→A: new cycle {A, C}
step 2: A → R, C → R     acyclic      final: A@R, B@R, C@R
```

Advancing only A:

```
step 1: A → C            cycle {A, C}
step 2: A → R            acyclic      final: A@R, B under A, C under A
```

Two fixpoints from one op set (a Law I violation). The synchronous run
also shows **transient justification** (B loses its placement to a cycle
that A's reversion dissolves anyway) and **bystander cascade** (C, never
on the original cycle, is reverted).

## Candidates

| | rule | verdict |
|---|---|---|
| D1 | synchronous iterate-and-revert (advance all cycle members one step, repeat) | rejected: deterministic and terminating, but has transient justification and bystander cascade, and an incremental cache must reproduce the exact synchronous stage sequence |
| D2 | minimal reversion (max registers at `p₀`, subject to acyclicity) | rejected: minimum-feedback-arc-set, NP-hard (R5); symmetric ties breakable only by id (R3). Restoration passes reintroduce winner-picking or oscillation |
| D3 | sequential acceptance over a canonical op order | rejected: order-dependence is the amplification machine (MOVE.md); an id order is grindable |
| — | jump cycle members straight to the root | rejected: mixed root/`p₀` graphs still cycle, so it iterates again (D1 with bigger steps) |
| **D4** | detach every member of a cyclic SCC of the `p₀` graph, once | **adopted** |

Why D4:

- **Confluence is structural**: detaching only removes edges, so it
  cannot create cycles — no iteration, no schedule.
- **Smallest blast radius**: the flagged set is exactly the `p₀`-cycle
  members (R4, R6). In the counterexample {A, B} detach and C keeps its
  placement.
- **No winner**: all members detach equally; a fabricated cycle earns the
  attacker a flag, never a destination (R3).
- **Standard incremental algorithm**: recompute SCCs of the affected
  component per op, O(component), with no stage replay.
- Cost: a detached cluster is more jarring UX than reverting to an old
  place; the next `Place` naming the heads resolves it either way.

## Acceptance criteria

An implementation of D4 must ship with:

1. the definitional function, with proofs of totality, termination,
   determinism, acyclicity, and R3;
2. schedule-independence (free for D4) and an incremental algorithm
   proven equal to it under all delivery orders (the Law II cache
   invariant);
3. per-op cost bounded by the affected component, with the cycle-bomb row
   of the amplification audit re-verified (PLACEMENT_SPEC.md);
4. a property harness: definitional recompute vs incremental maintenance
   over randomized op sets *and* delivery orders, generators biased toward
   chained supersessions, mixed root/moved edges, and cycle bombs — the
   containment analog of `prop_index_matches_iterator`;
5. UI semantics for the detached cluster (a product decision as much as a
   protocol one).

PLACEMENT_SPEC.md open threads 1–2 track items 2–4.
