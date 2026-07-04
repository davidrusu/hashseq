# Cycle revert: the full problem statement (open design problem)

Status: 2026-07-01, problem statement — resolved the same day by
dissolution (same-container restriction). **Reopened and decided
2026-07-04**: cross-container containment returned as the `Place` op
(PLACEMENT_SPEC.md), and **D4 — detach the SCC — is adopted**, exactly as
the leaning below records. The counterexample against the naive
iterate-and-revert rule stands and remains the reason D1 is rejected. The
acceptance criteria below are the implementation obligations
(PLACEMENT_SPEC.md open threads 1–2 track the incremental-cache and
property-harness items).

## Resolution history: first dissolved, then decided

*2026-07-01:* `Move` restricted to same-container destinations
(HASHSEQ_SPEC.md — a stable gate): parent edges never change, containment
stays the creation forest, and placement cycles are unrepresentable. The
problem below has no input. Cross-container relocation is remove + insert
of the object's link; its concurrent-relocation residue is detectable
multi-link duplication, flagged rather than frozen (MOVE.md
"Reparenting").

*2026-07-04:* that residue proved live (APP_NOTES.md #29 — duplicated
blocks and images in a real document under concurrent structural edits).
`Place` reintroduces mutable containment as a per-object register; the
problem below has an input again, scoped exactly as the setup assumes
(one containment register per object), and D4 is the adopted rule:
contested registers freeze at last-agreed, frozen edges feed the p₀ graph
like any other (an old agreed edge can still join a fresh cycle), and
cyclic SCCs of the p₀ graph detach as flagged root-level clusters, one
pass, no schedule.

Two findings from the analysis survive elsewhere:

1. **Link cycles need only a render guard.** An embedding renderer embeds
   each object at most once per root-to-leaf path and degrades to a
   navigation link on repetition — deterministic, local, convergent. For
   links, unreachability is *deletion* (intended), not loss, so no
   protocol mechanism exists or is needed.
2. **The move-cycle problem is an island problem.** Cyclic placements make
   content unreachable from every root — the candidates below are answers
   to "where does orphaned content surface," not to renderer termination.
   If cross-container moves ever return, **D4 (detach the SCC)** is the
   recorded leaning: it is the only candidate whose confluence is
   structural rather than proved.

---

## Setup

Objects form a containment structure via placement registers
(HASHSEQ_SPEC.md `Move`). For this problem only the **parent projection**
matters: sibling reorder within one container cannot create containment
cycles, so the cycle machinery sees each register as a parent edge.

Per register `x`, the op set determines a **fallback chain**

```
L(x) = [ p₀(x), p₁(x), …, creation(x) ]
```

where `p₀(x)` is the normal rendered placement (the single head's anchor,
or the last-agreed placement if multi-head — the freeze rule), and each
`pᵢ₊₁` is the next agreed placement strictly below `pᵢ` in the register's
overwrites-DAG, bottoming at the creation placement. `L(x)` is finite,
non-empty, and a pure function of the op set.

Two structural facts:

- **all-creation is acyclic**: to create X inside Y, Y must exist first, so
  creation edges follow causal order and the assignment "everything at
  creation" is always a forest — the guaranteed floor;
- **cycles need no conflicts**: "A under B" and "B under A" can be two
  clean single-head registers that jointly cycle — so the cycle machinery
  must run even when no register is contested.

(Scope assumption: one placement register per object; transclusion's
multi-slot containment is a separate problem with a separate mechanism —
render-cycle placeholders, HETEROGENEITY.md.)

## The task

Define a function

```
Render : op set → ( x ↦ a position in L(x) )
```

satisfying:

- **(R1) acyclic** — the chosen placements form a forest;
- **(R2) pure** — a function of the op set alone (Law I): no delivery
  order, no clock, no replica identity;
- **(R3) no winner** — no member of a cycle keeps its placement by id
  (grindable) or by *any* rule a fabricated conflict could exploit
  (withholding): entering the conflicted path must confer nothing;
- **(R4) bounded blast radius** — registers not implicated by a cycle
  render at `p₀`; the reverted/flagged set should be as small as the other
  requirements allow ("revert everything to creation" is trivially correct
  and useless);
- **(R5) component-local cost** — recomputation after one op is bounded by
  the affected component, never the oplog (the amplification audit row);
- **(R6) deterministic flagging** — the set of registers surfaced as
  cycle-conflicted is itself a pure function of the op set.

Additionally, per Law II the definitional function must admit an
**incremental cache**: a replica maintaining the render op-by-op must
provably reach the same result as batch recomputation, for every delivery
order.

## Why it is hard

### The step operator is non-monotone

The natural rule — "registers on a cycle advance down their chains" — has a
step operator whose input includes cycle membership, and advancing one
register can *remove* another from a cycle (that is the point) or *add* a
third to a new one. Positions only ratchet downward, but justification does
not: a register can be advanced because of a cycle that a later step would
have dissolved anyway. Classic fixed-point confluence arguments (monotone
chaotic iteration) do not apply.

### Counterexample: the naive rule is not schedule-independent

Creation: A, B, C all at root R. Histories give chains

```
L(A) = [ under B, under C, under R ]     (a move to C, superseded by a move to B)
L(B) = [ under A, under R ]
L(C) = [ under A, under R ]
```

`p₀` edges: A→B, B→A, C→A — one cycle {A, B}, with C hanging off A.

Two runs of "revert members of a cycle until acyclic":

**Synchronous** (advance every member of every current cycle):

```
step 1: A → under C, B → under R     new graph: A→C, B→R, C→A — NEW cycle {A, C}
step 2: A → under R, C → under R     acyclic
final:  A@R, B@R, C@R
```

**Chaotic, advancing only A** (a legal schedule under the naive wording):

```
step 1: A → under C                  graph: A→C, B→A, C→A — cycle {A, C}
step 2: A → under R                  graph: A→R, B→A, C→A — acyclic
final:  A@R, B@under A, C@under A
```

Two different fixpoints from one op set. **The rule as currently worded
does not define a function** — it defines a family of results indexed by
schedule, which is exactly the order-dependence Law I forbids. Any solution
must either canonicalize the schedule or adopt a rule with no schedule.

The example also exposes two semantic warts of the synchronous schedule:

- **transient justification**: B is reverted at step 1 because of a cycle
  that A's continued reversion dissolves anyway — in the final synchronous
  state, restoring B to `p₀` would be acyclic, i.e. B lost its placement
  for a reason that no longer exists;
- **bystander cascade**: C was never on the original cycle, yet the
  synchronous run reverts it — reversion *radiates outward* through
  induced cycles.

### Minimality is not available

The tempting objective — choose the assignment maximizing registers at
`p₀` subject to acyclicity — is the minimum-feedback-arc-set family:
NP-hard on adversarially-shaped graphs, so not a definitional candidate
(R5), and its ties in symmetric cases could only be broken by id (R3
forbids) or by reverting all (collapsing back to the symmetric rule).

Note the chaotic run above found a *strictly less reverted* fixpoint than
the synchronous one — the synchronous result is not even a minimal fixed
point. Adding a "restoration pass" (after acyclicity, restore any register
whose `p₀` no longer cycles) reintroduces the same problem one level up:
two registers may each be restorable alone but not both, and choosing is
winner-picking. Restore-all-simultaneously can recreate cycles and
oscillate. Every road back toward minimality reopens either complexity,
ids, or schedules.

## Candidate definitions

### D1 — synchronous iterate-and-revert (the rule the specs currently sketch)

Advance **every** member of **every** cycle in the current assignment, one
chain step, simultaneously (members already at creation skip — every cycle
has at least one non-creation edge, so progress is guaranteed); repeat
until acyclic.

- ✓ deterministic (the synchronous schedule is a function), pure,
  terminating (Σ remaining chain positions strictly decreases;
  iteration depth ≤ moves in the component — attacker-op-linear);
- ✗ transient justification and bystander cascade (above): honest
  registers can be permanently reverted for reasons that dissolve, and
  reversion radiates;
- ✗ the Law II obligation is heavy: an incremental implementation must
  reproduce the exact synchronous stage sequence per component, not just
  "reach some acyclic state" — the counterexample shows nearby schedules
  land elsewhere.

### D2 — minimal reversion: rejected

NP-hard + tie-breaking forbidden (above).

### D3 — sequential acceptance over a canonical op order: rejected

Order-dependence is the amplification machine (MOVE.md); an id-derived
order is additionally grindable.

### D4 — detach the SCC (no iteration at all)

Compute cycles **once**, on the `p₀` graph: every register in a cyclic SCC
renders as **detached** — removed from the containment forest and surfaced
as a root-level conflict cluster (the SCC's internal edges can be kept for
display: "these blocks contend in a loop"). Everything else renders at
`p₀`, unconditionally.

Detaching only removes edges, so it cannot create new cycles — there is no
iteration, hence no schedule, hence confluence is trivial:

- ✓ pure, one-pass, deterministic; flagged set = exactly the `p₀`-cycle
  members (R6), the smallest blast radius of any candidate — in the
  counterexample, {A, B} detach and C keeps its intended placement, versus
  the synchronous rule reverting all three;
- ✓ no winner: all members detach equally; fabricating a cycle detaches
  the fabricator's target but confers no placement anywhere — the
  adversary gets a flag, not a destination;
- ✓ incremental story is standard: recompute SCCs of the affected
  component per op (single-edge change), O(component), matching the audit
  row with no stage-replay obligation;
- ✗ UX: a detached cluster is more jarring than "the block went back to
  where it was" — though a revert can also teleport content to an old
  location, and either way the next honest drag (naming the heads)
  resolves it;
- ✗ **it reopens a spec'd rule**: HASHSEQ_SPEC.md and MOVE.md currently
  say "all members revert to their previous agreed placement." D4 replaces
  revert-downward with detach-and-flag.

(A variant — jump cycle members straight to creation placement — inherits
the guaranteed-acyclic floor per member but mixed creation/`p₀` graphs can
still cycle, so it needs iteration again; it is D1 with bigger steps, not
an escape.)

## Acceptance criteria for a decision

Whichever rule is adopted must ship with:

1. the definitional function, with proofs of totality, termination,
   determinism, acyclicity, and R3;
2. either schedule-independence, or a canonical schedule plus an
   incremental algorithm proven to reproduce it under all delivery orders
   (the Law II cache invariant — this is where D1 pays and D4 doesn't);
3. per-op cost bounded by the affected component, and the MOVE.md
   amplification table re-verified (cycle-bomb row);
4. the property-test harness: definitional recompute vs incremental
   maintenance over randomized op sets *and* delivery orders, with
   generators biased toward chained supersessions, mixed creation/moved
   edges, and adversarial cycle bombs — the containment analog of
   `prop_index_matches_iterator`;
5. UI semantics for the flagged state (reverted-to-ancestor vs detached
   cluster) — this is a product decision as much as a protocol one.

## Leaning

**D4.** It is the only candidate where confluence is free rather than
proved, its blast radius is exactly the honest minimum (the cycle's own
members), the incremental algorithm is standard, and its adversarial story
is at least as good as D1's (a cycle-bomb yields a flagged cluster, never a
placement). The cost is honest UX in the rare honest-cycle case and a spec
change: HASHSEQ_SPEC.md's cycle row and MOVE.md's cycle section would
replace "revert all members to last agreed" with "detach the SCC, surface
as a cluster; any member's next move (or any move naming the contending
heads) re-places it." If D1 is preferred for its
return-to-a-sensible-place UX, the price is written above: stage-faithful
incremental maintenance and the bystander cascade, both of which must be
accepted knowingly.
