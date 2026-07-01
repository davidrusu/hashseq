# Framework: dependencies as state commitments, conflicts resolved at read time

Status: 2026-06-28. The op-agnostic frame shared by every projection in the
family. Each `XXX_SPEC.md` (HASHSEQ_SPEC, HASHKV_SPEC, HASHLIST_SPEC,
HASHDOC_SPEC, and marks) instantiates the **Op · Deps · Resource · Conflict ·
Resolution · Apply · Validation** skeleton defined here. Nothing here is
specific to sequences, maps, or any one op shape.

## The frame in one sentence

**An operation's dependency set is a content-addressed commitment to the exact
document version its author observed; convergence is the rule that maps the set
of all such commitments to a rendering, and every order-sensitive decision in
that rule is taken at read time, never at apply time.**

## Dependencies are a commitment to observed state

Every op is `{ extra_dependencies: BTreeSet<Id>, op }` with
`id = BLAKE3::derive_key(<layer context>, canonical_encoding)`. Its full
dependency set is

```
named(u)            = the ids the op semantically references (anchor / targets / overwrites)
extra_dependencies  = tips − named            // computed, not author-chosen
deps(u)             = extra_dependencies ∪ named = tips    // the observed frontier, exactly
```

Because every element of `tips` is a BLAKE3 id and the frontier's transitive
closure recovers all causal history beneath it, **`deps(u)` is a Merkle root
over the precise document version `u` was authored against** — minimal (only the
maximal elements are named) and unforgeable (you cannot name a hash you have not
seen, nor alter what you committed to without changing your own id).

This splits every op into two commitments:

- **named — "replaces" / "attach-to".** The semantically load-bearing ids the
  op acts on (`overwrites` in HashKv/Marks/Move; `targets` and `anchor` in the
  sequence). The op's intent is expressed entirely here.
- **extra-deps — the rest of the frontier.** Carries no intent; pins the
  observed version so that concurrency is decidable.

### Concurrency is a precise, read-time-computable relation

With `deps*(u)` = transitive closure of `deps(u)` = the version `u` saw:

```
u → v   (u precedes v)        iff   u ∈ deps*(v)
u ∥ v   (u, v concurrent)     iff   u ∉ deps*(v)  ∧  v ∉ deps*(u)
```

Total, deterministic, decidable from the op set alone — no timestamps, no
replica clocks, nothing forgeable.

## The two laws

**Law I — state is a function of the op set.** Render is a pure function of the
*set* of applied ops (`merge` = set union with orphan buffering; commutative,
associative, idempotent). Delivery order, wall-clock, and replica identity are
not inputs.

**Law II — arbitration happens at read time.** Apply is bookkeeping (intern,
attach to named deps, update tips); it takes no decision that depends on what
else has arrived. Anything an adversary could perturb by reordering or
late-delivering ops is recomputed from the op set when read — so there is no
order-dependent decision, hence no undo/redo and no replay.

Operationally a projection may keep an incremental **cache** of the read-time
function (e.g. the sequence's `run_index` treap). The cache is not a second
source of truth: a standing invariant pins it equal to the definitional walk for
all delivery orders, and every maintenance step must be a local consequence of
the definitional order, never an independent decision.

## What a conflict is

A **conflict** is a set of pairwise-concurrent ops (`∥`) contending for the same
**resource**. Each spec defines its resource; conflict is then a predicate over
`∥` restricted to that resource. An op is never in conflict with what it names
in `deps` — those are causally prior.

Resources seen so far are of two shapes:

- a **gap** — an `(anchor, side)` insertion point; the contended thing is the
  *order of the contenders' own content* within it;
- a **register** — a per-key / per-(char,kind) / per-element cell holding a
  value or placement that is *other people's content*.

## Resolution and the locality dividing line

The resolution rule is fixed by **what the contended resource is**:

> id-order is a sound resolution **iff** the contended resource is the
> contenders' own content (a gap). The instant the resource is other people's
> content (a value, a URL, a location), a grindable id must not decide it — the
> resolution is **MVR / freeze** instead.

This is the **locality invariant** (HASHDOC.md): an adversary may do only local,
attributable damage — garbage where they wrote — and grinding an id must buy
nothing. `max-Id` is at most a *display* tiebreak for cosmetic ambiguity; it
never decides anything whose consequences leave the contending ops' own content.

| resource | resolution | why |
|---|---|---|
| gap (intra-gap order) | **total order by id** | orders only the contenders' own bytes; displaces nothing |
| register (single live head) | that head's value/placement | uncontested |
| register (multiple heads) | **MVR**; freeze semantics-bearing to last agreed value | id-order would decide others' content → forbidden |

What the invariant does **not** promise is immunity to **dominating** ops — an
op that explicitly names and supersedes honest state (a Remove of honest
content, a Move overwriting a placement). That is permissionless write's
irreducible cost, but it is forward, attributable, per-op bounded, and
revertible; the invariant ensures grinding buys nothing on top of it.

## Stability requirement

Read-time resolution is sound only if **the relative order of any two already-
emitted elements is immutable** — no op, on arrival, reorders existing content;
new ops only subdivide gaps, and references (anchors, register placements) glued
to an element stay put for the document's life (tombstones keep their slot). If
this ever broke, a read-time verdict would have to be revised on arrival — the
replay trap (MOVE.md). Each spec must establish this for its op set.

## The family map

Every projection keeps the commitment frame and the two laws, differing only in
its resource and therefore its resolution:

| layer | op | resource | resolution |
|---|---|---|---|
| HashSeq | Insert | a gap | total order by id (sound: own bytes) |
| HashSeq | Remove | target liveness | union (absorbing) |
| HashKv | Put | a key's register | MVR; display max-id; semantics-bearing → surface |
| Marks | Mark | a (char, kind) register | MVR; conflicted links/URLs → freeze |
| HashList | MoveOp | an element's position/parent register | freeze to last agreed; cycles revert at read time |
| HashDoc | — | (routes to the above per object) | per the routed projection |

HashSeq is the base case where the cheap total order is sound; the rest of the
family is the machinery for everywhere it is not.
