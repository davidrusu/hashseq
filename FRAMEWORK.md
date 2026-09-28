# Framework: reference commitments, read-time arbitration

The op-agnostic frame shared by every projection in the family. Each spec
(HASHSEQ_SPEC.md, HASHKV_SPEC.md, HASHWEB_SPEC.md, MARKS.md,
PLACEMENT_SPEC.md) instantiates the skeleton **Op · Refs · Resource ·
Conflict · Resolution · Apply · Validation**. Nothing here is specific to
sequences, maps, or any one op shape.

## The frame in one sentence

**An op names the ids it acts on and pins the frontier its author observed;
everything it references, it commits to. State is a pure function of the op
set, and every decision an adversary could influence (by reordering
delivery, withholding what they saw, or grinding an id) is taken at read
time, under rules where none of those levers buys anything.**

## The op shape

```rust
struct Node {
    refs: BTreeSet<Id>,   // every id this op references — the commitment
    op: Op,               // the meaning; its fields carry the ids it acts on
}
// id = BLAKE3::derive_key(NODE_CONTEXT, canonical_encoding)
// ONE context for every op kind; kinds are tags inside the encoding
```

```
named(u) = the ids u's op fields carry (anchor / targets / overwrites)
pins(u)  = refs(u) ∖ named(u)          — refs no role addresses
```

- **named** carries the op's intent: *attach-to* (the anchor of an insert)
  or *replaces* (the targets of a remove, the heads a put / move / mark /
  place supersedes).
- **pins** carry no intent. Together with `named` they record the state the
  author wrote against (the honest frontier rule, below).

The split is positional (which refs a role field addresses), never a
flagged field: nothing consumes "which refs the author called frontier", so
the artifact does not record it (GRAMMAR_SPEC.md; OP_REFS.md).

**Naming is committing.** `named(u) ⊆ refs(u)` by definition, so a
reference outside the commitment is unrepresentable.

**Delivery waits on exactly `refs(u)`.** An op is buffered until every id in
`refs(u)` has applied in its object; nothing else in the op holds delivery
back. (Store-level routing adds one buffer for unopened objects,
HASHWEB_SPEC.md "Per-object tips".)

The typing fast path is `refs = {anchor}`: the anchor *is* the frontier.
The canonical encoding stores each id of `refs(u)` once, in a sorted refs
table that role fields address by index (GRAMMAR_SPEC.md). Equal op sets
encode to identical bytes, per op and per snapshot (ENCODING_SPEC.md).

### One namespace, committed kinds

All ids share one namespace: a reference is 32 opaque bytes whatever it
names. Resolving an id means holding its preimage, and the preimage carries
its kind inside the hash, so a referent's kind is discovered, unforgeably,
at dereference. Misreading one kind as another cannot be expressed; a
cross-kind reference is a well-defined query that the admission table
(HASHWEB_SPEC.md "Admission") answers. Rationale and consequences:
HETEROGENEITY.md.

## The honest frontier rule

> **Well-formedness (honest).** `refs(u)` contains the author's observed
> frontier of the op's layer: honest construction is
> `refs = frontier ∪ named`, nothing less.

A convention defining honest behavior, **not an admission rule**. Seeing
cannot be proven, so omission is unenforceable. An author who omits
frontier ops they saw is pulling the *withholding* lever (below), which is
answered at resolution, never at apply.

## What the commitment is — and is not

Ids are content hashes: you cannot name an op that does not exist. With
`refs*(u)` the transitive closure of `refs`:

```
everything in refs*(u) genuinely preceded u      — sound
refs*(u) may omit ops the author had seen        — NOT complete
```

`refs(u)` is an unforgeable **lower bound** on the author's observed state.
For an honest author it is tight: a Merkle root over exactly the version
they authored against. For a Byzantine author it is whatever prior state
they chose to admit.

Every rule must stay sound when the bound is not tight. An argument of the
form "refs = observed state" is the honest-author reading; the rule must
separately survive authors who under-claim (see "What a conflict is").

## Frontiers are per layer, not per document

The layer is the **object**: each object keeps its own tips
(HASHWEB_SPEC.md "Per-object tips"), and every op kind in an object (insert,
remove, move, mark, place) shares that one frontier (LAYERING.md). An op
pins its own object's tips. Other objects enter its history only through
the object's origin (the composition convention, HASHWEB_SPEC.md).

So causal order spans the store but is **sparse across objects**: two ops on
different objects, authored minutes apart by the same user, are typically
incomparable. The matching obligation:

> **No rule may rely on cross-object causal order beyond the named refs
> themselves.**

An op cannot buy that order by pinning another object's ops: delivery is
per object, so a foreign ref never arrives and the op orphans forever
(LAYERING.md "Commitment vectors").

## Causal order is definitional, not operational

```
u → v   (u prior to v)    iff   u ∈ refs*(v)
u ∥ v   (concurrent)      iff   u ∉ refs*(v) ∧ v ∉ refs*(u)
```

Decidable from the op set alone: no timestamps, no replica identity. No
projection computes `refs*` to make a decision. Delivery uses refs only for
orphan buffering, and every read-time decision is a function of local
structure (the siblings under an anchor, a register's head set and its
`overwrites` chain). The relation exists so the specs' claims and the
honest-author lemma can be stated; it is never an input to rendering.

## The two laws

**Law I — state is a function of the op set.** Render is a pure function of
the *set* of applied ops. `merge` is set union with orphan buffering:
commutative, associative, idempotent. Delivery order, wall-clock and replica
identity are not inputs.

**Law II — arbitration happens at read time.** Apply is bookkeeping (intern,
attach to named ids, update tips, maintain head sets) plus the stable
admission checks (see "Stability"). It takes no decision that depends on
what else has arrived. Anything reordering or late delivery could perturb is
recomputed from the op set when read, so there is no undo/redo and no
replay.

A projection may keep an incremental **cache** of a read-time function (the
sequence's run index, a move register's decider). The cache is not a second
source of truth: an invariant pins it equal to the definitional read for
every delivery order, and each maintenance step must be a local consequence
of the definitional rule.

## The adversary's three levers

Permissionless write plus content-hash ids leave an adversary three levers.

| lever | what it is | answered by |
|---|---|---|
| **grinding** | mint ops until the hash sorts where you want | id-order decides only the arrangement of the grinder's *own* content within a gap they already write to (locality dividing line) |
| **withholding** | omit seen ops from `refs` / `overwrites`, fabricating "concurrency" | the conflicted path confers no authority: contested registers surface every head and render the last *agreed* value. Residual: the choice of fork point selects *which* previously agreed value the freeze lands on, never a fresh value, and it is flagged as a conflict either way |
| **dominating** | explicitly name and supersede honest state | not prevented: the irreducible cost of permissionless write. Forward, attributable, bounded per op, revertible. The invariant is that the other two levers buy nothing on top of it |

Delivery games (reorder, delay, replay) are vacuous by Law I/II. Spam is
bounded per op: each spec's Validation section owes an **amplification
argument**: one adversarial op costs honest replicas bounded local work,
linear in attacker ops, never a replay of honest history.

## What a conflict is

Each spec names its **resource**, the thing ops contend for:

- a **gap**: an `(anchor, side)` insertion point, where the contended thing
  is the order of the contenders' *own* content;
- a **register**: a cell (per key, per element placement, per (element,
  kind), per object containment) whose value is *other people's* content
  once set.

**A conflict is non-supersession, not concurrency.** A register's live state
is its head set

```
heads(r) = { ops on r not named in any other op-on-r's overwrites }
```

A register conflict is `|heads(r)| > 1`; a gap conflict is ≥ 2 sibling
inserts on the same `(anchor, side)`. Neither test mentions `∥`.

> **Honest-author lemma.** Honest authors satisfy the frontier rule, name
> every head they see in `overwrites`, and anchor at the newest element
> they see. Restricted to honest ops: multi-head ⟺ genuine concurrency, and
> same-gap siblings arise only from genuine concurrency.

A Byzantine author breaks the ⟸ direction for free by omitting a seen head.
So the conflicted path must be one where nothing can be won: a fabricated
conflict yields exactly what a real one does, a surfaced flag and a frozen
value.

Only naming an op in a *replaces* role removes it from the head set.
Pinning it in `refs` does **not**: seeing without superseding leaves both
ops as heads.

## Resolution and the locality dividing line

> id-order is a sound resolution **iff** the contended resource is the
> contenders' own content (a gap). Once the resource is other people's
> content (a value, a URL, a placement), a grindable id must not decide it:
> the resolution is **MVR / freeze**.

| resource | resolution | why |
|---|---|---|
| gap | **total order by id** | orders only the contenders' own content; displaces nothing |
| register, one head | that head's value | uncontested |
| register, several heads | **MVR**: surface every head. Where one rendering is physically required (an element must sit somewhere), render the **last agreed** value: recurse on the maximal ops that *every* head transitively overwrites, with the creation value as the implicit root, so the recursion is total. Never a winner. | id-order would let a ground hash decide others' content; any winner-picking rule would let a fabricated conflict re-decide settled state. The next op naming all heads resolves it |

Two levers, one rule: **grinding** is why the winner must not be chosen by
id; **withholding** is why there must be no winner at all.

`max-Id` survives only as a *display* tiebreak for cosmetic ambiguity
(e.g. which of two identical concurrent bolds to attribute), never for
anything whose effect leaves the contenders' own content.

## Stability: the base order is immutable, placement is not

Two things are rendered, with different mutability:

- **the base order**: the linearization of the insert DAG, tombstones
  included. Immutable: new inserts only subdivide gaps, emitted elements
  never reorder, a tombstone keeps its slot, and a glued point
  (`Before(x)` / `After(x)`) holds for the object's life.
- **rendered placement**: where a register (position, parent) says an
  element currently lives. A read-time function of head sets that changes
  as ops arrive: Law II re-evaluation, not the base order changing.

> **Stability requirement.** A check may run **once, at apply time**
> (refusing the op) iff it is a function of hash-committed inputs and the
> immutable base: an op's shape, its referents' kinds, its anchors' base
> order, an object's kind. Anything that depends on *which other ops are
> present* (head counts, cycles, rendered placement) is unstable and must
> be resolved at **read time**.

Inverted spans (MARKS.md) and ill-typed references (HASHWEB_SPEC.md
"Admission") pass the test: apply-time verdicts, permanent and convergent
(the `Refused` variants in `src/lib.rs`). Head counts and placement fail it:
read-time arbitration. "This replica does not know the kind" also fails it:
unknown-ness can orphan but never refuse (HETEROGENEITY.md). Getting this
wrong in either direction reintroduces replay: an apply-time verdict on an
unstable fact must later be revised (the trap MOVE.md documents in
undo/redo move designs).

## The family map

Every projection keeps the commitment frame and the two laws; they differ
only in resource, hence resolution.

| object | op | resource | resolution |
|---|---|---|---|
| Seq | Insert | a gap | total order by id |
| Seq | Remove | target liveness | union (absorbing) |
| Seq | Move | an element's placement register (same container) | freeze to last agreed; intra-object placement cycles unrepresentable |
| Seq | Mark | an (element, kind) register | MVR; conflicted links / URLs freeze |
| Kv | Put | a key's register | MVR; max-id for display only |
| any | Place | the object's containment register | freeze to last agreed; cross-register cycles detached at read (PLACEMENT_SPEC.md) |
| HashWeb | — | routes each op to its object | per the routed projection |

The gap is the one resource where the cheap total order is sound; the rest
of the family is the machinery for everywhere it is not.
