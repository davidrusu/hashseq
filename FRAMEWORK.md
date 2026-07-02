# Framework: reference commitments, read-time arbitration

Status: 2026-07-01. The op-agnostic frame shared by every projection in the
family. Each spec (HASHSEQ_SPEC.md, HASHKV_SPEC.md, HASHWEB_SPEC.md, and
MARKS.md) instantiates the **Op · Refs · Resource · Conflict · Resolution ·
Apply · Validation** skeleton defined here. Nothing
here is specific to sequences, maps, or any one op shape.

## The frame in one sentence

**An op names the ids it acts on and pins the frontier its author observed —
everything it references, it commits to; the rendered document is a pure
function of the op set; and every decision an adversary could influence — by
reordering delivery, withholding what they saw, or grinding an id — is taken
at read time, under rules where none of those levers buys anything.**

## The op shape

Every op is

```rust
struct Node {
    refs: BTreeSet<Id>,   // every id this op references — the commitment
    op: Op,               // the meaning; its fields carry the ids it acts on
}
// id = BLAKE3::derive_key(NODE_CONTEXT, canonical_encoding)
// — ONE context string for every op kind; kinds are tags inside the encoding
```

with

```
named(u) = the ids u's op fields carry (anchor / targets / overwrites)
pins(u)  = refs(u) ∖ named(u)                  — refs no role addresses
```

The two parts carry different meaning but have the same status:

- **named — "attach-to" / "replaces".** The semantically load-bearing ids:
  the gap an insert claims (anchor), the ops a remove tombstones (targets),
  the heads a put or move supersedes (overwrites). The op's intent lives
  entirely here.
- **pins — the rest of the frontier.** No intent: together with `named`,
  they carry the author's claim of the state they authored against (next
  section). The split is positional — which refs the op's fields address —
  never a flagged field: nothing in the system consumes "which refs the
  author called frontier", so the artifact does not record it
  (GRAMMAR_SPEC.md).

**Naming is committing.** `named(u) ⊆ refs(u)` by definition — there is no
way to reference an op without committing to it, so "a reference outside the
commitment" is unrepresentable.

Delivery follows the same set: **an op is buffered until every id in
`refs(u)` has applied.** There is no other gating.

The typing fast path is `refs = {anchor}` — the anchor *is* the frontier,
one id total. The **canonical encoding** (the id preimage, and the wire
format with it) stores each id of `refs(u)` exactly once — a sorted refs
table that role fields address by index (GRAMMAR_SPEC.md). An id's hash
cost is |refs(u)| ids plus index bytes; indices are an encoding concern,
invisible at this level.

Canonicality is unconditional: ENCODING_SPEC.md fixes the encoding at both
granularities — per-op preimage and whole-document snapshot — so equal op
sets encode to identical bytes, with no encoder freedom anywhere.

### One namespace, committed kinds

All ids live in one namespace — a reference is 32 opaque bytes regardless of
what it names. But unlike a pointer, an id cannot be dereferenced blind:
resolving it means holding its preimage, and the preimage carries its kind
(the derive_key context and op tag are inside the hash). The kind of a
referent is discovered, unforgeably, at the moment of dereference — so
misinterpreting one kind as another is not an error to check for but a thing
that cannot be expressed. Cross-kind references are therefore always
well-defined queries; the edge table (HASHWEB_SPEC.md) states which
reference edges are meaningful and gates the rest. HETEROGENEITY.md
explores the object designs this licenses.

## The honest frontier rule

> **Well-formedness (honest).** `refs(u)` contains the author's observed
> frontier of the op's layer — honest construction:
> `refs = frontier ∪ named`, nothing less.

The rule is a convention defining honest behavior, **not a validation gate**.
Seeing cannot be proven, so omission is unenforceable; an author who omits
frontier ops they saw is exercising the *withholding* lever (below), which is
answered at resolution time, never at apply.

## What the commitment is — and is not

Ids are content hashes, so a reference is unforgeable in one direction: you
cannot name an op that does not exist, because you cannot predict its hash.
With `refs*(u)` the transitive closure of `refs`:

```
everything in refs*(u) genuinely preceded u          — sound
refs*(u) may omit ops the author had seen            — NOT complete
```

So `refs(u)` is an unforgeable **lower bound** on the author's observed
state. For an honest author — frontier rule satisfied, every acted-on head
named — the bound is tight: `refs(u)` is a Merkle root over precisely the
version they authored against. For a Byzantine author it is whatever prior
state they chose to admit having seen.

Every rule in the family must remain sound when the bound is not tight.
Where a spec's argument reads "refs = observed state", that is the
**honest-author reading**; the rule must separately survive authors who
under-claim (see Conflict, below — this asymmetry is exactly what the freeze
rule answers).

## Frontiers are per layer, not per document

Each object and layer keeps **its own tips** (HASHWEB_SPEC.md "per-object
tips"; marks and moves are their own downstream-only layers). Run compression
demands the fast path `refs = {previous op}`, and a document-global frontier
would fragment runs on every concurrent edit anywhere in the document.

The frontier rule is therefore per layer: an op pins **its own layer's**
tips. Other layers enter `refs` through named roles alone — an anchor into
the text, a move target, a creation bridge. The induced causal order spans
the whole document but is *sparse across layers*: two ops on different
objects, authored minutes apart by the same user, are typically incomparable.

This is a deliberate trade with a matching obligation: **no rule in any spec
may rely on cross-layer or cross-object causal order beyond the named refs
themselves.** (None does — e.g. remove-beats-move is an absorption rule, not
a causality test; HASHSEQ_SPEC.md.) A layer *may* opt into a stronger
cross-layer commitment by additionally pinning a foreign frontier (a move op
pinning its container's frontier — HASHSEQ_SPEC.md); the frame permits it,
no rule requires it.

## Causal order is definitional, not operational

```
u → v   (u prior to v)     iff   u ∈ refs*(v)
u ∥ v   (concurrent)       iff   u ∉ refs*(v)  ∧  v ∉ refs*(u)
```

Deterministic and decidable from the op set alone — no timestamps, no replica
identity, nothing forgeable. But note what the running system does with this
relation: **nothing.** No projection computes `refs*` to make a decision.
Delivery uses refs only for orphan buffering; every read-time decision is a
function of *local* structure — the sibling set under an anchor, the head set
of a register. The relation exists so the specs' claims are well-defined and
the honest-author lemma below can be stated; it is never an input to
rendering.

## The two laws

**Law I — state is a function of the op set.** Render is a pure function of
the *set* of applied ops. `merge` = set union with orphan buffering —
commutative, associative, idempotent. Delivery order, wall-clock, and replica
identity are not inputs.

**Law II — arbitration happens at read time.** Apply is bookkeeping (intern,
attach to named ids, update tips, maintain head sets); it takes no decision
that depends on what else has arrived. Anything an adversary could perturb by
reordering or late-delivering ops is recomputed from the op set when read — so
there is no order-dependent decision, hence no undo/redo and no replay.

Operationally a projection may keep an incremental **cache** of the read-time
function (e.g. the sequence's `run_index` treap). The cache is not a second
source of truth: a standing invariant pins it equal to the definitional walk
for all delivery orders, and every maintenance step must be a local
consequence of the definitional rule, never an independent decision.

## The adversary's three levers

Permissionless write plus content-hash ids leave an adversary exactly three
levers; the design is the answer to them.

| lever | what it is | answered by |
|---|---|---|
| **grinding** | mint ops until the hash sorts where you want | id-order decides nothing except the arrangement of the grinder's *own* content within a gap they were already writing to (locality dividing line, below) |
| **withholding** | omit seen ops from `refs`/`overwrites`, fabricating "concurrency" | the conflicted path confers no authority: contested registers surface all heads and render the last *agreed* value. Residual: fork-point choice selects *which* previously-agreed value the freeze lands on — never a fresh value of the adversary's choosing, and flagged as a conflict either way |
| **dominating** | explicitly name and supersede honest state (remove honest text, overwrite a placement) | not prevented — the irreducible cost of permissionless write. Forward, attributable, per-op bounded, revertible. The invariant is that the other two levers buy nothing *on top of* this baseline |

Delivery games (reorder, delay, replay) are made vacuous by Law I/II. Spam is
bounded per-op: each spec's Validation section owes an **amplification
argument** — one adversarial op costs honest replicas bounded local work,
linear in attacker ops, never a replay of honest history.

## What a conflict is

Each spec names its **resource** — the thing ops contend for. Two shapes so
far:

- a **gap** — an `(anchor, side)` insertion point; the contended thing is the
  order of the contenders' *own* content within it;
- a **register** — a per-key / per-element / per-(char, kind) cell whose
  value or placement is *other people's* content once set.

**Operationally, a conflict is non-supersession — not concurrency.** A
register's live state is its head set

```
heads(r) = { ops on r not named in any other op-on-r's overwrites }
```

and a register conflict is `|heads(r)| > 1`. A gap conflict is ≥2 sibling
inserts claiming the same `(anchor, side)`. Neither test mentions `∥`.

The connection to concurrency is a lemma, not the definition:

> **Honest-author lemma.** Honest authors satisfy the frontier rule, name
> every head they see in `overwrites`, and anchor at the newest char they
> see. Restricted to honest ops: multi-head ⟺ genuine concurrency, and
> same-gap siblings arise only from genuine concurrency.

A Byzantine author breaks the ⟸ direction at will: omitting a seen head
fabricates a "conflict" between causally ordered ops, for free. This is why
resolution must treat the conflicted path as a place where nothing can be won
— a fabricated conflict then yields exactly what a real one yields: a surfaced
flag and a frozen value.

Note the precise statement: an op is never in conflict with the ids it names
in a *replaces* role — `overwrites`-naming is what removes them from the head
set. Merely pinning an op in `refs` does **not** exempt it:
seeing-but-not-superseding leaves both ops as heads, which is the fabrication
above.

## Resolution and the locality dividing line

The resolution rule is fixed by **what the contended resource is**:

> id-order is a sound resolution **iff** the contended resource is the
> contenders' own content (a gap). The instant the resource is other people's
> content (a value, a URL, a placement), a grindable id must not decide it —
> the resolution is **MVR / freeze** instead.

| resource | resolution | why |
|---|---|---|
| gap (intra-gap order) | **total order by id** | orders only the contenders' own bytes; displaces nothing; grinding moves your own char within a gap you already write to |
| register, single head | that head's value / placement | uncontested |
| register, multiple heads | **MVR** — surface every head, collapse nothing. Where a single rendering is physically required (an element must sit somewhere), render the **last agreed** value: recurse on the maximal ops that *every* head transitively overwrites; the creation value is the implicit root each op overwrites, so the recursion is total and bottoms out. Never a winner. | id-order would let a ground hash decide others' content; *any* winner-picking rule would let a freely-fabricated conflict re-decide settled state. Freeze means the conflict path confers no authority; the next op naming all heads dominates and resolves it |

Two levers, one rule: **grinding** is why the winner must not be chosen by id;
**withholding** is why there must be no winner at all.

`max-Id` survives only as a *display* tiebreak for cosmetic ambiguity (which
of two identical concurrent bolds to attribute) — never for anything whose
consequences leave the contending ops' own content.

## Stability: the base order is immutable, placement is not

Two different things get rendered, with different mutability:

- **the base order** — the linearization of the insert DAG over element ids,
  tombstones included. Immutable by construction: new inserts only subdivide
  gaps, two emitted elements never reorder, a tombstone keeps its slot, an
  anchor point (`Before(c)` / `After(c)`) is glued for the document's life.
  `cmp_order` over element ids is a convergent, permanent total order.
- **rendered placement** — where a register (position/parent) says an element
  currently lives. A read-time function of head sets; it changes as ops
  arrive. That is arbitration being re-evaluated per Law II — not the base
  order changing.

The dividing rule for checks follows from this split:

> A check may run **once, at apply time** (quarantining permanently on
> failure) iff it is a function of hash-committed inputs and the immutable
> base — an op's shape, its anchors' base order, an object's committed type.
> Anything that depends on *which other ops are present* — head counts,
> cycle-ness, rendered placement — is unstable and must be resolved at **read
> time**, where re-evaluation is the normal case.

Inverted spans (MARKS.md) and ill-typed ops (HASHWEB_SPEC.md) pass the
test → apply-time gates, verdicts permanent and convergent. Head counts and
rendered placement fail it → read-time arbitration. Getting this wrong in
either direction reintroduces replay: an apply-time verdict about an
unstable fact must be revised on arrival — the trap MOVE.md documents in
undo/redo move designs.

## The family map

Every projection keeps the commitment frame and the two laws, differing only
in its resource and therefore its resolution:

| layer | op | resource | resolution |
|---|---|---|---|
| HashSeq | Insert | a gap | total order by id (sound: own bytes) |
| HashSeq | Remove | target liveness | union (absorbing) |
| HashSeq | Move | an element's placement register (same-container) | freeze to last agreed; placement cycles unrepresentable |
| HashKv | Put | a key's register | MVR; freeze semantics-bearing; max-id display only |
| Marks | Mark | a (char, kind) register | MVR; conflicted links/URLs freeze |
| HashWeb | — | (routes to the above per object) | per the routed projection |

HashSeq is the base case where the cheap total order is sound; the rest of the
family is the machinery for everywhere it is not.
