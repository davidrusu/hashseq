# Layering: what the frontier decision actually is (exploration)

Status: exploration, 2026-07-01. Companion to FRAMEWORK.md. Not a spec — maps
the design space around "the layered design" to find where the ideas factor,
what actually depends on the choice, and what would settle it.

## The decision, decomposed

"Layered" bundles three separable commitments:

1. **Per-object projection.** One op DAG hosting many objects, each with its
   own projection state (a Text's run index, a Map's key registers), ops
   routed by object. This is storage/dispatch architecture — it works under
   *any* frontier discipline and nothing below argues against it. Keep.
2. **Frontier granularity.** What does `tips(u)` pin — the document-global
   frontier, the op's object's frontier, or a per-layer frontier (marks and
   moves split from their host object)? This is the real open decision.
3. **Reference discipline.** Downstream-only: annotations (marks, moves)
   reference content; content never references annotations. Only meaningful
   when (2) is finer than global — under a global frontier there is no
   "text frontier" for a mark to pollute.

## The framework is granularity-agnostic

The frontier granularity touches exactly one sentence of FRAMEWORK.md: the
honest frontier rule ("`refs(u)` contains the author's observed frontier
**of the op's layer**"). Everything else is independent of it:

| idea                                                               | depends on granularity?                             |
|--------------------------------------------------------------------|-----------------------------------------------------|
| content-hash ids, naming-is-committing, one flat `refs` set        | no                                                  |
| Law I / Law II, orphan buffering (waits on refs, already global)   | no                                                  |
| conflict = non-supersession; heads/`overwrites` machinery          | no — registers name ids explicitly, never frontiers |
| locality dividing line, freeze rule, MVR                           | no                                                  |
| stability split (base order vs placement), apply-time gates        | no                                                  |
| canonical encoding (blocks, order, refs — one stream spans layers) | no                                                  |
| honest frontier rule                                               | **yes — this is the injection point**               |
| "no rule may rely on cross-layer causal order" obligation          | only exists when granularity is finer than global   |

So the substrate can treat granularity as a **parameter** — one function
answering "which frontier does this op pin" — and extraction does not have
to decide this first.

## What each granularity buys and costs

### Global frontier (one tips set for the document)

Buys:

- `refs*(u)` **is the document version**, for every op — the commitment is
  exact and global, with no per-layer scoping caveat;
- every cross-object concurrency question (move-vs-remove, mark-vs-remove)
  is causally decidable with no extra machinery;
- one frontier: `H(sorted tips)` fingerprints the whole document; frontier
  exchange in sync is a single set; the mental model is the simplest
  possible;
- no downstream-only discipline to specify or maintain.

Costs:
- **dep volume under live concurrency.** Every remote batch applied mid-run
  puts foreign ids into the author's next op. Note the honest cost is per
  *applied receipt*, not per keystroke — and runs no longer split over it
  (the fast path extends through interior extra-deps; the deps attach at
  their offset). So the cost is bytes and per-entry storage, not run
  structure. Under live collaboration (receipts at keystroke frequency) it
  approaches one dep ref per op; under batched sync it is one per round.
- **closure entanglement — the load-bearing cost.** Extra deps weld
  cross-object edges: an op in object A depping a concurrent op in object B
  makes A's causal closure include B's history. Under sustained concurrent
  editing, *every object's closure grows toward the whole document*.
  Partial replication, lazy object loading, and subtree sync (an object =
  the closure of its ops) all die — buffering an object's ops pulls the
  world in behind them.
- annotation churn enters the content frontier: an indent burst or a
  formatting pass threads its ids through the text runs typed across it.

### Per-object frontier

Buys:

- **closure locality**: an object's ops close over the object plus its
  creation-bridge path — subtree sync and partial loading are structural;
- run purity: concurrent edits to *other* objects never touch this
  object's deps;
- the causal guarantee is the JSON-CRDT standard (per-object order plus
  creation bridges) — the semantics collaborative-document users already
  accept.

Costs:

- cross-object causality is sparse: two ops by the same author, minutes
  apart, in different objects, are incomparable — the commitment claim must
  be scoped, and the specs carry the obligation that no rule relies on
  cross-object order beyond named refs;
- cross-object decidability (move-vs-remove) needs opt-in machinery (the
  commitment vector);
- state identity is a set of frontiers, not one.

### Per-layer frontier (marks/moves split from their host object)

Everything per-object buys, plus:

- content closures exclude annotations entirely — a replica can sync and
  render text without ever fetching marks or moves; annotations layer on
  *downstream*;
- annotation spam cannot touch the content frontier at all;
- annotation bursts (indent/outdent, format painting) never thread through
  content runs.

Additional cost: more frontier bookkeeping, and the downstream-only
discipline must be stated and preserved per layer.

## Where the ideas factor nicely

Three observations fall out of the decomposition:

**Global is a point on the layered spectrum, not an alternative to it.**
The commitment vector generalizes frontier pinning: an op pins its own
layer's frontier always, and may pin foreign frontiers when its semantics
warrant (a move pinning its container's tips). An op that pinned *every*
frontier would carry exactly the global commitment. So "layered +
commitment vectors" is the general position — per-op choice of commitment
breadth — and "global frontier" is the degenerate corner where every op
pins everything, paying closure entanglement for commitment breadth it
rarely needs. The factoring favors the general position: local frontiers by
default, breadth purchased explicitly where a rule needs it.

**The old justification for layering is stale; the real one is different.**
The recorded argument for per-object tips is run fragmentation ("a mark at
the tips becomes extra deps on the next insert and fragments runs"). Interior
extra-deps weakened that: runs extend through deps now, so a global frontier
costs dep *bytes*, not run *structure* — a real but bounded, benchmarkable
cost. The argument that actually decides the question is **closure
locality**: whether an object's history is loadable without the document's.
That reframes the decision as a requirements question, not a perf question:

> Is partial replication (loading one object/subtree without the rest) a
> goal? If yes, frontiers must be at least per-object, and downstream-only
> annotation layers make content closures maximally lean. If it is a
> non-goal, the global frontier's simplicity and exact global commitment
> are more defensible than the current docs suggest.

**There is no document to be global over.** HashWeb is not a datatype — a
document's root object is just a map, and everything HASHWEB_SPEC.md adds is
composition (creation bridges, routing, frontier policy, reachability). So
documenthood is not a protocol concept: any object can be the root of
someone's replica, and with transclusion one object lives in several
"documents" at once. The global-frontier option quietly presupposes a
privileged boundary to be global over — a boundary the object model does
not have. Transclusion makes it incoherent rather than merely costly (which
document's frontier does an op on a shared object pin? pinning either
entangles the shared object with that document's whole history for every
other document containing it), and in the one case where a global frontier
is coherent — a standalone single object — it coincides with per-object
anyway. This shrinks the genuinely open part of the decision to per-object
vs per-layer (do annotations split from their host object), which is pure
economics; the requirements call above then decides emphasis, not
coherence.

## What would settle it

1. **The requirements call**: partial replication — goal or non-goal. This
   is the fork in the road; everything else is tuning.
2. **A dep-volume benchmark**: two-writer live-collab traces under global
   vs per-object frontiers — measure encoded size, memory, and run counts.
   The fragmentation question is empirical now; the answer is probably
   "global costs a few percent", which would confirm that closure locality,
   not compression, is the deciding axis.
3. **A closure-size experiment**: object closure size as a fraction of
   document size under sustained concurrent editing, global vs per-object.
   This quantifies the entanglement cost directly.

## Leaning

Keep per-object projection unconditionally (it was never really in
question). Implement frontier granularity as a substrate parameter — the
honest frontier rule is one function, and FRAMEWORK is already written so
that only that sentence varies. Default to per-object frontiers with
downstream-only annotation layers and opt-in commitment vectors: it is the
general position on the spectrum, it keeps closure locality (hard to
retrofit later — closures, once entangled, are entangled in the permanent
DAG), and it degrades gracefully to breadth where semantics want it. Flip
to global only on an explicit decision that partial replication is a
non-goal — and note the asymmetry: choosing local frontiers now and
loosening later is cheap (pin more), while choosing global now and
tightening later is impossible (pins are forever in the hash graph).
