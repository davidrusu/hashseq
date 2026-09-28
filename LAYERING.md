# Layering: frontier granularity

Companion to FRAMEWORK.md. Records what "layered" means, which choice it
turned on, and why that choice is settled at **per-object frontiers**.

## The decision, decomposed

"Layered" bundles three separable commitments:

1. **Per-object projection.** One store hosts many objects, each with its
   own projection state (a seq's run index, a kv's key registers); ops are
   routed by object. Storage and dispatch architecture, valid under any
   frontier discipline.
2. **Frontier granularity.** Which frontier does an op pin: the document's,
   its object's, or a per-layer one (marks and moves split from their host
   object)? **Settled: per object.**
3. **Reference discipline: downstream-only.** Annotations (marks, moves)
   reference content; content never references annotations. This is a fact
   about what ops *reference*, independent of (2).

## Only one rule depends on granularity

Granularity enters FRAMEWORK.md in exactly one place, the honest frontier
rule ("the observed frontier **of the op's layer**"), plus the obligation
it induces in "Frontiers are per layer" (no rule may rely on cross-object
causal order). Content-hash ids, naming-is-committing, Law I/II, orphan
buffering, the head-set machinery, the locality dividing line, the
stability requirement and the canonical encoding are all
granularity-independent. The substrate treats granularity as one function:
"which frontier does this op pin".

## The options

| granularity | buys | costs |
|---|---|---|
| global (one tips set for the document) | `refs*(u)` is the whole document version; every cross-object concurrency question is causally decidable; one fingerprint | **closure entanglement**: an op in A pinning a concurrent op in B puts B's history in A's closure, so under sustained concurrency every object's closure grows toward the whole document, killing partial replication, lazy loading and subtree sync. Also dep volume (about one foreign pin per applied remote batch) and annotation churn threaded through content |
| **per object** (chosen) | **closure locality**: an object's history closes over the object plus its origin chain, so subtree sync and partial loading are structural; edits elsewhere never touch this object's refs | cross-object causality is sparse, so the commitment claim is scoped and the specs carry the obligation above; state identity is a set of frontiers, not one |
| per layer (marks / moves split from their object) | content closures exclude annotations; annotation spam and bursts never touch the content frontier | a second tip set per object on the wire and in every clock, plus a downstream-only discipline to state per layer |

## Why per object

- **The deciding axis is closure locality, not compression.** The old
  argument for local frontiers was run fragmentation. Interior pins
  (`interior_pins` on runs, src/run.rs) let runs extend through extra
  refs, so a broader frontier costs bytes, not run structure. What a broad
  frontier costs irrecoverably is closure locality.
- **There is no document to be global over.** HashWeb is a flat store, not
  a datatype: any object can be the root of someone's replica, and with
  transclusion one object lives in several documents at once. Which
  document's frontier would an op on a shared object pin? Either choice
  entangles the shared object with that document's whole history. Where a
  global frontier is coherent (a standalone object) it coincides with
  per-object.
- **Per-layer is rejected (2026-09-12).** The frontier is a property of the
  DAG, not of a layer built on it, so every op kind in an object shares the
  object's one tip set: a mark or place enters the tips and is pinned by
  whatever is authored next. Downstream-only survives as a reference
  discipline, which is all closure locality needs. Interior pins make the
  extra pins a mark adds to the next insert cost bytes, not run structure.
- **Asymmetry.** Pins are permanent in the hash graph. Local frontiers can
  later be broadened (pin more); a global frontier can never be narrowed.

## Commitment vectors

A **commitment vector** is an op pinning, in addition to its own frontier,
a foreign frontier its semantics wants ordered against (the historical
example: a move pinning its container's tips, making move-vs-remove
causally decidable). Global is the degenerate corner where every op pins
every frontier.

Current status:

- **Within an object it is automatic.** Moves are same-container
  (HASHSEQ_SPEC.md), so a move already pins its container's frontier.
- **Across objects it is unavailable.** Delivery is per object: an op
  waits for its refs in its own object's buffer, so a pin on another
  object's op never arrives and the op orphans forever (HASHWEB_SPEC.md
  "Admission", `Remove . target` row). Adopting cross-object vectors would
  require store-level delivery changes.
