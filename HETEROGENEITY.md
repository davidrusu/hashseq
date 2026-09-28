# Heterogeneity: one namespace, committed kinds

Companion to FRAMEWORK.md ("One namespace, committed kinds"). Covers why
ops of any kind may reference each other safely, what that enables, and how
replicas treat kinds and payloads they cannot interpret. The normative
rules live in HASHWEB_SPEC.md "Admission" and GRAMMAR_SPEC.md.

## The property

| | pointer / `(ctr, actor)` id | content-hash id |
|---|---|---|
| dereference | read the location, *then* interpret | requires already holding the preimage |
| referent's kind | reader's assumption or an external schema | inside the hash (derive_key context + kind tag) |
| wrong-kind access | misinterpretation: a bug or an exploit | **unrepresentable**: you hold the typed artifact or nothing |

Dereference is a lookup in a store of decoded nodes, and every hit is a
kind-tagged artifact whose tag the id commits to (collision resistance is
the only assumption). A reference to a node of the "wrong" kind is a
well-formed query with one of three answers:

- **meaningful**: the edge is admitted for that role; apply proceeds;
- **inert**: tolerated, no effect (a remove naming a non-insert);
- **refused**: ill-typed; the op is dropped. Kinds are immutable, so the
  verdict meets the stability requirement (FRAMEWORK.md).

This is dynamic typing with unforgeable tags, not static typing: an author
cannot prove a referent's kind inside their own artifact, so the verdict
lands at admission, when both artifacts are present.

## Edge typing, not object homogeneity

> **One object's DAG may mix op kinds freely.** Safety comes from edge
> typing: each op kind declares, per role, the kinds it may reference, and
> one shared admission table checks every edge.

The table (HASHWEB_SPEC.md "Admission") is the whole discipline: extending
the family means adding rows, not mechanisms. It holds two constraint
classes that must stay distinct: **kind checks** (stable, verdicts at
admission) and **value-dependent filters** such as same-key for
`Put . overwrites` (applied in the definitional read, never a verdict).
Keeping value-dependent constraints out of admission is what keeps every
verdict permanent.

**One derive_key context.** Per-kind contexts would be safe too, but they
split the namespace for no gain. The point is that any id can appear in any
role and the table, not the id space, says what it means there.

**Payloads are ids.** An insert's payload, a key and a value are ids: of a
content-addressed value artifact, an object (a link, or transclusion when
the object already lives elsewhere), or an op node (HASHSEQ_SPEC.md
"Payload", HASHKV_SPEC.md "Keys and values are ids"). They are **values,
not references**: never in `refs(u)`, never buffered on, never edge-checked,
and `pending` when unresolvable.

## What mixed kinds enable

- **Rich text as one object**: elements, marks and moves in one DAG and one
  frontier (LAYERING.md).
- **Any payload in any slot**: text and list are one seq kind; images,
  mentions and widgets are inserts whose payload is a blob or object id.
  Textness is a rendering convention, never a committed type
  (HASHWEB_SPEC.md "Objects").
- **Annotations on annotations**: a comment anchored on a mark op survives
  churn inside the span and dies with the mark; `After(move_op)`, the
  splice-point anchor, is the same move.
- **An "about" register on anything**: puts keyed by any node id (review
  status, moderation labels, votes), with the subject's kind discovered at
  dereference.
- **Typed links**: link values that are object ids make the document graph
  readable from the artifacts, each edge kind-committed at both ends.

## Forward compatibility: the extension path

"I do not recognize this kind" is a fact about a replica's software, not
about hash-committed inputs. By the stability requirement it can never be an
admission verdict: a replica that refused unknown kinds would permanently
diverge from upgraded peers. Extension is handled by semantics, not
rejection.

### The envelope / body split

```
envelope — kind-independent: kind tag, refs table, body length
body     — kind-dependent: opaque unless the kind is known
```

- **Envelope semantics**: identity (the id hashes envelope ‖ body),
  commitment (refs), buffering, encoding. Every replica of every version
  computes these identically.
- **Body semantics**: what the op does **and where it sits**. Placement is
  kind-level meaning, and not every kind has one (a `Put` does not). An
  unknown kind has no effect: the op is carried, forwarded, and surfaced as
  present but uninterpretable.

The encoding owes **skippability**: unknown-kind nodes are kind-tagged,
length-prefixed, envelope first, and their body bytes are stored and
re-emitted verbatim (GRAMMAR_SPEC.md Part A).

### Unknown referents orphan

An op whose role references a node of unknown kind has no resolvable meaning
or place, so it **orphans** until the kind is known. Orphaning decides
nothing permanent, which is exactly what the stability requirement demands
of unknown-ness. On upgrade, orphans apply as ordinary late deliveries, and
late arrival never reorders existing content, so the replay trap stays
closed across versions by the same property that closes it across delivery
orders. For **known** kinds anchorability is a stable fact, and a violation
is refused permanently. Unknown kinds neither pass nor fail; they wait.

### The convergence contract across versions

- the **op set** converges universally (sync, ids and buffering are envelope
  concerns);
- the **base order** agrees on everything all parties can interpret; ops on
  unknown kinds arrive on upgrade as late deliveries (insertion, never
  reordering);
- **render is version-parameterized**: `render_v(S)` differs across
  versions by exactly the unknown kinds' body effects and their orphaned
  dependents; same-version replicas converge as always.

Version skew needs no new machinery. Ops name ids, never positions, so a
v1 remove names exactly the ids its author saw and cannot swallow an
invisible v2 element. A v1 put that overwrites only the heads v1 can read
looks, to v2 peers, like honest withholding, which resolution already
survives: multi-head, surfaced, frozen, dominated by the next op that names
everything.

**Tighten never, loosen carefully**: admission rows are versioned
semantics (HASHWEB_SPEC.md "Admission").

## Placeholder semantics

A replica renders what it cannot interpret as honestly present-but-unknown,
never as a guess:

| case | render |
|---|---|
| payload artifact not yet received | `pending`, surfaced to the app |
| payload of a value kind the renderer cannot interpret | an opaque atom placeholder in its slot |
| op of unknown kind | no effect; carried and forwarded |
| op whose role references an unknown kind | absent until upgrade (orphaned), not a placed placeholder |
| transcluded object repeated on a root-to-leaf path (render cycle) | embedded once per path; repetitions degrade to a navigation link (CYCLE_REVERT.md) |

`pending` and "unknown kind" are different states and a read must keep them
apart.

## Interplay with layering

Orthogonal: **type safety comes from the hash commitment, free and
universal; frontier sharing is economics** (LAYERING.md). "Can these ops
coexist and reference each other?" is always yes. "Should they pin each
other's tips?" is a separate question.

## Costs and cautions

- **The admission table is load-bearing.** It must be total (every
  role × kind has a verdict), convergent (verdicts from hash-committed
  inputs only) and versioned with the encoding. A missing row is a
  divergence bug.
- **Combinatorial semantics.** Each new kind multiplies potential edges;
  the table forces each combination to be decided rather than discovered.
- **Bare ids are opaque in flight.** Kind is knowable only with the
  preimage; relays route by the block tags ops travel in.

## Open problems

1. **Implement the extension path.** The decoders reject unknown op kinds
   (`DecodeError::InvalidOpTag`, src/encoding.rs `decode_node_with`), and
   the standalone wire node form has no body length to skip by: adding a
   kind is currently a hard fork (QUEUE.md #48). `Value::decode` conflates
   unknown value kind with pending (QUEUE.md #25).
2. **Retrofit audit.** Check that every admission verdict is a function of
   hash-committed facts computable by any replica knowing the relevant
   kinds, never of availability or version, and that every unknown-referent
   path orphans. Reading a *known* referent's body (a move op's `to`, for
   its splice point) is fine.
3. **Transclusion.** Render cycles are solved (above). Open: per-slot
   deletion (tombstoning a slot does not kill the object; unreachability
   from *every* slot is the GC condition, HASHWEB_SPEC.md), aliasing (edits
   visible through every container), and whether one object live-linked
   twice is surfaced generically by the renderer.
