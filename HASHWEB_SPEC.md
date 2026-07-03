# HashWeb op spec

Framework: FRAMEWORK.md (one reference set + honest frontier rule; Law I/II;
resource → conflict → resolution). Design rationale: HETEROGENEITY.md
(composition by reference, one namespace) and LAYERING.md (frontiers).

HashWeb is the **composition**: one op DAG hosting many objects. It adds *no
new conflict type* — every op routes to a per-object projection that resolves
per its own spec (HASHSEQ_SPEC.md, HASHKV_SPEC.md, MARKS.md). What this spec
defines is objects and creation, routing, per-object tips, the schema gate,
the value side store, and deletion.

## Objects

```rust
enum Object {
    Seq(SeqState),   // HASHSEQ_SPEC.md — ONE sequence kind: text, lists, mixed
    Map(HashKv),     // HASHKV_SPEC.md
}
```

There is no `Value` enum anywhere in the op layer: payloads, keys, and map
values are **ids** of content-addressed artifacts (HASHSEQ_SPEC.md
"Payload"; HASHKV_SPEC.md "Keys and values are ids") — scalar value
artifacts, blobs, creation artifacts, op nodes, or origin ids (object
links; naming a *foreign* object's origin id is transclusion).

**Text and List are unified**: one seq kind, any payload in any slot.
"Textness" is a rendering and export convention — a seq whose payloads are
char values renders as a string; a mixed seq renders chars, embeds, and
placeholders per HETEROGENEITY.md — never a committed type, never a
convergence concern. JSON fidelity is an exporter's projection choice.

## Op: creation and routing

No new op shape.

- **Create**: an op whose payload/value is a creation artifact
  (`NewSeq`/`NewMap` — ordinary derived value ids, computed constants)
  creates a child object: `Insert { at, payload: New* }` from a sequence
  slot, `Put { key, value: New* }` from a map slot, identically. The
  object's identity is its **origin id**,
  `object_id = derive_key(OBJECT_CONTEXT, X)` for creating op `X` — a
  virtual node, never an op, generalizing origin unification (a root
  object's origin is the recursion's out-of-band base — chosen with its
  kind, one agreement). Child ops anchor at and ref the origin id; the
  closure of an origin id is defined as `{X} ∪ closure(X)`, so the
  creation bridge welds each root's tree into one connected DAG,
  and buffering resolves origin ids by derivation when creation ops apply.
  Because `X` (the parent element) and `object_id` (the child origin) are
  distinct ids, refs are never ambiguous between parent and child.
- **Edit**: the seq (insert/remove/move), map, or mark op of the target
  object. **Routing**: an op's object is *derived* — from its named refs
  (anchor/target/overwrites are same-object by the edge table) whenever it
  has any, else from its refs as a whole (a fresh `Put`'s refs are the
  object's own frontier, which begins at the origin id — the origin-id
  split above is what makes this unambiguous). There is no route field
  (GRAMMAR_SPEC.md). Refs that determine no single object fail the
  apply-time gate (each ref's object is hash-committed — stable). Handles
  (`NodeIdx`) are replica-local and never appear in artifacts (the
  interning invariant).

```
refs(u) = named(u) ∪ frontier pins   // honest: pins = the object's observed
                                     // frontier; named per the op's projection
```

## Resource / Conflict / Resolution

Routed, not new:

| object | resource / conflict / resolution |
|---|---|
| `Seq` | gap → intra-gap order → total order by id; tombstone union; placement register → freeze, same-container (HASHSEQ_SPEC.md) |
| `Map` | key register → multi-head → MVR / freeze (HASHKV_SPEC.md) |
| marks | (element, kind) register → multi-head → MVR / freeze (MARKS.md) |

The substrate routes by object; the projection applies and resolves. State
is a function of the op set per object; cross-object causality holds through
creation bridges and named refs only (FRAMEWORK "Frontiers are per layer").

## Per-object tips

Each object keeps **its own tips**. The run/write-run fast path needs
`tips = {previous op of this object}`; a document-global tips set would
thread every concurrent edit anywhere in the document through this object's
deps — the frontier-granularity trade is LAYERING.md's subject. First op of
a child refs its origin id. Orphan buffering stays **global** (one
`missing_ref_id → waiting ops` map) since refs cross objects at creation
bridges and anchors.

## The edge table (the apply-time gate)

Two facts are hash-committed and version-independent: an object's **type**
(the creation artifact is in the creating op's preimage — replicas can
never disagree, and object links need no type annotation) and every
referent's **kind**. So reference validation is one shared gate, run when
an op leaves the orphan buffer (all refs present), issuing per-edge
verdicts that are total, convergent, and stable:

- **meaningful** — apply proceeds;
- **inert** — tolerated, no effect;
- **gated** — permanent quarantine; no honest op ever depends on a gated op;
- a referent of **unknown kind** yields no verdict: the op parks until the
  kind is known (HETEROGENEITY.md — unknown-ness can never gate).

| op . role | admits | otherwise |
|---|---|---|
| `Insert . at` | insert, move op (its splice point), or the object's origin id — in one `Seq` | gate |
| `Remove . target` | insert, in the op's own `Seq` | inert (non-insert); gate (cross-object, via routing) |
| `Move . target` | insert, in the object `to` resolves in (same-container rule) | gate |
| `Move . to` | insert, move op (any — including ops of `target`'s own chain: excision precedes placement and op ranks are permanent, so "put x where that op placed it" is well-defined), or the origin id — in `target`'s object; not `target` itself (self-move) | gate |
| `Mark . anchor` (start, end) | insert, move op (its splice point — brackets wherever the op's target renders; anchored ops retain their rank fragment for life), or the origin id, in one `Seq`; inverted spans gate (MARKS.md) | gate |
| `Mark . overwrites` | — | never gated: entries that are not covering same-kind marks are ignored by the definitional suppression filter (same class as `Put . overwrites` — kind- and coverage-scoping live in the read, not the gate) |
| `Put . overwrites` | — | never gated: entries that are not puts on the same key are ignored by the definitional head-set filter |
| op kind vs object type | seq ops (`Insert`/`Remove`/`Move`/`Mark`) in a `Seq`; `Put` in a `Map` | gate |
| routing | the op's refs must determine one object | gate |
| pins (unroled refs) | anything | always meaningful — pure frontier pins |
| payloads / keys / values | any id | never edge-checked: values are not references, and payload kinds are not schema-gated (Objects, above) — schema is the renderer's concern |

**Gate vs filter.** Two constraint classes, deliberately distinct: **kind
checks** (the "gate" rows) are stable apply-time verdicts; **value-dependent
constraints** (same key, same `kind_v`) live in the definitional read —
free at apply, no verdict to get wrong, and the incremental head-set update
filters identically.

**Tighten never, loosen carefully.** Gate verdicts are permanent and must
be computed identically by every replica, so this table is versioned
semantics. *Tightening* a row after launch would quarantine ops already
applied inside honest documents — a true fork, forbidden. *Loosening* a row
(e.g. someday admitting move-op splice points as mark anchors) is an
upgrade-with-re-evaluation: quarantined ops are re-judged under the new
rules, and the cross-version divergence is the same park-until-upgrade
class as unknown kinds. Rule of thumb: **when in doubt, gate** —
strictness is recoverable, laxness is forever.

## The value side store

Value artifacts at or below the hash size encode inline (identity-neutral:
the value id is derived at decode — ENCODING_SPEC.md); larger artifacts live
in a content-addressed side store.

- identical values dedupe; large values sync lazily — the op DAG verifies
  with no payload bytes at all;
- **erasure without breaking verification**: drop an artifact's bytes, keep
  its id — every op id still verifies (moderation / GDPR-style deletion);
- an artifact whose bytes never arrive is the `pending/unavailable` value
  state — for payloads, map keys, and map values alike — surfaced to the
  app, never papered over.

## Object deletion / GC

Tombstoning an object's slots makes the child unreachable and its op
subgraph locally droppable, but a BFT peer can re-present those ops.
"Deleted" for sync = honest peers stop forwarding unreachable subgraphs;
re-receiving one is harmless (applying it cannot resurrect reachability).
With transclusion, unreachable means unreachable from *every* slot
(HETEROGENEITY.md open problems).

## Substrate (shared, replica-local)

`IdIndex` + `ids` interning, tips maintenance, orphan buffering + apply
skeleton (dedup → missing-ref → tips update → dispatch), encoding (dict
header, positional refs, run sections; canonical per ENCODING_SPEC.md).
Hashseq is one instantiation; HashKv a second; HashWeb the composition.
Interning invariant: handles are replica-local; everything
convergence-relevant (sibling order, head sets, hashing, wire) stays in `Id`
space.
