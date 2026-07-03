# HashWeb op spec

Framework: FRAMEWORK.md (one reference set + honest frontier rule; Law I/II;
resource → conflict → resolution). Design rationale: HETEROGENEITY.md
(composition by reference, one namespace) and LAYERING.md (frontiers).

HashWeb is a **flat store of objects**: it holds every object this replica
knows about — roots opened out-of-band and children born by creation ops
alike — and adds *no new conflict type*. Every op is delivered in a routing
envelope to a per-object projection that resolves per its own spec
(HASHSEQ_SPEC.md, HASHKV_SPEC.md, MARKS.md). The store itself has no
identity, no root, and no state beyond its objects: merge is an
unconditional union of knowledge. What this spec defines is objects and
creation, the routing envelope, per-object tips, the schema gate, the value
side store, and deletion.

## Objects

```rust
seqs: Map<Id, HashSeq>,   // HASHSEQ_SPEC.md — ONE sequence kind: text, lists, mixed
kvs:  Map<Id, HashKv>,    // HASHKV_SPEC.md
```

Per-kind maps keyed by **object id** — no `Object` enum, no store-level
type tag: an object's kind is committed inside its id
(`object_id(kind ‖ seed)`, GRAMMAR_SPEC.md), so which map an id lives in
is derived, never negotiated.

There is no `Value` enum anywhere in the op layer: payloads, keys, and map
values are **ids** of content-addressed artifacts (HASHSEQ_SPEC.md
"Payload"; HASHKV_SPEC.md "Keys and values are ids") — scalar value
artifacts, blobs, creation artifacts, op nodes, or object ids (object
links; naming a *foreign* object's id is transclusion).

**Text and List are unified**: one seq kind, any payload in any slot.
"Textness" is a rendering and export convention — a seq whose payloads are
char values renders as a string; a mixed seq renders chars, embeds, and
placeholders per HETEROGENEITY.md — never a committed type, never a
convergence concern. JSON fidelity is an exporter's projection choice.

## Op: creation and routing

No new op shape.

- **Create**: an op whose payload/value is a creation artifact
  (`NewSeq`/`NewKv` — ordinary derived value ids, computed constants)
  creates a child object: `Insert { at, payload: New* }` from a sequence
  slot, `Put { key, value: New* }` from a map slot, identically. The
  child's ops anchor at `id(X)` — the creating op's id is the child's
  origin — and its store address is `object_id(kind ‖ id(X))`
  (GRAMMAR_SPEC.md), a derived name that never appears in a preimage. A
  root object's origin is the recursion's out-of-band base; its kind
  rides inside the derived address, so there is nothing else to agree on.
  The child's refs bottom at `X` itself, so the creation bridge welds
  each root's tree into one connected DAG literally; store-level
  buffering derives child addresses when creation ops apply. `X` in the
  parent's envelope means the parent element, `X` in the child's means
  the origin anchor — the envelope split leaves no dual-role ambiguity.
- **Edit**: the seq (insert/remove/move), map, or mark op of the target
  object, delivered in the **routing envelope** `obj_id ‖ node` — pure
  transport metadata, never hashed (there is no route field in the
  preimage — GRAMMAR_SPEC.md). The envelope needs no trust: an op
  enveloped to the wrong object never applies there (its refs never
  arrive inside that object), the same fate as any garbage ref — bounded
  and attributable, no verdict required. Handles (`NodeIdx`) are
  replica-local and never appear in artifacts (the interning invariant).

```
refs(u) = named(u) ∪ frontier pins   // honest: pins = the object's observed
                                     // frontier; named per the op's projection
```

## Resource / Conflict / Resolution

Routed, not new:

| object | resource / conflict / resolution |
|---|---|
| `Seq` | gap → intra-gap order → total order by id; tombstone union; placement register → freeze, same-container (HASHSEQ_SPEC.md) |
| `Kv` | key register → multi-head → MVR / freeze (HASHKV_SPEC.md) |
| marks | (element, kind) register → multi-head → MVR / freeze (MARKS.md) |

The substrate routes by object; the projection applies and resolves. State
is a function of the op set per object; cross-object causality holds through
creation bridges and named refs only (FRAMEWORK "Frontiers are per layer").

## Per-object tips

Each object keeps **its own tips**. The run/write-run fast path needs
`tips = {previous op of this object}`; a document-global tips set would
thread every concurrent edit anywhere in the document through this object's
deps — the frontier-granularity trade is LAYERING.md's subject. First op of
a child refs its origin (its creation op's id). Buffering is **two-level**: envelopes naming
an object id the store does not know park store-wide (birth or adoption
wakes them — the store's only delivery state); ops inside a live object
park on their first missing ref in that object's own buffer.

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
| `Insert . at` | insert, move op (its splice point), or the object's origin — in one `Seq` | gate |
| `Remove . target` | insert, in the op's own `Seq` | inert (non-insert); a ref living in another object never arrives here — parks forever, no verdict |
| `Move . target` | insert, in the object `to` resolves in (same-container rule) | gate |
| `Move . to` | insert, move op (any — including ops of `target`'s own chain: excision precedes placement and op ranks are permanent, so "put x where that op placed it" is well-defined), or the object's origin — in `target`'s object; not `target` itself (self-move) | gate |
| `Mark . anchor` (start, end) | insert, move op (its splice point — brackets wherever the op's target renders; anchored ops retain their rank fragment for life), or the object's origin, in one `Seq`; inverted spans gate (MARKS.md) | gate |
| `Mark . overwrites` | — | never gated: entries that are not covering same-kind marks are ignored by the definitional suppression filter (same class as `Put . overwrites` — kind- and coverage-scoping live in the read, not the gate) |
| `Put . overwrites` | — | never gated: entries that are not puts on the same key are ignored by the definitional head-set filter |
| op kind vs object kind | seq ops (`Insert`/`Remove`/`Move`/`Mark`) in a `Seq`; `Put` in a `Kv` | gate (reachable only by enveloping ops at a wrong-kind or colliding out-of-band seed — the kind is inside the derived object id, so honest kind mis-agreement is unrepresentable) |
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
