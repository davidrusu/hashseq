# HashDoc op spec

Framework: FRAMEWORK.md (deps = observed-version commitment; Law I/II; resource →
conflict → resolution). Design rationale: HASHDOC.md.

HashDoc is the **composition**: one op DAG hosting many objects. It adds *no new
conflict type* — every op routes to a per-object projection that resolves per
its own spec (HASHSEQ_SPEC.md, HASHKV_SPEC.md, HASHLIST_SPEC.md, MARKS.md). What this
spec defines is object creation, routing, recursion, per-object tips, type
validation, and value indirection.

## Object types and the value column

```rust
enum Value {
    Char(char), Int(i64), Bool(bool), Bytes(Box<[u8]>),  // leaf scalars
    Blob(Hash),                                          // ≥32B side store
    Ref(Id),                                             // link to a child object
    NewText, NewList, NewMap,                            // this op *creates* a child object
}

enum Object {
    Text(Seq<char>),    // hashseq, HASHSEQ_SPEC.md
    List(Seq<Value>),   // hashseq over the value column
    Map(HashKv),        // HASHKV_SPEC.md
}
```

`Text`/`List` share all of `Seq`'s machinery (DAG, anchoring, runs, removes,
cursors); only the value column (`String` vs `Vec<Value>`) differs.

## Op

No new op shape — the substrate's ops carry an object route and a `Value`:

- **Create**: `InsertAfter(anchor, NewText|NewList|NewMap)` with id `X` **is**
  the child object `X`. Child ops anchor into `X`'s context.
- **Edit**: the existing seq/map/move/mark op of the target object, tagged with
  its object's `NodeIdx` (dict/positional encoded).

```
deps(u) = extra_dependencies ∪ named(u)        // per the op's own projection
```

A child object's first op depends on its creation op `X` (the bridge into the
parent's context), welding the document into one connected DAG.

## Resource / Conflict / Resolution

Routed, not new:

| object | resource / conflict / resolution |
|---|---|
| `Text` / `List` | gap → intra-gap order → total order by id; tombstone union (HASHSEQ_SPEC.md) |
| `Map` | key register → multi-head → MVR / freeze (HASHKV_SPEC.md) |
| position/parent | element register → multi-head + cycle → freeze / read-time revert (HASHLIST_SPEC.md) |
| marks | (char, kind) register → multi-head → MVR / freeze (MARKS.md) |

The substrate routes by object id; the projection applies and resolves. State is
a function of the op set per object; cross-object causality holds through
creation bridges (automerge-level guarantee).

## Per-object tips

Each object keeps **its own tips**. The run/write-run fast path needs
`tips = {previous op of this object}`; a global tips set would fragment runs
whenever any other object is concurrently edited. First op of a child deps on
its creation op `X`. Orphan buffering stays **global** (one
`missing_dep_id → waiting ops` map) since deps cross objects at creation
bridges.

## Type validation (apply-time gate)

An object's type is committed by its creation-op id (`NewText`/`NewList`/`NewMap`
is in the hash preimage), so replicas can never disagree on an object's type and
`Ref(id)` needs no type annotation. What commitment cannot prevent is an
ill-typed *child* op (e.g. `InsertAfter(elem_of_text_obj, Ref(..))`).

Rule — **validate before apply; ill-typed ops never enter the DAG or tips**:

- check = "payload type matches the anchor object's committed type"; total and
  convergent (both inputs hash-committed).
- an unknown anchor parks the op as a normal orphan; validation runs when the
  anchor arrives.
- a failed check is **permanent** (types are immutable) → quarantined like a
  failed inverted-span check (MARKS.md). No honest op can depend on it; anything
  that does was authored by a faulty peer and orphans forever.

## Value indirection (content-addressed blobs)

Threshold rule: a `Value` whose canonical encoding is ≤ 32 bytes (hash size)
embeds inline; larger → `Blob(hash)`, bytes in a content-addressed side store.

- ids commit to values (directly inline, or indirectly via the blob hash) →
  no same-id/different-value equivocation.
- ops stay small/chain-compressible regardless of value size; identical values
  dedupe; large values sync lazily (DAG verifies without payloads); blob bytes
  are erasable while every id still verifies (moderation/GDPR).
- **Sequence chars are exempt** — text bytes double as id-derivation material;
  they stay inline forever.
- A referenced blob whose bytes never arrive → `pending/unavailable` value
  state, surfaced to the app.

## Object deletion / GC

Tombstoning a `Ref` makes the child unreachable and its op subgraph locally
droppable, but a BFT peer can re-present those ops. "Deleted" for sync = honest
peers stop forwarding unreachable subgraphs; re-receiving one is harmless
(applying it cannot resurrect reachability). (HASHDOC.md open problem 1.)

## Substrate (shared, replica-local)

`IdIndex` + `ids` interning, tips maintenance / `tips_minus`, orphan buffering +
apply skeleton (dedup → missing-dep → tips update → dispatch), encoding (dict
header, positional refs, run sections). Hashseq is one instantiation; HashKv a
second; HashDoc the composition. Interning invariant: handles are replica-local;
everything convergence-relevant (sibling order, head sets, hashing, wire) stays
in `Id` space.
