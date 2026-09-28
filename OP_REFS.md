# Op refs: ids, not positions

How ops reference other ops, and why the two natural alternatives were
rejected. The op shape itself is FRAMEWORK.md "The op shape"; the byte
layout is GRAMMAR_SPEC.md Part A.

## The model

- An op carries **one flat reference set** `refs(u)` of full 32-byte ids.
  Its role fields (anchor, targets, overwrites) *name* members of that set;
  the rest are frontier *pins*. Naming is committing: a named id is a ref by
  construction.
- The preimage stores `refs(u)` once, as a sorted, unique table; role
  fields address it by index. Indices are encoding, never identity.
- The named / pin split is positional and unflagged: nothing consumes it,
  so the artifact does not record it.
- In memory a node is stored normalized, `HashNode { pins, op }` with
  `pins = refs ∖ named` (src/hash_node.rs); `pins ∩ named = ∅` is an
  invariant every constructor keeps and the decoders enforce
  (`DecodeError::RedundantPin`).
- Values (payloads, keys, map values, `Place . placed_at`) are ids too, but
  they are **not refs**: never in the table, never buffered on
  (HETEROGENEITY.md "Payloads are ids").

## Why not positional refs

*Proposal:* the op commits only to its observed tips, and anchors, targets
and overwrites are positions in a canonical enumeration of the committed
closure `S(u) = refs*(tips)`. Attractive because the wire already encodes
refs by index, it would shrink large multi-target preimages, and it would
make "refs cannot escape the commitment" hold by type.

*Rejected* because no enumeration is both deterministic and local:

- **Sorted by id**: resolving "the i-th id within `S(u)`" means
  materializing ancestry, and positions carry no recency.
- **Frontier walk with dedup**: honest refs become small integers, but at a
  merge a position depends on what the branches shared, so the enumeration
  cannot be maintained incrementally and repeated merges force repeated
  O(|S|) walks.
- **Frontier walk without dedup** (path addressing): incremental, but
  position widths grow exponentially with merge depth.
- **The cold anchor**: the first edit after clicking into an old document
  anchors thousands of ops deep. Every scheme needs a full-id "far pointer"
  for it, which is the id-based model again.
- Positional ops are not **self-describing**: a remove's targets cannot be
  read, filtered or relayed without the DAG.

What the proposal wanted is delivered elsewhere: canonical bytes
(ENCODING_SPEC.md), one copy of each id per op (the refs table), and range
compression of removes (the wire encoding).

## Why not tips-only commitment

*Proposal:* commit only to the tips; resolve named ids against the closure
without committing them individually.

*Rejected:* a named id outside the commitment becomes representable, so
soundness needs a containment check `named ⊆ refs*(tips)`. Checking presence
at apply is delivery-order dependent (two replicas can reach opposite
permanent verdicts), and exact containment is an ancestry oracle, expensive
exactly in the honest cold-anchor case. With naming as committing the check
does not exist: a cold anchor costs one id, resolved in O(1), and buffering
waits on `refs(u)`.

## Why not a flagged tips field

An explicit `tips` field separate from the role fields would record which
refs the author called frontier. Nothing reads that partition, so it would
be committed bytes with no semantics; the flat set with roles as indices is
the shape.
