# Canonical encoding spec

Status: 2026-07-01. Framework: FRAMEWORK.md (op shape `{refs, op}`; one
flat reference set, roles as body indices). Defines the canonical byte
encoding at both
granularities — the per-op id preimage and the whole-document snapshot — so
that the claim is unconditional:

> **Equal op sets encode to identical bytes.** `encode(·)` is a pure function
> of the op set, with no encoder choices at any level. There is no caveat
> distinguishing "the op encoding is canonical" from "the document encoding
> is canonical" — both are.

Scope: **snapshots** (the durable artifact — storage, fingerprints,
signatures, dedupe). **Deltas** (incremental sync payloads) are transport —
consumed and discarded — and stay encoder-optimized; see Open problems.

## Per-op canonical form: the id preimage

An op's id is the hash of its canonical wire bytes:

```
id(u) = BLAKE3::derive_key(NODE_CONTEXT, encode_node(u))
// one context string for every op kind; kinds are tags in the encoding
```

- `encode_node(u)` is the node's self-contained canonical encoding
  (GRAMMAR_SPEC.md envelope ‖ body): the ids of `refs(u)` sorted ascending,
  once each (the per-op refs table), and the op body with *reference* fields
  as table indices. Hash cost is `|refs(u)|` ids plus index bytes — an id is
  never double-hashed whatever mix of roles references it.
- **Value fields are not references** (HASHSEQ_SPEC.md "Payload"): a payload
  appears in the body as its value id — always raw 32 B, never a dictionary
  index (GRAMMAR_SPEC.md) — while the *wire* form inlines the value's bytes
  **iff** the canonical value encoding is at or below the hash size — a
  rule, not an encoder choice. The id is derived at decode, so inline vs
  indirect never changes a preimage.
- **Injective because decodable**: tags and lengths reconstruct the node
  from its bytes, so distinct ops have distinct preimages. Versioning and
  domain separation live in the derive_key context string — bump it whenever
  the canonical encoding changes.
- An implementation that streams the preimage into the hasher without
  materializing it must be pinned, by test, to byte-equality with
  `encode_node`.

*Code today* (aligned 2026-07-02): `encoding::encode_node_preimage` is this
encoder; `HashNode::id` streams the identical bytes (locked by
`id_preimage_is_the_canonical_encoding`), with a single-buffer fast path for
the typing chain. The Part B stream is normalized: blocks derive from the
op set (shallowest-extender chain continuation for insert chains; the
pins-exactly-`{prev}` relation for remove chains, same fork rule) — equal
op sets encode to identical bytes across replicas and delivery orders,
asserted by the merge props and locked by the `canonical_snapshot_vector`
test. Strict acceptance is `decode_hashseq_strict` (re-encode and
compare). Measured on the editing traces: encode-time unchanged, wire
1.6–5.1% SMALLER than the old arrival-order grouping (canonical joins
chains storage kept split; causal-depth continuation keeps blocks
temporally contiguous).

## Canonical snapshot

A snapshot of op set `S` is a **dict header** followed by **one interleaved,
dependency-ordered stream of tagged blocks**. Three rule sets pin it: what
the blocks are, what order they emit in, and how references encode.

### Blocks: chains derived from the op set

Ops group into blocks by kind — **runs** (insert chains, in both anchor
flavors) and **remove chains** (link chains, contiguous spans, singles) —
under rules that are functions of `S` alone:

- A chain member continues its predecessor per the fast-path relation
  (anchor/target-link = prev). Members may carry additional refs: a run
  extends *through* interior extra-deps, which attach at their interior
  offset — typing across a delete does not split the canonical run.
- **Fork rule: at a fork, the causally shallowest extender continues the
  chain** (ties by smallest id); every other extender heads its own block.
  Causal depth — `depth(u) = 1 + max depth(refs(u))`, origin 0 — is the
  op-set-visible proxy for authoring time: the true typing continuation
  was authored the moment after its anchor (depth ≈ anchor+1), while a
  come-back-later fork child pins a later frontier and sits deeper.
  Choosing the shallowest keeps blocks temporally contiguous, which keeps
  the block dependency graph near-acyclic and the dictionary small
  (measured: beats even arrival-order grouping — see Open problems 5).
  Normatively: `refs(u)` is the node's full reference set (`pins ∪ named`,
  FRAMEWORK.md) over every op kind, so depth — hence the whole derivation —
  is a pure function of the op set; the rule governs insert chains and
  remove chains alike (a remove link's extenders are the removes whose
  pins are exactly its id).

Block derivation must not consult replica storage. Stored chain groupings
are arrival-order artifacts (under concurrency, whichever extension applied
first extended the stored run); deriving blocks from the op set is what
keeps storage layout from leaking into bytes — and is exactly why
`encode(decode(encode(x))) = encode(x)` holds.

### Order: dependency-ordered emission with deterministic cycle breaks

Blocks emit in Kahn order over block-level reference edges, **smallest head
id first** among ready blocks. Two edge classes:

- **Hard edges — a remove never precedes a run it targets.** Remove→run
  target edges are mandatory. They are bipartite (removes point only at
  runs), hence acyclic, so the hard constraints are always schedulable.
  This matters because target references are the highest-volume refs in
  delete-heavy documents: if a wide multi-target remove could emit before
  the runs it targets, every one of its targets would spill to the dict as
  a full id.
- **Soft edges — everything else** (run→run anchors and deps, run→remove
  interior deps, remove→remove chain deps). Block-level cycles among soft
  edges are possible even though the element DAG is acyclic — two
  concurrent writers whose runs each reference an element of the other's;
  a run typed across a delete of its *own* element (run↔remove 2-cycle).
  **Deterministic break: when no block is ready, force-emit the blocked
  block with the smallest head id**; its unresolved refs fall back to the
  dict.

The stream is therefore *near*-topological: every positional reference
points backward, and the residual forward references are full ids carried
in the dict.

### References: tagged refs, within-kind ranks, range compression

All id-valued positions in block bodies are tagged varint **refs** whose low
bits select the form:

- **run-element ref** `(run_rank, offset)` — rank counts run blocks in
  emission order. This is the dominant form by orders of magnitude, so it
  owns the cheapest (1-bit) tag;
- **remove ref** `(remove_rank, offset)` — removes are addressable in their
  own compact rank space. Within-kind ranks, not global emit positions:
  interleaving kinds in one stream must not widen the millions of
  run-element refs a delete-heavy document carries;
- **dict ref** — index into the dict header, the escape hatch.

Multi-target remove sets encode as **ranges**: targets resolve to
`(run_rank, offset)`, sort, and consecutive offsets coalesce into
`(run_rank, start, count)` segments; non-element and non-contiguous targets
stay singletons. Set semantics make the sort free, segment order is
determined by the sorted resolution, and decode rebuilds the exact target
set — so the op's id survives roundtrip. Batch deletes are
position-contiguous almost by definition, which makes this the difference
between one segment and thousands of per-target refs.

Chain interiors elide per-op structure (tips/anchor are implicitly `prev`);
the decoder reconstructs each member's full preimage deterministically —
which is what makes recomputed ids well-defined.

### The dict: small, enumerable, deterministic

The dict header carries the full 32-byte ids that cannot be positional:

- entry 0, implicit: the **origin** (`doc_id`);
- ids spilled by force-broken soft cycles.

A snapshot is self-contained, so no other class exists (transport encodings
may additionally dict-reference ops the receiver already holds). The dict's
contents are a deterministic consequence of the order rule, so it is part
of the canonical form, not an encoder choice.

Raw ids therefore appear in a snapshot only as: the origin, cycle spills,
blob hashes, and `Ref` *values* (HASHWEB_SPEC.md — those are values, not
references). Everything else is positional.

## Decode and verification

Decoding reconstructs preimages and recomputes ids from the received bytes —
the authoritative derivation; there are no claimed ids to trust. Each
element hashes exactly once (chain hashing in block order, ids threaded
through to apply). Apply order is near-topological: orphan buffering
engages only within force-broken cycles, bounded by their membership.

Acceptance modes:

- **strict** (canonical artifacts): after decode, verify the bytes are the
  canonical encoding of the decoded set — replay the block derivation and
  emission order (the encoder's own O(n log n) cost), or re-encode and
  compare hashes. Only strict-verified bytes may be cached, deduped, or
  fingerprinted as canonical.
- **transport** (interop): any well-formed stream decodes to its op set —
  ops are self-certifying regardless — but the bytes carry no canonical
  status.

**Amplification.** An adversarial snapshot costs linear work in bytes read:
per-op hashing is the same work an honest snapshot costs, the first
malformed block rejects the artifact, and engineered soft cycles are
bounded by the force-emit rule — each costs the attacker the ops that form
it and costs the stream one dict spill. No input makes decode superlinear.

## What canonicality buys

- **state fingerprint**: `H(snapshot)` is a well-defined document checksum.
  (For pure set-equality the tip set is already canonical —
  `H(sorted tips)` Merkle-commits the op set in O(|tips|); the snapshot
  hash adds *byte* identity: equal tips ⟺ equal op set ⟺ identical
  snapshot bytes.)
- **reproducible artifacts**: exports, backups, and signatures over
  snapshots are stable across replicas and re-encodes;
- **content-addressed storage**: identical documents and chunk-aligned
  subhistories dedupe at the byte level;
- **no transport malleability** for anything labeled a canonical snapshot.

## Interaction with the family

One stream, one order, all layers: HashWeb snapshots interleave blocks of
all objects and layers (creation bridges connect the DAG, so the dependency
order spans objects; blocks carry their object route per HASHWEB_SPEC.md).
Each layer brings its chain analog as a block kind — kv write-runs (session
chains), mark ops (individually encoded, MARKS.md), move chains — with the
same ref forms addressing into any earlier block. Standalone hashseq is the
one-object special case. Blob payloads are content-addressed side objects,
not op-stream bytes (their hashes are; erasability per HASHWEB_SPEC.md is
unaffected).

## Conformance obligations

- **Determinism**: same op set → identical bytes, across replicas,
  delivery orders, and storage histories — not merely repeat-encoding of
  one replica.
- **Roundtrip byte identity**: `encode(decode(encode(x))) = encode(x)`.
- **Preimage lock**: the streamed hash input is byte-equal to
  `encode_node`.
- **Structural invariance**: block counts double as a structure checksum —
  a change intended to be wire-only must not move them; a change to block
  derivation is a canonical-form change and bumps the context string.
- **Identity preservation**: range and elision forms must rebuild exact
  ref sets; op ids are the regression test (any lossy compression changes
  a preimage and shows up as a different id).

## Open problems

1. **Byte grammar — drafted.** GRAMMAR_SPEC.md pins the preimage grammar
   (Part A, identity-frozen) and the stream grammar (Part B,
   stream-versioned). The snapshot test vector is locked
   (`canonical_snapshot_vector`); stream-ref bit-packing widths remain.
2. **Dict-residual refinements — adopted** (GRAMMAR_SPEC.md): the run-split
   rule and `RemoveChain` consolidation are canonical form, decided before
   the freeze.
3. **Delta canonicalization — punted.** A delta is transport; if
   content-addressed patch exchange appears, define it per
   `(from-frontier, to-frontier)` pair with these same rules restricted to
   the difference set.
4. **Chunk alignment.** Byte-level dedupe across *versions* wants chunk
   boundaries that survive appends; blocks give natural boundaries — decide
   whether to spec a chunking discipline or leave it to storage.
5. **Encoder cost — measured; resolved by the causal-depth fork rule.**
   Block derivation from the op set is encode-time-only (build times and
   structure checksums unchanged). The experiments (2026-07-02): a
   smallest-id fork rule cost +4–8% wire vs arrival grouping, and a
   longest-continuation rule was no better — the delta was never chain
   cuts but **dictionary growth**: rules uncorrelated with authoring time
   glue temporally distant segments into one block, producing
   force-broken soft cycles in the emission order, each spilling full
   ids. Causal depth is the op-set-visible proxy for authoring time;
   the shallowest-extender rule reconstructs first-arrival grouping on
   honest histories and additionally joins chains storage kept split —
   final bytes 1.6–5.1% below the old arrival-order encoder. Residual
   levers if ever needed: same-block backward positional refs (needs a
   two-pass run decode), cycle-aware emission (must stay deterministic).
