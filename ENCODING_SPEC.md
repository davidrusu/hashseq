# Canonical encoding spec

Framework: FRAMEWORK.md (op shape `{refs, op}`; one flat reference set,
roles as body indices). This spec fixes the canonical encoding at both
granularities — the per-op id preimage and the whole-document snapshot:

> **Equal op sets encode to identical bytes.** `encode(·)` is a pure
> function of the op set, with no encoder choices at any level.

Scope: **snapshots** — the durable artifact (storage, fingerprints,
signatures, dedupe). **Deltas** are transport — consumed and discarded —
and are not canonical (Open problem 3); their frames are listed under
"Transport frames".

## Per-op canonical form: the id preimage

```
id(u) = BLAKE3::derive_key(NODE_CONTEXT, encode_node(u))
```

- `encode_node(u)` is GRAMMAR_SPEC.md Part A (`envelope ‖ body`): the ids
  of `refs(u)` sorted ascending, once each, and the body with reference
  fields as table indices. An id is hashed once however many roles name it.
- **Value fields are not references** (HASHSEQ_SPEC.md "Payload"): a
  payload, key or value appears in the body as its short form
  (GRAMMAR_SPEC.md "Value fields"), never a dictionary index. Whether the
  wire inlines the artifact never changes the preimage; the id is derived
  at decode.
- **Injective because decodable**: tags and lengths reconstruct the node
  from its bytes, so distinct ops have distinct preimages. Versioning and
  domain separation live in the context string; bump it iff the preimage
  grammar changes.
- `encoding::encode_node_preimage` is the reference encoder. `HashNode::id`
  streams the same bytes (with stack-assembled fast paths for the typing
  and backspace shapes) and is locked to it by
  `id_preimage_is_the_canonical_encoding`.

## Canonical snapshot

A snapshot of op set `S` is a **dict header** followed by **one
interleaved, dependency-ordered stream of tagged blocks**. Three rule sets
pin it: which blocks, in what order, and how references encode. Block
derivation reads the op set only, never replica storage: stored chain
groupings are arrival-order artifacts, and keeping them out of the bytes
is what makes `encode(decode(encode(x))) = encode(x)` hold.

### Blocks: chains derived from the op set

Ops group into **runs** (insert chains) and **remove chains**:

- **Run**: element `x` continues `p`'s chain iff `x` is
  `Insert{After(p)}`. The head may be anchored either side. A run extends
  *through* interior extra-deps, which attach at their offset — typing
  across a delete does not split it. Atoms (non-char payloads) never chain.
- **Remove chain**: single-target remove `r₂` continues `r₁` iff
  `pins(r₂) = {id(r₁)}`. Each chain is segmented into maximal spans that
  step ±1 in one direction within one canonical run (forward/backward
  spans); an isolated link is a single; a link whose target is not a run
  element, and every multi-target remove, is an "other" block.
- **Fork rule: at a fork, the causally shallowest extender continues the
  chain** (ties by smallest id); every other extender heads its own block.
  `depth(u) = 1 + max depth(refs(u))`, origin 0, where `refs(u)` is the
  full reference set (`pins ∪ named`) over every op kind — so the whole
  derivation is a function of the op set. The rule applies to insert and
  remove chains alike.

  *Why depth:* it is the op-set-visible proxy for authoring time. A true
  typing continuation was authored right after its anchor (depth ≈
  anchor + 1); a come-back-later fork child pins a later frontier and sits
  deeper. Picking the shallowest keeps blocks temporally contiguous, which
  keeps the block graph near-acyclic and the dict small (Open problem 5).

Every other op (moves, marks, places, atoms, orphans) travels as an
individual node (Part B `Node` block; the trailing section in the current
layout).

### Order: dependency-ordered emission with deterministic cycle breaks

Blocks emit in Kahn order over block-level reference edges, **smallest
head id first** among ready blocks. Two edge classes:

- **Hard edges — a remove never precedes a run it targets.** Remove→target
  edges have no dict fallback. They point from removes to runs only, so
  the hard graph is bipartite, hence acyclic and always schedulable. This
  matters because targets are the highest-volume refs in delete-heavy
  documents: a wide remove emitted early would spill every target to the
  dict.
- **Soft edges — everything else** (run anchors and deps, run→remove
  interior deps, remove→remove chain deps). Block-level soft cycles occur
  even though the op DAG is acyclic: two writers whose runs each reference
  the other's element; a run typed across a delete of its own element.
  **Deterministic break: when no block is ready, force-emit the causally
  shallowest blocked block (depth of its head) whose hard edges are
  satisfied, ties by smallest head id.** Its unresolved refs fall back to
  the dict. Choosing by depth emits the old chain in a cycle first, so
  anchors into it resolve positionally and only its own late deps spill.

The stream is *near*-topological: every positional ref points strictly
backward; the residual forward refs are full ids in the dict.

### References: tagged refs, within-kind ranks, range compression

Every id-valued position in a block body is a tagged varint **ref**; the
low bits select the form:

| form        | head varint               | then           | addresses                                   |
|-------------|---------------------------|----------------|---------------------------------------------|
| run element | `(run_rank << 1) \| 1`    | `offset:varint`| element `offset` of the `run_rank`-th run   |
| dict        | `dict_idx << 2`           | —              | dict entry                                  |
| remove      | `(remove_rank << 2) \| 2` | `offset:varint`| op `offset` of the `remove_rank`-th remove block |

Ranks count blocks *within their kind* in emission order, not global emit
positions: interleaving removes into the stream must not widen the
millions of run-element refs a delete-heavy document carries. The
dominant run-element form owns the 1-bit tag. A ref may only address a
strictly earlier block; same-block and forward refs use the dict. A ref
set is `count:varint` then its members in ascending id order.

**Range compression.** A multi-target remove resolves its targets, sorts
them, and coalesces consecutive run offsets into `(run_rank, start,
count)` segments; other targets stay singletons. Set semantics make the
sort free and decode rebuilds the exact target set, so the op id survives
the roundtrip. Batch deletes are position-contiguous, so this is one
segment instead of thousands of refs.

**Elision.** Chain interiors carry no per-op pins/anchor (implicitly
`prev`); the decoder reconstructs each member's full preimage
deterministically, which is what makes recomputed ids well-defined.

### The dict

Full 32-byte ids that cannot be positional:

- entry 0, implicit: the **origin** (`doc_id`);
- ids referenced before (or within) the block that exposes them — the
  spills of force-broken soft cycles;
- refs from individually-carried nodes to ids no block exposes (e.g. an
  orphan's missing deps).

The dict (entries after 0 sorted ascending) is a deterministic consequence
of the order rule, so it is part of the canonical form. Other raw ids in a
snapshot are values, not references: by-id payloads, `Put`/`Mark`/`Place`
value fields, and blob hashes (HASHWEB_SPEC.md `Ref` values included).

## Byte layouts

The current encoder (`src/encoding.rs`). It follows the rules above;
GRAMMAR_SPEC.md Part B is the versioned target grammar. `ref` / `ref_set`
are the positional forms above unless stated otherwise.

### Seq snapshot (`encode_hashseq`)

```
seq      := origin:id ‖ n_dict:varint ‖ n_dict × id     -- dict entries 1.., ascending
            n_blocks:varint ‖ block*  ‖ n_trail:varint ‖ trail_node*
```

| tag | block        | body                                                                                           | exposes          |
|-----|--------------|------------------------------------------------------------------------------------------------|------------------|
| 0   | run, `After` head  | `ref anchor ‖ ref_set first_pins ‖ text_len:varint ‖ utf8 ‖ n:varint ‖ n × (offset:varint ‖ ref_set)` | its elements |
| 1   | run, `Before` head | same                                                                                     | its elements     |
| 2   | remove span, forward  | `ref_set first_pins ‖ run_rank:varint ‖ start:varint ‖ end:varint`                   | its links        |
| 3   | remove span, backward | same, `start > end`                                                                  | its links        |
| 4   | remove single | `ref_set pins ‖ ref target`                                                                    | the remove       |
| 5   | remove other  | `ref_set pins ‖ n_seg:varint ‖ segment*`                                                       | the remove       |

- Run interior offsets are ascending and ≥ 1; element ids are derived by
  hashing each member's reconstructed preimage.
- A span removes elements `start..=end` (forward) or `start` down to `end`
  (backward) of the run at `run_rank`, one link each: the first link pins
  `first_pins`, each later link pins `{previous link}`.
- A segment reuses the ref head: a run-element head is a range
  (`offset ‖ count`); dict and remove heads are single refs. Segments emit
  as ranges (ascending), then remove singles, then dict singles.
- **Trailing section**: every applied op not in a block (moves, marks,
  places, atom inserts) plus orphans, sorted by node id, each in the
  standalone node form with refs positional (any block may be addressed).

### Standalone node form

Used for trailing nodes, deltas, and anywhere a node travels outside a
block. Refs are raw 32-byte ids and `ref_set = count:varint ‖ ids`, except
in the seq trailing section, where they are positional.

```
node   := tag:u8 ‖ pins:ref_set ‖ fields
anchor := side:u8 (0 = Before, 1 = After) ‖ ref
```

| tag | op      | fields                                                                                  |
|-----|---------|-----------------------------------------------------------------------------------------|
| 0   | run     | `side:u8 (0 = After, 1 = Before) ‖ anchor:id ‖ first_pins ‖ text ‖ interior` (standalone only; no `pins` field) |
| 1   | Insert  | `anchor ‖ payload`                                                                      |
| 2   | Remove  | `ref_set targets`                                                                       |
| 3   | Move    | `ref target ‖ anchor ‖ ref_set overwrites`                                              |
| 4   | Put     | `key:id ‖ value:id ‖ ref_set overwrites`                                                |
| 5   | Mark    | `anchor start ‖ anchor end ‖ kind_v:id ‖ value:id ‖ ref_set overwrites`                 |
| 6   | Place   | `placed_at:id ‖ ref_set overwrites`                                                     |

`payload := 0x00 ‖ len:varint ‖ artifact` (inline; `len ≤ 32`, larger is
malformed) `| 0x01 ‖ id`. The encoder inlines `Char` payloads and sends
every other payload by id. These are transport tags, distinct from the
Part A kind tags. Decoders reject a node whose pins repeat a named id
(`RedundantPin`) or that has no refs (`NoRefs`).

## Interaction with the family

The kv and web snapshots are canonical and nest the seq form.

**Kv snapshot** (`encode_hashkv`). Applied kv nodes are puts and places
with causal refs, so the stream is a plain topological order (smallest id
first among ready nodes) with no cycle machinery and a one-entry implicit
dict. A kv ref is `0` = origin, `k` = the `k`-th node of the stream.

```
kv   := origin:id ‖ n:varint ‖ kv_node* ‖ n_trail:varint ‖ node*  ‖ n_art:varint ‖ (len:varint ‖ artifact)*
kv_node := tag:varint (0 = Put, 1 = Place) ‖ np:varint ‖ np × kref
           ‖ (Put: key:id ‖ value:id | Place: placed_at:id) ‖ no:varint ‖ no × kref
```

Trailing nodes are orphans, id-sorted, in the standalone form with full
ids. Artifacts are sorted by value id and never identity-form (a decoder
drops such entries, so strict decode rejects them).

**Web snapshot** (`encode_hashweb`):

```
web := "HWB2" ‖ n_art:varint ‖ (len:varint ‖ artifact)*        -- sorted by value id
       ‖ n_obj:varint ‖ (kind:u8 ‖ len:varint ‖ inner)*         -- sorted by object id
       ‖ n_trail:varint ‖ (obj_id:id ‖ node)*                   -- sorted by (object, node id)
```

- Each object nests its own canonical seq or kv stream. The object id is
  not on the wire: it is derived as `object_id(kind, inner origin)`, so a
  non-derived key cannot survive strict decode. Any object section is a
  complete replica root (holonic).
- The trailing section is store-orphaned routing envelopes (`obj_id ‖
  node`).
- **One document-wide artifact section**, the union of the web store and
  every inner store; inner kv streams carry an empty artifact section
  (inner stores are replica-local views and would break byte canonicality
  across merge orders).
- Decoders also accept the legacy form without the magic (kv nodes
  untagged, all `Put`); the encoder always emits `HWB2`.
- `encode_hashweb_ops` is a transport variant that ships only artifacts ≤
  `WIRE_ARTIFACT_MAX` (1024 B); larger blobs verify by id and arrive
  separately. It is not a canonical snapshot.

Strict acceptance mirrors the seq (`decode_hashkv_strict`,
`decode_hashweb_strict`).

**Scope of the claim with artifacts.** Op-stream sections are pure
functions of the op set; artifact sections are functions of the replica's
content-addressed store. Equal op sets AND equal artifact stores encode
identically. Availability is not op-set state: blob payloads are side
objects, only their hashes are op-stream bytes, and erasability
(HASHWEB_SPEC.md) is unaffected.

The target refinement is one fully interleaved stream — blocks of all
objects in one dependency order, cross-object origins connecting the DAG,
blocks carrying their object route, kv write-run chains as a block kind —
worth taking when cross-object volume justifies shared dicts and
positional refs across sections.

## Transport frames

Not canonical; consumed and discarded. Frames address objects by
**(kind, origin)**, never by object id: the derivation is one-way, so a
receiver could not open an unknown object from its id.

| tag    | frame    | layout                                                                              |
|--------|----------|-------------------------------------------------------------------------------------|
| `0xDE` | delta    | `0xDE ‖ [kind:u8 ‖ origin:id ‖ n:varint ‖ n × (len:varint ‖ node)]*` — standalone nodes, full ids; run form rejected |
| `0xAF` | artifact | `0xAF ‖ artifact bytes` — content-addressed at the receiver; artifacts never ride deltas |
| `0xC1` | frontier | `0xC1 ‖ [kind:u8 ‖ origin:id ‖ tips:(count:varint ‖ ids)]*` — the sender's clock; an absent object is one it does not hold |

Untagged bytes starting `HWB2` are a web snapshot. Applying a delta twice
is a no-op; out-of-order ops orphan on their refs.

## Decode and verification

Decoding reconstructs preimages and recomputes ids from the received
bytes — there are no claimed ids to trust. Each element hashes exactly
once (chain hashing in block order, ids threaded through to apply). Apply
order is near-topological: orphan buffering engages only within
force-broken cycles, bounded by their membership.

Acceptance modes:

- **strict** (canonical artifacts): after decode, verify the bytes are the
  canonical encoding of the decoded set (re-encode and compare, or replay
  derivation and order). Only strict-verified bytes may be cached, deduped,
  or fingerprinted as canonical.
- **transport**: any well-formed stream decodes to its op set — ops are
  self-certifying — but the bytes carry no canonical status.

**Amplification.** An adversarial snapshot must cost work linear in the
bytes read: per-op hashing is what an honest snapshot costs, the first
malformed block rejects the artifact, and each engineered soft cycle
costs the attacker the ops that form it and the stream one dict spill.
No input may make decode superlinear.

## What canonicality buys

- **State fingerprint**: `H(snapshot)` is a document checksum. (For set
  equality alone, `H(sorted tips)` already commits the op set in
  O(|tips|); the snapshot hash adds byte identity: equal tips ⟺ equal op
  set ⟺ identical bytes.)
- **Reproducible artifacts**: exports, backups, and signatures over
  snapshots are stable across replicas and re-encodes.
- **Content-addressed storage**: identical documents and chunk-aligned
  subhistories dedupe at the byte level.
- **No transport malleability** for anything labeled a canonical snapshot.

## Conformance obligations

- **Determinism**: same op set → identical bytes across replicas, delivery
  orders and storage histories — not merely repeat encodes of one replica
  (the merge properties; `canonical_snapshot_vector` locks one concurrent
  document's bytes).
- **Roundtrip byte identity**: `encode(decode(encode(x))) = encode(x)`.
- **Preimage lock**: the streamed hash input is byte-equal to
  `encode_node`.
- **Structural invariance**: block counts double as a structure checksum —
  a wire-only change must not move them. A change to block derivation or
  the stream grammar is a canonical-form change: it bumps the stream
  version and `canonical_snapshot_vector`, never the identity context.
- **Identity preservation**: range and elision forms must rebuild exact
  ref sets; op ids are the regression test (lossy compression changes a
  preimage and shows up as a different id).

## Open problems

1. **Byte grammar.** GRAMMAR_SPEC.md pins the preimage grammar (Part A,
   identity-frozen) and the target stream grammar (Part B,
   stream-versioned, not yet implemented). Stream-ref bit-packing widths
   remain to pin.
2. **Dict-residual refinements — adopted, not yet implemented**
   (GRAMMAR_SPEC.md Part B): the run-split rule and `RemoveChain`
   consolidation are canonical form.
3. **Delta canonicalization — punted.** A delta is transport. If
   content-addressed patch exchange appears, define it per
   `(from-frontier, to-frontier)` with these rules restricted to the
   difference set.
4. **Chunk alignment.** Byte-level dedupe across versions wants chunk
   boundaries that survive appends; blocks are natural boundaries. Decide
   whether to spec a chunking discipline or leave it to storage.
5. **Encoder cost — resolved by the causal-depth fork rule.** Deriving
   blocks from the op set is encode-time only. A smallest-id fork rule cost
   +4–8% wire versus arrival-order grouping, and a longest-continuation
   rule was no better: the cost was never chain cuts but **dict growth** —
   rules uncorrelated with authoring time glue temporally distant segments
   into one block, creating soft cycles that spill full ids. The
   shallowest-extender rule reconstructs first-arrival grouping on honest
   histories and also joins chains storage kept split: 1.6–5.1% smaller
   than the arrival-order encoder. Residual levers: same-block backward
   positional refs (needs a two-pass run decode), cycle-aware emission
   (must stay deterministic).
