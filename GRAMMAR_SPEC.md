# Grammar spec: the freeze set

Status: 2026-07-02, draft for review. The byte-level grammar in two parts
with two different freeze strengths:

- **Part A — identity grammar**: everything in the id preimage. Changing
  any of it later is an identity hard fork (context-string bump; every id
  changes). This is the freeze set proper.
- **Part B — snapshot stream grammar**: the canonical artifact form
  (ENCODING_SPEC.md's rules made concrete). Versioned by a stream header;
  changing it re-fingerprints snapshots but never touches op identity.

Rationale for contested calls is recorded inline, where each rule is
defined.

## Primitives and canonicality meta-rules

- `varint` — LEB128 unsigned, 7 bits per byte, low group first, high bit =
  continuation. **Minimal form mandatory** (no zero-padded continuation) —
  a non-minimal varint is malformed.
- `id` — 32 raw bytes (BLAKE3 output).
- signed integers — zigzag, then varint.
- **One value, one encoding.** Every set is sorted ascending and
  duplicate-free; every optional is a presence tag with a mandatory rule
  for when it may appear; every length must be exact. Any violation is
  malformed — a grammar reject, which is total, convergent, and stable
  (bytes are the input), hence a permanent verdict.

## Part A: identity grammar

### Contexts and id functions

```
NODE_CONTEXT   = "hashweb v1 node id"
VALUE_CONTEXT  = "hashweb v1 value id"
OBJECT_CONTEXT = "hashweb v1 object id"

id(u)        = BLAKE3::derive_key(NODE_CONTEXT,   node_bytes(u))
value_id(a)  = BLAKE3::derive_key(VALUE_CONTEXT,  artifact_bytes(a))
object_id(X) = BLAKE3::derive_key(OBJECT_CONTEXT, id(X))   -- X a creation op
```

One node context for every op kind; one value context for every value kind;
one object context deriving every object's **origin id** from its creation
op. Kinds are tags inside the encodings. Bump a context string ⟺ identity
hard fork; there is no other versioning at this layer.

### The node grammar: envelope ‖ body

```
node     := envelope body
envelope := kind      : varint          -- op kind tag (table below)
            ref_count : varint          -- |refs(u)|, ≥ 1
            refs      : ref_count × id  -- refs(u), sorted ascending, unique
            body_len  : varint          -- exact byte length of body
```

The envelope is the kind-independent parse (HETEROGENEITY.md): a replica
that does not know `kind` still reads the refs (buffering and commitment)
and skips `body_len` bytes. Placement is **body semantics** — where an op
sits in its container is kind-level meaning, and not every kind has a place
(a `Put` does not); an op anchoring on a node of unknown kind parks until
the kind is known (Op kinds, below). The refs table doubles as the
body's dictionary: role fields address it by index, and any entry no role
addresses is a pure frontier pin — the named/pin split is positional, never
flagged (there is no tips marker; the partition is semantically inert, so
the artifact does not record it). The preimage is these bytes verbatim — the id commits
to the envelope and body exactly as transmitted, and the streaming hasher
is pinned by test to this layout.

`ref_count ≥ 1`: every op pins at least its frontier, and a frontier is
never empty (every object has an origin id from birth). A zero-ref node is
malformed. Refs may be op ids or origin ids — one namespace; an object's
origin id is recognized by derivation once its creation op is known, while
the recursion's base is the **genesis**: an arbitrary, typeless,
meaningless id chosen out-of-band when a web is opened — not an object, no
creation op, present axiomatically on every replica of the web. The only
ops that anchor at the genesis are creations (each births its own object;
anything else fails the edge table — there is no projection there). A
web's first op is therefore `refs = {genesis}` — causally empty, but never
anchor-free. That is the rule's real content: no op floats outside the
commitment chain, which is what roots routing and confines ops to their
web (an op bottoming at web A's genesis orphans forever in web B).

### Op kinds

```
anchor := varint( (ref_idx << 1) | side )     -- side: 0 = Before, 1 = After
```

| tag | kind     | body                                                                                                                 |
|-----|----------|----------------------------------------------------------------------------------------------------------------------|
| 0   | `Insert` | `at: anchor`, `value` — the payload                                                                                  |
| 1   | `Remove` | `count:varint`, then `count` × `ref_idx:varint`, ascending                                                           |
| 2   | `Move`   | `target: ref_idx`, `to: anchor`, `count`, `count` × `ref_idx` ascending (`overwrites`)                               |
| 3   | `Put`    | `key: value`, `val: value`, `count`, `count` × `ref_idx` ascending (`overwrites`)                                    |
| 4   | `Mark`   | `start: anchor`, `end: anchor`, `kind_v: value`, `val: value`, `count`, `count` × `ref_idx` ascending (`overwrites`) |

Unknown kind tags are **not** malformed: the node is carried opaquely
(envelope semantics only), per the extension path; ops that reference it in
roles park until the kind is known.

`Insert` carries a **single** anchor by decision — Fugue-style dual
left/right origins were rejected: a committed interval hands every
malicious peer an inverted `(right, left)` pair (and crossing intervals
from several peers form constraint sets with no consistent order), forcing
per-insert interval validation and a new arbitration surface onto the
system's hottest op, on top of a second ref in every insert preimage and
an absent-right sentinel. Non-interleaving is convention-scoped; full
rationale and accepted residuals: HASHSEQ_SPEC.md, Resolution.

There is **no route field** — routing derives. Objects are identified by
their **origin ids** (`object_id(X)` above; a standalone document's
`doc_id` is the same class): the origin is a virtual node, never an op, so
a creation op has no dual role — refs to `X` always mean the parent
element, refs to `object_id(X)` always mean the child object. An op's
object is then the single object its refs resolve in: named refs by the
edge table; a fresh `Put`'s pins are its object's own frontier, which
begins at the origin id. Refs that determine no single object gate the op
(stable — every ref's object is hash-committed). Buffering: an op waiting
on an origin id parks normally; applying a creation op derives its origin
id and wakes the waiters — no inversion needed.

### Value fields: always by id in the preimage

```
value := id            -- 32 raw bytes: a value_id or an op-node id
```

In the preimage, a payload/key/value is **always the 32-byte id**, never
inline bytes and never a ref-table index. Rationale: identity must be
availability-independent — an inline-iff-small rule inside the preimage
would make "is this encoding canonical?" depend on whether a replica holds
the artifact bytes, and no id-level verdict may depend on availability.
Inline transport lives in Part B, where the bytes are present by
construction. (Cost note: this is the wider-chain-hash-input the payload
decision already carries — value ids for chars are a fixed, cacheable
universe; the benchmark obligation stands.)

### Value artifact grammar

```
artifact := kind:varint ‖ payload
```

| tag | kind        | payload                            | notes                                                                         |
|-----|-------------|------------------------------------|-------------------------------------------------------------------------------|
| 0   | `Tombstone` | empty                              | `TOMBSTONE = value_id(0x00)` — derived constant                               |
| 1   | `Bool`      | 1 byte, 0x00/0x01                  |                                                                               |
| 2   | `Int`       | zigzag varint                      | i64 range; out-of-range is app-level                                          |
| 3   | `Char`      | minimal UTF-8, one scalar          | text payloads                                                                 |
| 4   | `String`    | UTF-8 bytes                        |                                                                               |
| 5   | `Bytes`     | raw bytes                          |                                                                               |
| 6   | `F64`       | 8 bytes, IEEE 754 LE, bit-verbatim | every bit pattern is a distinct value; NaN normalization is the app's concern |
| 7   | `NewSeq`    | empty                              | creation artifact — derived constant                                          |
| 8   | `NewMap`    | empty                              | creation artifact — derived constant                                          |

Artifact bytes are the `value_id` preimage; there is **no length prefix
inside an artifact** — an artifact is a leaf, hashed whole, and every
carrier already frames it (`ValueStore` entries are `len ‖ artifact`; the
stream's inline value form carries `len`). An internal length would be
redundant, and redundancy in canonical bytes is a liability: a second copy
of a fact is a new mismatch class and frozen identity spent on nothing.
The general rule: a length appears exactly where a boundary is otherwise
undecidable *within* one encoding (op bodies count their interior lists for
this reason), and nowhere else — so a future artifact kind with more than
one variable-length field must self-delimit all but its final field.
Unknown artifact tags are carried opaquely — the id verifies, renderers
show placeholders.
`TOMBSTONE`, `NewSeq`, `NewMap` are ordinary derived ids: computed
constants, published as test vectors, never magic bytes in id space.

### Grammar-level validation (all stable)

Malformed — reject permanently, quarantine anything that refs it: non-
minimal varint; unsorted/duplicated refs table or index list;
`ref_count = 0`; `body_len` mismatch; ref index ≥ `ref_count`; trailing
bytes. **Not**
malformed: unknown op kinds, unknown artifact kinds (carried), and any
semantic property of referents (those verdicts belong to the edge-table
gate, which runs when referents are present).

## Part B: snapshot stream grammar

Concrete form of ENCODING_SPEC.md's block/order/ref rules. Softer freeze:
the header carries `stream_version`; bumping it re-fingerprints snapshots
without touching identity.

### Header

```
stream  := magic "hwb1" ‖ stream_version:varint ‖ genesis:id ‖ block*
```

`genesis` is implicit dict entry 0 of the stream-level reference space.

### Blocks

```
block := kind:varint ‖ len:varint ‖ body        -- skippable by construction
```

Block kinds: `Run` (insert chains, both anchor flavors, interior extra-deps
at offsets), `RemoveChain` (one block per maximal remove chain: deps once,
then direction-tagged segments — ranges where contiguous, singles otherwise;
subsumes spans and singles), `Node` (any op verbatim in Part A form, with
ref-table ids replaced by stream refs — the fallback for ops that fit no chain,
and the carrier for unknown kinds), `ValueStore` (artifact bytes:
`count`, then `count` × (`len` ‖ artifact); artifacts referenced by the
stream, sorted by value_id; erased blobs simply absent). Emission order and
cycle-breaking per ENCODING_SPEC.md (hard remove→run edges, force-emit
smallest blocked head, dict spill).

**Run-split rule (adopted):** when a run's interior dep participates in a
run↔remove 2-cycle and splitting the run at that offset evacuates the
spilled id from the dict entirely, the canonical form *is* the split
(run-prefix, remove, run-suffix — all refs backward). The condition is a
function of the op set; no encoder choice.

### Stream references and value elision

Refs in block bodies are tagged varints with within-kind rank spaces
(run-element `(run_rank, offset)` keeps the cheapest tag; remove rank;
dict) — per ENCODING_SPEC.md. **Value elision**: where a Part A `value`
field appears and the artifact is in this stream's `ValueStore` with
artifact bytes ≤ 32, the stream form inlines `0x00 len bytes`; otherwise
`0x01 id`. Inline is mandatory when present-and-small — no choice — and
the decoder derives the value_id to reconstruct the Part A preimage
exactly. Chain interiors elide tips/anchor (implicit `prev`); the decoder
reconstructs each member's full envelope deterministically.

## Open items

1. **Test vectors — generated and locked** (2026-07-02, by the first
   implementation; mirrored in `tests/grammar_vectors.rs`, which fails on
   any drift):

   ```
   TOMBSTONE            = 37e7b9a9496baa6bc45fc76168e02a70e2b640a7ae2ca826fb5990f48f772f8a
   NEW_SEQ              = 8fff7f38a876c8f8dc821a2acd0027539f496194f366d1c7401f2e1d765d0ef7
   NEW_MAP              = 76796526efce6c555148595918fd9cf934753cd7e06f8f22e26b7f2501c60e26
   value_id(Char 'a')   = 555c4ad3f1f89bacc6d46a3d7c6cf897f83e8c0500da8f2dc9a46fc85a740638
   object_id(0x11 × 32) = d416a55b29373e72c670e830928ce84935a766321e5eb977e450705a3a00ed02

   with origin = 0x00 × 32:
   Insert{After(origin), 'a'} (no pins)      = 796e3d6b9739303167ce099a5e801545aee245227e1d0c483592fc839a3e66d2
   Remove{that insert} (no pins)             = d4758f38bc31acaafd5412c51024fb487b71fe85ac212775337cc32b033054c3
   Move{that insert → Before(origin)}        = 7380372f8478f820e04005cc1623df522408f612934db03ded6b9e27a35d3ffb
   Put{'k' → TOMBSTONE, pins={origin}}       = d6a4f360e484441bec208b2510bf4412b74b2f8ea258f09275799ae485cb80a5
   ```

   Still owed: a small canonical snapshot vector once the Part B stream
   encoder is normalized (block derivation from the op set).
2. **Stream ref bit-packing.** The exact tag/rank/offset packing for
   stream refs (Part B) — carry over the implemented `r1`/`00`/`10` scheme
   and its measured trade-offs; pin the widths when the encoder is ported.
3. **No `MarkChain` block in v1.** Part B has chain blocks only where
   volume demands them (`Run` for insert chains, `RemoveChain` for delete
   chains); each mark op travels uncompressed as its own `Node` block,
   since mark volume is orders of magnitude below element volume —
   compression would buy noise. The one workload that could change the
   math is a "format painter" sweep: one gesture emitting many same-kind,
   same-value marks chained `refs = {prev}`, which a `MarkChain` block
   could encode as kind/value/deps once plus two anchors per entry. If
   profiles ever show it, that is a stream-version addition — Part B is
   header-versioned, so a new block kind re-fingerprints snapshots but
   never touches op identity.
