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

id(u)              = BLAKE3::derive_key(NODE_CONTEXT,   node_bytes(u))
value_id(a)        = BLAKE3::derive_key(VALUE_CONTEXT,  artifact_bytes(a))
object_id(k, origin) = BLAKE3::derive_key(OBJECT_CONTEXT, k ‖ origin)
                       -- k: the object kind tag (KIND_KV = 0x00 /
                       --    KIND_SEQ = 0x01, one byte); origin: an
                       --    arbitrary 32-byte value the object's creator
                       --    chose
```

One node context for every op kind; one value context for every value kind;
one object context deriving every object's store address. **Origins and
object ids are distinct, and they live at different layers**: the origin
(an arbitrary 32-byte value — often another op's id, by app convention)
is the op-level anchor — what an object's ops ref and bottom out at; the
object id, derived from
**kind ‖ origin**, is the store-level address (routing envelope + index)
and **never appears in any preimage**. The kind is inside the address, so
the same origin opened as a Seq and as a Kv is two different objects, and
kind mis-agreement is unrepresentable rather than gated. Kinds are tags
inside the encodings. Bump a context string ⟺ identity hard fork; there
is no other versioning at this layer.

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
never empty (every object has an origin from birth). A zero-ref node is
malformed. An object's origin is the recursion's base: an arbitrary
32-byte value its creator chose. Choosing another op's id is the standard
*composition convention* — it welds the new object into that op's causal
closure (ownership-style nesting), but the store attaches no semantics to
the choice: creation is not an op-layer concept. Object ids never appear
among refs — they are store-level addresses, not op-level anchors; naming
an object id in an envelope *is* naming its kind, since the kind is
inside the derivation. There is
no store-level anchor above root objects; each object's closure is its own
commitment domain. An object's first op is `refs = {origin}` — causally
empty, but never anchor-free. That is the rule's real content: no op
floats outside a commitment chain, which is what roots routing and
confines ops to their object (an op bottoming at object A's origin can
never merge into object B).

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
| 5   | `Place`  | `placed_at: value`, `count`, `count` × `ref_idx` ascending (`overwrites`) — containment register, valid in any object's DAG (PLACEMENT_SPEC.md; added 2026-07-04 via the extension path) |

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

There is **no route field in the preimage** — delivery rides a **routing
envelope**, `obj_id ‖ node`, that is pure transport metadata (never
hashed, never part of identity). The envelope address is the derived
object id (`object_id(k, origin)` above; a standalone document's `doc_id`
is the same class); it disambiguates every ref for free — an op id `X`
used as another object's origin names the element in its own object's
envelope and the origin anchor in the other's, with no dual-role
ambiguity because the two streams never mix. The envelope needs no trust
and no verdict: an op enveloped to the wrong object simply never applies
there (its refs never arrive inside that object), the same fate as any
garbage ref — bounded and attributable. Buffering is two-level: envelopes
naming unknown object ids park store-wide until the object is opened or
adopted; ops inside a live object park on their first missing ref in that
object's own buffer.

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
`TOMBSTONE` is an ordinary derived id: a computed constant, published as
a test vector, never magic bytes in id space.

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
   value_id(Char 'a')   = 555c4ad3f1f89bacc6d46a3d7c6cf897f83e8c0500da8f2dc9a46fc85a740638
   object_id(seq, 0x11 × 32) = dec2ca1db8abc0150e54eac174fdbf56a0ffeb833d83ba0d53eb91e4b063b58b
   object_id(kv,  0x11 × 32) = d17caee6e539818d5cf8c5f5087d3e6ad43797cf2674b3196b3b4c0dc601f757

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
