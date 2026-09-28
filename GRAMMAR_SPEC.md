# Grammar spec: the freeze set

The byte grammar, in two parts with different freeze strengths:

- **Part A — identity grammar**: everything inside an id preimage. Changing
  any of it is an identity hard fork (context-string bump; every id
  changes). This is the freeze set proper.
- **Part B — snapshot stream grammar**: the canonical snapshot form
  (ENCODING_SPEC.md's rules made concrete). Versioned by a stream header;
  changing it re-fingerprints snapshots but never touches op identity.

## Primitives and canonicality meta-rules

- `varint` — LEB128 unsigned, 7 bits per byte, low group first, high bit =
  continuation. **Minimal form only**: a zero-padded continuation (e.g.
  `80 00`), or a 10th byte carrying more than the top bit, is malformed.
- `id` — 32 raw bytes (BLAKE3 output).
- signed integers — zigzag, then varint.
- **One value, one encoding.** Every set is sorted ascending and
  duplicate-free; every optional is a presence tag with a mandatory rule
  for when it appears; every length is exact. A violation is malformed — a
  grammar reject, which depends only on the bytes and so is total,
  convergent, and permanent.
- **No redundancy.** A length appears exactly where a boundary is
  otherwise undecidable within one encoding, and nowhere else. A second
  copy of a fact is a new mismatch class and frozen identity spent on
  nothing.

## Part A: identity grammar

### Contexts and id functions

```
NODE_CONTEXT   = "hashweb v1 node id"
VALUE_CONTEXT  = "hashweb v1 value id"
OBJECT_CONTEXT = "hashweb v1 object id"

id(u)                = BLAKE3::derive_key(NODE_CONTEXT, node_bytes(u))
value_id(a)          = len ‖ a ‖ 0^(31 − len)            if 1 ≤ len ≤ 15  -- identity form
                     = BLAKE3::derive_key(VALUE_CONTEXT, a)  otherwise
                       -- a = artifact bytes, len = |a| as one byte
object_id(k, origin) = BLAKE3::derive_key(OBJECT_CONTEXT, k ‖ origin)
                       -- k: object kind, one byte (KIND_KV = 0x00, KIND_SEQ = 0x01)
                       -- origin: any 32-byte value the object's creator chose
```

One context per id class; op kinds and value kinds are tags inside the
encodings. Bumping a context string is an identity hard fork — there is no
other versioning at this layer.

**Origin vs object id.** The origin is the op-level anchor: an object's
ops ref it and bottom out at it. The object id is the store-level address
(routing envelope, index) and **never appears in any preimage**. Because
the kind is inside the derivation, the same origin opened as a Seq and as
a Kv is two different objects; kind disagreement is unrepresentable.

### Identity-form value ids

An artifact of 1–15 bytes is its own value id — no hash. Inverting it is a
parse (`identity_artifact`): check `id[0] ∈ 1..=15` and that
`id[1+len..]` is all zero. Every replica reads the same value out of the
same id while holding nothing.

- **Why.** A payload's rendering must be a function of the node set. With
  hashed ids only, a small value sent by id renders as the value where a
  replica can invert the hash and as an opaque atom elsewhere — one node
  id, two states. The identity form needs no inversion table.
- **No tag bit.** Identity ids are recognized by shape (`len ∈ 1..=15` plus
  ≥ 16 zero bytes). That set is ≈ 2^120 of 2^256 ids, so landing a BLAKE3
  value id on one costs ≥ 2^128 work, BLAKE3's own collision bound. The
  bound on `len` is what keeps it there: **do not raise it.** Hashed ids
  keep all 256 bits.
- **Kind-agnostic.** The form depends on artifact length alone: a new small
  artifact kind changes no id, and a replica that does not know the kind
  still derives, verifies and carries its id (rendering a placeholder). The
  length byte frames the artifact, so trailing `0x00` bytes inside it are
  unambiguous.
- **Consequences.** Identity-form values are never held in a value store
  and never shipped as artifact bytes; a stream carrying one in an
  artifact section or `ValueStore` is non-canonical. Value ids are not
  uniform — an author picks small ones directly — so nothing may key an
  unseeded hash table by value id (or by any id read raw off the wire).

### The node grammar: envelope ‖ body

```
node     := envelope body
envelope := kind      : varint          -- op kind tag (Op kinds)
            ref_count : varint          -- |refs(u)|, ≥ 1
            refs      : ref_count × id  -- refs(u), sorted ascending, unique
            body_len  : varint          -- exact byte length of body
```

The preimage is these bytes verbatim. An implementation that streams the
preimage into the hasher must be pinned by test to this layout
(`id_preimage_is_the_canonical_encoding`).

- **Kind-independent parse** (HETEROGENEITY.md). A replica that does not
  know `kind` still reads the refs (for buffering and commitment) and skips
  `body_len` bytes. Placement is body semantics — not every kind has a
  place (`Put` does not).
- **The refs table is the body's dictionary.** Role fields address it by
  index. An entry no role addresses is a frontier pin; the named/pin split
  is positional and never flagged (it is semantically inert).
- **`ref_count ≥ 1`.** Every op pins at least its frontier, and a frontier
  is never empty: every object has an origin from birth. An object's first
  op is `refs = {origin}`. No op floats outside a commitment chain, which
  roots routing and confines ops to their object — an op bottoming at A's
  origin can never merge into B.
- **Origins are the base case.** An origin is an arbitrary 32-byte value.
  Choosing another op's id is the standard composition convention (it
  welds the new object into that op's causal closure), but creation is not
  an op-layer concept and the store attaches no meaning to the choice.
  There is no store-level anchor above root objects; each object's closure
  is its own commitment domain.
- **Object ids never appear among refs.** They are store addresses, not
  anchors.

### Op kinds

```
anchor := varint( (ref_idx << 1) | side )     -- side: 0 = Before, 1 = After
set    := count:varint ‖ count × ref_idx:varint   -- indices strictly ascending
```

| tag | kind     | body                                                                  |
|-----|----------|-----------------------------------------------------------------------|
| 0   | `Insert` | `at: anchor` ‖ `payload: value`                                        |
| 1   | `Remove` | `targets: set`                                                         |
| 2   | `Move`   | `target: ref_idx` ‖ `to: anchor` ‖ `overwrites: set`                   |
| 3   | `Put`    | `key: value` ‖ `val: value` ‖ `overwrites: set`                        |
| 4   | `Mark`   | `start: anchor` ‖ `end: anchor` ‖ `kind_v: value` ‖ `val: value` ‖ `overwrites: set` |
| 5   | `Place`  | `placed_at: value` ‖ `overwrites: set` — containment register, valid in any object's DAG (PLACEMENT_SPEC.md) |

`value` fields use the short form of "Value fields" below.

**Unknown kind tags are not malformed**: the node is carried opaquely
(envelope semantics only), and ops that reference it in roles orphan until
the kind is known.

**`Insert` has a single anchor.** Fugue-style left/right origins were
rejected: a committed interval lets a malicious peer author an inverted
`(right, left)` pair (and crossing intervals from several peers can admit
no consistent order), forcing interval validation and a new arbitration
surface onto the hottest op. Rationale and residuals: HASHSEQ_SPEC.md,
Resolution.

**No route field in the preimage.** Delivery rides a routing envelope,
`obj_id ‖ node`, which is transport metadata — never hashed. Its address is
the derived object id (a standalone document's `doc_id` is the same
class). It disambiguates every ref: an op id `X` used as another object's
origin names an element in its own object's envelope and the origin anchor
in the other's, and the two streams never mix. The envelope needs no trust:
an op enveloped to the wrong object never applies there (its refs never
arrive in that object). Buffering is two-level — envelopes naming an
unknown object id orphan store-wide until the object is opened; ops inside
a live object orphan on their first missing ref in that object's buffer.

### Value fields: a function of the id in the preimage

```
value := len:u8 ‖ artifact     -- identity-form id (len 1..=15): id[..1+len]
       | 0x20 ‖ id             -- any other id, 32 raw bytes (hashed value id,
                               --   op-node id, object id, origin)
```

A payload/key/value field is the id's **short form** — never a ref-table
index.

- **A function of the 32-byte id alone**, so identity never depends on
  whether a replica holds the artifact bytes (which is why an
  inline-iff-held preimage is forbidden).
- **Injective**: the first byte is ≤ 15 exactly for the short form, which
  zero-pads back to the id.
- **Why not always 32 bytes**: a typing-path insert preimage
  (`kind ‖ ref_count ‖ anchor_id ‖ body_len ‖ anchor ‖ value`) is then
  39–42 bytes — one BLAKE3 block instead of two (measured −6 to −11% on
  the sequential traces).

### Value artifact grammar

```
artifact := kind:varint ‖ payload
```

| tag | kind        | payload                            | notes                                                                        |
|-----|-------------|------------------------------------|------------------------------------------------------------------------------|
| 0   | `Tombstone` | empty                              | `TOMBSTONE = value_id(0x00)` — a derived constant (identity form)            |
| 1   | `Bool`      | 1 byte, `0x00` / `0x01`            |                                                                              |
| 2   | `Int`       | zigzag varint                      | i64 range; out-of-range is app-level                                         |
| 3   | `Char`      | minimal UTF-8, one scalar          | text payloads                                                                |
| 4   | `String`    | UTF-8 bytes                        |                                                                              |
| 5   | `Bytes`     | raw bytes                          |                                                                              |
| 6   | `F64`       | 8 bytes, IEEE 754 LE, bit-verbatim | every bit pattern is a distinct value; NaN normalization is the app's concern |

- Artifact bytes are the `value_id` preimage. There is **no length prefix
  inside an artifact**: it is a leaf, hashed whole, and every carrier
  frames it (store entries are `len ‖ artifact`; the stream's inline form
  carries `len`; the identity form carries its own length byte). A future
  kind with more than one variable-length field must self-delimit all but
  its last field.
- Unknown artifact tags are carried opaquely: the id verifies, renderers
  show a placeholder.
- `TOMBSTONE` is an ordinary derived id — a computed constant published as
  a test vector, never magic bytes in id space.

### Grammar-level validation (all stable)

Malformed — reject permanently and drop; anything that refs it orphans on
the missing ref:

- non-minimal varint;
- unsorted or duplicated refs table or index set;
- `ref_count = 0`;
- `body_len` mismatch, or trailing bytes;
- ref index ≥ `ref_count`.

**Not** malformed: unknown op kinds, unknown artifact kinds (both carried),
and any semantic property of referents — those verdicts belong to the
admission table, which runs when the referents are present.

## Part B: snapshot stream grammar

Concrete form of ENCODING_SPEC.md's block, order and ref rules. Softer
freeze: the header carries `stream_version`; bumping it re-fingerprints
snapshots without touching identity.

Status: Part B is the target grammar and is **not yet implemented**. The
current encoder emits the unversioned form in ENCODING_SPEC.md "Byte
layouts", which follows the same block/order/ref rules.

### Header

```
stream := magic "hwb1" ‖ stream_version:varint ‖ genesis:id ‖ block*
```

`genesis` is implicit dict entry 0 of the stream-level reference space.

### Blocks

```
block := kind:varint ‖ len:varint ‖ body        -- skippable by construction
```

| block         | carries                                                                                                  |
|---------------|----------------------------------------------------------------------------------------------------------|
| `Run`         | an insert chain, either head anchor side; interior extra-deps at their offsets                            |
| `RemoveChain` | one maximal remove chain: deps once, then direction-tagged segments (ranges where contiguous, singles otherwise) |
| `Node`        | any op verbatim in Part A form with ref-table ids replaced by stream refs — the fallback for ops no chain fits, and the carrier for unknown kinds |
| `ValueStore`  | `count ‖ count × (len ‖ artifact)`: artifacts the stream references, sorted by value id; erased blobs absent; never identity-form |

Emission order and cycle breaking follow ENCODING_SPEC.md.

**Run-split rule.** When a run's interior dep participates in a
run↔remove 2-cycle and splitting the run at that offset removes the
spilled id from the dict entirely, the canonical form *is* the split
(run-prefix, remove, run-suffix — all refs backward). The condition is a
function of the op set, not an encoder choice.

### Stream references and value elision

Refs in block bodies are tagged varints with within-kind rank spaces
(run-element `(run_rank, offset)` has the cheapest tag; remove rank; dict),
per ENCODING_SPEC.md.

**Value elision.** Where a Part A `value` field appears:

- artifact in this stream's `ValueStore` and ≤ 32 bytes → `0x00 len bytes`
  (mandatory — no choice);
- identity-form `Char` → `0x00 len bytes`;
- otherwise → `0x01 id` (for any other identity-form value, the id carries
  the bytes).

The decoder derives the value id to rebuild the Part A preimage exactly.
Chain interiors elide pins and anchor (implicitly `prev`); the decoder
reconstructs each member's full envelope deterministically.

## Open items

1. **Test vectors.** Locked by `tests/grammar_vectors.rs` (printed by
   `examples/grammar_vectors.rs`); any drift fails the test:

   ```
   TOMBSTONE            = 0100000000000000000000000000000000000000000000000000000000000000
   value_id(Char 'a')   = 0203610000000000000000000000000000000000000000000000000000000000
   value_id(String 'x'×14) = 0f04787878787878787878787878787800000000000000000000000000000000
   value_id(String 'x'×15) = a9361588e0f7f0a7645285a5229fa64588d56d4ad5572efbf79d439d086e0b16
   object_id(seq, 0x11 × 32) = dec2ca1db8abc0150e54eac174fdbf56a0ffeb833d83ba0d53eb91e4b063b58b
   object_id(kv,  0x11 × 32) = d17caee6e539818d5cf8c5f5087d3e6ad43797cf2674b3196b3b4c0dc601f757

   with origin = 0x00 × 32:
   Insert{After(origin), 'a'} (no pins)      = 727ed23ef98ab2cefcb93482e388c1681fb4320b4fb4e2d6d86c4b91b72b984a
   Remove{that insert} (no pins)             = 45fb743e2035d8b2419358e3132e1639f6236581a84c2034ad1710f1b2f08e6b
   Move{that insert → Before(origin)}        = f1c0ee1237aaf7be2263f9674bb600e0d5e604cce6e7a63a3691de5aebcd487a
   Put{'k' → TOMBSTONE, pins={origin}}       = 517f394b1f2f4134a5096ac64e95d9891c2675e4c2039e377827817a0bfb2c93
   Put{'x'×15 → 'x'×14, pins={origin}}       = bc713a67d53f9485334cbe2d44cfb193ff0c93a6c9354dd45a7dcf445918c47d
                                               (both value-field forms)
   ```

   Still owed: `Mark` and `Place` vectors, and a Part B snapshot vector
   once Part B is implemented (the current stream's bytes are locked by
   `canonical_snapshot_vector` in `src/encoding.rs`).
2. **Stream ref bit-packing.** Pin the exact tag/rank/offset widths for
   Part B refs; carry over the implemented `r1` / `00` / `10` scheme
   (ENCODING_SPEC.md "Byte layouts").
3. **No `MarkChain` block in v1.** Chain blocks exist only where volume
   demands (`Run`, `RemoveChain`); each mark travels as its own `Node`
   block, since mark volume is orders of magnitude below element volume.
   The one workload that could change this is a "format painter" sweep —
   many same-kind, same-value marks chained `refs = {prev}`, encodable as
   kind/value/deps once plus two anchors per entry. If profiles show it,
   add it as a stream-version bump; op identity is untouched.
