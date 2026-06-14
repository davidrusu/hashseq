# HashDoc: recursive BFT documents (design sketch)

Status: design sketch, 2026-06-11. Nothing here is implemented. Companion to
PERFORMANCE.md (which covers the hashseq internals this design builds on).

## Goal

Extend hashseq's model — self-certifying hash ids, explicit causal deps,
deterministic convergence without trusted actors — from a single sequence to
full documents: maps, sequences, and scalars nested arbitrarily (seqs inside
maps inside seqs), automerge's data model with BFT identities.

Two steps, each independently useful:

1. **HashKv** — a standalone key-value CRDT built from hashseq's parts.
2. **HashDoc** — one op DAG hosting many objects (seqs + maps) with dynamic
   nesting.

## The core observation: a map Put is a hashseq Remove

Hashseq's `Op::Remove(targets)` already has the exact shape a map write needs:
"supersede these specific prior ops, causally anchored at the current tips"
(`extra_deps = tips − targets`). A Put supersedes the prior writes to its key
and asserts a new value:

```rust
enum MapOp {
    Put {
        key: Box<[u8]>,
        value: Value,
        /// The per-key heads this put knew and replaces — the analog of
        /// Remove's target set (automerge calls this `pred`).
        overwrites: BTreeSet<Id>,
    },
    // Del is Put { value: Value::Tombstone, .. }
}

struct MapNode {
    extra_dependencies: BTreeSet<Id>, // tips − overwrites, as in hashseq
    op: MapOp,
}
// id = BLAKE3(encoding) — self-certifying, identical to HashNode
```

### Read semantics: MVR with deterministic arbitration

The value of key `k` is the set of puts on `k` not named in any other put's
`overwrites` — a multi-value register. Explicit `overwrites` makes apply O(1)
(`heads = heads − overwrites ∪ {new}`), no causal-ancestor walks, exactly like
`apply_remove`.

Concurrent puts leave multiple heads. The API is MVR-first (expose all heads);
when a single answer is needed, take max-`Id` — arbitrary but convergent and
unforgeable. There is deliberately **no LWW**: wall-clock timestamps are
forgeable, so honest BFT arbitration is hash order. Silent arbitration hides
exactly the conflicts a BFT setting cares about, hence MVR as the primary API.
Caveat (see the locality invariant below): max-`Id` is a *display* tiebreak.
Anything semantics-bearing — placement, URLs, config — must surface the
conflict or freeze, never silently follow the largest hash, because hash
order is grindable.

### State shape

```rust
struct HashKv {
    // shared substrate, lifted from hashseq:
    // IdIndex (u64-prefix intern map), ids: Vec<Id>, tips, orphaned
    keys: FxHashMap<KeyHash, KeyState>, // KeyState { heads: SmallVec<[NodeIdx; 1]>, .. }
}
```

Convergence story is hashseq's: state is a grow-only op set, merge = union
with orphan buffering, same merge-law props (commutative / associative /
reflexive) and the same quickcheck harness.

### Runs translate along the writer chain, not per key

Hashseq runs compress because sequential ops form a hash chain with implicit
deps. The map analog is the **session chain**: a writer doing `k1=a, k2=b,
k1=c` emits ops each depending on the previous (tips = {prev}). The chain
encodes as one "write run": `(first_extra_deps, [(key, value, overwrite_refs),
…])`, ids recomputed by chain-hashing at decode — same as `Run`/`RemoveRun`.
The wire format reuses the playbook: dict header for foreign ids, positional
refs for `overwrites` (they almost always reference ops in recently-encoded
runs, exactly like remove targets).

## Recursion: by reference, not by type parameter

Most of hashseq's machinery is already value-agnostic — the run index,
`IdIndex`, interning, remove chains, tips/orphans, sibling ordering never look
at a `char`. The value type touches three places only: `StoredRun.text`,
`char_at`, and the `RunText` wire section. Parameterizing *storage* over the
element type is cheap.

But a Rust type parameter is the wrong recursion mechanism:
`HashSeq<HashKv<HashSeq<…>>>` fixes nesting depth statically, blows up
monomorphization, and a child embedded *by value* in a parent op would change
the parent's hash on every child edit — breaking the identity model. Recursion
is dynamic, by reference:

```rust
enum Value {
    Char(char), Int(i64), Bool(bool), Bytes(Box<[u8]>), // leaf scalars
    NewSeq,   // this op *creates* a child sequence
    NewMap,   // this op *creates* a child map
}
```

**The creating op is the child object.** `InsertAfter(anchor, Value::NewSeq)`
with id `X` creates object `X`; child ops anchor into `X`'s context. This
gives:

- unbounded dynamic nesting;
- causal welding: you can't know a child's edits without the op that created
  it, so the document stays one connected DAG;
- natural subtree sync: an object's ops are transitively reachable from its
  creation op.

This is automerge's object model with self-certifying ids in place of
actor/counter pairs.

### Architecture: one op log, per-object projections

```rust
struct HashDoc {
    // shared: IdIndex, ids, locs, orphans, encoding
    objects: FxHashMap<NodeIdx, Object>,
}

enum Object {
    Seq(SeqState),  // runs + run_index — today's hashseq internals
    Map(MapState),  // key -> heads — the HashKv sketch above
}
```

Ops carry their object (cheap: a `NodeIdx`-resolvable id, dict/positional
encoded). The substrate routes; the projection applies.

### Tips are per-object

The run fast path requires empty `extra_deps`, i.e. tips = {previous op}. With
one global tips set, typing in a list while *any* other object is edited
fragments runs — recreating the 1-char-run problem at document scale
(PERFORMANCE.md idea 1 exists to fight precisely this).

So: **each object keeps its own tips**, and run formation inside each sequence
stays as clean as standalone hashseq. The first op in a child object depends
on the creation op `X` (the bridge into the parent's context). The cost:
causal ordering is guaranteed within an object plus through parent-creation
links, not across sibling objects — the standard automerge-level guarantee,
and the right default here.

Orphan buffering stays global (one `missing dep id → waiting ops` map),
since deps can cross objects at creation bridges.

### Text and lists are one sequence type: the value column seam

A JSON CRDT needs both text (chars) and lists (values). They share *all* of
hashseq's machinery — the DAG, anchoring, sibling order, run index, splits,
remove chains, cursors are value-agnostic; only `StoredRun.text`, `char_at`,
and the `RunText` wire section know about `char`. So the seq is generic over
a thin **value column**:

```rust
trait Element {
    type Column;  // contiguous per-run storage: push / split_at / get / len,
                  // canonical encode (id preimage + wire)
}
type TextSeq = Seq<char>;   // Column = String — keeps RunText compactness
type ListSeq = Seq<Value>;  // Column = Vec<Value>
```

Lists get run compression for the same reason text does: consecutive pushes
are a hash chain anchored once. Text and lists stay distinct object types
(JSON semantics; automerge agrees): `Object::{Text(Seq<char>),
List(Seq<Value>), Map(HashKv)}`.

**Type enforcement rule (BFT):** an object's type is committed by its id
(the creation op's payload — `NewText`/`NewList`/`NewMap` — is in the hash
preimage), so replicas can never disagree about what type an object is, and
`Ref(id)` needs no type annotation. What the commitment *cannot* prevent is
an ill-typed child op: `InsertAfter(elem_of_text_obj, Value::Ref(..))` is a
well-formed, well-hashed artifact — the mismatch lives in the link between
two separately-committed artifacts, not inside either hash.

Rule: **validate before apply; ill-typed ops never enter the DAG or tips.**
The check ("payload type matches the anchor object's committed type") is
total and convergent since both inputs are hash-committed. Because every
honest replica validates before applying, a bad op never reaches honest
tips — so no honest op can ever depend on one, and anything that *does*
depend on one was authored by a faulty peer and correctly orphans forever.
An ill-typed op is semantically an op with a permanently unsatisfiable
dependency: it is quarantined in the orphan set like any other (an unknown
anchor parks it as a normal orphan first; validation runs when the anchor
arrives, and a failed check is permanent since types are immutable). Purging
unsatisfiable orphans — type-invalid or plain garbage deps — is a single
caller-decided policy.

**Refactor ordering:** substrate extraction first (untouched by this); then
thread `V` through `Seq` with `char` as the only instantiation (pure
refactor, benchmark must stay flat); then `Value` + `ListSeq` is nearly free.

### Content-addressed large values

Node ids must commit to values (otherwise same-id/different-value
equivocation breaks convergence), but the commitment can be indirect.
**Threshold rule:** a `Value` whose canonical encoding is ≤ 32 bytes (the
hash size) embeds inline in the op; anything larger becomes `Value::Blob(hash)`
with bytes in a content-addressed side store. Below the hash size indirection
is strictly worse; above it:

- ops stay small and chain-compressible regardless of value size;
- identical values dedupe;
- large values sync lazily — the DAG verifies without any payloads;
- **erasure without breaking verification**: drop a blob's bytes, keep its
  hash, every id still verifies (moderation / GDPR-style deletion).

Sequence *chars* are exempt and stay inline forever: the text bytes double as
the id-derivation material, which is what makes run storage and id
recomputation work at all.

**Pending-blob state:** a peer can reference a blob hash whose bytes it never
provides. The DAG converges; the value is unresolvable. The API needs an
honest "pending/unavailable" state for blob values — an application-visible
condition, not something the CRDT layer papers over.

## Pre-extraction simplifications

Status: **both done** (2026-06-11). Two simplifications were identified to
avoid baking warts into the substrate:

1. ✅ **Origin unification** — `InsertRoot` removed; `HashSeq::new(doc_id)`
   anchors top-level inserts at the document id (interned as the tombstoned
   virtual `NodeIdx(0)`). This *is* the object-embedding mechanism: in HashDoc
   a child sequence is exactly `HashSeq::new(creation_op_id)`. Documents with
   different origins can no longer merge; doc identity is self-certifying.
2. ✅ **Orphan buffering rework** — orphans are parked as
   `missing_dep_id → Vec<(Id, HashNode)>` (std HashMap: keys are
   adversary-chosen dep bytes, SipHash stays) plus an `orphan_ids: IdSet`
   re-delivery dedup (FxHash: keys are our own BLAKE3 outputs). Applying an
   op wakes exactly its waiters via an iterative worklist; multi-dep orphans
   re-park on their next missing dep. No recursion, no global retries —
   `reverse_delivered_chain_applies_iteratively` covers a 10k-chain in
   reverse. Hot path is allocation-free and benchmark-neutral
   (`after-orphan-rework.txt`).

## What gets extracted from hashseq

The implementation order is forced: both HashKv and HashDoc want the **hash-DAG
substrate** factored out of hashseq first:

- `IdIndex` + `ids` interning (handle space, replica-local);
- tips maintenance and `tips_minus`;
- orphan buffering and the apply skeleton (dedup check → missing-dep check →
  tips update → dispatch);
- the encoding machinery (dict header, positional refs, run sections).

Hashseq becomes one instantiation of the substrate; HashKv a second; HashDoc
the composition. The substrate inherits hashseq's test assets: merge-law
props, roundtrip/determinism props, and the index-vs-iterator invariant test
pattern.

The interning invariant carries over unchanged and bears repeating: handles
are replica-local; anything convergence-relevant (sibling order, key-head
sets, hashing, wire format) stays in `Id` space.

## BFT properties (unchanged from hashseq)

- Ids are BLAKE3 hashes of op content: unforgeable, no trusted allocation,
  collisions require breaking the hash. The preimage convention (set with the
  origin unification) is `derive_key(<domain context>, canonical op
  encoding)` — injective because the encoding is decodable, versioned by the
  context string. HashKv/HashDoc ops use the same convention with their own
  op tags in the shared tag space.
- Causality is explicit and can't be fabricated retroactively (deps are
  hashes).
- A malicious writer's worst case is creating concurrent siblings — surfaced
  as MVR conflicts / hash-ordered forks, never divergence between honest
  replicas.
- Adversarial keys are fine: key hashing for `KeyHash` uses the same
  reasoning as `IdMap` (FxHash is safe only for ids that are already hashes;
  raw user keys need SipHash or BLAKE3-derived `KeyHash`).

## The locality invariant

A design goal of hashseq that predates this doc and needs stating: **an
adversary can only do local damage** — garbage where they wrote. Precisely:
the rendered effect of any op is confined to the content it explicitly names
(its anchor gap, its remove targets, its register) plus its own payload. No
op, however crafted, may trigger a global reshuffle of honest content.

Hashseq core satisfies this structurally: applied elements never reorder
(new ops only insert *between*), so hash-ordered sibling arbitration only
decides how concurrent inserts at one gap arrange relative to *each other* —
it places the contender's own content, it never displaces honest content.

The corollary every layer above (HashKv values, marks, move registers) must
preserve: **max-`Id` is a display tiebreak, never a semantics arbiter.** Any
deterministic function of op bytes is grindable, so a hash-ordered choice
must not decide anything whose consequences extend beyond the contending
ops' own content — not a block's location, not a link's URL, not a config
value. Contested registers freeze at their last agreed value and surface the
conflict (MOVE.md specifies the rule) rather than flip to the largest hash.

What the invariant does *not* promise: immunity to vandalism via
**dominating** ops — an op that explicitly names and supersedes honest state
(a Remove of honest chars, a move that overwrites the current placement
heads). That capability is the unavoidable cost of permissionless write. But
dominating-op vandalism is forward, visible, attributable, per-op bounded,
and revertible — and the invariant's job is to ensure grinding buys nothing
*on top of* it.

## Open problems (decide before building, in this order)

1. **Object deletion / GC.** Tombstoning a `Ref` makes the child unreachable
   and its op subgraph locally droppable — but a BFT peer can always
   re-present those ops. What does "deleted" mean for sync? (Probably: honest
   peers stop forwarding unreachable subgraphs; receiving one again is
   harmless because applying it can't resurrect reachability.)
2. **Move.** Moving a subtree between parents is the classically hard CRDT
   problem (concurrent moves duplicate or cycle). Un-punted: MOVE.md sketches
   register-based move (position/parent registers, MVR with
   freeze-on-conflict, read-time cycle reverts) — order-free by
   construction, so no undo/redo/replay, no adversarial oplog re-org, and
   no placement ever decided by grindable hash order.
3. **Key storage.** Keys verbatim (readable wire, iteration order) vs hashed
   with a reverse table (fixed per-op cost for long keys). Leaning verbatim in
   ops + `KeyHash` only for the in-memory map.
4. **Per-key history retention.** Superseded values: keep (time travel), or
   keep ids only (the `value_hash` indirection makes dropping bytes trivial
   while preserving the DAG for convergence).

## Sanity targets

HashKv standalone should hit roughly hashseq's per-op numbers (the substrate
cost is the same; map projection is cheaper than the seq index). The
benchmark-first habit applies: port `sequential_traces`'s discipline — min
times, structure checksums, memory breakdown — before optimizing anything.
