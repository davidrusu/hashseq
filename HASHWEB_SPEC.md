# HashWeb op spec

HashWeb is a **flat store of objects**. It adds no conflict type: every op
is delivered in a routing envelope to one object, which resolves it per
its own spec (HASHSEQ_SPEC.md, HASHKV_SPEC.md, MARKS.md,
PLACEMENT_SPEC.md). The store has no identity, no root, no creation
semantics, and no state beyond its objects; merge is an unconditional
union. Framework: FRAMEWORK.md. Rationale: HETEROGENEITY.md (composition
by reference), LAYERING.md (frontiers).

## Objects

```rust
seqs: Map<Id, HashSeq>,   // one sequence kind: text, lists, mixed
kvs:  Map<Id, HashKv>,
```

- Keyed by **object id** `object_id(kind ‖ origin)` (GRAMMAR_SPEC.md).
  The kind is inside the id, so there is no store-level type tag, and the
  same origin opened as a Seq and as a Kv is two distinct objects.
- The object id is a store address (envelope + index) and never appears
  in a preimage; ops anchor at the **origin**, an arbitrary 32-byte value
  the creator chose.
- Payloads, keys and map values are **ids** (HASHSEQ_SPEC.md "Payload";
  HASHKV_SPEC.md "Keys and values are ids"): value artifacts, op nodes,
  or object ids (links; a foreign object's id is transclusion).
- **Text and List are one kind.** "Textness" is a rendering and export
  convention (a seq of char values renders as a string), never a
  committed type.

## Op: opening and delivery

There is no creation op. `open(kind, origin)` (`create_seq` /
`create_kv`) instantiates the object at `object_id(kind ‖ origin)`; it is
replica-local, idempotent, and carries no authority. Opening wakes
envelopes orphaned on that object id.

**Composition is app convention.** To nest a child, commit an op in the
parent (a `Put`, or an `Insert` marking the slot), then open the child at
that op's id. The child's ops bottom at the parent op, so the child sits
inside the parent's causal closure (sync closure, reachability-based
deletion) and its address is recomputable from the parent op. The store
never interprets values: a replica opens the child when its app
recognizes the convention. A **link** is an object id carried as a value,
naming an independent object.

**Envelope.** Edits travel as `obj_id ‖ node`: transport metadata, never
hashed (no route field in the preimage — GRAMMAR_SPEC.md). Handles
(`NodeIdx`) are replica-local and never appear in artifacts.

**The envelope needs no trust** — intended rule: an op enveloped to the
wrong object never applies there, because its refs never arrive in that
object. *Known gap (QUEUE.md 56, open decision):* a child opened at
parent op X has origin X, which is also an element of the parent, so an
op whose refs are ⊆ {X} ∪ child ops is valid in both objects and the
envelope picks which (text, puts, and the child's birth `Place` can be
replayed across). Per-object convergence holds; integrity and attribution
do not. Fix under decision: a per-object domain separator in the
preimage, or declare parent and child one commitment domain at X.

```
refs(u) = named(u) ∪ frontier pins    // pins = the object's own observed frontier
```

## Resource / Conflict / Resolution

Routed, not new:

| object | resource → conflict → resolution |
|---|---|
| `Seq` | gap → intra-gap order → total order by id; tombstone union; move register → freeze, same container (HASHSEQ_SPEC.md) |
| `Kv` | key register → multi-head → MVR (HASHKV_SPEC.md) |
| marks | (element, kind) register → multi-head → MVR / freeze (MARKS.md) |
| any object | containment register → multi-head → freeze at last-agreed; cycles → detach the SCC (PLACEMENT_SPEC.md) |

Cross-object causality exists only through origins that are other
objects' op ids, never through the store (FRAMEWORK.md "Frontiers are
per layer").

## Per-object tips

Each object keeps **its own tips**; the first op of an object refs its
origin. A store-global tips set would thread every concurrent edit
anywhere through every object's deps and break the run fast path
(`tips = {previous op of this object}`); LAYERING.md covers the trade.

Buffering is two-level:
- an envelope for an object id the store does not know orphans
  store-wide until the object is opened or adopted by merge (the store's
  only delivery state);
- an op inside a known object orphans in that object's buffer on its
  first missing ref.

Orphans are never evicted today (QUEUE.md 54, policy undecided).

## Admission: the edge table

Two facts are hash-committed and version-independent: an object's kind
(inside its object id) and every referent's kind. Reference validation is
therefore one shared table, run when an op leaves the orphan buffer (all
refs present). Each edge gets a total, convergent, permanent verdict:

- **meaningful** — apply proceeds;
- **inert** — tolerated, no effect;
- **refused** — dropped: never applied, stored, or re-presented by merge
  or snapshot. Dependents orphan on the missing ref, so refusal cascades
  without keeping the refused op; no honest op depends on one.
- A referent of **unknown kind** yields no verdict: the op orphans until
  the kind is known. Unknown-ness never refuses.

| op . role | admits | otherwise |
|---|---|---|
| `Insert . at` | insert, move op (its splice point), or the object's origin — in the same `Seq` | refused (`NotAGluePoint`) |
| `Remove . target` | insert in the same `Seq` | inert (a ref in another object never arrives here: orphans, no verdict) |
| `Move . target` | insert in the same `Seq` | refused (`NotAnElement`); an id of another object never arrives here: orphans, no verdict |
| `Move . to` | insert, move op (any, including ops of `target`'s own chain — excision precedes placement and op ranks are permanent), or the origin; not `target` itself | refused (`NotAGluePoint` / `SelfMove`) |
| `Mark . anchor` (start, end) | insert, move op (its splice point), or the origin, in the same `Seq`; start must not sort after end | refused (`NotAGluePoint` / `InvertedSpan`) |
| `Mark . overwrites` | — | never refused; non-covering or other-kind entries are ignored by the read's suppression filter (MARKS.md) |
| `Put . overwrites` | — | never refused; entries that are not puts on the same key are ignored |
| `Place . placed_at` | any id | never checked: a value commitment; a non-matching id is inert at read (PLACEMENT_SPEC.md) |
| `Place . overwrites` | — | never refused; entries that are not `Place` ops of the same object are ignored |
| op kind vs object kind | `Insert`/`Remove`/`Move`/`Mark` in a `Seq`; `Put` in a `Kv`; `Place` in either | refused (`WrongObjectKind`) — reachable only by mis-enveloping, since the kind is inside the object id |
| pins (unroled refs) | anything | always meaningful |
| payloads / keys / values | any id | never checked: values are not references |

Verdict reasons are `Refused` in `src/lib.rs`.

**Admission vs filter.** Kind checks (the refused rows) are apply-time
verdicts. Value-dependent constraints (same key, same `kind_v`, same
object's `Place`) live in the definitional read, which the incremental
head-set update filters identically — nothing to get wrong at apply.

**Tighten never, loosen carefully.** The table is versioned semantics:
every replica must compute identical, permanent verdicts. Tightening a row
after launch would refuse ops already applied in honest documents — a
fork, forbidden. Loosening a row is recovered by re-sync: the refused op
was dropped, so after the upgrade one frontier exchange with a peer
holding it re-delivers it. When in doubt, refuse: strictness is
recoverable, laxness is not.

## The schema gate

There is none at this layer. Payload, key and value kinds are never
checked at admission: any id may sit in any slot, and a renderer shows
what it cannot interpret as a placeholder (HETEROGENEITY.md). Schema (e.g.
"this slot holds an object link") is an app / renderer concern, never
convergence. This is what lets Text and List be one kind.

## The value side store

- Artifacts of **≤ 15 bytes** are their own value id (GRAMMAR_SPEC.md
  "Identity-form value ids"): resolved from the id alone, never stored,
  never shipped as artifact frames.
- Larger artifacts live in one content-addressed store per replica
  (`HashWeb::values`), keyed by BLAKE3 value id. Identical values dedupe.
- The op DAG verifies with no payload bytes; artifacts sync lazily.
- **Erasure**: drop an artifact's bytes, keep its id — every op id still
  verifies (moderation, GDPR-style deletion).
- Missing bytes are the **`pending`/unavailable** value state, for
  payloads, keys and values alike, surfaced to the app.
- Value ids are not uniform (an author picks identity-form ids directly),
  so no table keyed by a value id, or any id read off the wire, may use an
  unseeded hash.

## Sync

A delta is a DAG diff against the peer's clock; the store keeps no
per-peer or outbox state.

- **Clock** (`Clock`, `src/lib.rs`): a set of tips for one object; every
  ancestor is implied. **`HashWebClock`**: one `Clock` per object id.
  A clock is a value, never edited: either what the peer stated (a 0xC1
  frame, or `clock()` of its decoded snapshot) or what we last sent
  (`web.clock()` after a drain). The transport owns one per peer.
- **Delta** (`deltas_for(&clock)`): per object, every applied node
  outside the peer's closure, in apply order; objects sorted by object id.
  An object absent from the clock ships whole.

Wire frames (`src/encoding.rs`):

| tag | frame | notes |
|---|---|---|
| `0xC1` | `[ kind:u8 ‖ origin:32 ‖ n:varint ‖ n × tip:32 ]*` | frontier: the sender's clock. The hello costs the tips, not the history |
| `0xDE` | `[ kind:u8 ‖ origin:32 ‖ n:varint ‖ n × (len:varint ‖ node) ]*` | delta: standalone tagged nodes. Apply opens each addressed object and delivers normally; echo and replay are no-ops, out-of-order nodes orphan |
| `0xAF` | `raw artifact bytes` | one artifact, content-addressed by the receiver |
| `HWB2…` | canonical store snapshot | ENCODING_SPEC.md; `decode_hashweb` also reads the pre-`Place` legacy form, `encode_hashweb` always writes v2 |

- Frames address objects by **(kind, origin)**, never object id: the
  derivation is one-way, so a receiver could not open an unknown object
  from its id and its ops would orphan forever.
- Artifacts never ride deltas. With delta sync enabled
  (`enable_delta_sync`), artifacts minted locally of ≤ `WIRE_ARTIFACT_MAX`
  (1024) bytes are queued for `take_new_artifacts`, and the caller sends
  each as a 0xAF next to the delta that uses it. Received bytes are not
  re-queued; larger blobs are pushed by the uploader or fetched by
  content-addressed GET.
- Wire snapshots (`encode_hashweb_ops`) carry artifacts ≤
  `WIRE_ARTIFACT_MAX` only; the full snapshot (`encode_hashweb`) carries
  all.

Known gap: `apply_delta` opens every addressed object before validating
its nodes, so rejected or empty groups leave junk objects (QUEUE.md 47).

## Object deletion / GC

Tombstoning the slots that reach a child makes its op subgraph
unreachable and locally droppable, but a BFT peer can re-present it.
"Deleted" for sync means honest peers stop forwarding unreachable
subgraphs; re-receiving one is harmless (it cannot resurrect
reachability). With transclusion, unreachable means unreachable from
*every* slot (HETEROGENEITY.md open problems).

## Substrate

Shared, replica-local mechanics: id interning (handles are replica-local;
everything convergence-relevant — sibling order, head sets, hashing, wire
— stays in `Id` space), tips maintenance, orphan buffering, and the apply
skeleton (dedup → missing ref → admission → interpret → tips → wake).
