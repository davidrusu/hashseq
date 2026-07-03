<a href="https://crates.io/crates/hashseq"><img src="https://img.shields.io/crates/v/hashseq.svg"></a>

# HashSeq

A Byzantine-Fault-Tolerant (BFT) CRDT family for unpermissioned networks with
an unbounded number of collaborators. This crate implements the **HashWeb**
design: content-addressed ops with self-certifying BLAKE3 ids, an explicit
causal hash-DAG, and one design law — *state is a pure function of the op
set; every order-sensitive decision resolves at read time*.

Projections implemented here:

- **HashSeq** — the sequence CRDT (text, lists): `Insert` / `Remove` /
  `Move` / `Mark`, run-compressed, heterogeneous (elements are chars or
  value commitments — links, artifacts, embedded objects), ~2M+ ops/sec
  on real editing traces.
- **HashKv** — the key-value CRDT: `Put` with explicit supersession,
  multi-value registers, no LWW.

The full design lives in the spec set: `FRAMEWORK.md` (the op model and
adversary analysis), `HASHSEQ_SPEC.md`, `HASHKV_SPEC.md`, `HASHWEB_SPEC.md`
(composition), `ENCODING_SPEC.md` + `GRAMMAR_SPEC.md` (canonical bytes and
the identity grammar).

## Merge semantics at a glance

Concurrent runs never interleave (`hello` ∥ `goodbye` merges to
`hellogoodbye` or `goodbyehello`, never `hgeololdobye`), common prefixes
deduplicate (`hello earth` ∥ `hello mars` → `hello earthmars` or
`hello marsearth`), and merging is op-set union — commutative, associative,
idempotent (quickcheck'd), with orphan buffering for out-of-order delivery.

## Why BFT, and what it costs

Non-BFT CRDTs (Automerge, Yjs) order concurrent edits with Lamport
timestamps and actor ids — both forgeable, and both grow per-collaborator
metadata. HashSeq's ids are BLAKE3 hashes of each op's content and
references:

- **unforgeable ordering** — a malicious actor cannot tamper with a clock or
  forge an actor id; grinding a hash buys nothing (id order only ever
  arranges the grinder's *own* content — the locality invariant);
- **zero per-collaborator state** — no vector clocks, no actor registry;
  anyone can join anonymously;
- **self-certifying documents** — every op hash transitively commits to the
  document id; different documents can never merge;
- **honest conflicts** — contested registers surface every head (MVR) and
  freeze at the last agreed value rather than silently picking a winner.

Measured cost of all of the above on the text hot path: single-digit
percent (see `PERFORMANCE.md`). Real-trace throughput is 1.9–3.2M ops/sec.

## The op model

Every op is one flat reference set plus a meaning over it:

```rust
pub enum Anchor { Before(Id), After(Id) }     // THE glued point

pub enum Op {
    Insert { at: Anchor, payload: Payload },  // claim a gap
    Remove(BTreeSet<Id>),                     // claim liveness (tombstones)
    Move { target: Id, to: Anchor,            // claim placement
           overwrites: BTreeSet<Id> },        //   (same-container registers)
    Put { key: Id, value: Id,                 // claim a key's register
          overwrites: BTreeSet<Id> },
}

pub struct HashNode { pins: BTreeSet<Id>, op: Op }
// refs(u) = pins ∪ named(u)
// id = BLAKE3::derive_key("hashweb v1 node id", envelope ‖ body)
```

Honest clients pin their observed frontier in `refs`; concurrency needs no
clocks. Payloads, keys, and values are **ids of content-addressed value
artifacts** — a char, an int, a blob hash, or another object's origin id (a
link) — so the same op shape carries text, JSON-ish data, and object graphs.

### Ordering without timestamps

The underlying structure is a causal insertion tree: every insert anchors at
another node (or the document origin). Forks — multiple nodes sharing an
anchor — order by hash, depth-first, so concurrent runs stay contiguous:

```
"hi sam" ∥ "hi dan", common prefix deduplicated:

'h' → 'i' → ' ' ─┬─ 's' → 'a' → 'm'
                 └─ 'd' → 'a' → 'n'      ⇒  "hi samdan" (or "hi dansam")
```

Hash order is legitimate *only* there, where either order is equally valid
and only the writers' own content is arranged. Everything with intent is
explicit. Inserting between causally-ordered characters uses
`Before(right)`, pinning the result:

```
'h' → 'l' → 'l' → 'o'      fix the typo with Insert{ at: Before(first l) }:

'h' → 'l' → 'l' → 'o'
      ↑
     'e' (before-child)     ⇒  "hello", regardless of hash values
```

Sequential typing compresses into **runs** — `'h','e','l','l','o'` chained
by anchor stores as one `Run("hello")`; the per-char ids recompute from the
text, so storage stays near the text size while identity stays per-char.

### Moves that freeze instead of flip

`Move` gives every element a placement register with explicit supersession
(`overwrites`). Two users dragging the same element concurrently is a
surfaced conflict, and the element **stays put** — the last agreed
placement — until someone's next move names both heads. Never a silent
teleport, never a hash-ground winner. Moves are same-container by design:
cross-container relocation is remove + re-insert of a link, so placement
cycles are unrepresentable.

### Keys without LWW

`HashKv::put` supersedes exactly the heads the writer saw. Concurrent puts
are a multi-value read (`Read::Conflict`) that `get()` refuses to collapse —
wall-clock LWW is forgeable and does not exist here.

## Usage

```rust
use hashseq::{HashKv, HashSeq, Value};

let mut seq = HashSeq::default();
seq.insert_batch(0, "hello".chars());
assert_eq!(seq.iter().collect::<String>(), "hello");

let mut a = HashKv::default();
let mut b = HashKv::default();
a.put(Value::String("k".into()), Value::Int(1));
b.put(Value::String("k".into()), Value::Int(2));
a.merge(b);                    // conflict surfaced, not resolved
assert!(a.get(&Value::String("k".into())).is_none());
a.put(Value::String("k".into()), Value::Int(3)); // dominates both heads
```

## Performance

Real-world editing traces from the
[editing-traces](https://github.com/josephg/editing-traces) suite, 50
iterations, min build times: 1.9–3.2M ops/sec; memory and encoded sizes in
`PERFORMANCE.md`. The benchmark doubles as a structure checksum — run counts
are asserted stable across refactors.

## Wasm Demo

A two-peer browser demo lives in `web/`. It compiles HashSeq to WebAssembly
and wires two CodeMirror editors to independent CRDT instances so you can
edit each side and merge them with a Sync button.

Prerequisites: [`wasm-pack`](https://rustwasm.github.io/wasm-pack/installer/)
and any static file server.

```sh
wasm-pack build --target web --out-dir web/pkg
python3 -m http.server --directory web 8000
```

Then open <http://localhost:8000>, type into both editors, click **Sync**.

## Status

The identity grammar (preimages, contexts, derived constants) is implemented
exactly per `GRAMMAR_SPEC.md` and locked by test vectors
(`tests/grammar_vectors.rs`). Moves render: placement registers relocate
elements in the position index (origin ghosts, frozen conflicts render at
the last agreed placement), with splice-point anchors for typing adjacent
to moved content. Marks render: Peritext-style span annotations with
anchor-encoded edge expansion and add-wins concurrent unmark; marks are
regional — points stay glued to base slots, elements moved out of a span
shed it, elements moved in acquire it (`marks_at` / `marked_spans`). Snapshots are
canonical across the whole family: blocks derive from the op set, never
replica storage — equal op sets encode to identical bytes across replicas
and delivery orders (quickcheck'd, hash-locked), with strict decode modes
(`decode_*_strict`) as the verifying acceptance path. HashKv and HashWeb
snapshots nest per-object canonical streams with one document-wide
artifact section.
