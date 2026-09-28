<a href="https://crates.io/crates/hashseq"><img src="https://img.shields.io/crates/v/hashseq.svg"></a>

# HashSeq

A Byzantine-fault-tolerant CRDT family for open networks with any number of
anonymous collaborators. Every op is content-addressed: its id is a BLAKE3
hash of its content and the ops it references, so the history is a
self-certifying hash DAG. One design law: *state is a pure function of the op
set; every order-sensitive decision resolves at read time*.

- **HashSeq** — sequences (text, lists): `Insert`, `Remove`, `Move`, `Mark`.
  Elements are chars or value commitments (links, artifacts, embedded
  objects).
- **HashKv** — maps: `Put` with explicit supersession; multi-value registers,
  no last-writer-wins.
- **HashWeb** — a store of many seqs and kvs, composed by links, synced by
  DAG diff against each peer's frontier.

Every object also carries a containment register (`Place`), so objects move
between containers without duplicating.

## Merge semantics

Merging is op-set union: commutative, associative, idempotent, with
out-of-order ops buffered until their references arrive. Concurrent typing
never interleaves (`hello` ∥ `goodbye` → `hellogoodbye` or `goodbyehello`,
never `hgeololdobye`), and conflicts surface instead of being silently
resolved: a contested move or placement freezes at the last agreed value,
and a contested map key reads as every concurrent value.

## Why BFT

Automerge and Yjs order concurrent edits with Lamport timestamps and actor
ids, both forgeable, and both grow per-collaborator metadata. Here:

- **Ordering can't be forged.** Ids are hashes; grinding one only reorders
  the grinder's own content among its concurrent siblings.
- **No per-collaborator state.** No vector clocks, no actor registry;
  anyone can join.
- **Ops are self-certifying.** Each op commits to its causal history and
  its object's origin; replicas verify every id they receive.
- **Conflicts are honest.** Concurrent writes to a register stay visible
  (`Read::Conflict`) until a later write names them all.

## The op model

```rust
pub enum Anchor { Before(Id), After(Id) }   // a glued point beside a node
pub enum Payload { Char(char), Id(Id) }     // a char, or any value's id

pub enum Op {
    Insert { at: Anchor, payload: Payload },            // claim a gap
    Remove(BTreeSet<Id>),                               // tombstone elements
    Move { target: Id, to: Anchor,                      // an element's
           overwrites: BTreeSet<Id> },                  //   placement register
    Mark { start: Anchor, end: Anchor,                  // formatting span
           kind_v: Id, value: Id, overwrites: BTreeSet<Id> },
    Put { key: Id, value: Id, overwrites: BTreeSet<Id> },   // a key's register
    Place { placed_at: Id, overwrites: BTreeSet<Id> },      // containment
}

pub struct HashNode { pub pins: BTreeSet<Id>, pub op: Op }
// refs = pins ∪ the ids the op names; id = BLAKE3 over a canonical preimage
```

Honest writers pin the frontier they observed, so concurrency needs no
clocks. Payloads, keys and values are **value ids**: a small value (≤ 15
bytes — a char, an int, a short string) is its own id; anything larger is
hashed and its bytes live in a content-addressed store. Links are just
another object's id, so one op shape carries text, structured data and
object graphs.

**Ordering without timestamps.** Every insert anchors beside another node,
forming an insertion tree; siblings at the same anchor order by id, and
the anchor rule (Fugue's) keeps concurrent runs contiguous. Sequential
typing compresses into **runs**: `"hello"` stores as one run whose per-char
ids are recomputed from the text.

**Moves freeze instead of flip.** Two users moving the same element
concurrently is a surfaced conflict: the element stays at its last agreed
placement until a later move names both heads. Moves stay within one
container; moving an object between containers inserts its link in the
destination and `Place`s it there, which the object's own register decides.

**Keys without LWW.** `put` supersedes exactly the heads the writer saw;
concurrent puts read as a conflict that `get()` refuses to collapse.

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
a.merge(b);                                      // conflict surfaced, not resolved
assert!(a.get(&Value::String("k".into())).is_none());
a.put(Value::String("k".into()), Value::Int(3)); // supersedes both heads
```

## Performance

On the [editing-traces](https://github.com/josephg/editing-traces) suite,
4.0–5.7M char edits/sec (Apple silicon, single thread). A canonical
snapshot, full history included, is 2–11× the size of the final text. Hashing is about a third of the time; each
typed char costs one BLAKE3 block. Details, memory and history in
`PERFORMANCE.md`; `cargo run --release --example sequential_traces` runs
the suite (traces expected in `../editing-traces`).

## Apps in this repo

- **Knowledge base** (`web/kb.html` + `sync-server/`): a collaborative
  block editor — pages, nested layout, drag and drop, marks, comments,
  tables — syncing through a relay that stores the canonical state.
  ```sh
  wasm-pack build --target web --out-dir web/pkg --release
  cargo run --release --manifest-path sync-server/Cargo.toml -- \
      --port 8093 --web-dir web --state kb-state.bin
  # open http://localhost:8093/kb.html in two tabs
  ```
- **Insertion-tree visualizer** (`web/index.html`, same server): watch the
  tree grow as you type, or replay an editing trace.
- **`nool`** (`cargo run --bin nool`): git-like history for plain files —
  a `<file>.nool` sidecar, or a `.nool/` repo tracking a tree; commit,
  diff, merge, move, and file-based delta sync.

## Specs

| Doc | Covers |
|---|---|
| `FRAMEWORK.md` | the op model, honest frontiers, the adversary, conflict resolution |
| `HASHSEQ_SPEC.md`, `MOVE.md`, `MARKS.md` | sequence ops, moves, formatting marks |
| `HASHKV_SPEC.md` | the map |
| `HASHWEB_SPEC.md` | the object store, admission, sync |
| `PLACEMENT_SPEC.md`, `CYCLE_REVERT.md` | containment and cycles |
| `GRAMMAR_SPEC.md`, `ENCODING_SPEC.md` | identity preimages and canonical bytes |
| `LAYERING.md`, `HETEROGENEITY.md`, `OP_REFS.md` | design rationale |

## Status

Pre-release (0.1). The identity grammar is locked by test vectors
(`tests/grammar_vectors.rs`); snapshots are canonical — equal op sets
encode to identical bytes on every replica, with strict decoders that
verify it. Known gaps and planned work are tracked in `QUEUE.md`,
including extension of the op set without a hard fork, containment-cycle
detachment in the crate, and ops of a child object replaying into its
parent (the composition convention).
