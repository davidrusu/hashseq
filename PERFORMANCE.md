# Performance notes & remaining optimization ideas

Status as of 2026-06-11, after the optimization push. Benchmark:
`cargo run --release --example sequential_traces` (traces from
`../editing-traces/sequential_traces`, 50 iterations each). Snapshots of full
runs live in `target/perf/` (wiped by `cargo clean` — the key numbers are
reproduced here).

## Where we are

Min build times (per the hygiene note below — averages carry right-tail
machine noise), measured 2026-06-14 after positional remove references, the
`removed` bitset, the packed `Loc`, the `SortedIdVec` sibling sets, the
`IdxSet` deps sets, and multi-target-remove range compression.

| Trace               | Build time (min) | Throughput  | Memory  | Encoded  |
|---------------------|------------------|-------------|---------|----------|
| automerge-paper     | 123 ms           | 2.09M ops/s | 24.0 MB | 447 KB   |
| rustcode            | 326 ms           | 2.94M ops/s | 48.1 MB | 834 KB   |
| sveltecomponent     | 48 ms            | 3.47M ops/s | 9.1 MB  | 247 KB   |
| seph-blog1          | 123 ms           | 2.94M ops/s | 20.8 MB | 659 KB   |
| clownschool\_flat   | 9.0 ms           | 2.67M ops/s | 2.4 MB  | 130 KB   |
| friendsforever_flat | 10.3 ms          | 2.51M ops/s | 2.5 MB  | 115 KB   |
| json-crdt-blog-post | 15.1 ms          | 3.33M ops/s | 3.8 MB  | 91 KB    |

(Encoded reflects positional remove references + multi-target-remove range
compression; Memory reflects the `removed` bitset, the packed `Loc`, the
`SortedIdVec` sibling sets, and the `IdxSet` deps sets — all below. Build time /
throughput are within machine noise of the prior baseline.)

What got us here (in order): Cursor as the single insertion path; before nodes
became Before-runs and `insert_before` stopped splitting runs (boxes −40%);
in-memory `RemoveRun` chains mirroring the wire format (remove storage 67 → 16
MB on automerge, plus a multi-target-remove identity bug fix); `NodeIdx`
interning — one global `Id → NodeIdx` map, with `ids`/`locs`/`removed` Vecs and
all secondary structures in handle space (memory −30..45%, time −15..25%);
the intern map keyed by u64 id prefix (`IdIndex`: prefix map verified
against `ids[idx]`, full-key spill map for true collisions — memory −8..25%,
min times −2..8%, snapshots `baseline-before-id-prefix-map.txt` /
`after-id-prefix-map.txt`); and the run-granular positional index
(`src/run_index.rs`, see below — memory −5..28%, min times −17..28%, snapshot
`after-run-index.txt`).

**The run-granular index** replaced the per-visible-char
`AssociativePositionalList` (dependency dropped) with an order-statistics
treap over run *fragments* (a run fragments only when a span lands strictly
inside it, so fragments stay O(runs)), each fragment carrying a visibility
bitmap for rank/select. All apply-path positioning is now *relative* — "insert
this span directly before/after element X", with X found by `region_first` /
`subtree_last` walks over the causal structure — so no absolute positions are
computed on inserts or removes at all. This also **fixed a real bug**: the old
index placed a forked sibling at `find(anchor)+1` / filtered tombstoned
siblings, which diverged from the document iterator (the source of truth) —
`id_at`/`position_of`/`cursor_at` could disagree with rendered text on forked
documents. Covered now by `prop_index_matches_iterator` and
`index_orders_concurrent_siblings_like_the_iterator`; the index module has its
own randomized model test (`randomized_against_model`).

**Origin unification** (2026-06-11, hash-breaking format change): `InsertRoot`
is gone. A document is constructed as `HashSeq::new(doc_id)` (`default()` =
all-zero "anonymous" origin); top-level inserts are ordinary runs anchored at
the origin id, which is interned as the tombstoned virtual `NodeIdx(0)`.
`Loc::Root`/`CausalRoot`/`root_nodes` and the wire Roots section are deleted;
the encoding header now carries the origin (implicit dict entry 0). Every op
hash transitively commits to the doc id, so different documents can no longer
merge (`merge` asserts equal origins). Perf is unchanged (snapshot
`after-origin-unification.txt`; `Runs` counts identical — old root chars
merged *into* their runs, `elems` +1 per root). This is groundwork for
HASHDOC.md, where a child object's "roots" are inserts anchored at its
creation op.

**Node-id preimage** (2026-06-11, hash-breaking, landed with the origin
unification): `HashNode::id` is now
`BLAKE3::derive_key("hashseq v1 node id", encode_hash_node(node))` — the hash
of the node's canonical wire bytes, streamed without allocation from a cloned
pre-keyed hasher template. Injective because the encoding is decodable;
domain-separated and versioned by the derive-key context (bump it when the
canonical encoding changes); `id_preimage_is_the_canonical_wire_encoding`
locks the streaming copy to `encode_hash_node`. Also faster: 159 → 105 ns per
chained node hash (fewer hasher updates, no ASCII tag strings), raising the
hash-only ceiling to ~9.5M ops/s on this machine. Snapshot
`after-id-preimage.txt`.

**Orphan buffering** (2026-06-11): orphans now park on their first missing
dep (`HashMap<Id, Vec<(Id, HashNode)>>`, SipHash — keys are adversary-chosen
bytes) with an `orphan_ids` dedup set; applying an op wakes only its waiters
via an iterative worklist. Replaces the retry-everything drain that was
quadratic on out-of-order batches and recursed once per link on
reverse-delivered chains. Hot path allocation-free; benchmark-neutral
(snapshot `after-orphan-rework.txt`).

**Interning invariant** (do not break): handles are replica-local. Anything
convergence-relevant — sibling order in `afters`/`befores_by_anchor` values,
root order, `tips`, hashing, the wire format — must stay in `Id` space.

The temporary `memory_breakdown` instrumentation in
`examples/sequential_traces.rs` is deliberately still there; keep it until the
memory work below is done.

**Interior extra-deps in runs** (2026-06-11, wire change, node hashes
unchanged): `Run`/`StoredRun` carry a sparse `interior_extra_deps:
BTreeMap<offset, BTreeSet<Id>>`; the insert fast path extends through
non-empty extra deps (typing across a delete no longer forks a run);
`decompress`/`from_text`/`split_at` attach, rebuild, and re-home them (deps at
a split point become the right run's first deps, preserving ids). Structural
results (snapshot `after-interior-deps.txt`): run counts −11..77%
(clownschool 4,555 → 1,067; friendsforever 3,699 → 1,477; automerge 7,931 →
7,067), `afters` storage −36..53%, memory −28..34% on the delete-heavy flat
traces (clownschool 6.8 → 4.5 MB) and −0..6% elsewhere. Encoded sizes ≈ flat
(±0.7% — bytes redistribute: deps move from per-run headers to interior
entries, and before-runs absorb continuations from the after-runs section);
the dict barely shrinks because dep *ids* still need entries — releasing them
is exactly idea 1 (positional refs) below. Times (re-measured on a recovered
machine; an earlier attempt was invalidated by thermal throttling, caught by
running the `hash_cost` canary alongside — worth doing for any suspicious
benchmark delta): min times −9.3% / −4.3% on clownschool / friendsforever
(fewer runs → fewer index fragments) and within ±2% noise elsewhere.

**Positional anchor references** (2026-06-11, wire change): anchors and all
extra-deps sets are now tagged *refs* — a varint whose low bit picks dict
index vs positional `(run_idx, elem_idx)` into an earlier-encoded run. Runs
are emitted in dependency order (Kahn over "references an element of" edges,
min-id tie-break); run-level reference cycles are real (concurrent typists
whose runs reference each other's elements — element DAG stays acyclic) and
are broken deterministically by force-emitting the smallest-id blocked run,
whose unresolved refs fall back to the dict. Encoded sizes −21..35%
(automerge 794 → 587 KB, clownschool 210 → 137 KB, friendsforever 184 →
126 KB); dict −43..61%; build times unchanged (encode cost is off the build
path). Covered by `prop_roundtrip_after_merge` (merged two-replica seqs) and
`roundtrip_with_run_level_dep_cycle`. Snapshot `after-positional-refs.txt`.

**Positional remove references** (2026-06-13, wire change — idea 1 below,
landed): the dict's long tail was remove-op ids referenced from deps sets
(run→remove from typing across a delete, remove→remove from sequential
batches) — removes weren't positionally addressable, so each spent a 32-byte
entry. Now runs *and* removes are one interleaved dependency-ordered stream of
tagged blocks, each exposing ids addressable by rank within its kind:
`(run_rank, offset)` for a run element, `(remove_rank, offset)` for a remove
op. A `ref`'s low bits pick the form (`r1` run element / `00` dict / `10`
remove), keeping the dominant run-element ref at its prior 1-bit cost so the
millions of remove targets in delete-heavy docs don't pay for interleaving.

Two design corners, each caught by the benchmark before it shipped: (1) a naive
unified order let a wide multi-target remove get force-emitted ahead of the
hundreds of runs it targets, dumping every target into the dict (rustcode
2.79 → 4.36 MB) — fixed by making every remove→target edge *hard* (a remove
never precedes a run it deletes; the hard graph is bipartite remove→run, hence
acyclic). (2) addressing by global emit position instead of within-kind rank
inflated rustcode's ~2M target-ref varints (2.79 → 2.88 MB) — fixed by the
separate compact rank spaces above. The only refs left in the dict are the
origin, orphan deps on unknown nodes, and a run typed across a delete of its
*own* element (a true run↔remove 2-cycle), one entry each. Encoded sizes
−2.8..23.9% (automerge 587 → 447 KB, dict 293 → 139 KB; seph 1,293 → 1,133;
json 139 → 116; rustcode 2,790 → 2,712); build time / memory / `Runs`
unchanged (wire-only). Covered by the existing roundtrip/determinism/merge
props (run at 20k cases) and `Correct=T` on every trace.

**Single-hash decode / zero-hash merge** (2026-06-11): element ids were being
recomputed up to three times per element on the sync paths (chain hashing in
`from_text`, `prev.id()` chaining in `decompress`, identity hashing in
`apply`). Now `Run::decompress_with_ids` chains through the cached
`elements`, `StoredRun::to_run(&seq.ids)` copies ids instead of rehashing,
and `apply_with_id` (debug-asserted against a fresh hash) consumes them.
Decode hashes each element once (`from_text`, computed from the received
bytes — the authoritative derivation, so no trust change); merge hashes not
at all (ids come from the local table). Measured on a 128k-char doc: decode
150 → 98 ms, merge 148 → 85 ms, encode unchanged. `Run.elements` stays — it
is the cache that makes this work; `StoredRun.elements` is irreplaceable
(`locs` is its inverse) but could be range-compressed someday (idea 3).

**Index-based iteration** (2026-06-11): `iter`/`iter_ids`/`iter_idxs` now
ride the run index — an in-order treap fragment walk (parent pointers, no
allocation), slicing each fragment's text sequentially and skipping
tombstones via the visibility bitmaps. This replaced the per-element causal
traversal whose `char_at` (`chars().nth(pos)`) made full-text iteration
quadratic per run — interior-deps made runs longer and exposed it (rustcode's
69k-char run: 47 ms to iterate). The benchmark gained an `Iter(ms)` column
(min, measured around the correctness check): iteration is now **10–77×
faster** — rustcode 47.3 → 0.61 ms, automerge 8.74 → 0.64 ms, all traces
sub-millisecond. The causal traversal stays compiled as the semantic
*definition* of document order (`iter_idxs_causal` + `HashSeqIdxIter`), and
`check_index_matches_iter` now explicitly asserts fragment-walk ≡ causal on
ids and text — a latent index bug fails the prop instead of corrupting
content. Snapshots `baseline-before-index-iteration.txt` /
`after-index-iteration.txt`.

**Encoding is not byte-canonical across replicas** (noted 2026-06-11, found
by `prop_roundtrip_after_merge`): chain and run *storage* is arrival-order
dependent under concurrency (two removes claiming the same parent extend
vs fork depending on who applied first; same for run extension races), so
`encode(decode(encode(x)))` may differ from `encode(x)` byte-wise even though
the logical state roundtrips exactly. Same-storage encoding remains
deterministic (`prop_encoding_is_deterministic`). A canonical storage
normalization (e.g. smallest-id child continues a chain) would enable
content-addressed snapshots — relevant to HASHDOC.md sync; not needed today.

## Remaining ideas, in rough value order

### 1. Positional remove references (the dict's long tail) — DONE (2026-06-13)

Landed; see "Positional remove references" above. Removes are now positionally
addressable in their own compact rank space within an interleaved run/remove
block stream. Dict −36..52%, encoded sizes −2.8..23.9%. The "catch" turned out
to be two distinct traps (wide-multi-target force-emit, and global-position
ref inflation) rather than the predicted section-ordering problem — both
solved by hard target edges + within-kind rank addressing.

### 1b. Split wire runs at cyclic remove-deps (the dict's residual)

The residual dict after idea 1 is two piles (instrumented on automerge: 4,346
entries ≈ 139 KB): `dict_in_remove` (~1,641) — remove ids that a run references
through a run↔remove 2-cycle, the one case that still can't go positional — and
`dict_in_run` (~2,671) — run-element ids stuck in the dict by concurrent
run↔run cycles (unrelated to removes). This idea attacks only the first pile.

The 2-cycle is "type across a delete *in the same run*": run R = `abc`, delete
`b` (remove M targets `b ∈ R`, so M→R is a hard edge), type `d` continuing R
(R's interior dep is M, so R→M). M must precede R *and* R must precede M; the
hard edge wins, R emits first, and R's interior dep on M spills to the dict.

The element DAG is acyclic (`id(d)` needs `id(M)` needs `id(b)`; `id(b)` doesn't
need M) — only the *block* is cyclic. So break the block: emit R as two wire
fragments split at the interior-remove-dep — R1 = `abc`, then M, then R2 = `d…`
with `anchor = id(c)` and `first_extra_deps = {M}`, both backward refs. Chain
`R1, M, R2`, zero dict entries, decoder stays single-pass (no two-pass decode —
that would regress the 98 ms hot decode path). It's wire-only: decoding the
three blocks re-coalesces into the identical in-memory run via the same
interior-dep mechanism, and `StoredRun::split_at` / `Run::from_text` already do
the fragmenting, so the equality/roundtrip props carry over.

Conditional, bounded win: each split costs a fresh run header (~5–8 B: tag +
anchor ref + extra string-length and `num_interior` varints) to evacuate a 32 B
dict entry, so only split when it removes M from the dict *entirely* (all of M's
referencers go backward) — otherwise the entry persists and the header is
wasted. Estimate ~5–10% on the dict-heavy traces. Does nothing for the
`dict_in_run` pile; that needs the analogous run↔run split, which is murkier
(a run's anchor, not just an interior dep, can be the back-reference).

### 2. rustcode's multi-target removes (encoding) — DONE (2026-06-14)

A `remove_batch` deletes a contiguous span, so its targets are mostly
consecutive elements of a run — but they used to cost one positional ref each.
The `REMOVE_OTHER` block now coalesces them into `(run_rank, start, count)`
ranges: at encode the targets (a set — order is free) are resolved to
`(run_rank, off)`, sorted, and runs of consecutive offsets become one segment;
remove-op and non-element targets stay singletons. A segment's head reuses the
ref low bits (`r1` run-element range + a count varint / `00` dict / `10`
remove), so decode rebuilds the *exact* target set and the node id survives.

Way past the predicted 5–10×: `RmOther` −90..97% (rustcode 1,936 → 59 KB, ~33×;
seph 544 → 69 KB; svelte 269 → 29 KB). Totals: **rustcode 2,712 → 834 KB
(−69%)**, svelte 487 → 247 (−49%), seph 1,133 → 659 (−42%), json 116 → 91
(−21%); automerge/friendsforever unchanged (no multi-target removes). Build/
memory untouched (wire-only). Covered by `test_multi_target_remove_roundtrip_identity`
+ `prop_batch_remove_roundtrip` + `prop_encoding_is_deterministic` (50k cases) —
identity preservation and deterministic segment ordering are the two risks.

### 3. Smaller / opportunistic

- **Pack `Loc`** into 8 bytes vs the 12-byte enum — DONE (2026-06-14).
  `PackedLoc(u64)` = 2-bit kind | 32-bit handle | 30-bit pos (full `NodeIdx`
  range kept; `pos` is a within-run offset, ~69k max in the corpus vs the 1.07B
  ceiling). Stored as `locs: Vec<PackedLoc>`; the public `Loc` enum and every
  match/construction site are unchanged — `loc_of` unpacks, `intern` packs via
  `From<Loc>`. Memory −3.5% (automerge 30.07 → 29.03 MB, rustcode 56.50 →
  54.39 MB), and build/iter times are if anything a hair faster (narrower Vec →
  better cache locality). Covered by `prop_packed_loc_roundtrips` plus every
  existing test, which all go through `loc_of`.
- **`removed: Vec<bool>` → bitset** — DONE (2026-06-14). `src/bitset.rs`:
  append-only `Vec<u64>` packing, `push`/`set`/`get`, panics on OOB like
  `Vec<bool>` did. `removed` stays the global tombstone truth (the iterator and
  sibling scans want O(1) per-handle access; the run-granular index keeps its
  own per-fragment bitmaps). As predicted, small: the structure is 8× smaller,
  total memory −0.7..0.8% (automerge 30.29 → 30.07 MB, rustcode 56.97 →
  56.50 MB). `HashSeq` equality is tips-only so the type swap is invisible to
  the merge laws; covered by `matches_vec_bool_under_random_ops`.
- **`afters`/`befores_by_anchor` values** as Id-sorted `Vec<NodeIdx>` instead
  of `BTreeSet<Id>` — DONE (2026-06-14). `SortedIdVec` in hashseq.rs: a
  `Vec<NodeIdx>` (4 B/entry, no per-node tree alloc) kept in `Id` order;
  ordering methods (`insert`, `first_ge`) binary-search through a passed `&[Id]`
  so order stays in convergence-safe Id space while the payload is the compact
  handle. Iterating yields handles directly, so the consumers also drop a
  per-sibling `idx_of` hash lookup. Memory −3.3..9.3% (automerge 29.0 →
  27.2 MB, seph 29.7 → 26.9 MB, rustcode 54.4 → 52.6 MB); build/iter times
  unchanged (fork sets are tiny, so the O(n) Vec insert never bites). Order
  correctness covered by the merge laws + `prop_order_is_stable` /
  `prop_index_matches_iterator` (run at 30k cases). Note: the predicted
  "clownschool has oddly large afters" no longer holds — its afters map is
  small now (the interior-deps run-coalescing cut its fork count); clownschool
  saved only ~245 KB here, the big wins were automerge/seph/rustcode.
- **`Id` interning for deps sets** — DONE (2026-06-14). `IdxSet(Box<[NodeIdx]>)`
  in hashseq.rs, replacing the `BTreeSet<Id>` deps on `StoredRun`
  (`first_extra_deps` + `interior_extra_deps` values), `RemoveRun`, and
  `CausalRemove`. Built once at apply time from the (already Id-sorted) op deps
  via `idx_of_known`; rebuilt to `BTreeSet<Id>` for the wire / hashing at
  `to_run` / encode / merge. **Much bigger than this note guessed** — these sets
  are almost always 1–3 tips, and a 1-element `BTreeSet<Id>` allocates a ~400 B
  B-tree node for one 32 B id, so `Box<[NodeIdx]>` (4 B/entry, 16 B inline like
  the BTreeSet) is a ~100× cut on each. Memory −8.6..41.8%: automerge 27.2 →
  24.0 MB, seph 26.9 → 20.8 MB (−6.1 MB), rustcode 52.6 → 48.1 MB; the
  delete-heavy flats win most because the remove deps (`RemoveRun` /
  `CausalRemove`) dominate them — clownschool 4.1 → 2.4 MB (−42%),
  friendsforever 3.8 → 2.5 MB (−34%). Build/iter times unchanged. Order stays in
  convergence-safe `Id` space (handles are just the payload); covered by the
  merge laws + roundtrip/determinism props at 30k cases.

### 3b. Consolidate remove chains on the wire (`RmSing`)

Forward/backward remove-runs are already ranges (`start..end`). The remaining
`RmSing` bytes (automerge 18 KB, rustcode 11 KB) are chain links that couldn't
join a span — a chain gets split into one wire block per run-boundary and per
non-adjacent step, and every piece after the first **re-encodes its dependency
on the previous piece** (`{prev link id}`). Fix: collapse a whole chain into one
`RemoveChain` block — `first_extra_deps` once, then targets *in chain order* as a
segment list (forward/backward ranges where contiguous, singles otherwise) that
crosses runs freely; links chain implicitly, so the per-piece deps refs vanish.
Subsumes `RemoveSpan` + `Single`. Modest (~1–2% of the encoding, concentrated on
scatter-delete traces) and fiddlier than #2 (chain order must be preserved,
ranges need a direction bit, decode reconstructs across runs). Below #1b.

### 3c. Range-compress the in-memory remove structures (parked)

Measured handle-contiguity of the `Vec<NodeIdx>` payloads in `RemoveRun` /
`CausalRemove`:

- **`RemoveRun.links` is 100% contiguous** (every trace) and in fact
  *derivable*: removes intern sequentially, so `links[pos] = chain_head + pos`.
  Replace `links: Vec<NodeIdx>` with `LinkSeq::Contiguous { count }` (+ a
  `List(Vec)` fallback for out-of-order delivery, which can break the run).
  Drops ~half of `remove_runs` — ~0.8 MB automerge, ~0.5 MB seph; zero risk.
  But nothing for rustcode (whose `remove_runs` is only 0.5 MB).
- **`CausalRemove.nodes` / `RemoveRun.targets` hold the big memory** (rustcode
  nodes = 3.3 MB) but are only 0.2–18% contiguous *by handle* — yet they
  range-compress 33× on the wire, because the wire ranges by element *position*
  (`run_rank, off`), and a batch delete is position-contiguous even when the
  interning order is scrambled. To capture that in memory you'd store
  `(run_head, start_pos, count)` instead of handles — but `split_run_at`
  relocates/fragments a span's positions, so it needs a reverse index (run →
  removes targeting it) updated on every split. Real overhead and bug surface;
  parked alongside #4. (automerge's `targets` are genuinely scattered — 18%
  contiguous, which is exactly why they're wire singles — so not compressible in
  either space.)

### 4. The floor, if it's ever needed

`ids: Vec<Id>` (32 B per node ever applied, ~10.8 MB automerge) is the
irreducible cost of caching every node's hash. Element ids within a run are a
hash *chain* — droppable and recomputable by walking the run forward
(~100–200 ns/element). That trades real CPU (full-text iteration, splits,
encoding all start rehashing) for the last big memory block. Don't do this
unless a use case actually demands it; `IdIndex` prefix-hit verification also
relies on `ids` being present.

## Perf-testing hygiene

- Compare *min* times across runs, not averages — the machine noise shows up
  as right-tail outliers.
- Run counts in the benchmark output double as a structure checksum: if a
  change is supposed to be structure-neutral, the `Runs` column must not move.
- `prop_encoding_is_deterministic`, the roundtrip props, and the merge laws
  (commutative/associative/reflexive) are the safety net for all of the wire
  and storage ideas above; extend them first when adding format features
  (the multi-target remove identity bug existed because no prop exercised
  `remove_batch(amount > 1)`).
