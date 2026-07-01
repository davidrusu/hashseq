# HashKv op spec

Framework: FRAMEWORK.md (deps = observed-version commitment; Law I state = f(op
set); Law II arbitration at read time; resource → conflict → resolution).
Design rationale: HASHDOC.md §"map Put is a hashseq Remove".

## Op

```rust
enum Value {
    Bytes(Box<[u8]>), Int(i64), Bool(bool),   // leaf scalars (≤32B inline)
    Blob(Hash),                                // ≥32B, content-addressed side store
    Ref(Id),                                   // child object (HashDoc)
    Tombstone,                                 // Del
}

enum MapOp {
    Put {
        key: Box<[u8]>,
        value: Value,
        overwrites: BTreeSet<Id>,   // the per-key heads this put saw and replaces
    },
    // Del ≡ Put { value: Tombstone, .. }
}

struct MapNode { extra_dependencies: BTreeSet<Id>, op: MapOp }
// id = BLAKE3::derive_key("hashkv v1 node id", canonical_encoding)
```

## Deps

```
named(u)            = overwrites(u)
extra_dependencies  = tips − overwrites          // map-layer tips
deps(u)             = extra_dependencies ∪ overwrites = tips
```

`overwrites` is the "replaces" commitment (automerge `pred`); extra_deps pins
the observed version. A Put with `overwrites = ∅` asserts a fresh value seeing
no prior write to the key.

## Resource

One **register per key**: `KeyState { heads: SmallVec<[NodeIdx; 1]> }`. A Put
claims the value of its key.

## Conflict

The live head set of key `k` is

```
heads(k) = { p ∈ Puts(k) : ∄ q ∈ Puts(k). p.id ∈ overwrites(q) }
```

A **conflict** is `|heads(k)| > 1`: ≥2 puts on `k`, pairwise concurrent (neither
in the other's `overwrites`), so neither superseded the other.

A Put is *never* in conflict with the puts it names in `overwrites` — those are
in `deps*`, causally prior, and removed from `heads` on apply.

## Resolution (read time)

- **MVR is primary.** `read(k)` returns `heads(k)` — the full multi-value set.
- **`|heads| = 1`** → that value (`Tombstone` head → key absent).
- **`|heads| > 1`** → conflict, surfaced to the app. When a single value is
  demanded:
  - *cosmetic ambiguity only* (display, attribution) → `max-Id` head — a
    **display tiebreak**, convergent and unforgeable, never semantics.
  - *semantics-bearing value* (URL, config, anything whose consequences leave
    the key's own cell) → **do not collapse**; render the conflict state. Hash
    order is grindable; a ground id must not silently win. (Locality invariant,
    FRAMEWORK.md "locality dividing line": a key's value is "other people's
    content," so id-order is not a sound resolution here — only a display hint.)
- **No LWW.** Wall-clock is forgeable; there is no timestamp input.

## Apply

```
heads(k) = heads(k) − overwrites(u) ∪ {u}        // O(|overwrites|)
```

No causal-ancestor walk (overwrites are explicit). Identical shape to
`apply_remove`. Session chains (`k1=a, k2=b, k1=c` each dep'ing the previous)
compress to one write-run on the wire (HASHDOC.md §"runs translate along the
writer chain").

## Validation

- Keys are adversary-chosen bytes → in-memory `KeyHash` uses SipHash or a
  BLAKE3-derived hash (FxHash only for ids that are already hashes).
- `Blob(hash)` may be unresolvable (bytes never provided): DAG still converges;
  API exposes a `pending/unavailable` value state, not papered over.
- No apply-time quarantine is needed for well-formed Puts (any key/value is
  authorable; conflict is the surfaced symptom).
