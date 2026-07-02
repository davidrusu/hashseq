# HashKv op spec

Framework: FRAMEWORK.md (one reference set + honest frontier rule; Law I
state = f(op set); Law II arbitration at read time; resource → conflict →
resolution). Keys and values follow the payload-as-id view (HASHSEQ_SPEC.md
"Payload"; HETEROGENEITY.md). The op is the Remove pattern applied to keys:
supersede the prior writes you saw, causally anchored (automerge's `pred`).

## Op

```rust
enum MapOp {
    Put {
        key: Id,                    // value commitment — the committed key artifact
        value: Id,                  // value commitment — a value artifact or an op node
        overwrites: BTreeSet<Id>,   // the per-key heads this put saw and replaces
    },
    // Del ≡ Put { value: TOMBSTONE, .. }   (the well-known tombstone artifact)
}

struct MapNode { refs: BTreeSet<Id>, op: MapOp }
// id = BLAKE3::derive_key(NODE_CONTEXT, canonical_encoding) — the family's
// single context; op kinds are tags in the encoding (HETEROGENEITY.md).
// TOMBSTONE and the creation artifacts are ordinary derived value ids —
// computed constants, never magic ids.
```

## Refs

```
named(u) = overwrites(u)
refs(u)  = named(u) ∪ frontier pins   // key and value are values, not references
```

`overwrites` is the "replaces" role (automerge `pred`); the remaining refs
pin the observed map-layer frontier (FRAMEWORK honest frontier rule). A Put with
`overwrites = ∅` asserts a fresh value seeing no prior write to the key.

Key and value are **value commitments, not references**: neither is in
`refs(u)`, buffering never waits on them, and an unresolvable artifact is
the app-visible `pending` state, not a delivery condition.

## Keys and values are ids

There is no `Value` enum at the op layer. Key and value are ids of
content-addressed artifacts; an artifact's kind is committed in its own
encoding and discovered at dereference (HETEROGENEITY.md), so one id field
covers everything a tagged union would — and more:

| the id names | you get |
|---|---|
| a value artifact (bytes, string, int, bool, …) | a scalar. Encodes inline on the wire at ≤ hash size, id derived at decode — inline vs indirect is transport, never identity |
| the well-known tombstone artifact | `Del` |
| a creation artifact (`NewSeq` / `NewMap`) | **this Put creates a child object**, identified by its origin id — `object_id = derive_key(OBJECT_CONTEXT, <this Put's id>)` (GRAMMAR_SPEC.md). Creation works identically from a map slot and a sequence slot |
| an object's origin id (`object_id`, GRAMMAR_SPEC.md) | a link to that object — `Ref` with no wrapper needed. Linking an object that already lives elsewhere is transclusion (HETEROGENEITY.md open problems) |
| an op node | a reference to that op — e.g. the subject of an "about" register (below) |

Keys get the same generality: bytes/strings are the common case, but any
committed value is a key — including a **node id**, which makes "a register
about op X" (status, votes, moderation labels on any op) a plain Put with no
new machinery. The only total order on keys is id order (key bytes can be
pending); display ordering over resolved keys is a render concern.

## Resource

One **register per key id**: `KeyState { heads: SmallVec<[NodeIdx; 1]> }`.
A Put claims the value of its key.

## Conflict

The live head set of key `k` is

```
heads(k) = { p ∈ Puts(k) : ∄ q ∈ Puts(k). p.id ∈ overwrites(q) }
```

A **conflict** is `|heads(k)| > 1`: ≥2 puts on `k`, none superseded (neither
named in the other's `overwrites`). Among honest authors this coincides with
pairwise concurrency (FRAMEWORK honest-author lemma); a Byzantine author can
fabricate it by omission — which is why resolution confers nothing on the
conflicted path.

A Put is *never* in conflict with the puts it names in `overwrites` — naming
removes them from `heads` on apply. Merely pinning a put in `refs` does not
exempt it (seeing-but-not-superseding is the withholding fabrication,
FRAMEWORK).

## Resolution (read time)

- **MVR is primary.** `read(k)` returns `heads(k)` — the full multi-value
  set.
- **`|heads| = 1`** → that value (tombstone head → key absent).
- **`|heads| > 1`** → conflict, surfaced to the app. When a single value is
  demanded:
  - *cosmetic ambiguity only* (display, attribution) → `max-Id` head — a
    **display tiebreak**, convergent and unforgeable, never semantics.
  - *semantics-bearing value* (URL, config, anything whose consequences
    leave the key's own cell) → **do not collapse**; render the conflict
    state. Hash order is grindable; a ground id must not silently win.
    (Locality dividing line, FRAMEWORK: a key's value is other people's
    content, so id-order is not a sound resolution here — only a display
    hint.)
- **No LWW.** Wall-clock is forgeable; there is no timestamp input.

## Apply

```
heads(k) = heads(k) − overwrites(u) ∪ {u}        // O(|overwrites|)
```

No causal-ancestor walk (overwrites are explicit). Identical shape to
`apply_remove`. The incremental step filters `overwrites` to same-key puts,
matching the definitional head set — cross-key or non-Put entries are
ignored, not errors. Session chains (`k1=a, k2=b, k1=c`, each pinning the
previous) compress to one write-run on the wire — the session chain is the map's
analog of a typing run (ENCODING_SPEC.md block kinds).

## Validation

- **In-memory keying is by the key's id** — already a BLAKE3 output, so
  fast hashing is safe (the standing rule: fast hashes only over ids that
  are already hashes). The adversarial-key concern dissolves rather than
  being mitigated: raw attacker-chosen bytes never key a table, and a giant
  key costs its author blob indirection while op size stays bounded.
- An unresolvable key or value artifact (bytes never provided): the DAG
  still converges and the register still functions — identity is the id.
  The API exposes `pending/unavailable` (for keys and values alike), not
  papered over.
- `overwrites` naming a non-Put or a put on another key: ignored by the
  definitional head-set filter — no gate needed, verdicts never depend on
  it.
- No apply-time quarantine is needed for well-formed Puts: any key/value id
  is authorable; conflict is the surfaced symptom. (Payload-kind policy —
  e.g. an app rejecting object links in certain slots — is schema, not
  convergence: the decided Text/List-unification stance, HASHWEB_SPEC.md
  "The schema gate".)
