# HashKv op spec

A multi-value-register map. Framework: FRAMEWORK.md (reference set +
honest frontier rule; Law I state = f(op set); Law II arbitration at read
time). The op is the Remove pattern applied to keys: supersede the prior
writes you saw (automerge's `pred`).

## Op

```rust
enum MapOp {
    Put {
        key: Id,                    // value commitment (not a ref)
        value: Id,                  // value commitment (not a ref)
        overwrites: BTreeSet<Id>,   // the per-key heads this put saw and replaces
    },
    // Del ≡ Put { value: TOMBSTONE, .. }
}
struct MapNode { refs: BTreeSet<Id>, op: MapOp }
```

- Node ids: `BLAKE3::derive_key(NODE_CONTEXT, canonical preimage)`, the
  family's single node context; op kinds are tags in the encoding
  (GRAMMAR_SPEC.md).
- `TOMBSTONE = value_id([VK_TOMBSTONE])`: an ordinary derived value id (in
  identity form), never a magic constant.
- A Kv object also carries `Place` ops, its containment register
  (PLACEMENT_SPEC.md). Every other op kind is refused (HASHWEB_SPEC.md
  "Admission").

## Refs

```
named(u) = overwrites(u)
refs(u)  = named(u) ∪ frontier pins      // key and value are NOT refs
```

The pins are the rest of the observed map frontier; a fresh map's
frontier is `{origin}`. `overwrites = ∅` asserts a first write to the key.
Key and value are not in `refs(u)`: buffering never waits on them, and an
unresolvable artifact is the `pending` state, not a delivery condition.

## Keys and values are ids

There is no `Value` enum at the op layer. Key and value are ids; what the
id names is discovered at dereference (HETEROGENEITY.md):

| the id names | meaning |
|---|---|
| a value artifact (string, int, bytes, …) | a scalar. ≤ 15 bytes: the id *is* the bytes (identity form, GRAMMAR_SPEC.md "Identity-form value ids"); larger: a BLAKE3 id resolved from the value store (HASHWEB_SPEC.md "The value side store") |
| `TOMBSTONE` | `Del` |
| an object id (`object_id`, GRAMMAR_SPEC.md) | a link to that object; linking an object that lives elsewhere is transclusion |
| an op node id | a reference to that op |

Keys are equally general. A node-id key makes "a register about op X"
(status, votes, moderation labels) a plain Put. The only total order on
keys is id order (key bytes may be pending); display order over resolved
keys is a render concern.

## Resource and conflict

One register per key id. Its live head set:

```
heads(k) = { p ∈ Puts(k) : ∄ q ∈ Puts(k). p.id ∈ overwrites(q) }
```

A **conflict** is `|heads(k)| > 1`. Only naming a put in `overwrites`
supersedes it; pinning it in `refs` does not (seeing-but-not-superseding
is the withholding fabrication, FRAMEWORK.md). Among honest authors a
conflict means concurrency; a Byzantine author can fabricate one by
omission, so the conflicted path must confer nothing.

## Resolution (read time)

`read(k)` returns the whole head set (MVR):

- `heads = ∅`, or one head whose value is `TOMBSTONE` → **absent**;
- one head → its value;
- more → **conflict**, surfaced to the app. If one value must be shown:
  - cosmetic only (display, attribution) → the max-id head, as a display
    tiebreak, never semantics;
  - anything whose consequences leave the key's own cell (URL, config) →
    do not collapse; render the conflict. Ids are grindable, so a ground
    id must not silently win (FRAMEWORK.md locality dividing line).
- **No LWW.** There is no timestamp input; wall clocks are forgeable.

Code: `HashKv::read_id` → `Read::{Absent, One, Conflict}`; `get` returns
`Some` only for one resolvable head. Open decision (QUEUE.md 22): heads
with identical values (two concurrent `del`s) are still a `Conflict`, and
`keys()` lists the key as live.

## Apply

```
heads(k) = heads(k) − overwrites(u) ∪ {u}        // O(|overwrites|)
```

No ancestor walk. Only `k`'s head list is touched, so `overwrites`
entries naming a put on another key or a non-put are ignored — the same
result as the definitional filter, never an error.

## Validation

- **No apply-time refusal** for a well-formed Put: any key/value id is
  authorable; conflict is the surfaced symptom. Payload-kind policy (e.g.
  no object links in some slot) is app schema, not convergence
  (HASHWEB_SPEC.md "The schema gate").
- **Keying.** Registers are keyed by the key id, which rides the wire raw
  and which an author picks freely (identity-form ids are not hash
  outputs), so the key table uses a seeded hasher, never an unseeded fast
  hash. A large key costs its author an artifact; op size stays bounded.
- **Pending.** A key or value whose bytes never arrive leaves the DAG and
  the register fully functional (identity is the id); the API exposes
  `pending`/unavailable for keys and values alike, never papered over.

Known gaps (QUEUE.md): a kv inside a `HashWeb` mirrors artifact bytes only
when a node is delivered, so bytes arriving after the op leave
`HashKv::get` pending while the store resolves them (57), including a
value minted via `HashWeb::provide_value` then `put_ids` locally (21).

## Encoding

Kv nodes encode one record each (tag, pins, key, value, overwrites) in
Kahn order, smallest id first (`encode_hashkv`). Compressing session
chains (`k1=a, k2=b, k1=c`, each pinning the previous) into write-runs is
a target refinement (ENCODING_SPEC.md), not implemented.
