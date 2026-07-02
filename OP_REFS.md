# Op refs: tips as the only commitment, positions as the only references (exploration)

Status: exploration, 2026-07-01 — **resolved same day; see Resolution below.**
The positional-ref proposal in the body was rejected; the body is kept as the
record of why positions don't work. The landing point is FRAMEWORK.md.

## Resolution: rejected in stages, landed on tips ∪ named

The exploration ran three rounds:

1. **Positional refs (this doc's body) — rejected.** Positions are
   frontier-relative, and concurrent editing makes them non-local: after a
   merge, a position depends on what the branches shared (candidate B), and
   the dedup-free fix (candidate C) hides exponential path-counting in its
   indices. The resolution machinery is unrealistic under real concurrency.
2. **Tips as the *only* commitment (semantic refs resolved against the
   closure, not committed individually) — rejected.** Once semantic refs are
   not themselves commitments, a ref outside the commitment becomes
   representable, and soundness needs a containment check
   `named ⊆ closure(tips)`. Presence-at-apply is delivery-order-dependent
   (divergent: two replicas can reach opposite permanent verdicts on the
   same op); exact containment is an ancestry oracle — too heavy, and the
   expensive case is an honest gesture (the cold anchor).
3. **Landed** (FRAMEWORK.md): `Node { tips, op }` with
   `refs(u) = tips(u) ∪ named(u)` — the tips field is the frontier pin
   (honest rule: the observed layer frontier, verbatim), the op's fields
   carry the ids it acts on, and both have the same status: **naming is
   committing**, so an escape-ref is unrepresentable and the containment
   problem does not exist. Buffering waits on `refs(u)`; a cold anchor costs
   one 32-byte id, resolved O(1). Indexing (per-op dictionary, op fields as
   indices) lives at the encoding level only. Operationally identical to the
   existing implementation up to the stored shape (`extra_dependencies =
   tips − named` today; tips verbatim in the spec) — align at substrate
   extraction. *(Addendum 2026-07-02: grammar work showed the tips/named
   partition is consumed by nothing, so the artifact records one flat refs
   set with roles as body indices — the shape is `Node { refs, op }` after
   all; GRAMMAR_SPEC.md.)*

What survived into the framework: the clean two-commitment separation
(frontier pin vs semantic intent, now two fields instead of one entangled
set), the per-op dictionary encoding (each id of `refs(u)` stored once), and
— as an explicit option, not a requirement — the **commitment vector** (#5):
a layer may pin a foreign frontier to make cross-layer concurrency decidable
(recorded in HASHLIST_SPEC.md's Refs section). What died with positional
refs: semantic range compression (#4), stream-level canonical encoding (#6),
and preimage compactness for huge multi-target ops (#3) — the wire layer
keeps providing these as encoding, which is where they belonged.

---

## The claim

Today every op entangles two roles in one hash set:

```
deps(u) = named(u) ∪ extra_dependencies(u)
        = (semantic references: anchor / targets / overwrites)
        ∪ (frontier padding: tips − named)
```

The proposal separates them completely:

1. **Commitment to document state** — the op carries its observed **tips**,
   full 32-byte hashes, and nothing else. Since ids are content hashes, tips
   are a Merkle root over `deps*(tips)` — the author's entire claimed op set.
2. **Operation semantics** — anchors, targets, and overwrites are **op refs**:
   positions into a canonical enumeration of that committed op set.

```
commitment:   tips(u)                      // hashes; determines S(u) = deps*(tips)
semantics:    OpRef = position in enum(S(u))   // varints; resolve inside S(u)
```

Because tips determine `S(u)` exactly, and `enum(·)` is a deterministic
function of the set, `(tips, i)` resolves to the same op on every replica —
a pure function of the op set, per Law I.

The wire format already believes this: dict headers + positional refs encode
ids by position today. The exploration is whether the *identity layer* — the
hash preimage itself — should adopt it, deleting the translation between
"refs as positions" (wire) and "refs as hashes" (semantics).

## Op shapes, before and after

```rust
// today
struct HashNode { extra_dependencies: BTreeSet<Id>, op: Op }
enum Op { InsertAfter(Id, char), InsertBefore(Id, char), Remove(BTreeSet<Id>) }

// proposed
struct HashNode { tips: BTreeSet<Id>, op: Op }
enum Op { InsertAfter(OpRef, char), InsertBefore(OpRef, char), Remove(OpRefRange) }
```

A move op, which references a *foreign* layer, carries one tip set per layer
it touches — a **commitment vector**:

```rust
struct MoveNode {
    move_tips: BTreeSet<Id>,        // own layer frontier
    container_tips: BTreeSet<Id>,   // observed frontier of the container object
    op: MoveOp { target: OpRef,     // into enum(deps*(container_tips))
                 to: AnchorRef,     //   "
                 overwrites: OpRefSet },  // into enum(deps*(move_tips))
}
```

## What this buys

**1. The framework claim becomes structural.** FRAMEWORK rev 2 had to
carefully say "deps = observed frontier is the honest-author reading, since
named and extra entangle." Under this proposal `deps(u) = tips(u)` *by
construction* for every author, honest or not (under-claiming remains
possible — that asymmetry is inherent and unchanged). The named/extra split,
`tips_minus`, and the "which half is the anchor in" bookkeeping all dissolve.

**2. References cannot escape the commitment.** An op ref is a position in
`S(u)` — it is *syntactically impossible* to reference an op you did not
commit to having seen. Today this holds by the convention `named ⊆ deps`;
here it holds by type.

**3. One reference space instead of three.** Today: `Id` (semantic, 32 B),
positional refs (wire), `NodeIdx` (in-memory). The first two unify. Hash
preimages shrink accordingly: a 1000-target remove carries ~32 KB of target
ids in its preimage today, versus one range ref — smaller ops, faster
hashing, and the dict-header machinery stops being a second implementation
of reference resolution.

**4. Multi-target ops range-compress semantically.** Removes of a typing run
and session-chain overwrites are contiguous in any recency-flavored
enumeration — the range encoding stops being a wire trick and becomes the op
itself ("remove positions 5..25"), which is what the packed remove work was
already reaching for.

**5. It fixes the cross-layer commitment gap.** The review's finding: a move
op commits only to its own layer's frontier plus two named ops, so
move-vs-remove concurrency is undecidable and "remove wins" is forced by
fiat. The commitment vector fixes this — `container_tips` pins the exact
container version the mover observed, so a remove is either inside
`deps*(container_tips)` (seen, superseded knowingly) or genuinely concurrent.
Cost: roughly neutral — today's move carries `target` + `anchor` hashes
(2×32 B); the vector carries 1–2 container tip hashes + varints, and buys a
strictly stronger commitment. Whether the *resolution* rule should then
distinguish seen-remove from concurrent-remove is a new, now-answerable
design question (today's "remove wins regardless" remains the simple sound
default).

**6. A canonical encoding — for the first time.** Today's wire format cannot
be canonical: positional refs are positions in the *encoding stream*, so ref
values depend on the encoder's chosen batch boundaries and section ordering —
the same op set has many valid byte encodings and none is distinguished. Op
refs take that freedom away: positions are defined by the DAG
(`enum(S(u))`), not by the stream, so the encoder has no choices left and
`encode(op set)` becomes a deterministic function. A full document state even
carries its own canonical stream order: first-occurrence order of
`enum(state tips)` (elisions like run-chain tips stay deterministic because
they are recomputable). What canonicality buys:

- **state fingerprints** — `H(encode(state))` is a well-defined document
  checksum; replicas can assert equality, diff, and anti-entropy-sync by
  hash (the benchmark suite's structure checksums graduate from test
  scaffolding to a protocol feature);
- **dedupe and content-addressed chunks** — identical subhistories encode to
  identical bytes, so chunk stores and caches dedupe across replicas;
- **verification without re-encoding** — received bytes *are* (a
  deterministic elision of) the preimages; ids recompute by hashing what
  arrived, not by reconstructing a separate canonical form first;
- **no transport malleability** — "same ops, different bytes" disappears as
  an ambiguity for caching, snapshot signatures, and reproducible exports.

## The crux: the canonical enumeration

Everything above is bought by `enum(S)` — and this is where the design work
lives. Requirements:

- **(R1) determinism**: a pure function of the op set (Law I);
- **(R2) locality**: resolution cost proportional to how *recent* the
  referenced op is, because honest refs cluster at the frontier (run
  continuation → previous op; session chains → previous put; overwrites →
  current heads);
- **(R3) incrementality**: successive frontiers of one editing session share
  almost everything; resolving against each new frontier must not re-walk
  history.

Candidates:

### A. Sort the op set by id — reject

The obvious "canonically ordered op list." Fails R2 and R3 completely:
resolving "the i-th smallest id *within* `S(u)`" requires deciding membership
of `S(u)`, i.e. materializing ancestry — the exact computation the whole
design avoids at runtime. Position also carries no recency, so honest refs
get arbitrary-magnitude indices.

### B. Frontier walk with dedup

`enum(S)` = deterministic traversal backwards from the tips (tips in id
order, then their deps, breadth- or depth-first, id tiebreak, first
occurrence wins). Recency-first, so honest refs are tiny integers, and
resolution is a lazy walk: O(depth of the referenced op). Chain behavior is
perfect: for `tips = {prev}`, `enum` is `[prev] ++ enum(prev's view)` — the
run fast path references index 0 forever.

The problem is R3 at **merges**: first-occurrence dedup makes the
enumeration of a merged frontier a non-local function of both branches
(positions after the merge point depend on what the branches shared), so a
persistent/incremental structure is hard, and recomputing per frontier is
O(|S|) — an adversary who merges repeatedly forces repeated walks.

### C. Frontier walk without dedup ≡ path addressing

Drop dedup: `enum(u) = [u] ++ concat(enum(d) for d in deps(u), id order)`,
duplicates allowed. An op may appear at many positions; all resolve to the
same op, so semantics stay unambiguous. This enumeration is the **tree
unfolding** of the DAG, and a position in it is exactly a **path from the
tips** (which tip, which dep, which dep, …) in flat clothing.

- R3 works: `enum` values are per-op and immutable, built by O(1) persistent
  concatenation (a rope/treap of subtree lengths — the codebase's favorite
  structure), giving O(log)-ish position→op resolution.
- Amplification is exactly right: a deep ref costs the author bytes
  proportional to what it costs the replica to resolve — the attacker pays
  for the walk they force.
- The wart: subtree *lengths* count paths, which grow exponentially with
  merge depth, so deep positions need wide varints (or the equivalent
  explicit path encoding, which is the same bytes by another spelling).
  Near-frontier refs — the honest case — stay 1–2 bytes.

### D. The escape hatch: deep refs by hash

Whatever the scheme, there is one honest case with *no* frontier locality:
the first edit after clicking into the middle of an old document — the
anchor is thousands of ops deep. Forcing a deep walk (B) or a long path (C)
on the most common cold-start gesture is wrong. The fix is graceful
degradation to today's design: **a ref deeper than a small bound is encoded
as a full 32-byte id and added to the commitment set** (it is an ancestor of
the tips, so the closure — and therefore `S(u)` — is unchanged; buffering
and resolution treat it exactly like today's named dep, O(1) via `IdIndex`).

That is: today's `named` mechanism doesn't die — it becomes the *far
pointer*, used exactly when position-locality runs out. Honest ops use it
once per cold anchor; runs, chains, and supersessions never do.

**Leaning: C + D.** C for everything near the frontier (persistent
structure, attacker-pays cost model, trivial chain case), D for cold
anchors. B's flat dense indices are seductive but merge-incrementality looks
fundamental, and C-with-ropes subsumes B's honest-case compactness.

## Validation and amplification

- **Malformed refs** (position out of range, path step into a node with
  fewer deps, far-pointer id not present): all total functions of
  hash-committed inputs and the immutable DAG — **stable**, per FRAMEWORK's
  dividing rule → apply-time gate, permanent quarantine, convergent
  verdicts. One new row in each spec's Validation table, same gate as
  inverted spans and ill-typed children.
- **Far-pointer containment**: a far pointer must be an ancestor of the tips
  to keep "refs cannot escape the commitment." Checking ancestry is not
  O(1). Options: (a) don't check — treat the far pointer as an *additional
  commitment* (it joins the tip set; closure grows; the invariant holds by
  definition); (b) check lazily at read time. Option (a) is cleaner and
  costs nothing: a far pointer that isn't an ancestor is just a redundant
  tip, harmless exactly like today's non-antichain `extra_dependencies`.
  Leaning (a).
- **Resolution cost table** (the audit each spec owes):

| action                               | cost under C+D              | bound                                       |
|--------------------------------------|-----------------------------|---------------------------------------------|
| run continuation / session chain ref | O(1) (position 0/near-0)    | constant                                    |
| honest near-frontier ref             | O(log F) rope lookup        | logarithmic                                 |
| honest cold anchor                   | far pointer, O(1) `IdIndex` | constant                                    |
| adversarial deep position/path       | O(bytes the attacker paid)  | linear in attacker bytes — no amplification |
| adversarial merge spam (against B)   | — avoided by choosing C     | n/a                                         |

## Interaction with the family

- **HashSeq**: run fast path unchanged in cost, cleaner in shape
  (`tips = {prev}`, anchor = position 0 — a structural constant). Removes
  become position ranges; the packed-remove/bitset work becomes the
  semantic encoding rather than a compression layer.
- **HashKv**: `overwrites` are positions (heads are frontier-near by
  definition — you overwrite what you currently see). Session chains
  compress as today, with smaller preimages.
- **Marks / HashList**: the commitment vector (own tips + container/text
  tips) replaces per-ref named deps; downstream-only layering is preserved
  (committing a foreign frontier does not put the op *into* that frontier).
  Move-vs-remove and mark-vs-remove concurrency become decidable (buys #5).
- **HashDoc**: per-object tips already exist; the commitment vector is the
  same idea made explicit. Creation bridges: a child's first op has
  `tips = {creation op}` — unchanged. `Ref(Id)` values keep full hashes
  (they are values, not refs into a frontier).

## What it costs

- **Ops are no longer self-describing.** A remove's targets are
  uninterpretable without the DAG — no filtering, relaying, or debugging by
  inspection. (Today an op names its targets' ids in plaintext.) Tooling
  must resolve through a replica. This is the biggest ergonomic regression;
  worth an explicit decision.
- **The resolution structure is new machinery** — a persistent
  order-statistics rope over the DAG unfolding, per object. It replaces the
  dict-header/positional-ref layer rather than adding to it, but the
  replacement is subtler than what it removes.
- **DAG skeleton retention becomes strictly mandatory.** Resolving positions
  requires the dep structure of everything above the referenced op. Today
  the skeleton is already the non-droppable core (ids + edges), so this is
  a hardening of an existing assumption, not a new one — but GC/erasure
  designs (HASHDOC_SPEC blobs, MOVE spine retention) must never drop edges.
- **All ids change.** The preimage format is different; nothing migrates.
  Fine today (no users), decisive later — this is the kind of change to
  make exactly once, before substrate extraction bakes in the op shape.

## Open problems

1. **Pin the enumeration.** C+D is the leaning; the C rope needs a worked
   design: per-op subtree lengths (bignum or capped-width?), the exact
   dep-ordering rule (id order vs encoding order), and the position/path
   wire format. Decide whether positions or explicit paths are the wire
   spelling (they are information-equivalent; paths avoid bignum lengths).
   The same decision fixes the canonical whole-state stream order
   (first-occurrence of `enum(state tips)`) — specify both together.
2. **The far-pointer depth bound.** Fixed constant, or author's choice
   (encode whichever is smaller)? Leaning: author's choice — the formats
   are distinguishable by tag and both validate; no protocol constant
   needed.
3. **Anchor refs inside `Anchor`** (MARKS.md `Before/After/DocEnd`): the
   char refs inside anchors follow the same scheme; `DocEnd`/origin
   sentinels are unaffected. Confirm glued-anchor validation still reads
   naturally over positions.
4. **Does the commitment vector change any resolution rule?** With
   move-vs-remove concurrency now decidable, "remove wins regardless" is a
   choice rather than a necessity. Revisit HASHLIST_SPEC's cross-op table
   once this lands — the simple absorbing rule is probably still right, but
   it should be argued, not forced.
5. **Spec updates on adoption.** FRAMEWORK's "two halves" section simplifies
   to "one commitment + derived semantics"; every spec's Deps section
   rewrites; HASHSEQ_SPEC's claim to describe `src/hashseq.rs` needs a
   status marker until the code follows.
