# Place op spec: cross-container containment

`Place` gives every object one **containment register** in its own op
DAG, deciding which link atom — anywhere in the store — places it.
Containers decide *order*; the placed object decides *membership*.
Concurrent relocation therefore cannot duplicate an object: contention
freezes instead. It is the fourth instance of the supersession pattern
(`Remove`, `Put.overwrites`, `Mark.overwrites`, `Move.overwrites`).
Framework: FRAMEWORK.md. Rationale: MOVE.md (same-container `Move`),
CYCLE_REVERT.md (cycle analysis), APP_NOTES.md #29 (the duplicate-link
incident under remove + insert relocation that motivated it).

**Status.** The register, `Place` op, wire encoding and read surface
(`placement()`, wasm `placeAt` / `placementOf`) are in the crate
(`src/placement.rs`). The membership read rule, freeze walk and D4 cycle
detachment are **not**: the crate exposes the fallback chain, and
`web/kb.js` implements an approximation of the read layer (QUEUE.md 58;
deviations noted below).

## Op

```rust
// valid in ANY object (Seq or Kv): tag 5 of the shared node grammar
// (GRAMMAR_SPEC.md "Op kinds")
Place {
    placed_at:  Id,            // value commitment, never a ref: an Insert op id
                               //   in some container (the link atom placing
                               //   this object), or TOMBSTONE (detach/delete)
    overwrites: BTreeSet<Id>,  // the placement heads this op saw and replaces
                               //   (refs, in THIS object's DAG)
}
```

- `placed_at` is a **value commitment in a foreign DAG** (HASHSEQ_SPEC.md
  "Payload"): not in `refs(u)`, never waited on, never refused, never
  dereferenced. Supersession, verification, frontiers, sync and orphaning
  stay strictly per-object.
- There is no `parent` field: the parent is `home(placed_at)`, the object
  whose DAG holds that insert, derived at read time. A stored copy would
  be a second copy of a fact and a new mismatch class (GRAMMAR_SPEC.md
  meta-rules).

A cross-container move is **two ops in two objects, one gesture**:

1. `Insert { at, payload: X.origin }` in the destination — claims the
   order slot; its op id is the new link atom `a₂`;
2. `Place { placed_at: a₂, overwrites: heads }` in X — claims membership.

Neither op needs the other to converge; the coupling is interpretive.
A move does **not** remove the source atom: it goes dead by the
membership rule and stays as a ghost (Retention).

## Refs

```
named(u) = overwrites
refs(u)  = named(u) ∪ frontier pins      // placed_at is NOT a ref
```

The first `Place` of an object refs at least its origin. `Place` ops are
ordinary ops in the object's own tips (a separate placement frontier is
the LAYERING.md parameter shared with `Move`; open thread 3).

## Containment links vs references (the typing rule)

Membership applies to atoms whose payload is the placed object's
**origin** (the instantiation capability). Atoms carrying an **object
id** are references (links, transclusion embeds): unlimited in number,
invisible to placement registers, and governed only by the render guard
(CYCLE_REVERT.md finding 1). The origin / object-id asymmetry
(APP_NOTES.md #1) is the type tag. An object has at most one containment
placement and any number of references.

## Resource / Conflict

```
heads(X) = { p ∈ Places(X) : ∄ q ∈ Places(X). p.id ∈ overwrites(q) }
```

A conflict is `|heads(X)| > 1`. The **register root** — the implicit
placement every `Place` transitively overwrites — is the atom whose id is
X's origin when X was born by the composition convention (HASHWEB_SPEC.md
"Op: opening and delivery"); otherwise X starts **unplaced**.

## Membership (the read rule)

A link atom `a` in container P whose payload is X's origin is **live**
iff `a` itself is live in P (not tombstoned) and:

```
Places(X) = ∅       : live by presence                               (legacy rule)
|heads(X)| = 1      : live iff heads(X).placed_at = a.id
|heads(X)| > 1      : FREEZE — only the last-agreed atom is live; the
                      contenders' atoms are dead; conflict surfaced
winning value = TOMBSTONE : X is unplaced (detached by intent; deletion
                      if nothing re-places it)
```

- **Remove-wins absorption**: a tombstoned atom confers no membership
  even when the register names it, so tombstoning a link still means
  deletion.
- **Legacy rule**: objects with no `Place` op (older data, never-moved
  objects) are members by presence; duplicates heal deterministically
  (first occurrence in a canonical walk wins). An object's first `Place`
  switches it to register membership permanently.

*kb.js deviations:* for blocks, `winningAtomOf` (web/kb.js:221) falls
back down the chain past a dead head atom to the next present atom
instead of leaving X unplaced, and treats a `TOMBSTONE` anywhere in the
chain as deleted; the `chain()` doc (src/placement.rs:111-116) and wasm
`placementOf` doc describe the same fallback. For pages, `childrenOf`
(web/kb.js:406) checks only `chain[0]`, with no fallback.

## Freeze

`|heads| > 1` renders X at the **last agreed placement**: the maximal ops
every head transitively overwrites, recursing down to the register root.
The walk skips an entry whose atom is tombstoned, whose home object this
replica does not hold, or whose value is `TOMBSTONE` with further history
below, landing on the first placement that renders; if none does, X
renders **detached**, flagged.

- Never max-id: placement of honest content must not be decided by
  anything grindable or fabricatable (MOVE.md "Contested registers
  freeze"). The next `Place` naming both heads resolves.
- Skipping tombstoned atoms is an op-set function; skipping unheld objects
  is ordinary eventual consistency. `placed_at` ids are raw 32-byte
  values; no artifact is ever dereferenced to render placement.

`PlacementRegister::chain()` returns this fallback chain: `placed_at`
values in descending agreement (single head: the head then its history;
conflict: starting below the heads at the last agreed op). It does not
append the register root; an empty chain means unplaced.

## Cycles — D4, detach the SCC

Containment edges `parent(X) = home(p₀(X))`, where `p₀` is X's rendered
placement after the membership and freeze rules, form the p₀ graph.
Cycles need no conflicts (two clean single-head registers can jointly
cycle).

Compute the SCCs of the p₀ graph **once**. Every member of a cyclic SCC
renders **detached**, surfaced as a root-level flagged cluster (its
internal edges may be shown); everything else renders at p₀
unconditionally. Detaching only removes edges, so no iteration and no
schedule: confluence is structural. Any member's next `Place` re-places
it. A self-cycle (`placed_at` inside X's own subtree) is the SCC-of-one
case; no admission rule. Why D4 and not revert-downward:
CYCLE_REVERT.md.

**Not implemented in the crate** (QUEUE.md 58). kb.js approximates it:
its tree walks guard against revisits, and registered pages the walk
never reaches (cycles included) surface in an "unplaced" strip
(web/kb.js:449); no SCCs are computed.

## Apply

`heads(X) = heads(X) − overwrites(u) ∪ {u}`, no replay. Containment is a
read-time projection, so there is no index to relocate and a container's
own order is untouched. Late or adversarial delivery costs the same as
on-time apply.

## Validation

Admission rows live in HASHWEB_SPEC.md "Admission": `Place` is admitted
in both `Seq` and `Kv`; `placed_at` is never checked; `Place.overwrites`
entries that are not `Place` ops of the same object are ignored. A
`placed_at` naming a non-insert, an atom whose payload is not X's origin,
or garbage never matches any atom — inert by the membership rule. Nothing
here is refused, because `placed_at` cannot be checked without foreign
state and admission must never depend on what a replica holds.

Pre-`Place` replicas are specified to carry tag 5 opaquely and render by
the legacy rule (GRAMMAR_SPEC.md); current decoders reject unknown kinds
instead (QUEUE.md 48).

## Retention

- **The register spine is retained**: every `Place` op's id,
  `overwrites` edges and `placed_at` value — the freeze walk reads them.
- **Ghost atoms** superseded in a register's history SHOULD be retained
  (the freeze walk may land on them). They MAY be tombstoned for hygiene:
  the walk then falls further down the chain, deterministically.
- Atoms never named by any register are droppable per ordinary deletion.

## Amplification audit (deltas over MOVE.md's table)

| adversarial action | honest cost | bound |
|---|---|---|
| fork-spam on one register | head-set growth; object frozen at last-agreed, flagged | linear in attacker ops |
| cycle bomb (K registers forming loops) | SCC recompute over the component; members detach, flagged; no placement conferred | O(component) per op; the component is attacker-growable, so per-op cost is linear in the attacker's prior spend |
| placing honest content somewhere bad | one dominating `Place` | the permissionless-write baseline; attributable, revertible |
| `placed_at` garbage / mismatch spam | none | never matches; no verdicts to grind |
| stale replicas | legacy membership (duplicates, healed) | no divergence in op state |

## Open threads

1. **Incremental SCC maintenance** under single-edge p₀ changes, plus the
   reverse index (atom id → home object) as a substrate cache — both Law
   II obligations (cache pinned to the definitional recompute). Candidate:
   an eager per-atom `live`/`home` bit maintained on `Place` apply (flip
   the ≤ 2 affected atoms), a pure, reversible function of the op set — the
   `Move` index-relocation discipline (MOVE.md "Apply cost"). The
   decision may be *cached* eagerly but never *stored* as a tombstone in
   the container: a `Place` is never known uncontended at apply time,
   tombstones are append-only while freeze/detach are reversible, and X
   cannot author ops in P. Also: `chain()` is O(n³–n⁴) (closures
   recomputed inside `maximal`; QUEUE.md 58).
2. **Property harness** per CYCLE_REVERT.md "Acceptance criteria":
   definitional recompute vs incremental maintenance over randomized op
   sets and delivery orders, biased toward contended registers, chained
   supersessions, cycle bombs, and register/atom arrival races.
3. **Placement frontier** — shared LAYERING.md parameter with `Move`
   (HASHSEQ_SPEC.md open thread 2).
4. **Orphan-cluster UI** — detached SCCs and frozen conflicts need product
   surface (flagged strip, one-drag resolution); APP_NOTES.md.
5. **Test vectors** for tag 5 (GRAMMAR_SPEC.md open item 1).
