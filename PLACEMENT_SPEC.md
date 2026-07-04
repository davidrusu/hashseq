# Place op spec: cross-container containment

Status: 2026-07-04, draft for review. Framework: FRAMEWORK.md (Law I/II;
resource → conflict → resolution; locality dividing line; stability
requirement). Design rationale: MOVE.md (same-container move; the
"Reparenting" section this spec supersedes), CYCLE_REVERT.md (the cycle
analysis whose D4 leaning this spec adopts). Motivating incident:
APP_NOTES.md #29 — concurrent structural edits produced duplicate
containment links in a live document, the exact residue MOVE.md's
remove+insert story accepted.

`Place` is the fourth instance of the supersession pattern (`Remove`
targets, `Put.overwrites`, `Mark.overwrites`, `Move.overwrites`): one
**containment register per object**, living in the object's own op DAG,
deciding which link atom — anywhere in the store — places it. Containers
keep deciding *order*; the placed object itself decides *membership*.
Duplication under concurrent relocation becomes unrepresentable rather
than heal-able.

## Op

```rust
// valid in ANY object's DAG (Seq or Kv) — one new tag in the shared
// node grammar, not a per-family op
Place {
    placed_at:  Id,             // value field (commitment, never a ref):
                                //   an Insert op id in some container
                                //   (the link atom that places this object),
                                //   or TOMBSTONE (detach/delete)
    overwrites: BTreeSet<Id>,   // the placement heads this op saw and
                                //   replaces — refs, in THIS object's DAG
}
```

`placed_at` is a **value commitment in a foreign DAG** — the standing
payload discipline (HASHSEQ_SPEC.md "Payload"): it is not in `refs(u)`,
buffering never waits on it, and nothing about the referent is gated. The
supersession DAG (`overwrites`) stays entirely within the placed object's
own history: verification, frontiers, sync, and parking remain strictly
per-object. There is deliberately **no `parent` field**: the parent is
`home(placed_at)` — the object whose DAG contains the named insert —
derivable at read time via the store's id interning. An explicit copy
would be redundancy in canonical bytes: a second copy of a fact and a new
mismatch class (GRAMMAR_SPEC.md meta-rules), spent on nothing.

A cross-container relocation is therefore **two ops, two objects, one
gesture**:

1. `Insert { at, payload: X.origin }` in the destination container —
   claims the *order* slot (an ordinary link atom; its op id is the
   element id `a₂`);
2. `Place { placed_at: a₂, overwrites: heads }` in X — claims
   *membership*.

Neither op needs the other for its own object's convergence; the coupling
is interpretive (the read rules below), never causal. The source
container's atom is **not removed by a move** — it goes dead by the
membership rule and remains as a ghost (Retention, below).

## Refs

```
named(u) = overwrites
refs(u)  = named(u) ∪ frontier pins     // placed_at is NOT a ref
```

Honest construction per the frontier rule; the first `Place` of an object
refs at least its origin. Whether `Place` ops enter the object's own tips
or keep a separate placement frontier is the LAYERING.md granularity
parameter, exactly as for `Move` (HASHSEQ_SPEC.md open thread 2); v1:
ordinary ops, object's own tips.

## Containment links vs references (the typing rule)

The membership rule below applies to atoms whose payload is the placed
object's **origin** — the instantiation capability. Atoms carrying an
**object id** are references (links/transclusion embeds): unlimited in
number, governed by the render guard (embed once per root-to-leaf path,
degrade to navigation link — CYCLE_REVERT.md finding 1), and invisible to
placement registers. The origin/object-id asymmetry (APP_NOTES.md #1) is
the type tag; no new vocabulary is introduced. One object has at most one
containment placement, and any number of references — which is the
transclusion answer MOVE.md open problem 4 asked for.

## Resource / Conflict / Resolution

- **Resource**: the containment register of object X — "which link atom
  places X" — claimed by `Place` ops in X's DAG.

```
heads(X) = { p ∈ Places(X) : ∄ q ∈ Places(X). p.id ∈ overwrites(q) }
```

- **Root of the register** (the implicit placement every op transitively
  overwrites): the atom whose id equals X's **origin**, if X was born by
  the composition convention (origin = the creating link's insert op id —
  the weld); otherwise **unplaced**. Objects with an empty register render
  by the legacy rule (below).

### Membership (the read rule)

A link atom `a` in container P whose payload is X's origin is **live**
iff:

```
── Places(X) = ∅        : a is live by presence            (legacy rule)
── |heads(X)| = 1       : a is live iff heads(X).placed_at = a.id
── |heads(X)| > 1       : FREEZE — X renders at the last-agreed
                          placement (below); the named atoms of the
                          contending heads are all dead; conflict
                          surfaced
── winning value =
   TOMBSTONE            : X is unplaced (detached by intent — deletion
                          semantics if nothing re-places it)
```

plus, in every case, `a` itself must be live in P (not tombstoned).
**Remove-wins absorption**, inherited from the seq matrix: a tombstoned
atom confers no membership even when the register names it — dead is
absorbing, and legacy deletion (tombstone the link) keeps meaning
deletion.

The **legacy rule** covers every object written before this spec and
every object never moved: presence decides, with the deterministic
duplicate-heal (first occurrence in a canonical walk wins) as the
transitional repair. An object's first `Place` op upgrades it to register
membership permanently.

### Freeze — last-agreed, exactly as `Move`

`|heads| > 1` renders X at the **last agreed placement**: recurse on the
maximal ops every head transitively overwrites, bottoming at the
register's root. The walk **skips** entries whose named atom is
tombstoned, or whose home object this replica does not hold, or whose
value is TOMBSTONE with further history below — landing at the first
placement that renders; if none does, X renders **detached** (flagged).
Freeze, never max-id: placement of honest content is never decided by
anything grindable or fabricatable (MOVE.md "Contested registers freeze");
the next `Place` naming both heads dominates and resolves. Skipping on
tombstones is an op-set function; skipping on missing objects is ordinary
eventual consistency (op-set membership, never artifact availability —
`placed_at` ids are raw 32-byte values, no artifact is ever dereferenced
to render placement).

### Cycles — D4, detach the SCC

Containment edges `parent(X) = home(winning placed_at)` form the p₀
graph. Cycles need no conflicts (two clean single-head registers can
jointly cycle — CYCLE_REVERT.md). Per that analysis's recorded leaning,
now adopted: compute SCCs of the p₀ graph **once**; every member of a
cyclic SCC renders **detached** — surfaced as a root-level flagged
cluster (internal edges displayable: "these contend in a loop") — and
everything else renders at p₀ unconditionally. Detaching only removes
edges, so no iteration, no schedule, confluence structural. Any member's
next `Place` re-places it. The naive revert-downward rule remains
rejected (non-confluent — the counterexample stands).

An SCC of one — `placed_at` resolving inside X's own subtree — needs no
gate: it is just the smallest cycle, detached and flagged like any other.

## Apply

O(1), no replay: `heads(X) = heads(X) − overwrites(u) ∪ {u}`. There is no
per-object index to relocate — containment is a store-level read-time
projection (the container's own rendered order is untouched by
membership). Late or adversarial delivery costs the same as on-time
apply.

## Validation (edge table deltas — HASHWEB_SPEC.md)

| op . role | admits | otherwise |
|---|---|---|
| `Place . placed_at` | any id | never edge-checked: a value commitment, payload class. A `placed_at` that names a non-insert, an atom whose payload is not this object's origin, or garbage simply never matches any atom during a container walk — inert by the membership rule, no verdict needed |
| `Place . overwrites` | — | never gated: entries that are not `Place` ops in the same object are ignored by the definitional head-set filter (same class as `Put . overwrites`) |
| op kind vs object kind | `Place` admitted in **both** `Seq` and `Kv` (the register concerns the object's placement, not its content projection) | — |

No new gate rows: every malformed relationship is inert at read time
rather than quarantined, because nothing about `placed_at` is verifiable
at apply time without foreign state — and the gate must never depend on
what a replica happens to hold.

**Extension path.** `Place` is a new tag in the shared node grammar
(GRAMMAR_SPEC.md "Op kinds", tag 5). Replicas that predate it carry the
nodes opaquely (envelope semantics — unknown kinds are not malformed) and
park ops that reference them in roles; they render by the legacy rule
until upgraded, which degrades to today's behavior (presence membership,
duplicates healed deterministically) and corrupts nothing.

## Retention and GC

- **The register spine is retained**: superseded `Place` ops' ids,
  `overwrites` edges, and each op's `placed_at` value — the last-agreed
  walk reads them (mirror of HASHSEQ_SPEC.md Move retention).
- **Ghost atoms**: a link atom superseded in a register's history SHOULD
  be retained while unreferenced by any head (the freeze fallback may
  land on it); it MAY be tombstoned (hygiene) — the walk's skip rule
  degrades deterministically, at the cost of falling further down the
  chain (the fork-pinning residual MOVE.md already accepts, one step
  worse). Apps that never tombstone superseded links get exact freeze
  behavior; apps that GC aggressively trade fallback fidelity for space.
- Atoms never named by any register history are droppable per ordinary
  deletion semantics.

## Amplification audit (deltas over MOVE.md's table)

| adversarial action | honest cost | bound |
|---|---|---|
| fork-spam on one object's containment register | head-set growth; object pins at last-agreed, flagged | linear in attacker ops; a fresh destination is never attacker-chosen (inherited) |
| cycle bomb (K registers forming loops) | SCC recompute over the affected component; members detach, flagged; no placement conferred anywhere | O(component) per op; **the component is attacker-growable**, so per-op honest cost is linear in the attacker's own prior spend — same shape CYCLE_REVERT.md accepted for D1's iteration depth; incremental SCC maintenance is the open engineering item |
| placement of honest content to garbage | one dominating `Place` — relocation | the permissionless-write baseline; attributable, revertible |
| `placed_at` garbage / mismatch spam | none — inert at read, no gate verdicts to grind | membership simply never matches |
| stale-replica games | old replicas render legacy membership (duplicates, healed) | park-until-upgrade class; no divergence in op state |

## Open threads

1. **Incremental SCC maintenance** under single-edge p₀ changes, and the
   reverse index (atom id → home object) as a required substrate cache —
   both pure Law II obligations (cache pinned to the definitional
   recompute).
2. **Property harness** per CYCLE_REVERT.md acceptance criteria:
   definitional recompute vs incremental maintenance over randomized op
   sets *and* delivery orders; generators biased toward contended
   registers, chained supersessions, cycle bombs, and register/atom
   arrival races.
3. **Placement frontier** — shared LAYERING.md parameter with `Move`
   (HASHSEQ_SPEC.md open thread 2).
4. **Orphan-cluster UI semantics** — detached SCCs and frozen conflicts
   need product surface (flagged strip, one-drag resolution); tracked in
   APP_NOTES.md.
5. **Test vectors** for the new tag (GRAMMAR_SPEC.md open item 1) once
   the encoding lands.
