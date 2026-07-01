# HashSeq — Ideas, in Diagrams

A **Byzantine-Fault-Tolerant Sequence CRDT** for collaborative text editing in
open networks with an *unbounded, untrusted* set of collaborators.

---

## 1. The one big idea: content-addressed nodes

Every edit is a node. A node's ID is the **BLAKE3 hash of its content + its
dependencies** — *not* a Lamport clock and *not* an actor ID.

```mermaid
flowchart LR
    subgraph Node["HashNode"]
        op["op: InsertAfter(anchor, 'x')"]
        deps["extra_dependencies: { tip₁, tip₂ }"]
    end
    Node -->|"BLAKE3(content ‖ deps)"| Id["Id = 0x9f3a… (32 bytes)"]
```

Why this matters:

| Traditional RGA / vector-clock CRDT | HashSeq |
|---|---|
| Order depends on Lamport timestamp | Order depends on a hash you can't forge |
| Per-collaborator metadata grows forever | **Zero** per-collaborator state |
| Malicious actor can lie about its clock | ID *is* the hash; lying breaks it |
| Need a permissioned actor registry | Anyone can join anonymously |

> Hash comparison **only** breaks ties between *truly concurrent* inserts that
> share the same anchor. Real ordering intent is captured by the operations
> themselves (`InsertAfter` / `InsertBefore`).

---

## 2. The four operations

```mermaid
flowchart TD
    Op["Op"]
    Op --> R["InsertRoot(char)<br/>first char, no anchor"]
    Op --> A["InsertAfter(Id, char)<br/>place after anchor"]
    Op --> B["InsertBefore(Id, char)<br/>place before anchor"]
    Op --> D["Remove(set of Id)<br/>tombstone these nodes"]
```

Each node also carries `extra_dependencies` — the causal context (current
**tips**) at the moment of the edit, so causality is preserved even though
there are no clocks.

---

## 3. Why `InsertBefore` exists — the interleaving problem

Say the document is `a → b` (b was typed after a) and we want to insert `x`
to get **`axb`**.

```mermaid
flowchart LR
    subgraph bad["Using InsertAfter(a, 'x') — AMBIGUOUS"]
        a1["a"] --> b1["b"]
        a1 --> x1["x"]
        note1["x and b are now siblings of a.<br/>Tie broken by hash → could be 'axb' OR 'abx'"]
    end
```

```mermaid
flowchart LR
    subgraph good["Using InsertBefore(b, 'x') — DETERMINISTIC"]
        a2["a"] --> x2["x"] --> b2["b"]
        note2["x is constrained to precede b.<br/>Always yields 'axb'"]
    end
```

**The rule on insert** (`insert_batch`): when typing *between* a left and right
neighbour —

```mermaid
flowchart TD
    start["Insert between left & right"] --> q{"is left causally-before right?"}
    q -->|yes, left → right| ib["InsertBefore(right)<br/>(avoid interleaving)"]
    q -->|no, concurrent| ia["InsertAfter(left)<br/>(hash decides order)"]
```

---

## 4. The causal graph & how it reads out as a sequence

Nodes form a DAG. The visible text is a **depth-first, befores-first**
traversal of that DAG.

```mermaid
flowchart TD
    root["InsertRoot('h')  — roots ordered by hash"]
    root --> e["'e'"] --> l1["'l'"] --> l2["'l'"] --> o["'o'"]

    classDef run fill:#e8f0ff,stroke:#3366cc;
    class root,e,l1,l2,o run;
```

Traversal order for each node:
1. visit all **befores** (InsertBefore children) first,
2. emit the node itself,
3. then visit **afters** (InsertAfter children), sorted by hash.

Depth-first keeps concurrent runs from interleaving; hash-sorting makes the
choice deterministic across all replicas.

---

## 5. Concurrent forks resolve by hash

Two people insert different chars after the same anchor, with no knowledge of
each other:

```mermaid
flowchart TD
    a["anchor 'o'"] --> x["'X'  (hash 0x2a…)"]
    a --> y["'Y'  (hash 0x91…)"]
    a -.->|"afters = { 0x2a…, 0x91… }<br/>sorted ascending → X before Y"| sorted["result: …o X Y"]
```

`afters: IdMap<BTreeSet<Id>>` keeps the children sorted, so every replica picks
the same branch order. Depth-first means we fully drain branch `X` before
starting branch `Y` — no `oXY` ↔ `oYX` flapping, no interleaving.

---

## 6. Runs: sequential typing is compressed

Typing "ello" one key at a time is logically four `InsertAfter` nodes chained
head-to-tail. HashSeq coalesces them into **one Run**.

```mermaid
flowchart LR
    subgraph logical["Logical (4 nodes)"]
        h0["h"] --> e0["e"] --> l0["l"] --> l0b["l"] --> o0["o"]
    end
    subgraph stored["Stored compactly"]
        ir["InsertRoot('h')"]
        rn["Run(insert_after=h, 'ello')<br/>+ cached element IDs"]
    end
    logical -->|coalesce| stored
```

```rust
pub struct Run {
    insert_after: Id,            // anchor of the first char
    first_extra_deps: BTreeSet<Id>,
    run: String,                 // "ello"  (highly compressible)
    elements: Vec<Id>,           // cached IDs → O(1) lookup, no re-hashing
}
```

**Invariant:** a run always starts with an `InsertAfter`; never `InsertRoot`
or `InsertBefore`. An `InsertBefore` landing mid-run **splits** the run in two.

This is what gets HashSeq to >1M ops/sec — the common case (sequential typing)
is just a string append + index bump.

---

## 7. Out-of-order delivery: orphans

Operations can arrive before their dependencies (open network, no ordering
guarantees). Apply is **idempotent** and buffers what it can't place yet.

```mermaid
flowchart TD
    rcv["receive node"] --> contains{"already have it?"}
    contains -->|yes| drop["ignore (idempotent)"]
    contains -->|no| missing{"all deps present?"}
    missing -->|no| orphan["stash in orphaned set"]
    missing -->|yes| apply["apply: update tips, mutate index"]
    apply --> retry["re-try every orphan<br/>(deps may now be satisfied)"]
    retry --> missing
```

---

## 8. Tips = a lightweight vector clock

`tips` is the set of DAG "heads" (nodes nobody has built on yet). It replaces a
per-actor vector clock with a single shared set.

```mermaid
flowchart LR
    n1["n₁"] --> n3["n₃"]
    n2["n₂"] --> n3
    n3 --> n4["n₄ (tip)"]
    n3 --> n5["n₅ (tip)"]
    classDef tip fill:#ffe8cc,stroke:#cc7a00;
    class n4,n5 tip;
```

- On apply: remove the node's deps from `tips`, add the node itself.
- On a new edit: `extra_dependencies = tips − anchor`, capturing "everything I
  had seen" without naming any collaborator.

---

## 9. Merge = decompress and re-apply

There is no bespoke merge algorithm. Merging another replica just **replays its
operations**; the same idempotent/commutative apply rebuilds the structure —
runs and all.

```mermaid
flowchart LR
    other["other HashSeq"] -->|"runs.decompress()"| ops["stream of HashNodes"]
    ops --> applyall["self.apply(each)"]
    applyall["self.apply(each)"] --> conv["converged state"]
    ops --> applyall
```

Because IDs are content hashes and apply is order-independent, this is provably
**commutative, associative, idempotent, and convergent** (verified with
QuickCheck `prop_commutative` / `prop_associative` / …).

---

## 10. The whole picture

```mermaid
flowchart TB
    subgraph edit["Editing API"]
        ins["insert(idx, text)"]
        rem["remove(idx, n)"]
    end

    subgraph ops["Operations → HashNodes (BLAKE3 IDs)"]
        ir["InsertRoot"]
        ia["InsertAfter"]
        ib["InsertBefore"]
        rm["Remove"]
    end

    subgraph state["In-memory state"]
        runs["runs : compressed sequential text"]
        roots["root_nodes : ordered by hash"]
        befores["before_nodes + befores_by_anchor"]
        afters["afters : concurrent forks, hash-sorted"]
        tombs["removed_inserts : tombstones"]
        tips["tips : causal heads"]
        orph["orphaned : buffered ops"]
        idx["index : the linear visible sequence"]
    end

    edit --> ops --> state
    state -->|"depth-first, befores-first traversal"| text["visible text"]
    other["remote replica"] -->|decompress + apply| state
```

---

### TL;DR

1. **Content-addressed IDs** (BLAKE3) → BFT, no clocks, no actor metadata.
2. **InsertBefore/InsertAfter** capture ordering intent; **hash** only breaks
   ties among truly-concurrent inserts.
3. **Runs** compress sequential typing for speed and compact storage.
4. **Tips** are a clock-free causal frontier; **orphans** handle async delivery.
5. **Merge is just replay** → commutative, associative, idempotent, convergent.
