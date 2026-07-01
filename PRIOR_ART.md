# Prior art

Status: literature survey, 2026-06-28. Companion to HASHDOC.md, MARKS.md, and
MOVE.md. Situates hashseq — a Byzantine-fault-tolerant CRDT family with
self-certifying content-hash op ids (BLAKE3), an explicit causal hash-DAG, no
trusted actors, and the design law that *state is a pure function of the op
set* with all order-sensitive arbitration pushed to read time — against the
work it builds on and the work it deliberately departs from.

Each entry gives the canonical citation and a line on what it **contributes to**
or **contrasts with** hashseq's design. Citations were gathered by a fan-out
search + adversarial-verification pass; every seed paper was located at a
primary source and no claim was refuted, but a handful of attribution
corrections (flagged ⚠️) are baked in below. Items marked *(search-only)* were
surfaced but not put through triple-vote verification.

The one-line placement: **OpSets data model + YATA/RGA sequence substrate +
Kleppmann's hash-DAG BFT identity model, with a novel quantitative
locality/amplification threat model and freeze-don't-flip read-time arbitration
as the response to it.** The closest published articulation of the *substrate*
is Merkle-CRDTs (§3); the closest articulation of the *identity/BFT model* is
Kleppmann's "Making CRDTs Byzantine Fault Tolerant" (§1). Neither develops the
per-op amplification budget that drives hashseq.

---

## 1. Byzantine / secure / access-control CRDTs and hash-DAG op ids

- **Kleppmann, M. "Making CRDTs Byzantine Fault Tolerant." PaPoC 2022.**
  DOI [10.1145/3517209.3524042](https://doi.org/10.1145/3517209.3524042) ·
  [PDF](https://martin.kleppmann.com/papers/bft-crdt-papoc22.pdf)
  → **The direct ancestor of hashseq's identity model.** §3.1 constructs a hash
  graph: ops identified by `H(u)` (a cryptographic hash "such as SHA-256 or
  SHA-3"), each carrying predecessor hashes, forming a DAG that "resembles a Git
  commit history." Tolerates any number of Byzantine nodes (Sybil-immune),
  guaranteeing strong eventual consistency. hashseq is this construction with
  BLAKE3 and a worked-out projection layer. *Caveat: the guarantee is
  convergence only — not garbage-op prevention or invariant enforcement, which
  is exactly the gap hashseq's locality/amplification model addresses.*

- **Kleppmann, M. & Howard, H. "Byzantine Eventual Consistency and the
  Fundamental Limits of Peer-to-Peer Databases." 2020.**
  [arXiv:2012.00472](https://arxiv.org/abs/2012.00472)
  → **The formal frame for what hashseq can and cannot promise.** Defines
  Byzantine Eventual Consistency, gives a Byzantine-causal-broadcast algorithm,
  and — the load-bearing limit — shows that enforcing invariants *beyond* SEC
  generally requires consensus. hashseq lives strictly inside the
  consensus-free SEC envelope by design; this paper is the boundary it hugs.

- **Jacob, F. & Hartenstein, H. "On Extend-Only Directed Posets and Derived
  Byzantine-Tolerant Replicated Data Types." PaPoC 2023.**
  DOI [10.1145/3578358.3591333](https://doi.org/10.1145/3578358.3591333) ·
  ext. ver. [arXiv:2304.04318](https://arxiv.org/abs/2304.04318)
  → **The generalization hashseq is an instance of.** EDPs unify DAG-based
  Byzantine-tolerant CRDTs (cites Kleppmann 2022 heavily): ops = (payload,
  hashed maximal-lower-bounds), recursive hash chains protecting the full
  formation history, with Byzantine behavior reduced to "multiple valid
  updates." Derives a key-value map and sketches an EDP-based access-control
  CRDT. *Contrast: proves qualitative consistency preservation, not a
  quantitative per-op cost bound.* **(Not in the original seed list — newly
  surfaced.)**

- **Keyhive.** Ink & Switch, Zelenka et al., 2024–2025.
  [inkandswitch.com/keyhive](https://www.inkandswitch.com/keyhive/notebook/)
  → **The access-control layer hashseq currently punts on, done coordination-
  free.** Capability-based, server-less access control for Automerge/local-
  first: "convergent capabilities" embedding CRDT state, documents identified by
  public key delegating to other public keys (self-certifying), a
  Group-Management CRDT with coordination-free revocation, and causal key
  management for E2EE. Explicitly avoids BFT/consensus. The natural reference if
  hashseq grows permissions. **(Newly surfaced.)**

- **Renaux, T., Van den Vonder, S., De Meuter, W. "Secure RDTs: Enforcing
  Access Control Policies for Offline Available JSON Data." OOPSLA 2023.**
  ⚠️ correct DOI [10.1145/3622802](https://doi.org/10.1145/3622802) *(the
  10.1145/3622846 I cited earlier is a different paper — memory management)*
  → **The trusted-server foil.** RBAC over JSON fields enforced by a *trusted
  application server* — the precise antithesis of hashseq's no-trusted-actor
  stance. Useful as the "what we are NOT" citation. *(Author list verified to a
  primary source but worth a final manual check.)* **(Newly surfaced.)**

---

## 2. Op-set data model (state = function of the op set)

- **Gomes, V., Kleppmann, M., Mulligan, D., Beresford, A. "Verifying Strong
  Eventual Consistency in Distributed Systems." OOPSLA 2017.**
  DOI [10.1145/3133933](https://doi.org/10.1145/3133933) ·
  [arXiv:1707.01747](https://arxiv.org/abs/1707.01747)
  → **The formal grounding of "state is a deterministic function of the op
  set."** Isabelle/HOL-machine-checked abstract convergence theorem, with the
  first mechanized proofs for RGA, OR-Set, and counter. (Assumes non-Byzantine
  delivery — hashseq supplies the Byzantine layer underneath.) OOPSLA 2017
  Distinguished Paper + Artifact.

- **Kleppmann, M., Gomes, V., Mulligan, D., Beresford, A. "OpSets: Sequential
  Specifications for Replicated Datatypes." ⚠️ OOPSLA 2018** *(not 2017)*.
  [arXiv:1805.04263](https://arxiv.org/abs/1805.04263) ·
  [AFP entry](https://www.isa-afp.org/entries/OpSets.html)
  → **The data-model blueprint.** Specifies maps, sets, lists, text, and trees
  as composable functions of an op set — including an *atomic tree move* "thought
  impossible without locking" — and flags the text-interleaving property later
  formalized by Fugue. hashseq's HashKv/HashDoc/marks/move are all OpSet-shaped
  projections; the contribution is self-certifying hash ids in place of
  `(counter, actor)` and the read-time freeze rule for contested registers.

- **Automerge.** ["Introducing Automerge 2.0"](https://automerge.org/) (2023);
  Da, L. ["What's behind Automerge?"](https://www.liangrunda.com/)
  → **The non-BFT sibling of hashseq's data model.** Same JSON-CRDT shape; the
  `pred`/`succ` supersession sets are exactly hashseq's `overwrites`.
  ⚠️ **Correction to my earlier claim:** Automerge op ids are Lamport clocks
  `counter@actorId`, **not** content hashes, and concurrent conflicts use
  largest-opID (Lamport) LWW — the forgeable, order-sensitive tie-break hashseq
  rejects. (Automerge does use SHA-256 for change/commit-DAG entries, not for
  per-op ids.)

- **Da, L. & Kleppmann, M. "Extending JSON CRDTs with Move Operations." PaPoC
  2024.** [arXiv:2311.14007](https://arxiv.org/abs/2311.14007)
  → **The closest published cousin of MOVE.md — and its instructive opposite.**
  Predecessor/successor supersession; concurrent moves resolved by
  largest-id-wins with cycle rejection, ops reapplied in ascending-id order.
  That is read-time arbitration *over a total order decided by op id* — exactly
  the grindable, order-sensitive resolution MOVE.md's freeze-don't-flip rule
  exists to avoid. The sharpest single contrast for the move design.

---

## 3. Deployed hash-DAG systems

- **Merkle-CRDTs — Sanjuán, H., Pöyhtäri, S., Teixeira, P., Psaras, Y.
  "Merkle-CRDTs: Merkle-DAGs meet CRDTs." Protocol Labs / IPFS, 2020.**
  [arXiv:2004.00107](https://arxiv.org/abs/2004.00107)
  → **The nearest published articulation of hashseq's *substrate*.** CRDT ops as
  nodes in a content-addressed Merkle-DAG, with the DAG itself acting as a
  logical clock. The key difference is the threat model: Merkle-CRDTs develop
  *no* explicit Byzantine/amplification analysis — the precise gap hashseq
  fills. **(Newly surfaced; the most important "we are not first to the
  substrate" citation.)**

- **Matrix State Resolution v2.**
  [spec.matrix.org rooms/v2](https://spec.matrix.org/latest/rooms/v2/) (MSC1442)
  → **The deployed adversarial hash-DAG to compare arbitration strategies
  against.** Events reference parent event-id hashes (a causal DAG); state
  events are treated as unordered sets; convergence under Byzantine actors with
  no consensus and no finality. But it resolves via power-level precedence with
  an `origin_server_ts` (forgeable-timestamp) tie-break — the order-sensitive
  arbitration hashseq's freeze rule is designed to avoid.

- **Secure Scuttlebutt — Tarr, D., Lavoie, E., Meyer, A., Tschudin, C. "Secure
  Scuttlebutt: An Identity-Centric Protocol for Subjective and Decentralized
  Applications." ICN '19.**
  DOI [10.1145/3357150.3357396](https://doi.org/10.1145/3357150.3357396)
  → **Self-certifying identities, opposite merge posture.** ed25519 identities,
  per-author append-only hash-backlinked logs. Forks are *fatal* (a log with two
  incoming backlinks freezes) — the inverse of hashseq's merge-as-union, where
  forks are first-class concurrent state.

- **Pijul / patch theory — Mimram, S. & Di Giusto, C. "A Categorical Theory of
  Patches." 2013.** [arXiv:1311.3903](https://arxiv.org/abs/1311.3903) ·
  [pijul.org/manual/theory](https://pijul.org/manual/theory.html)
  → **A repository-as-CRDT with content-hash-identified vertices.** Independent
  patches commute; vertices identified by the content hash of the introducing
  change plus position; conflicts represented structurally rather than resolved.
  Closest VCS analog to hashseq's "merge is union, conflicts are surfaced."

- **IPFS / IPLD — Benet, J. "IPFS: Content Addressed, Versioned, P2P File
  System." 2014.** [arXiv:1407.3561](https://arxiv.org/abs/1407.3561)
  *(search-only)* → Generalized Merkle-DAG and self-certifying namespace; the
  addressable-DAG ancestor, with no CRDT merge or read-time arbitration.

- **Dat / Hypercore — Ogden, M. et al. "Dat: Distributed Dataset Synchronization
  and Versioning." 2017;**
  [DEP-0002 Hypercore](https://datprotocol.com/deps/0002-hypercore/)
  *(search-only)* → Signed append-only log over a Merkle tree of **BLAKE2b**
  block hashes — a BLAKE-family content-addressing precedent for hashseq's
  BLAKE3. Single-writer/linear, not a causal merge DAG.

- **Git** → the hash-DAG idea both Kleppmann (2022) and Baird (2016) cite as
  prior art ("resembles a Git commit history"). Content-addressed, but merge is
  manual 3-way, not convergent.

---

## 4. Sequence CRDT substrate

- **Roh, H.-G., Jeon, M., Kim, J.-S., Lee, J. "Replicated Abstract Data Types:
  Building Blocks for Collaborative Applications" (RGA). JPDC 71(3):354–368,
  2011.** DOI [10.1016/j.jpdc.2010.12.006](https://doi.org/10.1016/j.jpdc.2010.12.006)
  → **The tombstoning sequence-CRDT lineage hashseq inherits.** Canonical RGA;
  insert/delete/update with commutativity + precedence transitivity. The
  baseline whose interleaving anomalies Fugue later fixes.

- **YATA — Nicolaescu, P., Jahns, K., Derntl, M., Klamma, R. "Near Real-Time
  Peer-to-Peer Shared Editing on Extensible Data Types." GROUP 2016.**
  DOI [10.1145/2957276.2957310](https://doi.org/10.1145/2957276.2957310)
  → **The structural twin of hashseq's insert tree (via Yjs).** left/right
  origin pointers — the same before/after-children anchoring hashseq uses. But
  YATA breaks positional ties by comparing *client ids* (higher after lower) —
  the forgeable-identity ordering signal hashseq's BFT model replaces with
  content-hash sibling order under the locality invariant.

- **Fugue — Weidner, M. & Kleppmann, M. "The Art of the Fugue: Minimizing
  Interleaving in Collaborative Text Editing." 2023; IEEE TPDS 36(11):2425–2437,
  2025.** [arXiv:2305.00583](https://arxiv.org/abs/2305.00583)
  → **The interleaving-correctness reference for the anchor design.** Defines
  "maximal non-interleaving," proves FugueMax achieves it (and that interleaving
  can't be fully eliminated with >2 concurrent sites). The yardstick for
  hashseq's anchor stability and the run/sibling-order behavior MARKS.md and
  MOVE.md lean on.

- **Logoot / LSEQ / Treedoc / WOOT** *(mentioned only)* → the position-identifier
  sequence-CRDT family; surfaced as substrate-agnostic alternatives but not
  separately verified here.

---

## 5. Rich text and move

- **Litt, G., Lim, S., Kleppmann, M., van Hardenberg, P. "Peritext: A CRDT for
  Collaborative Rich Text Editing." CSCW 2022.**
  DOI [10.1145/3555644](https://doi.org/10.1145/3555644)
  → **The direct basis for MARKS.md.** Append-only formatting spans anchored to
  stable character op ids; visible formatting is a deterministic,
  arrival-order-independent function of the span set. ⚠️ But Peritext op ids are
  Lamport `counter@nodeId` (not hashes) and direct format conflicts use LWW —
  MARKS.md keeps the anchoring and span model while replacing both with
  hash-committed ids and causal supersession (`overwrites`), and freezes
  semantics-bearing conflicts (e.g. link URLs) instead of LWW-flattening them.

- **Kleppmann, M., Mulligan, D., Gomes, V., Beresford, A. "A Highly-Available
  Move Operation for Replicated Trees." IEEE TPDS 33(7), 2022.**
  DOI [10.1109/TPDS.2021.3118603](https://doi.org/10.1109/TPDS.2021.3118603)
  → **The move design MOVE.md explicitly rejects importing.** Convergent
  concurrent subtree moves with no cycles — but via undo-do-redo that keeps the
  op log in timestamp order and resolves by LWW over a global total order. This
  is precisely the order-dependence MOVE.md identifies as an amplification
  machine under BFT; the whole document is the order-free counter-design.

- **Loro / crdt-richtext.** [loro.dev/blog/crdt-richtext](https://loro.dev/blog/crdt-richtext)
  (2023) → **The same Peritext + Fugue + movable-list/tree layering hashseq
  targets**, in a shipping Rust library — but with actor-assigned op ids and no
  adversarial threat model. The "industry state of the art, non-BFT" reference.

- **Fractional indexing (Figma-style order keys)** *(mentioned only)* → the
  industry-default reorder mechanism MOVE.md contrasts against: forgeable LWW
  writes to a shared keyspace with unbounded key-growth under adversarial gap
  splitting.

---

## 6. Self-certifying naming and fork detection

- **Mazières, D., Kaminsky, M., Kaashoek, M.F., Witchel, E. "Separating Key
  Management from File System Security." SOSP '99.**
  DOI [10.1145/319151.319160](https://doi.org/10.1145/319151.319160)
  → **The origin of "self-certifying."** Self-certifying pathnames embed a
  collision-resistant hash of (server location, public key), so the name *is*
  the integrity proof — the direct ancestor of hashseq's `id = BLAKE3(op)`. No
  trusted CA; bounded-damage threat model.

- **Li, J., Krohn, M., Mazières, D., Shasha, D. "Secure Untrusted Data
  Repository (SUNDR)." OSDI 2004.**
  DOI [10.5555/1251254.1251263](https://dl.acm.org/doi/10.5555/1251254.1251263)
  → **The canonical treatment of fork consistency** — clients detect any
  integrity failure as long as they observe each other's writes; equivocation
  becomes a permanent, attributable fork. The exact posture hashseq takes toward
  dominating ops: you can't prevent equivocation, you make it detectable and
  attributable. Write authority vested entirely in user public keys.

- **Wilcox-O'Hearn, Z. & Warner, B. "Tahoe – The Least-Authority Filesystem."
  ACM StorageSS '08.** [ePrint 2012/524](https://eprint.iacr.org/2012/524)
  → **Capabilities-as-keys with Merkle integrity and grinding discipline.**
  Verify-caps are one-to-one with contents via a Merkle tree; mutable files use
  `VC = H(VK)`; SHA256d with domain-separation tags. The
  collision/grinding-resistance hygiene is directly relevant to hashseq's
  content-hash op ids and the "grinding buys nothing" invariant.

---

## 7. Hash-DAG-of-events for BFT consensus (deliberate contrast)

hashseq uses a hash-DAG of events but **refuses to manufacture a total order**.
These build the same substrate to the opposite end:

- **Baird, L. "The Swirlds Hashgraph Consensus Algorithm." Swirlds Tech Report
  SWIRLDS-TR-2016-01, 2016.**
  [PDF](https://www.swirlds.com/downloads/SWIRLDS-TR-2016-01.pdf)
  → Each event hashes exactly two parents → a gossip hash-DAG; virtual voting
  derives a *fair total order* with asynchronous BFT (<1/3 attackers). Same
  substrate, opposite philosophy; also cites Git as hash-DAG prior art.

- **Danezis, G., Kokoris-Kogias, L., Sonnino, A., Spiegelman, A. "Narwhal and
  Tusk: A DAG-based Mempool and Efficient BFT Consensus." EuroSys 2022.**
  [arXiv:2105.11827](https://arxiv.org/abs/2105.11827)
  → Separates certified-DAG causal dissemination from ordering. hashseq adopts
  the DAG-dissemination half and discards the consensus/ordering half. **Bullshark**
  (Giridharan et al., 2022) is the variant over the same Narwhal DAG.

---

## Adjacent 2023–2026 work worth tracking

The survey surfaced a cluster of recent work directly in hashseq's neighborhood,
none of it in the original seed list:

| Work | Citation | Relevance to hashseq |
|---|---|---|
| **EDP / Byzantine-tolerant RDTs** | Jacob & Hartenstein, PaPoC 2023, [arXiv:2304.04318](https://arxiv.org/abs/2304.04318) | Generalizes DAG-based BFT CRDTs incl. Kleppmann's hash-DAG; hashseq is an instance. |
| **Keyhive** | Ink & Switch, 2024–2025 | Coordination-free, capability-based access control for local-first — the layer hashseq punts on. |
| **ERA: Epoch-Resolved Arbitration for Duelling Admins** | Dougal (Element), 2026, [arXiv:2601.22963](https://arxiv.org/abs/2601.22963) | Argues Byzantine admins exploiting concurrency need an *external arbiter* (bounded total order via epochs) — a direct counterpoint to pure read-time arbitration. Worth engaging head-on. |
| **Towards System-Oriented Formal Verification of Local-First Access Control** | Jacob, Stuber, Hartenstein (KIT), 2026, [arXiv:2604.23560](https://arxiv.org/abs/2604.23560) | Capabilities + "hash chronicles" (hash-linked DAGs); Verus/Z3-verified; names Matrix & Keyhive as canonical BFT local-first systems. |
| **The Blocklace: A Byzantine-repelling and Universal CRDT** | Lewis-Pye, Naor, Shapiro, [arXiv:2402.08068](https://arxiv.org/abs/2402.08068) | Argues equivocation must be *actively excluded*; qualifies the "Byzantine ⇒ just multiple valid updates" framing — relevant to bounding valid-update spam. |
| **Process-Commutative Distributed Objects** | 2023, [arXiv:2311.13936](https://arxiv.org/abs/2311.13936) *(search-only)* | When replicated objects can be made BFT via commutativity — directly relevant to "all order-sensitivity pushed to read time." |
| **p2panda convergent access control** | p2panda blog, 2025 | Offline-first convergent access control without a trusted coordinator. |

**Open thread for hashseq's positioning:** no surveyed work develops a
*quantitative per-op amplification budget* as the explicit design driver. EDP,
Merkle-CRDTs, and Kleppmann 2022 establish qualitative Byzantine convergence;
the Blocklace and ERA argue about bounding/excluding equivocation; none frames
"one adversarial op costs honest replicas O(?) work" as the invariant to
defend. That framing — the locality invariant and the MOVE.md amplification
audit — looks like hashseq's genuinely novel contribution.

---

## Citation corrections folded in (from my earlier informal list)

1. **OpSets** is **OOPSLA 2018**, not 2017. (The 2017 OOPSLA paper is the
   separate "Verifying Strong Eventual Consistency," DOI 10.1145/3133933.)
2. **Automerge op ids are Lamport `counter@actorId`, not content hashes** — do
   not describe Automerge as content-addressed at the op level.
3. **Secure RDTs** correct DOI is **10.1145/3622802** (the 10.1145/3622846 I
   gave is an unrelated memory-management paper).

All seed citations were located at a primary source; none were fabricated.
Items left as *(search-only)* / *(mentioned only)* above did not get full
triple-vote verification and are worth a manual confirm before formal citation.
