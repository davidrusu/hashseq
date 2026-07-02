# Heterogeneity: one namespace, committed kinds (exploration)

Status: exploration, 2026-07-01. Companion to FRAMEWORK.md ("one namespace,
committed kinds") and LAYERING.md. Explores what mixed-kind objects buy once
type confusion is structurally impossible — and what specifying that
seriously would require.

## The property

A pointer names a location; interpreting what lives there is the reader's
guess, checked (or not) out of band. Our ids invert this:

| | pointer / `(ctr, actor)` id | content-hash id |
|---|---|---|
| dereference | read the location, *then* interpret | requires already holding the preimage |
| type of referent | reader's assumption or external schema | inside the hash commitment (derive_key context + op tag) |
| wrong-kind access | misinterpretation — a bug or an exploit | **unrepresentable** — there is nothing to misinterpret; you either hold the typed artifact or you hold nothing |

Precisely: `id = H(kind-tagged canonical bytes)`, and dereference is a lookup
in a store of *decoded* nodes. Every successful dereference yields a
self-describing, kind-tagged artifact whose tag the id itself commits to
(collision resistance is the only assumption). A reference to a wrong-kind
node is not memory-unsafe — it is a well-formed query with one of three
defined answers:

- **meaningful** — the edge is in the op's declared reference types; apply
  proceeds;
- **inert** — the edge is tolerated but has no effect (a remove naming a
  non-insert sets a dead bit on nothing);
- **gated** — the edge is declared ill-typed; the op quarantines permanently
  (kinds are immutable, so the verdict is stable — FRAMEWORK's apply-time
  gate criterion).

What the property is *not*: static typing. An author cannot prove a
referent's kind inside their own artifact — the verdict lands at the gate,
when both artifacts are present (an unknown referent parks the op as a
normal orphan first). It is dynamic typing with unforgeable tags.

## The generative turn

The current specs use kind-commitment *defensively*: inert removes
(HASHSEQ_SPEC), anchor-kind checks (MARKS.md), ill-typed child quarantine
(HASHWEB_SPEC). The stronger position is to use it *generatively*:

> **One object's DAG may mix op kinds freely.** Safety is not object
> homogeneity; it is edge typing — each op kind declares, per role, the
> kinds it may reference, and one shared gate checks every edge.

### The typed-edge table

The whole discipline compresses into a table that is itself the validation
spec:

| op . role | may reference | otherwise |
|---|---|---|
| `Insert . at` (anchor) | insert, move op (its splice point), the object's origin id | gate |
| `Remove . target` | insert | inert |
| `Mark . anchor` | insert (char), the object's origin id | gate |
| `Mark . overwrites` | mark | gate |
| `Put . overwrites` | put on the same key | ignored (definitional filter) |
| `Move . target` | insert, in the object `to` resolves in (same-container rule) | gate |
| `Move . to` | glued point (insert, move op) in `target`'s object | gate |
| unroled refs (pins) | anything | always meaningful — pure frontier pins |

(The normative version now lives in HASHWEB_SPEC.md "The edge table"; this
sketch is the design rationale.) Every existing per-layer check is a row of
this table — the shared validate-before-apply gate the specs had been
reaching for piecemeal, now folded. Extending the
family = adding rows, not adding mechanisms. Note the table has two kinds of
constraint: **kind checks** (stable, gate-enforceable) and **value-dependent
filters** (same-key for `Put.overwrites` — enforced definitionally in the
head-set computation, not at the gate); keeping that distinction explicit is
what keeps every gate verdict permanent.

### Consequence: one derive_key context

Heterogeneity settles the open id-derivation question: **one shared context
string with op tags in the encoding**, not per-kind contexts. Per-kind
contexts would still be safe (an id matches whichever preimage hashes to it)
but split the namespace for no benefit — and the namespace being shared is
the point: any id can appear in any role, and the table, not the id space,
says what it means there.

### Payloads are ids too

The last raw value falls out of the op shape: a sequence insert's payload is
an **id** — of a content-addressed value artifact (char, int, bytes;
kind-tagged, so a payload's kind is committed like everything else) or of an
an object's origin id (a child being created, or an *existing* object's:
transclusion), or an op node. For text this changes identity only — chars still encode
inline, and the preimage hashes the derived value id (HASHSEQ_SPEC.md
"Payload"). What it opens:

- **anything is insertable** — images (blob ids), mentions (object refs),
  inline widgets: one insert op, any payload kind, and placeholder
  semantics for payloads you cannot resolve or interpret (the envelope
  story, again);
- **Text and List are one mechanism** — decided (HASHWEB_SPEC.md): one seq
  kind, any payload in any slot; a text is a seq whose payloads are char
  values, and textness is a rendering/export convention, never a committed
  type;
- **transclusion** — a payload naming an existing object's origin id puts
  one object in two containers. Powerful and sharp-edged: aliasing,
  read-time cycle handling for render, per-slot deletion (tombstoning a
  slot, not the object). See open problems.

Note where payloads sit in the ledger: with blob hashes and `Ref` values,
they are **values, not references** — never in `refs(u)`, never buffered
on, `pending`-surfaced when unresolvable.

## What mixed-kind objects look like

The gallery — each exists because references cross kinds safely:

1. **Rich text as one object.** Chars, marks, and moves in one DAG. Whether
   they share a *frontier* stays an economics choice (LAYERING.md); whether
   they share a *namespace and DAG* is now answered — safety was never the
   reason to separate them, and drops out of that ledger entirely.
2. **Annotations on annotations.** A comment thread anchored on a *mark op*
   rather than on the span's chars: the thread names the highlight itself,
   survives arbitrary char churn inside the span, and dies with the mark —
   semantics no char-anchored encoding can express. Reactions as puts keyed
   by a comment's id. `After(move_op)` — the splice-point anchor — is the
   same move, already made once.
3. **An "about" register on anything.** Puts keyed by an arbitrary node id:
   review status on an edit, moderation labels on a remove, signatures or
   votes on any op. Op-level metadata with no schema negotiation — the
   subject's kind is discovered at dereference and can never be
   misconstrued.
4. **Mixed streams.** A chat/feed object: a sequence of `Ref(message)`
   inserts, moderator removes, pin moves, reaction puts keyed by message
   ids — five op kinds, one DAG, one causal history, every cross-reference
   typed.
5. **Typed links.** Link marks whose value is `Ref(object)`: the document
   graph (backlinks, embeds, citations) is readable from the artifacts
   themselves, each edge kind-committed at both ends.

## Forward compatibility: the extension path

A shared namespace of kind-committed artifacts is how the system grows
without migrations — and the framework itself dictates *how*. "I do not
recognize this kind" is a fact about the replica's software, not about
hash-committed inputs, so by the stability criterion it can never be a gate
verdict: a replica that quarantined unknown kinds would diverge, on the
permanent record, from upgraded peers. Extension is therefore handled by
semantics, not rejection.

### The envelope / body split

Every op's canonical form parses in two parts:

```
envelope — kind-independent: the refs table, the kind tag, body length
body     — kind-dependent: opaque unless the kind is known
```

and each op's semantics split accordingly:

- **envelope semantics** — identity (the id hashes envelope ‖ body),
  commitment (refs), and reference structure (buffering, encoding). Every
  replica of every version computes these identically.
- **body semantics** — what the op *does* (insert a char, set a key, move
  an element) **and where it sits**: placement is kind-level meaning, and
  not every kind has a place (a `Put` does not — objects are not all
  linear). Unknown kind → no effect: the op is carried, forwarded, and
  surfaced as present-but-uninterpretable — the same honest state as a
  pending blob.

### Unknown referents park

Because placement lives in the body, an op that anchors on a node whose
kind a replica cannot interpret has no resolvable place there — so it
**parks as an orphan** until the kind is known. Parking is not a verdict:
nothing permanent is decided, which is exactly what the stability criterion
demands of unknown-ness (it can never gate). On upgrade, parked ops apply
as ordinary late deliveries, and late arrival never reorders existing
content — the replay trap stays closed across versions through the same
property that closes it across delivery orders. The accepted trade: an old
replica renders unknown-kind content and everything anchored on it as
*absent*, not as a placed placeholder.

Anchorability remains a stable, kind-committed fact for **known** kinds:
the edge table says which kinds bear glued points (`Insert . at` admits
inserts, move ops, the object's origin id), and a known-kind violation
gates permanently. Unknown kinds neither pass nor fail — they wait.

### The convergence contract across versions

- the **op set** converges universally — sync, ids, and buffering are
  envelope concerns;
- the **base order** agrees on everything all parties can interpret; ops
  referencing unknown kinds park and arrive on upgrade as late deliveries —
  insertion, never reordering;
- **render is version-parameterized**: `render_v(S)` differs across
  versions by exactly the unknown kinds' body effects (and their parked
  dependents); same-version replicas converge as always.

Version-skewed *interaction* is already covered by existing machinery, from
two directions. Referencing: ops name ids, never positions — a v1 user's
remove names exactly the ids they saw (it cannot swallow an invisible v2
element), and a skewed render can degrade what you see, never what you
reference. Racing: a v1 author whose Put overwrites only the heads their
version can read looks, to v2 peers, exactly like an honest author who
hadn't seen the others — genuine, non-malicious withholding — and the
resolution rules were built to survive precisely that: multi-head, surfaced,
frozen, dominated by the next op that names everything. Version skew is a
case the withholding-tolerance design already covers.

### What the encoding owes

Unknown-kind blocks must be **skippable**: kind-tagged, length-prefixed,
envelope at a fixed parse position — the unknown-fields discipline. A
replica stores and re-emits body bytes it cannot parse verbatim; id
verification needs nothing more.

## Interplay with layering

Orthogonal, and the orthogonality is clarifying: **type safety comes from
the hash commitment — free and universal; frontier isolation is purely
economics** (dep volume, closure locality — LAYERING.md). Mixing kinds in
one object does not make their ops pin each other's frontiers, and
separating frontiers does not make cross-kind references any less safe.
"Can these ops coexist and reference each other?" is always yes; "should
they observe each other's tips?" is the only real question, and it is a
different one.

## Costs and cautions

- **The edge table is load-bearing spec surface.** It must be total (every
  op-role × kind has a verdict), convergent (verdicts from hash-committed
  inputs only), and versioned with the encoding. A missing row is a
  divergence bug, not an oversight.
- **Combinatorial semantics.** Every new kind multiplies potential edges;
  the table is what forces each combination to be decided rather than
  discovered. The discipline is the feature — but it is a real authoring
  cost per kind.
- **Bare ids are opaque in flight.** Kind is knowable only with the
  preimage; a relay cannot route by kind from an id alone (block tags on
  the wire cover the common case — ops travel as tagged blocks).

## Open problems

1. **Retrofit audit.** Verify every gate verdict is a function of
   hash-committed facts computable by any replica that knows the relevant
   kinds — never of availability or version — and that every
   unknown-referent path parks rather than verdicts. (Reading a *known*
   referent's body — e.g. a move op's `to` for its splice point — is fine;
   placement is body semantics.) Run it against the normative edge table
   (HASHWEB_SPEC.md) and the envelope grammar (GRAMMAR_SPEC.md).
2. **Transclusion semantics.** Render cycles are solved: an embedding
   renderer embeds each object at most once per root-to-leaf path and
   degrades to a navigation link on repetition — deterministic, local,
   convergent (CYCLE_REVERT.md resolution). Remaining: per-slot deletion
   (a tombstoned slot does not kill the object; unreachability from *all*
   slots is the GC condition — HASHWEB_SPEC.md); the aliasing story (edits
   visible through every container); and whether multi-link duplication
   (one object live-linked twice — also the concurrent re-link residue,
   MOVE.md "Reparenting") is surfaced generically by the renderer.
