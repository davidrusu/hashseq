<a href="https://crates.io/crates/hashseq"><img src="https://img.shields.io/crates/v/hashseq.svg"></a>

# HashSeq

A Byzantine-Fault-Tolerant (BFT) Sequence CRDT suitable for unpermissioned networks with unbounded number of collaborators.

## Merge Semantics

### Concurrent Inserts are not interleaved:

| Site 1 | Site 2  |
|--------|---------|
|  hello | goodbye |

On merge we see:

`hellogoodbye` OR `goodbyehello`


### Common Prefix is Deduplicated:

| Site 1 | Site 2  |
|--------|---------|
|  hello earth | hello mars |

On merge we see:

`hello earthmars` OR `hello marsearth`

(i.e. the common prefix "hello " is not duplicated even though both sites inserted it.)

## Performance

HashSeq achieves over 1 million operations per second on real-world editing traces (tested on sequential traces from the [editing-traces](https://github.com/josephg/editing-traces) benchmark suite).

## Design

HashSeq is based on RGA (Replicated Growable Array) but introduces a key extension: the `InsertBefore` operation.

### The InsertBefore Problem

Traditional RGA uses Lamport timestamps to order concurrent insertions at the same position. This works because timestamps encode causal information - if you've seen an element, your clock is higher.

HashSeq is content-addressed: node IDs are hashes of their content and dependencies. This is crucial for BFT: a malicious actor cannot manipulate ordering by tampering with their clock or forging actor IDs. It also means no per-collaborator metadata - no actor IDs, no vector clocks that grow with each participant. Collaborators can join and leave without causing metadata bloat.

When specific ordering is needed, use `InsertAfter` or `InsertBefore`. Hash comparison only determines ordering for truly concurrent insertions that share the same anchor - where either ordering is equally valid.

Consider inserting between two causally related characters:

```
'a'
  \
  'b'    (b was inserted after a)
```

If we use `InsertAfter(a, 'x')`, then 'x' becomes a sibling of 'b':

```
'a'
  \---\
  'b' 'x'
```

The result could be `axb` or `abx` depending on whether `hash(x) < hash(b)` - essentially random.

HashSeq solves this with `InsertBefore(b, 'x')`, which explicitly constrains 'x' to appear before 'b':

```
'a'
  \
  'b'
  /
'x'    (x is before b)
```

This guarantees the result is `axb`, regardless of hash ordering.

### Operations

Each edit produces a HashNode containing an Op and extra dependencies:

```rust
pub enum Op {
    InsertRoot(char),
    InsertAfter(Id, char),
    InsertBefore(Id, char),
    Remove(BTreeSet<Id>),
}

pub struct HashNode {
    extra_dependencies: BTreeSet<Id>,
    op: Op,
}

impl HashNode {
    fn id(&self) -> Id;
}
```

* `InsertRoot` is used when the HashSeq is empty.
* `InsertAfter(id, char)` is used to constrain this HashNode to appear after the node with id `id`.
* `InsertBefore(id, char)` is used to constrain this HashNode to appear before the node with id `id`.
* `Remove(ids)` is used to remove a set of nodes.

Node IDs are content-addressed hashes (blake3) of the operation and its dependencies.

#### Example 1. Writing "hello" by appending to end

```
InsertRoot('h')       -- id = 0x0
InsertAfter(0x0, 'e') -- id = 0x1
InsertAfter(0x1, 'l') -- id = 0x2
InsertAfter(0x2, 'l') -- id = 0x3
InsertAfter(0x3, 'o') -- id = 0x4

'h'
  \
  'e'
    \
    'l'
      \
      'l'
        \
        'o'

-- "hello"
```

Since IDs are content-addressed, we can store the sequence succinctly as:

```
InsertRoot('h') 
Run(0x0, "ello")
```

Since text is highly compressible, we could further compress the string inside a run to achieve further storage compression. Then we can always decompress the run and reconstruct the operations and IDs of each character when needed to resolve order of concurrent edits.



#### Example 2. Concurrent editing

Two users concurrently insert `"hi sam"` and `"hi dan"`. The two underlying hashseq structures look like:

```
1. 'h'                      2. 'h'
     \                           \
     'i'                         'i'
       \                           \
       ' '                         ' '
         \                           \
         's'                         'd'
           \                           \
           'a'                         'a'
             \                           \
             'm'                         'n'
```

Upon syncing, the common prefix `"hi "` is dedupped and the causal tree becomes:

```
'h'
  \
  'i'
    \
    ' '
      \---\
      's' 'd'
        \   \
        'a' 'a'
          \   \
          'm' 'n'
```

The HashSeq underlying structure is a causal insertion tree (each node has exactly one anchor). Forks occur when multiple nodes share the same anchor and the visualization above shows two right children of the ' ' node. A naive traversal could produce interleavings like `hi sdaamn` or `hi sdanam`. We need a canonical ordering that preserves semantic information - keeping concurrent runs intact.

The choice made in HashSeq is to order child nodes by their hash. In a fork, we choose the branch whose starting element has the smaller hash, then to avoid interleaving of concurrent runs, our topological sort runs depth first. So in the above example, assuming `hash(s)` < `hash(d)`, we'd get: `hi samdan`.

#### Example 3. Fixing a typo (inserting in the middle of a run)

Consider a user who has typed "hllo" (missing an 'e'):

```
'h'
  \
  'l'
    \
    'l'
      \
      'o'
```

This is stored as a single run: `Run(root_h, "llo")`.

Now the user wants to insert 'e' between 'h' and 'l' to get "hello". Using only `InsertAfter(id_h, 'e')` would make 'e' a sibling of the first 'l':

```
'h'
  \---\
  'l' 'e'
    \
    'l'
      \
      'o'
```

The result could be "hello" or "hlloe" depending on hash ordering (depth-first traversal follows one branch completely before the other) - not what we want.

Instead, HashSeq uses `InsertBefore(id_l, 'e')` which creates an explicit constraint:

```
'h'
  \
  'l'
  / \
'e' 'l'
      \
      'o'
```

The left child `/` indicates 'e' is inserted before 'l'. This guarantees "hello" regardless of hash values since left children (insert-befores) are always traversed before right children (insert-afters).
