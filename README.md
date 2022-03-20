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

(i.e. hello is not duplicated even though Site 1 and Site 2 both inserted it.)

### Stable Ordering
let _S_,_R_ be HashSeq instances on Site 1, Site 2 respectively.

Both _S_ and _R_ form a montonic sub-sequence of _Q_ = merge(_S_, _R_).

Stated differently, for sequence elements _a_,_b_ ∈ _S_, if _a_ comes before _b_ in _S_, and _a_,_b_ ∈ _R_, then _a_ comes before _b_ in _R_.

## Current Complexity:

|   op   | time | space |
|--------|------|-------|
| insert | O(n) | O(n)  |
| remove | O(n) | O(1)  |

This is still a WIP, I think we can improve these substantially with some clever indexing strategies

## Design

Each insert produces a Node holding a value, the hashes of the immediate nodes to the left, and the immediate nodes to the right:

```
struct Node<V> {
   value: V,
   lefts: Set<Hash>,
   rights: Set<Hash>,
}
```
E.g.

Inserting 'a', 'b', 'c' in sequential order produces the graph:
```
 a <- b <- c
```

Inserting 'd' between 'a' and 'b'
```
a <- d -> b <- c
   \_____/
```

We linearize these Hash Graphs by performing a biased topological sort.

The bias is used to decide a canonical ordering in cases where multiple linearizations satisfy the left/right constraints.

E.g.
```
            s - a - m
           /         \
h - i - ' '           ! - !
           \         /
            d - a - n

```

The above hash-graph can serialize to `hi samdan!!` or `hi dansam` or even any interleaving of sam/dan: `hi sdaamn`, `hi sdanam`, ... . We need a canonical ordering that preserves some semantic information, (i.e. no interleaving of concurrent runs)

The choice we make is: in a fork, we choose the branch whose starting element has the smaller hash, then to avoid interleaving of concurrent runs, our topological sort runs depth first rather than the traditional breadth first.

So in the above example, assuming `hash(s)` < `hash(d)`, we'd get is: `hi samdan!!`.


## Optimizations:


If we detect hash-chains, we can collabse them to just the first left hashes and the right hashes:

```rust
struct Run<T> {
   run: Vec<T>
   lefts: Set<Hash>
   rights: Set<Hash>
}
```

i.e. in the first example, a,b,c are sequential, they all have a common right hand (empty set), and their left hand is the previous element in the sequence.

So we could represent this as:

```rust

// a <- b <- c == RUN("abc")

Run {
  run: "abc",
  lefts: {},
  rights: {}
}

```

Inserting 'd' splits the run:

```
a <- d -> RUN("bc")
   \_____/
```

And the fork example:

```
           RUN("sam")
          /          \
RUN("hi ")            RUN("!!")
          \          /
           RUN("dan")
```

This way we only store hashes at forks, the rest can be recomputed when necessary.
