# HashSeq

A Byzantine-Fault-Tolerant Sequence CRDT suitable for unpermissioned networks with unbounded number of collaborators.

# Design

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

We linearize these Hash Graphs by performing a _biased_ topological sort.

The bias is introduced to decide the canonical ordering when there are multiple linearizations that satisfy the left/right constraints.

E.g.
```
       b - c
      /     \
.. - a       f - ..
      \     /
       d - e

```

both `abcdef`, `adedef`, `abdcef`, `adbec`, ... are valid orderings. We need a canonical ordering that preserves some semantic information, (i.e. no interleaving of concurrent runs)

The choice we make is: when we notice a fork, we choose the branch whose starting element has the smaller hash, then to avoid interleaving of concurrent runs, our topological sort runs depth first rather than the traditional breadth first.

So in the above example, the resulting sequence (assuming hash(b) < hash(d)) is: `abcdef`.


## Optimizations:


If we detect hash-chains, we can collabse them to just the first left hashes and the right hashes:

i.e. in the first example, a,b,c are sequential, they all have a common right hand (empty set), and their left hand is the previous element in the sequence.

So we could represent this as:

```
Run(abc)
```

Inserting 'd' splits the run:

```
      __________________
     /                   \*
^ <- a <- d -> Run(bc) -> $
     *\_______/
```

And the fork example:

```
       Run(bc)
      /       \
.. - a         f - ..
      \       /
       Run(de)

```
