Op tree is the SOT for position, but it's slow O(n) to determine position of an arbitrary char.


Need a second position -> char index that is fast to update and query.

Fast query is easy

Fast update is harder since position SOT comes from the slow Op-Tree.


```
                   -
         *         |
     /      \      | Second fast position index 
    *        *     |
   /  \     /  \   -
  a <--|-- c <- d  | Op-Tree position 
     \-b           -


       *
     /  \
    *    \
  /  \    \
 a <- c <- d
```


Two hierarchies into the same data
