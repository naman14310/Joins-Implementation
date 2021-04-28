# Joins

Implementation of Sort merge join &amp; Hash join from scratch.

#### Language used : `Python`

### Problem Statement

Given M memory blocks and two large relations R(X,Y) and S(Y,Z). Develop iterator for the following operations.

● SortMerge Join

1. open() - Create sorted sublists for R and S, each of size M blocks.
2. getnext() - Use 1 block for each sublist and get minimum of R & S. Join this minimum Y value with the other table and return. Check for B(R)+B(S)<M 2
3. close() - close all files

● Hash Join

1. open() - Create M1 hashed sublists for R and S
2. getnext() - For each Ri and Si thus created, load the smaller of the two in the main memory and create a search structure over it. You can use M1 blocks
to achieve this. Then recursively load the other file in the remaining blocks and for each record of this file, search corresponding records (with same join attribute value) from the other file.
3. close() - close all files

#### Join condition (R.Y==S.Y).
