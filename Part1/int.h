#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#define L2 51       /* bits of information stored in L2 cache memory */



/** Type definition for a tuple */

typedef struct tuple {
    int32_t key;
    int32_t payload;
}tuple;

/**
* Type definition for a relation.
* It consists of an array of tuples and a size of the relation.
*/

typedef struct relation {
    tuple *tuples;
    uint32_t num_tuples;
}relation;



typedef struct result {  //NOTE: katholou sigouros gia to ti tha prepe na einai to struct result to vala etsi gia arxi pisteuo thelei allagi
    relation *r;
}result;


/** Partitioned Hash Join**/
result* PartitionedHashJoin(relation *relR, relation *relS);


/**Our Hash Function**/
int hashl(int x, int n);

relation* createPsum(relation* r,int n);
relation* relPartitioned(relation *r, relation *Psum, int n);
int findNumOfBuckets(relation *r);
tuple* SearchKey(relation *r,int key, int n);

void compareBuckets(relation *r, relation *s, relation *rPsum, relation *sPsum, int nR, int nS);

/** Functions used to create input**/

tuple* createTuple(int key);
relation* createRelation();
void printRelation(relation* myRelation);

void relationDelete(relation* myRelation);
void tupleDelete(tuple* myTuple);