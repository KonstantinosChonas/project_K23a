#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#define L2 128000      /* bits of information stored in L2 cache memory */


/** Type definition for a tuple */

typedef struct relationPayloadList{
    int data;
    struct relationPayloadList *next;
}relationPayloadList;

typedef struct tuple {
    int32_t key;
    relationPayloadList* payloadList;
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

relation* inputFromFile(char* s);


/** Partitioned Hash Join**/
result* PartitionedHashJoin(relation *relR, relation *relS);


/**Our Hash Function**/
int hashl(int x, int n);


relation* createHist(relation* r,int n);
relation* createPsum(relation* r,int n, relation* hist);
relation* relPartitioned(relation *r, relation *Psum, int n,relation* hist);
int findNumOfBuckets(relation *r);
tuple* SearchKey(relation *r,int key, int n);

struct hashMap** createHashForBuckets(relation *r, relation *pSum, int hashmap_size, int neighborhood_size);
relation* joinRelation(struct hashMap**, relation *r, relation *pSum);

/** Functions used to create and handle input**/

relationPayloadList* createRelationPayloadList(int data);
tuple* createTuple(int key);
tuple* createTupleFromNode(int key, int payload, int payloadNext);
relation* createRelation(int hop);
void printRelation(relation* myRelation);
void printPayload(relationPayloadList* payloadList);

void relationDelete(relation* myRelation);
void tupleDelete(tuple* myTuple);