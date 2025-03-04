#ifndef INT
#define INT

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include<unistd.h>
#include <time.h>
#include <pthread.h>
#include <semaphore.h>
// #include "queries.h"
#include "statistics.h"
#include "job.h"


/*  Semaphores  */
 
// sem_t queue_lock;   //lock the queue before accessing 
// sem_t queue_full;   //used for threads to wait until there is a job to be done


/*--------------*/

#define L2 256000      /* bits of information stored in L2 cache memory */
#define N_DISCRETE 50000000   /* maximum value of discrete values (50.000.000) apo ekfwnish */

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

typedef struct histThreadArgs{

    relation* relR;
    relation* relS;
    int nR;
    int nS;
    relation* histR;
    relation *histS;
    sem_t   lock;
    // pthread_t id;

}histThreadArgs;

typedef struct threadArray{
    int noThreads;                        //number of threads
    pthread_t* threads;                     //execution threads

}threadArray;


typedef struct relationInfo{
    int relation_num_total;
    int num_tuples;
    int num_cols;
    int **columns;
    columnStatistics* colStats;
}relationInfo;

typedef struct result {  //NOTE: katholou sigouros gia to ti tha prepe na einai to struct result to vala etsi gia arxi pisteuo thelei allagi
    relation *r;
}result;

relation* inputFromFile(char* s);


/** Partitioned Hash Join**/
relation* PartitionedHashJoin(relation *relR, relation *relS,JobScheduler* sch);


/**Our Hash Function**/
int hashl(int x, int n);


relation* createHist(relation* r,int n);
relation* createPsum(relation* r,int n, relation* hist);
relation* relPartitioned(relation *r, relation *Psum, int n,relation* hist);
int findNumOfBuckets(relation *r);
tuple* SearchKey(relation *r,int key, int n);

struct hashMap** createHashForBuckets(relation *r, relation *pSum, int hashmap_size, int neighborhood_size);
relation* joinRelation(struct hashMap**, relation *r, relation *smallerR, relation *pSum);

/** Functions used to create and handle input**/

relationPayloadList* createRelationPayloadList(int data);
tuple* createTuple(int key);
tuple* createTupleFromNode(int key, int payload, int payloadNext);
relation* createRelation(int hop);
void printRelation(relation* myRelation);
void printPayload(relationPayloadList* payloadList);
int* pruneRelation(relation* myRelation, char operand, int value);
unsigned long long int getSumRelation(relation* myRelation);

void relationDelete(relation* myRelation);
void deleteTuples(relation* myRelation);
void tupleDelete(tuple* myTuple);


int getNumRelations(char* str);


relation* createEmptyRelation(int num_tuples);

void addToPayloadList(relationPayloadList* p,int data);


relationInfo* parseRelations(char* workPath, int* numRelations);
void relationInfoDelete(relationInfo* relInfo, int relationNum);

#endif