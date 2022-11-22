#include "queries.h"
#include <time.h>
#include <stdio.h>
#include <unistd.h>

int main (int argc, char* argv[]){
     time_t t;
    srand((unsigned)time(&t));      //used to create random payload for tuples in our relation
    printf("hello\n");
    //relation* relR = inputFromFile("r0.tbl");
    relation* relR = createRelation(1);
    //relation* relS = inputFromFile("r1.tbl");
    relation* relS = createRelation( 1);

    //printRelation(relR);
    //printRelation(relS);
    //PartitionedHashJoin(relR, relS);

    //int* keyList = pruneRelation(relS, '<', 5);

    //int sum = getSumRelation(relS);
    //printf("result: %d\n", sum);

    sleep(1);
    /* start of query reading */
    FILE* queryFile = NULL;
    char* queries = argv[1];

    int i = parseQueries("build/release/workloads/small/small.work", NULL);

    //free(keyList);

    relationDelete(relR);
    relationDelete(relS);
}