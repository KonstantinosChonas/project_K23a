//#include "queries.h"
#include "intermediate.h"
#include <time.h>
#include <stdio.h>
#include <unistd.h>

int main (int argc, char* argv[]){

    printf("starting main\n");
    char work[50]="3 0 1|0.2=1.0&0.1=2.0&0.2>3499|1.2 0.1";


    char* token=strtok(work,"|");

    printf("%d num of relations \n",getNumRelations(token));


    intermediate* rowids=intermediateCreate(getNumRelations(token));




    printf("same rel returned: %d\n",sameRel("0.1=0.1"));

    sleep(1);
    /* start of query reading */
    FILE* queryFile = NULL;
    char* queries = argv[1];

    int i = parseQueries("workloads/small/small.work", NULL);

}