//#include "queries.h"
#include "intermediate.h"
#include <time.h>
#include <stdio.h>
#include <unistd.h>

int main (int argc, char* argv[]){

    printf("startingg main\n");

    relationInfo* relInfo = NULL;
    int relationNum;

    relInfo = parseRelations("workloads/small/small.init", &relationNum);

    printf("Value in r10, column 2, row 191: %d\n", relInfo[10].columns[1][190]);

    char work[50]="3 0 1|0.2=1.0&0.1=2.0&0.2>3499|1.2 0.1";

    char* token=strtok(work,"|");

    printf("%d num of relations \n",getNumRelations(token));

    intermediate* rowids=intermediateCreate(getNumRelations(token));

//    relationInfo* relInfo = NULL;
//    relInfo = (relationInfo*) malloc(15*sizeof(relationInfo));
//    FILE *f = fopen("relation.data", "rb");
//    fread(relInfo, sizeof(char), sizeof(relationInfo), f);
//
//    printf("%d\n", relInfo[2].columns[5][3]);
    printf("same rel returned: %d\n",sameRel("0.1=0.1"));

    sleep(1);
    /* start of query reading */
    FILE* queryFile = NULL;
    char* queries = argv[1];

    int i = parseQueries("workloads/small/small.work", relInfo, relationNum);


    //these should be a function
    free(rowids->relArray);
    free(rowids);

    relationInfoDelete(relInfo, relationNum);

}