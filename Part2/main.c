//#include "queries.h"
#include "intermediate.h"
#include <time.h>
#include <stdio.h>
#include <unistd.h>

int main (int argc, char* argv[]){

    //printf("starting main\n");

    relationInfo* relInfo = NULL;
    int relationNum;

    relInfo = parseRelations("workloads/small/small.init", &relationNum);

//    relationInfo* relInfo = NULL;
//    relInfo = (relationInfo*) malloc(15*sizeof(relationInfo));
//    FILE *f = fopen("relation.data", "rb");
//    fread(relInfo, sizeof(char), sizeof(relationInfo), f);
//
//    printf("%d\n", relInfo[2].columns[5][3]);
    sleep(1);
    /* start of query reading */
    FILE* queryFile = NULL;
    char* queries = argv[1];

    int i = parseQueries("workloads/small/small.work", relInfo, relationNum);


    //these should be a function
    // free(rowids->relArray);
    // free(rowids);

    relationInfoDelete(relInfo, relationNum);

}