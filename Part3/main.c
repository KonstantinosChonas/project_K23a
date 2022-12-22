#include "queries.h"
#include "intermediate.h"
#include "int.h"
#include "statistics.h"
#include <time.h>
#include <stdio.h>
#include <unistd.h>

int main (int argc, char* argv[]){

    printf("starting main\n");

    relationInfo* relInfo = NULL;
    int relationNum;

    char* relations = argv[1];
    if(relations == NULL){
        relations = "workloads/small/small.init";
    }
    relInfo = parseRelations(relations, &relationNum);
    //relInfo = parseRelations("workloads/small/small.init", &relationNum);

//    relationInfo* relInfo = NULL;
//    relInfo = (relationInfo*) malloc(15*sizeof(relationInfo));
//    FILE *f = fopen("relation.data", "rb");
//    fread(relInfo, sizeof(char), sizeof(relationInfo), f);
//
//    printf("%d\n", relInfo[2].columns[5][3]);
    sleep(1);
    /* start of query reading */
    char* queries = argv[2];
    if(queries == NULL){
        queries = "workloads/small/small.work";
    }


    int i = parseQueries(queries, relInfo, relationNum);
    //int i = parseQueries("workloads/small/small.work", relInfo, relationNum);


    //these should be a function
    // free(rowids->relArray);
    // free(rowids);

    relationInfoDelete(relInfo, relationNum);

}