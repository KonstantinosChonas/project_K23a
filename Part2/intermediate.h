#ifndef INTERMEDIATE
#define INTERMEDIATE

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "int.h"
#include "queries.h"


typedef struct intermediate{

    int **row_ids;
    int num_relations;
    int num_rows;


}intermediate;


/* Function Declarations */


void applyFilter(relationInfo *r, intermediate *rowidarray,char* filter);
intermediate* intermediateCreate(int numOfRelations );
relation* intermediateToRelation(intermediate *rowidarray, relationInfo *relInfo,int column,int relname);
void intermediateDelete(intermediate* inter);
intermediate* addToArray(intermediate *rowidarray, relation *phjRel,int relname1, int relname2);
void printIntermediate(intermediate *rowidarray);
#endif //INTERMEDIATE
