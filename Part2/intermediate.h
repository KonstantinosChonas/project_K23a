#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "int.h"



typedef struct intermediate{

    relation *rel;
    int num_relations;


}intermediate;


/* Function Declarations */


void applyFilter(relationInfo *r, intermediate *rowidarray,char* filter);
intermediate* intermediateCreate(int numOfRelations );