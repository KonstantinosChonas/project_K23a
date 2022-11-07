#include "int.h"
#include <time.h>

int main (void){
    time_t t;
    srand((unsigned)time(&t));      //used to create random payload for tuples in our relation
    //relation* relR = inputFromFile("r0.tbl");
    relation* relR = createRelation(1);
    //relation* relS = inputFromFile("r1.tbl");
    relation* relS = createRelation( 1);

    //printRelation(relR);
    //printRelation(relS);
    PartitionedHashJoin(relR, relS);

    relationDelete(relR);
    relationDelete(relS);
}