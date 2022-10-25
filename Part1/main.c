#include "int.h"

int main (void){

    srand((unsigned) time(NULL));

    relation* relR = createRelation();
    relation* relS = createRelation();
    result* res = NULL;

    res = PartitionedHashJoin(relR, relS);


//    printRelation(myRelation);
//    relationDelete(myRelation);
//  relation *R, *S, *res;






    relationDelete(relR);
    relationDelete(relS);
}