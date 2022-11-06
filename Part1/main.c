#include "int.h"
#include <time.h>

int main (void){
    time_t t;
    srand((unsigned)time(&t));
    //relation* relR = inputFromFile("r0.tbl");
     relation* relR = createRelation();
    //sleep(1);
    //relation* relS = inputFromFile("r1.tbl");
    relation* relS = createRelation();
    //printRelation(relR);
    //printRelation(relS);

    //result *r=PartitionedHashJoin(relR,relS);

    PartitionedHashJoin(relR, relS);

    relationDelete(relR);
    relationDelete(relS);
}