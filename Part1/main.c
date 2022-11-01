#include "int.h"
#include <time.h>

int main (void){
    time_t t;
    srand((unsigned)time(&t));
    relation* relR = createRelation();
    //sleep(1);                 
    relation* relS = createRelation();
    
    

    result *r=PartitionedHashJoin(relR,relS);


    PartitionedHashJoin(relR, relS);



    relationDelete(relR);
    relationDelete(relS);
}