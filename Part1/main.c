#include "int.h"
#include <time.h>

int main (void){
    time_t t;
    srand((unsigned)time(&t));
    relation* relR = createRelation();
    //sleep(1);                 
    relation* relS = createRelation();
<<<<<<< HEAD
    
    

    result *r=PartitionedHashJoin(relR,relS);
=======

>>>>>>> 378774f908513048290d696e004e242c38dc7d14

    PartitionedHashJoin(relR, relS);



    relationDelete(relR);
    relationDelete(relS);
}