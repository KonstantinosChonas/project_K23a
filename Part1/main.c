#include "int.h"

int main (void){

    relation* relR = createRelation();
    //sleep(1);                 
    relation* relS = createRelation();
    
    

    result *r=PartitionedHashJoin(relR,relS);




    relationDelete(relR);
    relationDelete(relS);
}