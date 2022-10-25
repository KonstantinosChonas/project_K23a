#include "int.h"

int main (void){

    relation* relR = createRelation();
    //sleep(1);                 
    relation* relS = createRelation();
    
    


    result *res;


    res = PartitionedHashJoin(relR,relS);


    relationDelete(relR);
    relationDelete(relS);
}