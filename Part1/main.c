#include "int.h"

int main (void){

<<<<<<< HEAD
    relation* relR = createRelation();
    //sleep(1);                 
    relation* relS = createRelation();
    
    
=======
    srand((unsigned) time(NULL));

    relation* relR = createRelation();
    relation* relS = createRelation();
    result* res = NULL;

    res = PartitionedHashJoin(relR, relS);


//    printRelation(myRelation);
//    relationDelete(myRelation);
//  relation *R, *S, *res;
>>>>>>> eec2546f54e13d6e1a9da4b899727be0af48ecf7


    result *res;


    res = PartitionedHashJoin(relR,relS);


    relationDelete(relR);
    relationDelete(relS);
}