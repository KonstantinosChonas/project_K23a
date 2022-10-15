#include "int.h"


int hashl(int x, int n){

    int i=0,j=1;
    for (i=0;i<n;i++){
        j=j*2;
    }
    j--;

    return x & j;

}


result* PartitionedHashJoin(relation *relR, relation *relS){

    /**     Step 1. Partitioning        **/





    /**     Step 2. Building            **/

    /**     Step 3. Probing             **/
}