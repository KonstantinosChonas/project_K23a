#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>




/** Type definition for a tuple */

typedef struct tuple {
    int32_t key;
    int32_t payload;
}tuple;

/**
* Type definition for a relation.
* It consists of an array of tuples and a size of the relation.
*/

typedef struct relation {
    tuple *tuples;
    uint32_t num_tuples;
}relation;



typedef struct result {  //NOTE: katholou sigouros gia to ti tha prepe na einai to struct result to vala etsi gia arxi pisteuo thelei allagi
    relation;
}result;


/** Partitioned Hash Join**/
result* PartitionedHashJoin(relation *relR, relation *relS);


/**Our Hash Function**/
int hashl(int x, int n);