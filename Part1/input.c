#include "int.h"


tuple* createTuple(int key){

    tuple *newTuple = malloc(sizeof(struct tuple));

    newTuple->key = key;
    newTuple->payload = rand() % 20;

    printf("new tuple created with key: %d, and payload: %d\n", newTuple->key, newTuple->payload);

    return newTuple;
}

tuple* createTupleFromNode(int key, int payload){
    tuple *newTuple = malloc(sizeof(struct tuple));

    newTuple->key = key;
    newTuple->payload = payload;

    return newTuple;
}

relation* createRelation(){
    int relationSize;
    tuple* prevTuple = NULL;
    tuple* newTuple = NULL;

    relation *newRelation = malloc(sizeof(struct relation));

    // newRelation->num_tuples = 20;
    //newRelation->num_tuples = rand() % 100 + 50;        //use for random number of tuples in relation
    //newRelation->num_tuples = 20;
    newRelation->num_tuples = rand() % 300 + 30000;        //use for random number of tuples in relation
    newRelation->tuples = malloc(sizeof(struct tuple) * newRelation->num_tuples);

    for(int i = 0; i < newRelation->num_tuples; i++){
        newTuple = createTuple(i);
        newRelation->tuples[i] = *newTuple;
        free(newTuple);
    }


    printf("new relation created with %d tuples\n", newRelation->num_tuples);

    return newRelation;
}


void printRelation(relation* myRelation){
    printf("\nPrinting Relation with %d tuples:\n\n", myRelation->num_tuples);
    for(int i = 0; i < myRelation->num_tuples; i++){
        printf("Tuple with key: %d and payload %d\n", myRelation->tuples[i].key, myRelation->tuples[i].payload);
    }
}

void relationDelete(relation* myRelation){
    //printf("deleting relation with %d tuples\n", myRelation->num_tuples);

    if(myRelation->tuples){
        free(myRelation->tuples);
    }

    if(myRelation != NULL){
        free(myRelation);
    }

}

void tupleDelete(tuple* myTuple){
    free(myTuple);
}