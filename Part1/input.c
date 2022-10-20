//
// Created by aris on 20/10/2022.
//

#include <stdio.h>
#include <stdlib.h>
#include "int.h"

tuple* createTuple(int key){
    tuple *newTuple = malloc(sizeof(struct tuple));

    newTuple->key = key;
    newTuple->payload = rand() % 20;
    newTuple->nextTuple = NULL;

    printf("new tuple created with key: %d, and payload: %d\n", newTuple->key, newTuple->payload);

    return newTuple;
}

relation* createRelation(){
    srand((unsigned) time(NULL));
    int relationSize;
    tuple* prevTuple = NULL;
    tuple* newTuple = NULL;

    relation *newRelation = malloc(sizeof(struct relation));

    newRelation->num_tuples = rand() % 15 + 1;

    for(int i = 0; i < newRelation->num_tuples; i++){
        newTuple = createTuple(i);
        newTuple->nextTuple = prevTuple;
        prevTuple = newTuple;
    }
    newRelation->tuples = newTuple;

    printf("new relation created with %d tuples\n", newRelation->num_tuples);

    return newRelation;
}

void printRelation(relation* myRelation){
    tuple* nextTuple = NULL;
    tuple* currTuple = myRelation->tuples;
    printf("Printing Relation with %d tuples:\n\n", myRelation->num_tuples);
    while(currTuple != NULL){
        nextTuple = currTuple->nextTuple;
        printf("Tuple with key:%d and payload:%d\n", currTuple->key, currTuple->payload);
        currTuple = nextTuple;
    }
}

void relationDelete(relation* myRelation){
    tuple* nextTuple = NULL;
    tuple* currTuple = myRelation->tuples;
    //printf("deleting relation with %d tuples\n", myRelation->num_tuples);
    for(int i = 0; i< myRelation->num_tuples; i++){
        nextTuple = currTuple->nextTuple;
        //printf("deleting tuple:%d\n", currTuple->key);
        tupleDelete(currTuple);
        currTuple = nextTuple;
    }
    free(myRelation);
}

void tupleDelete(tuple* myTuple){
    free(myTuple);
}