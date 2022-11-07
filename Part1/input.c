#include "int.h"

relationPayloadList* createRelationPayloadList(int data){
    relationPayloadList *temp;
    temp = malloc(sizeof(relationPayloadList));
    temp->data=data;
    temp->next = NULL;
    return temp;
}

tuple* createTuple(int key){

    tuple *newTuple = malloc(sizeof(struct tuple));

    newTuple->key = key;
    newTuple->payloadList = createRelationPayloadList(rand() % 20);
    //newTuple->payloadList->data = rand() % 20;

    printf("new tuple created with key: %d, and payload: %d\n", newTuple->key, newTuple->payloadList->data);

    return newTuple;
}

tuple* createTupleFromNode(int key, int payload, int payloadNext){
    tuple *newTuple = malloc(sizeof(struct tuple));

    newTuple->key = key;
    newTuple->payloadList = createRelationPayloadList(payload);
    newTuple->payloadList->next = createRelationPayloadList(payloadNext);
    //newTuple->payloadList->data = payload;

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
        printf("Tuple with key: %d and payload:", myRelation->tuples[i].key);
        printPayload(myRelation->tuples[i].payloadList);
    }
}

void printPayload(relationPayloadList* payloadList){

    printf(" %d", payloadList->data);

    payloadList = payloadList->next;
    while(payloadList != NULL){
        printf(", %d", payloadList->data);
        payloadList = payloadList->next;
    }

    printf("\n");
}

void relationDelete(relation* myRelation){
    //printf("deleting relation with %d tuples\n", myRelation->num_tuples);

    struct relationPayloadList* tempPayloadList = NULL;
    struct relationPayloadList* tempPayloadListNext = NULL;

    if(myRelation->tuples){
        for(int i = 0; i < myRelation->num_tuples; i++){
            tempPayloadList = myRelation->tuples[i].payloadList;
            tempPayloadListNext = myRelation->tuples[i].payloadList->next;
            free(tempPayloadList);
            while(tempPayloadListNext){
                tempPayloadList = tempPayloadListNext;
                tempPayloadListNext = tempPayloadListNext->next;
                free(tempPayloadList);
            }
        }
        free(myRelation->tuples);
    }

    if(myRelation != NULL){
        free(myRelation);
    }

}

void tupleDelete(tuple* myTuple){
    free(myTuple);
}