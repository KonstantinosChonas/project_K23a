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
    newTuple->payloadList = createRelationPayloadList(rand() % 10);//createRelationPayloadList(rand() % 50);
    printf("new tuple created with rowID: %d, and payload: %d\n", newTuple->key, newTuple->payloadList->data);

    return newTuple;
}

tuple* createTupleFromNode(int value, int rowId1, int rowId2){
    tuple *newTuple = malloc(sizeof(struct tuple));

    newTuple->key = value;
    newTuple->payloadList = createRelationPayloadList(rowId1);
    newTuple->payloadList->next = createRelationPayloadList(rowId2);
    //newTuple->payloadList->data = payload;

    return newTuple;
}

relation* createRelation(int hop){
    int relationSize;
    int j = 0;
    /* hop is used to "skip" keys when numbering
     * for example if hop is 3, keys will be created as such: 0,3,6,9 etc. */
    if(hop < 1){
        hop = 1;
    }
    tuple* prevTuple = NULL;
    tuple* newTuple = NULL;

    relation *newRelation = malloc(sizeof(struct relation));

    newRelation->num_tuples = rand() % 5 + 20;        //use for random number of tuples in relation
    newRelation->tuples = malloc(sizeof(struct tuple) * newRelation->num_tuples);

    for(int i = 0; i < newRelation->num_tuples; i++){
        newTuple = createTuple(j);
        newRelation->tuples[i] = *newTuple;
        free(newTuple);
        j = j + hop;
    }

    printf("new relation created with %d tuples\n", newRelation->num_tuples);

    return newRelation;
}


void printRelation(relation* myRelation){
    printf("\nPrinting Relation with %d tuples:\n\n", myRelation->num_tuples);
    for(int i = 0; i < myRelation->num_tuples; i++){
        printf("Tuple with value: %d and rowIDs:", myRelation->tuples[i].key);
        printPayload(myRelation->tuples[i].payloadList);
    }
    printf("\nRelation has %d tuples\n", myRelation->num_tuples);
}

void printPayload(relationPayloadList* payloadList){

    printf(" <%d", payloadList->data);

    payloadList = payloadList->next;
    while(payloadList != NULL){
        printf(", %d", payloadList->data);
        payloadList = payloadList->next;
    }

    printf(">\n");
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

int* pruneRelation(relation* myRelation, char operand, int value){
    int* keyList = malloc(sizeof(int) * myRelation->num_tuples + 1);
    int counter = 0;

    if(operand == '>'){
        for(int i = 0; i < myRelation->num_tuples; i++){
            if(myRelation->tuples[i].payloadList->data > value){
                keyList[counter++] = myRelation->tuples[i].key;
            }
        }
    }else if(operand == '<'){
        for(int i = 0; i < myRelation->num_tuples; i++){
            if(myRelation->tuples[i].payloadList->data < value){
                keyList[counter++] = myRelation->tuples[i].key;
            }
        }
    }else if(operand == '='){
        for(int i = 0; i < myRelation->num_tuples; i++){
            if(myRelation->tuples[i].payloadList->data == value){
                keyList[counter++] = myRelation->tuples[i].key;
            }
        }
    }

    //-1 so we know where keyList ends
    keyList[counter] = -1;
    return keyList;
}

int getSumRelation(relation* myRelation){
    int sum = 0;

    for(int i = 0; i < myRelation->num_tuples; i++){
        sum += myRelation->tuples[i].payloadList->data;
    }

    return sum;
}
