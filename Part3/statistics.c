#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "int.h"
#include "statistics.h"
#include "queries.h"

int getFilterStatistics(relationInfo* relInfo,predicate* curPred, int column, int relName){
    char filter = curPred->operation;
    int filterValue = curPred->value;

    columnStatistics* newStatistics = malloc(sizeof(struct columnStatistics));

    if(filter == '='){
        newStatistics->min_value = filterValue;
        newStatistics->max_value = filterValue;
        if(valueExistsInColumn(relInfo, column, relName, filterValue)){
            newStatistics->discrete_values = 1;
            newStatistics->value_count = relInfo[relName].colStats[column].value_count / relInfo[relName].colStats[column].discrete_values;
//            printf("NEW VALUE COUNT: %ld\n", newStatistics->value_count);
//            printf("NEW DISCRETE COUNT: %ld\n", newStatistics->discrete_values);

        }else {
            newStatistics->discrete_values = 0;
            newStatistics->value_count = 0;
        }

    }

    if(filter == '<'){
        if(relInfo[relName].colStats[column].max_value < filterValue){
            filterValue = relInfo[relName].colStats[column].max_value;
        }
        newStatistics->min_value = relInfo[relName].colStats[column].min_value;
        newStatistics->max_value = filterValue;
        newStatistics->discrete_values = (uint64_t)((float)((float) (relInfo[relName].colStats[column].max_value - filterValue) / (float) ( relInfo[relName].colStats[column].max_value - relInfo[relName].colStats[column].min_value) ) * (float)relInfo[relName].colStats[column].discrete_values);
        newStatistics->value_count =  (uint64_t)((float) ((float)(relInfo[relName].colStats[column].max_value - filterValue) / (float) ( relInfo[relName].colStats[column].max_value - relInfo[relName].colStats[column].min_value) ) * (float)relInfo[relName].colStats[column].value_count);
//        printf("NEW VALUE COUNT: %ld\n", newStatistics->value_count);
//        printf("NEW DISCRETE COUNT: %ld\n", newStatistics->discrete_values);
    }

    if(filter == '>'){
        if(relInfo[relName].colStats[column].min_value > filterValue){
            filterValue = relInfo[relName].colStats[column].min_value;
        }
        newStatistics->min_value = filterValue;
        newStatistics->max_value = relInfo[relName].colStats[column].max_value;
        newStatistics->discrete_values = (uint64_t)((float)((float) (filterValue - relInfo[relName].colStats[column].min_value) / (float) ( relInfo[relName].colStats[column].max_value - relInfo[relName].colStats[column].min_value) ) * (float)relInfo[relName].colStats[column].discrete_values);
        newStatistics->value_count =  (uint64_t)((float) ((float)(filterValue - relInfo[relName].colStats[column].min_value) / (float) ( relInfo[relName].colStats[column].max_value - relInfo[relName].colStats[column].min_value) ) * (float)relInfo[relName].colStats[column].value_count);
//        printf("NEW VALUE COUNT: %ld\n", newStatistics->value_count);
//        printf("NEW DISCRETE COUNT: %ld\n", newStatistics->discrete_values);
    }

    for(int i = 0; i < relInfo[relName].num_cols; i++){
        if(i != column){
            relInfo[relName].colStats[i].discrete_values = (uint64_t)((float)(relInfo[relName].colStats[i].discrete_values) * (1.0 - (float)pow((float)(1.0 - (float)((float)newStatistics->value_count/(float)relInfo[relName].colStats[column].value_count)), ((float)(relInfo[relName].colStats[i].value_count)/(float)(relInfo[relName].colStats[i].discrete_values)))));
            relInfo[relName].colStats[i].value_count = newStatistics->value_count;
        }
    }

    relInfo[relName].colStats[column] = *newStatistics;
    free(newStatistics);

    //cost is measured by number of elements
    //printf("returned cost: %ld\n", relInfo[relName].colStats[column].value_count);
    return relInfo[relName].colStats[column].value_count;
}

int getJoinStatistics(struct relationInfo* relInfo,struct predicate* curPred, int relName1, int relName2){
    int column1 = curPred->leftRelation->payloadList->data;
    int column2 = curPred->rightRelation->payloadList->data;
    uint64_t combinedMin = 0;
    uint64_t combinedMax = 0;
    uint64_t combinedCount = 0;
    uint64_t combinedDiscrete = 0;

    columnStatistics* newStatistics = malloc(sizeof(struct columnStatistics));


    if(relInfo[relName1].colStats[column1].min_value < relInfo[relName2].colStats[column2].min_value){
        combinedMin = relInfo[relName2].colStats[column2].min_value;
    }else combinedMin = relInfo[relName1].colStats[column1].min_value;

    newStatistics->min_value = combinedMin;

    if(relInfo[relName1].colStats[column1].max_value < relInfo[relName2].colStats[column2].max_value){
        combinedMax = relInfo[relName1].colStats[column1].max_value;
    }else combinedMax = relInfo[relName2].colStats[column2].max_value;

    newStatistics->max_value = combinedMax;

    int n = combinedMax - combinedMin + 1;
    combinedCount = (uint64_t)((float)(relInfo[relName1].colStats[column1].value_count * relInfo[relName2].colStats[column2].value_count) / (float) n);

    newStatistics->value_count = combinedCount;

    combinedDiscrete = (uint64_t)((float)(relInfo[relName1].colStats[column1].discrete_values * relInfo[relName2].colStats[column2].discrete_values) / (float) n);

    newStatistics->discrete_values = combinedDiscrete;

    /* updating rest of columns for each relation */

    for(int i = 0; i < relInfo[relName1].num_cols; i++){
        if(i != column1){
            relInfo[relName1].colStats[i].discrete_values = (uint64_t)((float)(relInfo[relName1].colStats[i].discrete_values) * (1.0 - (float)pow((float)(1.0 - (float)((float)newStatistics->discrete_values/(float)relInfo[relName1].colStats[column1].discrete_values)), (float)((float)(relInfo[relName1].colStats[i].value_count)/(float)(relInfo[relName1].colStats[i].discrete_values)))));
            relInfo[relName1].colStats[i].value_count = newStatistics->value_count;
        }
    }

    for(int i = 0; i < relInfo[relName2].num_cols; i++){
        if(i != column2){
            relInfo[relName2].colStats[i].discrete_values = (uint64_t)((float)(relInfo[relName2].colStats[i].discrete_values) * (1.0 - (float)pow((float)(1.0 - (float)((float)newStatistics->discrete_values/(float)relInfo[relName2].colStats[column2].discrete_values)), (float)((float)(relInfo[relName2].colStats[i].value_count)/(float)(relInfo[relName2].colStats[i].discrete_values)))));
            relInfo[relName2].colStats[i].value_count = newStatistics->value_count;
        }
    }

//    printf("NEW MIN: %ld\n", newStatistics->min_value);
//    printf("NEW MAX: %ld\n", newStatistics->max_value);
//    printf("NEW VALUE COUNT: %ld\n", newStatistics->value_count);
//    printf("NEW DISCRETE COUNT: %ld\n", newStatistics->discrete_values);

    relInfo[relName1].colStats[column1] = *newStatistics;
    relInfo[relName2].colStats[column2] = *newStatistics;
    free(newStatistics);

    //could be used for error handling
    return 1;
}

int valueExistsInColumn(relationInfo* relInfo, int column, int relName, int value){
    int curValue = 0;
    int foundValue = 0;

    for(int i = 0; i < relInfo[relName].num_tuples; i++){
        curValue = relInfo[relName].columns[column][i];
        if(curValue == value){
            foundValue = 1;
            break;
        }
    }
    return foundValue;
}

int joinEnumeration(predicate** predicateList, struct relationInfo* relInfo, int predicateNumber, int* relationsArray){
    int numOfFilters = 0;

    //to begin with, put all filter predicates in a list, and get their cost
    for(int i = 0; i < predicateNumber; i++){
        if(predicateList[i]->isFilter == 1){
            numOfFilters++;
        }
    }
    predicate* filterPredicates[numOfFilters];
    int counter = 0;
    for(int i = 0; i < predicateNumber; i++){
        if(predicateList[i]->isFilter == 1){
            filterPredicates[counter++] = predicateList[i];
        }
    }

    int column = 0;
    int relName = 0;
    int filterCost = 0;

    for(int i = 0; i < numOfFilters; i++){
        column = filterPredicates[i]->leftRelation->payloadList->data;
        relName = filterPredicates[i]->leftRelation->key;
        filterCost += getFilterStatistics(relInfo, filterPredicates[i], column, relationsArray[relName]);
        //printf("FILTER COST:%d\n", filterCost);
    }
}