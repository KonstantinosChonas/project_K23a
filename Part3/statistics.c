#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "int.h"
#include "statistics.h"
#include "queries.h"

int getFilterStatistics(relationInfo* relInfo,predicate* curPred, int column, int relName, columnStatistics** statistics){
    char filter = curPred->operation;
    int filterValue = curPred->value;

    columnStatistics* newStatistics = malloc(sizeof(struct columnStatistics));

    if(filter == '='){
        newStatistics->min_value = filterValue;
        newStatistics->max_value = filterValue;
        if(valueExistsInColumn(relInfo, column, relName, filterValue)){
            newStatistics->discrete_values = 1;

            //compromise in case discrete values of a column is 0
//            if(relInfo[relName].colStats[column].discrete_values == 0){
//                newStatistics->value_count = relInfo[relName].colStats[column].value_count / 1;
//            }else
            newStatistics->value_count = statistics[relName][column].value_count / statistics[relName][column].discrete_values;
//            printf("NEW VALUE COUNT: %ld\n", newStatistics->value_count);
//            printf("NEW DISCRETE COUNT: %ld\n", newStatistics->discrete_values);

        }else {
            newStatistics->discrete_values = 0;
            newStatistics->value_count = 0;
        }

    }

    if(filter == '<'){
        if(statistics[relName][column].max_value < filterValue){
            filterValue = statistics[relName][column].max_value;
        }
        newStatistics->min_value = statistics[relName][column].min_value;
        newStatistics->max_value = filterValue;
        newStatistics->discrete_values = (uint64_t)((float)((float) (statistics[relName][column].max_value - filterValue) / (float) ( statistics[relName][column].max_value - statistics[relName][column].min_value) ) * (float)statistics[relName][column].discrete_values);
        newStatistics->value_count =  (uint64_t)((float) ((float)(statistics[relName][column].max_value - filterValue) / (float) ( statistics[relName][column].max_value - statistics[relName][column].min_value) ) * (float)statistics[relName][column].value_count);
//        printf("NEW VALUE COUNT: %ld\n", newStatistics->value_count);
//        printf("NEW DISCRETE COUNT: %ld\n", newStatistics->discrete_values);
    }

    if(filter == '>'){
        if(statistics[relName][column].min_value > filterValue){
            filterValue = statistics[relName][column].min_value;
        }
        newStatistics->min_value = filterValue;
        newStatistics->max_value = statistics[relName][column].max_value;
        newStatistics->discrete_values = (uint64_t)((float)((float) (filterValue - statistics[relName][column].min_value) / (float) ( statistics[relName][column].max_value - statistics[relName][column].min_value) ) * (float)statistics[relName][column].discrete_values);
        newStatistics->value_count =  (uint64_t)((float) ((float)(filterValue - statistics[relName][column].min_value) / (float) ( statistics[relName][column].max_value - statistics[relName][column].min_value) ) * (float)statistics[relName][column].value_count);
//        printf("NEW VALUE COUNT: %ld\n", newStatistics->value_count);
//        printf("NEW DISCRETE COUNT: %ld\n", newStatistics->discrete_values);
    }

    for(int i = 0; i < relInfo[relName].num_cols; i++){
        if(i != column){
            statistics[relName][i].discrete_values = (uint64_t)((float)(statistics[relName][i].discrete_values) * (1.0 - (float)pow((float)(1.0 - (float)((float)newStatistics->value_count/(float)statistics[relName][column].value_count)), ((float)(statistics[relName][i].value_count)/(float)(statistics[relName][i].discrete_values)))));
            statistics[relName][i].value_count = newStatistics->value_count;
        }
    }

    statistics[relName][column].min_value = newStatistics->min_value;
    statistics[relName][column].max_value = newStatistics->max_value;
    statistics[relName][column].value_count = newStatistics->value_count;
    statistics[relName][column].discrete_values = newStatistics->discrete_values;

    free(newStatistics);

    //cost is measured by number of elements
    //printf("returned cost: %ld\n", relInfo[relName].colStats[column].value_count);
    return statistics[relName][column].value_count;
}

int getJoinStatistics(struct relationInfo* relInfo,struct predicate* curPred, int relName1, int relName2, struct columnStatistics** statistics, int updateStatistics){
    int column1 = curPred->leftRelation->payloadList->data;
    int column2 = curPred->rightRelation->payloadList->data;
    uint64_t combinedMin = 0;
    uint64_t combinedMax = 0;
    uint64_t combinedCount = 0;
    uint64_t combinedDiscrete = 0;

    columnStatistics* newStatistics = malloc(sizeof(struct columnStatistics));

    if(relName1 == relName2){
        if(column1 == column2){
            newStatistics->min_value = statistics[relName1][column1].min_value;
            newStatistics->max_value = statistics[relName1][column1].max_value;

            int n = newStatistics->max_value - newStatistics->min_value + 1;

            newStatistics->value_count = (uint64_t)((float)statistics[relName1][column1].value_count * (float)statistics[relName1][column1].value_count / (float)(n));
            newStatistics->discrete_values = statistics[relName1][column1].discrete_values;

            if(updateStatistics){

                for(int i = 0; i < relInfo[relName1].num_cols; i++){
                    if(i != column1){
                        statistics[relName1][i].value_count = newStatistics->value_count;
                    }
                }

                statistics[relName1][column1].min_value = newStatistics->min_value;
                statistics[relName1][column1].max_value = newStatistics->max_value;
                statistics[relName1][column1].value_count = newStatistics->value_count;
                statistics[relName1][column1].discrete_values = newStatistics->discrete_values;
            }

            free(newStatistics);

            if(updateStatistics){
                return statistics[relName1][column1].value_count;
            }else return newStatistics->value_count;
        }
        if(statistics[relName1][column1].min_value < statistics[relName2][column2].min_value){
            combinedMin = statistics[relName2][column2].min_value;
        }else combinedMin = statistics[relName1][column1].min_value;

        newStatistics->min_value = combinedMin;

        if(statistics[relName1][column1].max_value < statistics[relName2][column2].max_value){
            combinedMax = statistics[relName1][column1].max_value;
        }else combinedMax = statistics[relName2][column2].max_value;

        newStatistics->max_value = combinedMax;

        int n = combinedMax - combinedMin + 1;
        combinedCount = (uint64_t)((float)(statistics[relName1][column1].value_count/ (float) n));

        //printf("COMBINED:%ld\n", relInfo[relName1].colStats[column1].value_count);
        newStatistics->value_count = combinedCount;

        combinedDiscrete = (uint64_t)((float)(statistics[relName1][column1].discrete_values) * (1.0 - (float)pow((float)(1.0 - (float)((float)newStatistics->value_count/(float)statistics[relName1][column1].value_count)), ((float)(statistics[relName1][column1].value_count)/(float)(statistics[relName1][column1].discrete_values)))));

        newStatistics->discrete_values = combinedDiscrete;

        /* updating rest of columns for the relation */

        if(updateStatistics){
            for(int i = 0; i < relInfo[relName1].num_cols; i++){
                if(i != column1){
                    statistics[relName1][i].discrete_values = (uint64_t)((float)(statistics[relName1][column1].discrete_values) * (1.0 - (float)pow((float)(1.0 - (float)((float)newStatistics->value_count/(float)statistics[relName1][column1].value_count)), ((float)(statistics[relName1][column1].value_count)/(float)(statistics[relName1][column1].discrete_values)))));
                    statistics[relName1][i].value_count = newStatistics->value_count;
                }
            }

            statistics[relName1][column1].min_value = newStatistics->min_value;
            statistics[relName1][column1].max_value = newStatistics->max_value;
            statistics[relName1][column1].value_count = newStatistics->value_count;
            statistics[relName1][column1].discrete_values = newStatistics->discrete_values;

            statistics[relName2][column2].min_value = newStatistics->min_value;
            statistics[relName2][column2].max_value = newStatistics->max_value;
            statistics[relName2][column2].value_count = newStatistics->value_count;
            statistics[relName2][column2].discrete_values = newStatistics->discrete_values;
        }

//    printf("NEW MIN: %ld\n", newStatistics->min_value);
//    printf("NEW MAX: %ld\n", newStatistics->max_value);
//    printf("NEW VALUE COUNT: %ld\n", newStatistics->value_count);
//    printf("NEW DISCRETE COUNT: %ld\n", newStatistics->discrete_values);

        free(newStatistics);

        //could be used for error handling
        //printf("RETURNING %d FOR REL %d COL %d\n", statistics[relName1][column1].value_count, relName1, column1);
        if(updateStatistics){
            return statistics[relName1][column1].value_count;
        }else return newStatistics->value_count;
    }

    if(statistics[relName1][column1].min_value < statistics[relName2][column2].min_value){
        combinedMin = statistics[relName2][column2].min_value;
    }else combinedMin = statistics[relName1][column1].min_value;

    newStatistics->min_value = combinedMin;

    if(statistics[relName1][column1].max_value < statistics[relName2][column2].max_value){
        combinedMax = statistics[relName1][column1].max_value;
    }else combinedMax = statistics[relName2][column2].max_value;

    newStatistics->max_value = combinedMax;

    int n = combinedMax - combinedMin + 1;
    combinedCount = (uint64_t)((float)(statistics[relName1][column1].value_count * statistics[relName2][column2].value_count) / (float) n);

    //printf("COMBINED:%ld\n", relInfo[relName1].colStats[column1].value_count);
    newStatistics->value_count = combinedCount;

    combinedDiscrete = (uint64_t)((float)(statistics[relName1][column1].discrete_values * statistics[relName2][column2].discrete_values) / (float) n);

    newStatistics->discrete_values = combinedDiscrete;

    /* updating rest of columns for each relation */

    if(updateStatistics){
        for(int i = 0; i < relInfo[relName1].num_cols; i++){
            if(i != column1){
                statistics[relName1][i].discrete_values = (uint64_t)((float)(statistics[relName1][i].discrete_values) * (1.0 - (float)pow((float)(1.0 - (float)((float)newStatistics->discrete_values/(float)statistics[relName1][column1].discrete_values)), (float)((float)(statistics[relName1][i].value_count)/(float)(statistics[relName1][i].discrete_values)))));
                statistics[relName1][i].value_count = newStatistics->value_count;
            }
        }

        for(int i = 0; i < relInfo[relName2].num_cols; i++){
            if(i != column2){
                statistics[relName2][i].discrete_values = (uint64_t)((float)(statistics[relName2][i].discrete_values) * (1.0 - (float)pow((float)(1.0 - (float)((float)newStatistics->discrete_values/(float)statistics[relName2][column2].discrete_values)), (float)((float)(statistics[relName2][i].value_count)/(float)(statistics[relName2][i].discrete_values)))));
                statistics[relName2][i].value_count = newStatistics->value_count;
            }
        }

        statistics[relName1][column1].min_value = newStatistics->min_value;
        statistics[relName1][column1].max_value = newStatistics->max_value;
        statistics[relName1][column1].value_count = newStatistics->value_count;
        statistics[relName1][column1].discrete_values = newStatistics->discrete_values;

        statistics[relName2][column2].min_value = newStatistics->min_value;
        statistics[relName2][column2].max_value = newStatistics->max_value;
        statistics[relName2][column2].value_count = newStatistics->value_count;
        statistics[relName2][column2].discrete_values = newStatistics->discrete_values;
    }



//    printf("NEW MIN: %ld\n", newStatistics->min_value);
//    printf("NEW MAX: %ld\n", newStatistics->max_value);
//    printf("NEW VALUE COUNT: %ld\n", newStatistics->value_count);
//    printf("NEW DISCRETE COUNT: %ld\n", newStatistics->discrete_values);

    free(newStatistics);

    if(updateStatistics){
        return statistics[relName1][column1].value_count;
    }else return newStatistics->value_count;
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

int joinEnumeration(predicate** predicateList, struct relationInfo* relInfo, int predicateNumber, int* relationsArray, int relationNumber){
    int numOfFilters = 0;
    int numOfJoins = 0;
    int predicateOrder[predicateNumber];

    struct columnStatistics** statistics = copyStatistics(relInfo, relationNumber);

    for(int i = 0; i < predicateNumber; i++){
        predicateOrder[i] = -1;
    }

    //to begin with, put all filter predicates in a list, and get their cost
    for(int i = 0; i < predicateNumber; i++){
        if(predicateList[i]->isFilter == 1){
            numOfFilters++;
        }else numOfJoins++;
    }
    predicate* filterPredicates[numOfFilters];
    predicate* joinPredicates[numOfJoins];
    int filterCounter = 0;
    int joinCounter = 0;

    for(int i = 0; i < predicateNumber; i++){
        if(predicateList[i]->isFilter == 1){
            filterPredicates[filterCounter++] = predicateList[i];
        }else joinPredicates[joinCounter++] = predicateList[i];
    }

    int column = 0;
    int relName = 0;
    int filterCost = 0;

    for(int i = 0; i < numOfFilters; i++){
        column = filterPredicates[i]->leftRelation->payloadList->data;
        relName = filterPredicates[i]->leftRelation->key;
        filterCost += getFilterStatistics(relInfo, filterPredicates[i], column, relationsArray[relName], statistics);
        //printf("FILTER COST:%d\n", filterCost);
    }


    //temp fix because sometimes getFilterStatistics gives 0
    //getOriginalStatistics(relInfo, relationsArray, relationNumber, statistics);

    getOptimalPredicateOrder(predicateList, relInfo, predicateNumber, relationsArray, relationNumber, predicateOrder, statistics);

//    printf("OPTIMAL PREDICATE ORDER: ");
//    for(int i = 0; i < predicateNumber; i++){
//        printf("%d ", predicateOrder[i]);
//    }
//    printf(" (-1 means that precicate is filter and should be done first)\n");

    int newIndex = -1;
    predicate* tempPredList[predicateNumber];
    for(int i = 0; i < predicateNumber; i++){
        tempPredList[i] = predicateList[i];
    }

    for(int i = 0; i < predicateNumber; i++){
        if(predicateOrder[i] != -1){
            newIndex = predicateOrder[i];
            //tempPred = predicateList[i];
            predicateList[i] = tempPredList[newIndex];
        }
    }
    getOriginalStatistics(relInfo, relationsArray, relationNumber, statistics);
    for(int i = 0; i < relInfo[0].relation_num_total; i++){
        free(statistics[i]);
    }
    free(statistics);
}

int getOptimalPredicateOrder(struct predicate** predicateList, struct relationInfo* relInfo, int predicateNumber, int* relationsArray, int relationNumber, int* optimalOrder, columnStatistics** statistics){
    int predicateCost[predicateNumber];
    int doneFlag = 1;
    for(int i = 0; i < predicateNumber; i++){
        predicateCost[i] = -1;
    }

    for(int i = 0; i < predicateNumber; i++){
        if(predicateList[i]->isFilter == 0){
//            printf("rel1: %d\n", predicateList[i]->leftRelation->key);
//            printf("rel2: %d\n", predicateList[i]->rightRelation->key);

            if(optimalOrder[i] == - 1){
                doneFlag = 0;
                predicateCost[i] = getJoinStatistics(relInfo, predicateList[i], relationsArray[predicateList[i]->leftRelation->key], relationsArray[predicateList[i]->rightRelation->key], statistics, 0);
                //printf("predicate cost: %d\n", predicateCost[i]);
                //getOriginalStatistics(relInfo, relationsArray, relationNumber, statistics);
            }
        }
    }

    int minPredicateCost = -1;
    int index = -1;

    for(int i = 0; i < predicateNumber; i++){
        if(predicateCost[i] != -1){
            if(minPredicateCost == -1){
                minPredicateCost = predicateCost[i];
                index = i;
            }else{
                if(minPredicateCost > predicateCost[i]){
                    minPredicateCost = predicateCost[i];
                    index = i;
                }
            }
        }
    }

    /*update the statistics */
    if(index != -1){
        int newCost = getJoinStatistics(relInfo, predicateList[index], relationsArray[predicateList[index]->leftRelation->key], relationsArray[predicateList[index]->rightRelation->key], statistics, 1);
    }

    //if doneFlag = 1, it means that all join predicates have been ordered and the function just returns 0
    if(!doneFlag){
        int counter = 0;
        for(int i = 0; i < predicateNumber; i++){
            if(optimalOrder[i] != -1){
                counter++;
            }
        }
        optimalOrder[index] = counter;

        minPredicateCost += getOptimalPredicateOrder(predicateList, relInfo, predicateNumber, relationsArray, relationNumber, optimalOrder, statistics);
    }else minPredicateCost = 0;

    return minPredicateCost;
}


void getOriginalStatistics(struct relationInfo* relInfo, int* relationsArray, int relationNumber, struct columnStatistics** statistics){
    for(int i = 0; i < relationNumber; i++){
        int relName = relationsArray[i];

        for(int j = 0; j < relInfo[relName].num_cols; j++){
            statistics[relName][j].min_value = relInfo[relName].colStats[j].original_min_value;
            statistics[relName][j].max_value = relInfo[relName].colStats[j].original_max_value;
            statistics[relName][j].value_count = relInfo[relName].colStats[j].original_value_count;
            statistics[relName][j].discrete_values = relInfo[relName].colStats[j].original_discrete_values;
        }
    }
}

struct columnStatistics** copyStatistics(struct relationInfo* relInfo, int relationNumber){
    int totalRelations = relInfo[0].relation_num_total;
    struct columnStatistics** statisticsCopy = malloc(sizeof(struct columnStatistics*) * totalRelations);
    for(int i = 0; i < totalRelations; i++){
        statisticsCopy[i] = malloc(sizeof(struct columnStatistics) * relInfo[i].num_cols);
    }

    for(int i = 0; i < totalRelations; i++){
        for(int j = 0; j < relInfo[i].num_cols; j++){
            statisticsCopy[i][j].min_value = relInfo[i].colStats[j].min_value;
            statisticsCopy[i][j].max_value = relInfo[i].colStats[j].max_value;
            statisticsCopy[i][j].discrete_values = relInfo[i].colStats[j].discrete_values;
            statisticsCopy[i][j].value_count = relInfo[i].colStats[j].value_count;
        }
    }
    return statisticsCopy;
}