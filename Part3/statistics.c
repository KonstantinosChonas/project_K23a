#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "int.h"
#include "statistics.h"
#include "queries.h"

int getFilterStatistics(relationInfo* relInfo,predicate* curPred, int column, int relName,columnStatistics* newStatistics){
    char filter = curPred->operation;
    int filterValue = curPred->value;

    printf("relname %d max %ld  count %ld discrete %ld\n", relName, relInfo[relName].colStats[column].max_value, relInfo[relName].colStats[column].value_count, relInfo[relName].colStats[column].discrete_values);
    if(filter == '='){
        newStatistics->min_value = filterValue;
        newStatistics->max_value = filterValue;
        if(valueExistsInColumn(relInfo, column, relName, filterValue)){
            newStatistics->discrete_values = 1;
            newStatistics->value_count = relInfo[relName].colStats[column].value_count / relInfo[relName].colStats[column].discrete_values;
            printf("NEW VALUE COUNT: %ld\n", newStatistics->value_count);
            printf("NEW DISCRETE COUNT: %ld\n", newStatistics->discrete_values);

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
        printf("NEW VALUE COUNT: %ld\n", newStatistics->value_count);
        printf("NEW DISCRETE COUNT: %ld\n", newStatistics->discrete_values);
    }

    if(filter == '>'){
        if(relInfo[relName].colStats[column].min_value > filterValue){
            filterValue = relInfo[relName].colStats[column].min_value;
        }
        newStatistics->min_value = filterValue;
        newStatistics->max_value = relInfo[relName].colStats[column].max_value;
        newStatistics->discrete_values = (uint64_t)((float)((float) (filterValue - relInfo[relName].colStats[column].min_value) / (float) ( relInfo[relName].colStats[column].max_value - relInfo[relName].colStats[column].min_value) ) * (float)relInfo[relName].colStats[column].discrete_values);
        newStatistics->value_count =  (uint64_t)((float) ((float)(filterValue - relInfo[relName].colStats[column].min_value) / (float) ( relInfo[relName].colStats[column].max_value - relInfo[relName].colStats[column].min_value) ) * (float)relInfo[relName].colStats[column].value_count);
        printf("NEW VALUE COUNT: %ld\n", newStatistics->value_count);
        printf("NEW DISCRETE COUNT: %ld\n", newStatistics->discrete_values);
    }

    for(int i = 0; i < relInfo[relName].num_cols; i++){
        if(i != column){
            relInfo[relName].colStats[i].discrete_values = (uint64_t)((float)(relInfo[relName].colStats[i].discrete_values) * (1.0 - pow((float)(1.0 - (float)(newStatistics->value_count/relInfo[relName].colStats[column].value_count)), ((float)(relInfo[relName].colStats[column].value_count)/(float)(relInfo[relName].colStats[column].discrete_values)))));
            relInfo[relName].colStats[i].value_count = newStatistics->value_count;
        }
    }

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
