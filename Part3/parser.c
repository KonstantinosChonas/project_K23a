#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "int.h"

//to do: Make it so files don't need to be in same directory as main

relationInfo* parseRelations(char* workPath, int* numRel){
    //printf("entered parser\n");
    FILE* fp;
    char* line = NULL;
    size_t len = 0;
    ssize_t read;
    char* token;
    char* file = NULL;
    char* lineStr = NULL;
    int numRelations = 0;
    //printf("Start entering relation files one by one, and enter Done, to end\n");

    if(workPath == NULL){
        fp = stdin;
    }else
        fp = fopen(workPath, "r");

    relationInfo* relInfo = NULL;
    char** fileLocations = NULL;

    while((read = getline(&line, &len, fp)) != -1){
       lineStr = strtok(line, "\n");
       if(strcmp(lineStr, "Done") == 0)
           break;

       fileLocations = realloc(fileLocations, (numRelations + 1) * sizeof (char**));
       fileLocations[numRelations] = malloc(strlen(line) + 1);

       strcpy(fileLocations[numRelations], line);

       numRelations++;
    }
    fclose(fp);
    *numRel = numRelations;
    relInfo = malloc(numRelations * sizeof(struct relationInfo));

    for(int i = 0; i < numRelations; i++){
        char* currFile = fileLocations[i];

        //start reading the binary file
        //printf("parsing file: %s\n", fileLocations[i]);
        FILE* relationFile = NULL;
        relationFile = fopen(currFile, "rb");
        fread(&relInfo[i].num_tuples, sizeof(uint64_t), 1, relationFile);
        fread(&relInfo[i].num_cols, sizeof(uint64_t), 1, relationFile);
        relInfo[i].columns = malloc(relInfo[i].num_cols * sizeof(uint64_t));
        relInfo[i].colStats = malloc(relInfo[i].num_cols * sizeof(columnStatistics));
        uint64_t min = 0;
        uint64_t max = 0;


//        printf("tuples: %d\n", relInfo[i].num_tuples);
//        printf("columns: %d\n", relInfo[i].num_cols);

        for(int j = 0;  j < relInfo[i].num_cols; j++){
            relInfo[i].columns[j] = malloc(relInfo[i].num_tuples * sizeof(uint64_t));
            relInfo[i].colStats[j].value_count = relInfo[i].num_tuples;

            for(int k = 0; k < relInfo[i].num_tuples; k++){
                fread(&relInfo[i].columns[j][k], sizeof(uint64_t), 1, relationFile);
                if(k == 0){
                    min = relInfo[i].columns[j][k]; //first value is min by default
                }
                if(relInfo[i].columns[j][k] < min){
                    min = relInfo[i].columns[j][k];
                }
                if(relInfo[i].columns[j][k] > max){
                    max = relInfo[i].columns[j][k];
                }
            }
            relInfo[i].colStats[j].min_value = min;
            relInfo[i].colStats[j].max_value = max;

            /* using min and max values to get discrete values */
            uint64_t discreteValues = 0;
            int arraySize = 0;
            if((max - min + 1) > N_DISCRETE){
                arraySize = N_DISCRETE;
            }else arraySize = max - min + 1;

            int discreteValuesArray[arraySize];
            for(int k = 0; k < arraySize; k++){
                discreteValuesArray[k] = 0;
            }

            /* if 0 then we have encountered that value again, else add it to array */
            for(int k = 0; k < relInfo[i].num_tuples; k++){
                if(discreteValuesArray[(relInfo[i].columns[j][k] - min) % arraySize] == 0){
                    discreteValuesArray[(relInfo[i].columns[j][k] - min) % arraySize] = 1;
                    discreteValues++;
                }
            }
            relInfo[i].colStats[j].discrete_values = discreteValues;
        }

        fclose(relationFile);
    }

//    for(int i = 0; i < numRelations; i++){
//        printf("relation %s loaded\n", fileLocations[i]);
//    }

    for(int i = 0; i < numRelations; i++){
        free(fileLocations[i]);
    }
    free(fileLocations);
    if(line){
        free(line);
    }

    return relInfo;
}

void relationInfoDelete(relationInfo* relInfo, int relationNum){
    for(int i = 0; i < relationNum; i++){
        for(int j = 0; j < relInfo[i].num_cols; j++){
            free(relInfo[i].columns[j]);
        }
        free(relInfo[i].columns);
    }
    free(relInfo);
    return;
}