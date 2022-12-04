#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "int.h"

//to do: Make it so files don't need to be in same directory as main

relationInfo* parseRelations(){
    printf("entered parser\n");
    FILE* fp;
    char* line = NULL;
    size_t len = 0;
    ssize_t read;
    char* token;
    char* file = NULL;
    char* lineStr = NULL;
    int numRelations = 0;
    printf("Start entering relation files one by one, and enter Done, to end\n");
    fp = stdin;

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

    relInfo = malloc(numRelations * sizeof(struct relationInfo));

    for(int i = 0; i < numRelations; i++){
        char* currFile = fileLocations[i];

        //start reading the binary file
        printf("start reading the files\n");
        FILE* relationFile = NULL;
        relationFile = fopen(currFile, "rb");

        fread(&relInfo[i].num_tuples, sizeof(uint64_t), 1, relationFile);

        fread(&relInfo[i].num_cols, sizeof(uint64_t), 1, relationFile);
        relInfo[i].columns = malloc(relInfo[i].num_cols * sizeof(uint64_t));

//        printf("tuples: %d\n", relInfo[i].num_tuples);
//        printf("columns: %d\n", relInfo[i].num_cols);

        for(int j = 0;  j < relInfo[i].num_cols; j++){
            relInfo[i].columns[j] = malloc(relInfo[i].num_tuples * sizeof(uint64_t));

            for(int k = 0; k < relInfo[i].num_tuples; k++){
                fread(&relInfo[i].columns[j][k], sizeof(uint64_t), 1, relationFile);
            }
        }

    }

//    for(int i = 0; i < numRelations; i++){
//        printf("relation %s loaded\n", fileLocations[i]);
//    }

    return relInfo;
}