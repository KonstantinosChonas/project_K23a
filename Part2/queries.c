#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "queries.h"

int parseQueries(char* queryFileName, relationInfo* usedRelations){
    char *line = NULL;
    size_t len = 0;
    ssize_t read;
    char *token;
    char *endCheck;
    FILE *fp;

    char* relations;
    char* predicates;
    char* projections;

    printf("Start of parsing the query file %s...\n", queryFileName);

    fp = fopen(queryFileName, "r");
    if(fp == NULL){
        exit(EXIT_FAILURE);
    }

    while((read = getline(&line, &len, fp)) != -1){
        int isValid = 1;        //used to determine if the query contains formatting errors

        endCheck = strtok(line, "\n");

        if(strcmp(endCheck,"F") == 0){
           printf("continuing to next query set...\n");
            continue;
        }

        token = strtok(line, "|");
        relations = token;
        printf("relations: %s\n", relations);

        token = strtok(NULL, "|");
        predicates = token;
        printf("predicates: %s\n", predicates);

        token = strtok(NULL, "\n");
        projections = token;
        printf("projections: %s\n", projections);

    }

    fclose(fp);
    if (line){
        free(line);
    }
}