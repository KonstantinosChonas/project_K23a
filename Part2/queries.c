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


        /* code for relation handling */

        int relationCounter = 0;
        char tempRelations[20];
        strcpy(tempRelations, relations);
        printf("%s\n", tempRelations);
        token = strtok(tempRelations, " \t");
        relationCounter++;
        while(token != NULL){
            token = strtok(NULL, " \t");
            if(token == NULL){
                break;
            }
            relationCounter++;
        }
        int relationsArray[relationCounter];

        token = strtok(relations, " \t");
        relationsArray[0] = atoi(token);

        int i = 1;
        while(token != NULL){
            token = strtok(NULL, " \t");
            if(token == NULL){
                break;
            }
            relationsArray[i] = atoi(token);
            printf("%d\n", relationsArray[i]);
            i++;
        }

        /* code for predicates handling */

        printf("start of predicate handling\n");

        int predicateCounter = 0;
        char tempPredicates[20];
        strcpy(tempPredicates, predicates);
        printf("%s\n", predicates);

        token = strtok(tempPredicates, "&");
        predicateCounter++;
        while(token != NULL){
            token = strtok(NULL, "&");
            if(token == NULL){
                break;
            }
            predicateCounter++;
        }
        char* predicatesArray[predicateCounter+1];

        token = strtok(predicates, "&");
        predicatesArray[0] = malloc(20 * sizeof(char));
        strcpy(predicatesArray[0], token);
        //predicatesArray[0] = token;

        i = 1;
        while(token != NULL){
            token = strtok(NULL, "&");
            if(token == NULL){
                break;
            }
            predicatesArray[i] = malloc(20 * sizeof(char));
            strcpy(predicatesArray[i], token);
            printf("%s\n", predicatesArray[i]);
            i++;
        }

        for(i = 0; i < predicateCounter; i++){
            free(predicatesArray[i]);
        }
    }

    printf("all done with query handling\n");

    fclose(fp);
    if (line){
        free(line);
    }
}