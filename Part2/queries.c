#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "queries.h"
#include "intermediate.h"

int parseQueries(char* queryFileName, relationInfo* relInfo, int relationNum){
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

    // printf("Value in r10, column 2, row 191: %d\n", relInfo[10].columns[1][190]);
    fp = fopen(queryFileName, "r");
    if(fp == NULL){
        exit(EXIT_FAILURE);
    }

    while((read = getline(&line, &len, fp)) != -1){
        int isValid = 1;        //used to determine if the query contains formatting errors

        endCheck = strtok(line, "\n");

        if(strcmp(endCheck,"F") == 0){
        //    printf("continuing to next query set...\n");
            continue;
        }

        token = strtok(line, "|");
        relations = token;
        // printf("relations: %s\n", relations);

        token = strtok(NULL, "|");
        predicates = token;
        // printf("predicates: %s\n", predicates);

        token = strtok(NULL, "\n");
        projections = token;
        // printf("projections: %s\n", projections);


        /* code for relation handling */

        // printf("Start of relation handling\n");


        int relationCounter = 0;
        char tempRelations[20];
        strcpy(tempRelations, relations);
        // printf("%s\n", tempRelations);
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
            i++;
        }

        relationInfo usedRelations[relationCounter];

        //get appropriate relations from relInfo and put them in usedRelations in correct order
        for(i = 0; i < relationCounter; i++){
            usedRelations[i] = relInfo[relationsArray[i]];
//            printf("PRINTING VALUE %d FROM RELATION: %d\n", usedRelations[i].columns[0][100], relationsArray[i]);
        }

        /* code for predicates handling */

        // printf("Start of predicate handling\n");

        int predicateCounter = 0;
        char tempPredicates[50];
        strcpy(tempPredicates, predicates);
        // printf("%s\n", predicates);

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
        predicatesArray[0] = malloc(50 * sizeof(char));
        strcpy(predicatesArray[0], token);
        //predicatesArray[0] = token;

        i = 1;
        while(token != NULL){
            token = strtok(NULL, "&");
            if(token == NULL){
                break;
            }
            predicatesArray[i] = malloc(50 * sizeof(char));
            strcpy(predicatesArray[i], token);
            // printf("%s\n", predicatesArray[i]);
            if(isFilter(predicatesArray[i])){
                // printf("predicate: %s, is filter\n", predicatesArray[i]);
            }else
                // printf("predicate: %s, is not filter\n", predicatesArray[i]);
            i++;
        }

        predicate* predicateStructArray[predicateCounter];

        for(i = 0; i < predicateCounter; i++){
            predicateStructArray[i] = createPredicate(predicatesArray[i], i);
            // printf("predicate: %s, isFilter: %d\n",predicateStructArray[i]->predicate,predicateStructArray[i]->isFilter);
        }

        // for(i = 0; i < predicateCounter; i++){               //TODO edo prokalei thema na to eleutheroso argotera
        //     free(predicatesArray[i]);
        // }

        /* code for projection handling */

        // printf("Start of projection handling\n");

        int projectionCounter = 0;
        char tempProjections[50];
        strcpy(tempProjections, projections);
        token = strtok(tempProjections, " \t");
        projectionCounter++;
        while(token != NULL){
            token = strtok(NULL, " \t");
            if(token == NULL){
                break;
            }
            projectionCounter++;
        }
        char* projectionsArray[projectionCounter];

        token = strtok(projections, " \t");
        projectionsArray[0] = malloc(50 * sizeof(char));
        strcpy(projectionsArray[0], token);

        // printf("%s\n", projectionsArray[0]);


        i = 1;
        while(token != NULL){
            token = strtok(NULL, " \t");
            if(token == NULL){
                break;
            }
            projectionsArray[i] = malloc(50 * sizeof(char));
            strcpy(projectionsArray[i], token);
            // printf("%s\n", projectionsArray[i]);
            i++;
        }
        /*          adding to inermediate           */
/*----------------------------------------------------------------*/
        intermediate *rowidarray=intermediateCreate(relationCounter);

        printf("NUM OF RELATIONS=%d\n",relationCounter);
        int empty=1;
        for(int i=0 ; i<predicateCounter ; i++)                 // apply filter first
        {
            if ( predicateStructArray[i]->isFilter==1)
            {
                empty=0;
                applyFilter(&relInfo[relationsArray[predicateStructArray[i]->leftRel]],rowidarray,predicateStructArray[i]->predicate);
                predicateStructArray[i]->done=1;
            }
        }

        printf("key %d, payload %d\n",predicateStructArray[0]->leftRelation->key,predicateStructArray[0]->leftRelation->payloadList->data);

        for(int i=0 ; i<predicateCounter ; i++){

            if (empty==0){
                if(rowidarray->row_ids[predicateStructArray[i]->leftRel]==NULL){
                    if (rowidarray->row_ids[predicateStructArray[i]->rightRel]==NULL){
                        continue;
                    }
                                                                    //only left is null

                    predicateStructArray[i]->done=1;
                    relation *rel1=relationInfoToRelation(&relInfo[relationsArray[predicateStructArray[i]->leftRel]],predicateStructArray[i]->leftRelation->payloadList->data),
                    *rel2=intermediateToRelation(rowidarray,&relInfo[relationsArray[predicateStructArray[i]->rightRel]],predicateStructArray[i]->rightRelation->payloadList->data,relationsArray[predicateStructArray[i]->rightRel]);
                    
                    
                    result *res=PartitionedHashJoin(rel1,rel2);
                    if(biggerRel(rel1,rel2)){
                        addToArray(rowidarray,res->r,predicateStructArray[i]->rightRel,predicateStructArray[i]->leftRel);
                    }
                    else    addToArray(rowidarray,res->r,predicateStructArray[i]->leftRel,predicateStructArray[i]->rightRel);

                }
                else if (rowidarray->row_ids[predicateStructArray[i]->rightRel]==NULL){
                    
                    //only right is null

                    predicateStructArray[i]->done=1;
                    relation *rel1=intermediateToRelation(rowidarray,&relInfo[relationsArray[predicateStructArray[i]->leftRel]],predicateStructArray[i]->leftRelation->payloadList->data,relationsArray[predicateStructArray[i]->leftRel]),
                    *rel2=relationInfoToRelation(&relInfo[relationsArray[predicateStructArray[i]->rightRel]],predicateStructArray[i]->rightRelation->payloadList->data);
                    

                    result *res=PartitionedHashJoin(rel1,rel2);
                    if(biggerRel(rel1,rel2)){
                        addToArray(rowidarray,res->r,predicateStructArray[i]->rightRel,predicateStructArray[i]->leftRel);
                    }
                    else    addToArray(rowidarray,res->r,predicateStructArray[i]->leftRel,predicateStructArray[i]->rightRel);

                }
            }
            else{
                relation *rel1=relationInfoToRelation(&relInfo[relationsArray[predicateStructArray[i]->leftRel]],predicateStructArray[i]->leftRelation->payloadList->data),*rel2=relationInfoToRelation(&relInfo[relationsArray[predicateStructArray[i]->rightRel]],predicateStructArray[i]->rightRelation->payloadList->data);

                if (biggerRel(rel1,rel2)){
                    result *res=PartitionedHashJoin(rel1,rel2);
                    addToArray(rowidarray,res->r,predicateStructArray[i]->rightRel,predicateStructArray[i]->leftRel);

                }
                else{
                    result *res=PartitionedHashJoin(rel1,rel2);
                    addToArray(rowidarray,res->r,predicateStructArray[i]->leftRel,predicateStructArray[i]->rightRel);
                }
                predicateStructArray[i]->done=1;
                empty=0;
            }

        }



        // printIntermediate(rowidarray);

        return 1;                                       //TODO thelo na trexei gia ena pros to paron kai meta tha doume gia perissotera

        // intermediateDelete(rowidarray);
/*----------------------------------------------------------------*/
        /*            end of  intermediate          */
        //freeing memory used in query
        if(projectionsArray != NULL){
            for(int j = 0; j < i; j++){
                free(projectionsArray[j]);
            }
        }

        for(i = 0; i < predicateCounter; i++){
            if(predicateStructArray[i]->rightRelation != NULL){
                tupleDelete(predicateStructArray[i]->rightRelation);
            }

            if(predicateStructArray[i]->leftRelation != NULL){
                tupleDelete(predicateStructArray[i]->leftRelation);
            }
            free(predicateStructArray[i]);
        }
    }



    printf("all done with query handling\n");

    fclose(fp);
    if (line){
        free(line);
    }
}


int isFilter(char* str){            //      if str is filter returns 1

    int flag=0;
    for ( int i=0 ; str[i] ; i++)
    {
        if(str[i]=='<' || str[i]=='>' || str[i]=='=')
            flag = 1;

        if (flag)
            if (str[i]=='.')
                return 0;
    }

    return 1;
}

int sameRel(char* predicate){              /*      an eiani idio relation epistrefei 1 an einai idio relation kai idio column epistrefei 2 allios epistrefei 0     */

    int rel1=0,rel2=0,dot=0,op=0,col1=0,col2=0;
    int i=0;

    while(predicate[i]){
        if (predicate[i]=='.'){
            dot++;
            i++;
            continue;
        }
        if (predicate[i]=='=' || predicate[i]=='<' || predicate[i]=='>'){
            op++;
            i++;
            continue;
        }
        if(dot==0 && predicate==0)
            rel1=10*rel1+predicate[i]-'0';

        if (op==1 && dot==1)
            rel2=10*rel2+predicate[i]-'0';
        if (dot==1 && op==0)
            col1=10*col1+predicate[i]-'0';
        if (dot==2 && op==1)
            col2=10*col2+predicate[i]-'0';

        i++;
    }


    if (rel1==rel2 && col1==col2)
        return 2;

    return rel1==rel2;

}




relation* relColumn(relation* rel, int col){            /*      dexetai os orisma ena relation kai ena column kai epistrefei relation me mono auto to column        */

    relation* new=createEmptyRelation(rel->num_tuples);

    int data=0;
    relationPayloadList* temp;

    for(int i=0 ; i<rel->num_tuples ; i++){

        temp=rel->tuples[i].payloadList;

        for(int j=0; j<col ; j++){

            temp=temp->next;

        }

        addToPayloadList(new->tuples[i].payloadList,temp->data);

    }

    return new;

}

predicate* createPredicate(char* predicateStr, int order){
    predicate* newPredicate = malloc(sizeof(predicate));
    int isFilter = 0;
    int flag = 0;
    int filterFlag = 1;
    char tempPredicates[50];

    newPredicate->done=0;

    strcpy(tempPredicates, predicateStr);


    newPredicate->predicate=predicateStr;

    for ( int i=0 ; predicateStr[i] ; i++)
    {
        if(predicateStr[i]=='<' || predicateStr[i]=='>' || predicateStr[i]=='='){
           newPredicate->operation = predicateStr[i];
           flag = 1;
        }

        if (flag) {
            if (predicateStr[i] == '.') {
                filterFlag = 0;
                break;
            }
        }

    }
    if(filterFlag == 0){
        isFilter = 0;
        newPredicate->isFilter = isFilter;
    }else {
        isFilter = 1;
        newPredicate->isFilter = isFilter;
    }

    char* leftToken;
    char* rightToken;
    leftToken = strtok(tempPredicates, "><=");
    newPredicate->leftRel=returnRelation(leftToken);
    rightToken = strtok(NULL, "\0");
    newPredicate->rightRel=returnRelation(rightToken);
    char* leftRelationStr = strtok(leftToken, ".");
    tuple* leftRelation = createTuple(atoi(leftRelationStr));
    char* leftRelationColumnStr = strtok(NULL, "\0");
    leftRelation->payloadList->data = atoi(leftRelationColumnStr);
    newPredicate->leftRelation = leftRelation;

    if(isFilter != 1){
        char* rightRelationStr = strtok(rightToken, ".");
        tuple* rightRelation = createTuple(atoi(rightRelationStr));
        char* rightRelationColumnStr = strtok(NULL, "\0");
        rightRelation->payloadList->data = atoi(rightRelationColumnStr);
        newPredicate->rightRelation = rightRelation;
    }else{
        newPredicate->rightRelation = NULL;
        newPredicate->value = atoi(rightToken);
    }
    newPredicate->order = order;

//    printf("now printing predicate struct %s:\nleft relation: %d, left column %d\n",predicateStr, newPredicate->leftRelation->key, newPredicate->leftRelation->payloadList->data);
//    printf("isFilter: %d\n", newPredicate->isFilter);
//    printf("operator: %c\n", newPredicate->operation);
//
//    if(!newPredicate->isFilter){
//        printf("right relation: %d, right column %d\n", newPredicate->rightRelation->key, newPredicate->rightRelation->payloadList->data);
//
//    }else{
//        printf("value: %d\n", newPredicate->value);
//    }

    return newPredicate;
}




int biggerRel(relation* rel1,relation* rel2){               /* rel1>rel2 return 0 else return 1*/


    if (rel1->num_tuples > rel2->num_tuples)    return 0;
    return 1;

}



int returnRelation(char *str){      // str is of type 0.1
    int rel=0;

    for (int i=0 ; str[i] ; i++){
        if (str[i]=='.')
            return rel;
    rel=10*rel + str[i]-'0';
    }


}


relation* relationInfoToRelation(relationInfo* relin,int column){           // metatrepei ena relation info se relation (voithitiko gia phj)

    relation* rel=createEmptyRelation(relin->num_tuples);


    for (int i=0 ; i<relin->num_tuples ; i++){
        for ( int j=0 ; j<relin->num_cols ; j++){
            addToPayloadList(rel->tuples[i].payloadList, relin->columns[column][j]);
        }
    }

}



relation* intermediateToRelation(intermediate *rowidarray, relationInfo *relInfo,int column,int relname){


    relation *rel=createEmptyRelation(rowidarray->num_rows);


    for( int i=0; i<rowidarray->num_rows ; i++){

        rel->tuples[i].key=i;
        rel->tuples[i].payloadList->data=relInfo->columns[column][rowidarray->row_ids[relname][i]];

    }

    return rel;


}



int returnColumn(char* predicate){

    int col=0;

    int flag=0;

    for (int i=0 ; predicate[i] ; i++){
        if (predicate[i]=='.'){
            flag=1;
        }
    col=10*col + predicate[i]-'0';
    }


    return col;
}


