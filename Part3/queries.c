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
    char resultBuffer[1000] = "";
    char numBuffer[20] = "";

    char* relations;
    char* predicates;
    char* projections;

    //printf("Start of parsing the query file %s...\n", queryFileName);

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
            printf("%s", resultBuffer);
            resultBuffer[0] = '\0';
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
        //printf("TEMPPEICAT$ES:%s\n", tempPredicates);

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
        for(int j = 0; j < predicateCounter; j++){
            predicatesArray[j] = malloc(50 * sizeof(char));
        }

        i = 0;

        token = strtok(predicates, "&");
        strcpy(predicatesArray[0], token);
        //printf("PREDICATESARRAY[%d]= %s \n",i,token);
//        if(isFilter(predicatesArray[i])){
//                    printf("predicate: %s, is filter\n", predicatesArray[i]);
//                }else
//                    printf("predicate: %s, is not filter\n", predicatesArray[i]);
            
        
        while(token != NULL){
            
            token = strtok(NULL, "&");

            if(token == NULL){
                break;
            }
            else{
                i++;
                strcpy(predicatesArray[i], token);
//                printf("PREDICATESARRAY[%d]= %s \n",i,token);
//                if(isFilter(predicatesArray[i])){
//                    printf("predicate: %s, is filter\n", predicatesArray[i]);
//                }else
//                    printf("predicate: %s, is not filter\n", predicatesArray[i]);
            }
        }
        

        predicate* predicateStructArray[predicateCounter];

        for(i = 0; i < predicateCounter; i++){
            predicateStructArray[i] = createPredicate(predicatesArray[i], i);
            if(predicateStructArray[i]->operation == '>'){
                getFilterStatistics(relInfo, predicateStructArray[i], predicateStructArray[i]->leftRelation->payloadList->data, relationsArray[predicateStructArray[i]->leftRelation->key]);
            }else{
                if(predicateStructArray[i]->isFilter != 1){
                    getJoinStatistics(relInfo, predicateStructArray[i], relationsArray[predicateStructArray[i]->leftRelation->key], relationsArray[predicateStructArray[i]->rightRelation->key]);

                }
            }

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

        //printf("NUM OF RELATIONS=%d\n",relationCounter);
        int empty=1;

        int done_counter=0;


        for(int i=0 ; i<predicateCounter ; i++)                 // apply filter first
        {
            if ( predicateStructArray[i]->isFilter==1)
            {
                empty=0;
                applyFilter(&relInfo[relationsArray[predicateStructArray[i]->leftRel]],rowidarray,predicateStructArray[i]->predicate);
                predicateStructArray[i]->done=1;
                done_counter++;
            }
        }

        //printf("key %d, payload %d\n",predicateStructArray[0]->leftRelation->key,predicateStructArray[0]->leftRelation->payloadList->data);

        while(done_counter!=predicateCounter){


            for(int i=0 ; i<predicateCounter ; i++){
                // printf("CURRENT PREDICATE %s predicate counter %d, done counter %d \n", predicateStructArray[i]->predicate, predicateCounter,done_counter);
                if (predicateStructArray[i]->done==1) continue;
                //printf("first\n");
                if (empty==0){
//                    if(sameRel(predicatesArray[i])){
//                        selfJoin(relInfo, rowidarray, predicateStructArray[i]);
//                        predicateStructArray[i]->done=1;
//                        done_counter++;
//                        continue;
//                    }
                    // printf("2\n");
                    // if (rowidarray->row_ids[predicateStructArray[i]->rightRel]==NULL && rowidarray->row_ids[predicateStructArray[i]->leftRel]==NULL)
                    // printf("blabla\n");
                    if(rowidarray->row_ids[predicateStructArray[i]->leftRel]==NULL && (rowidarray->row_ids[predicateStructArray[i]->rightRel]!=NULL)){
                    //    printf("3\n");
                        if (rowidarray->row_ids[predicateStructArray[i]->rightRel]==NULL){
                            continue;
                        }
                        //only left is null
                        // printf(" done %s\n",predicatesArray[i]);
                        predicateStructArray[i]->done=1;
                        done_counter++;
                        relation *rel1=relationInfoToRelation(&relInfo[relationsArray[predicateStructArray[i]->leftRel]],predicateStructArray[i]->leftRelation->payloadList->data),
                        *rel2=intermediateToRelation(rowidarray,&relInfo[relationsArray[predicateStructArray[i]->rightRel]],predicateStructArray[i]->rightRelation->payloadList->data,predicateStructArray[i]->rightRel);
                        
                        relation *res=PartitionedHashJoin(rel1,rel2);
                        if(res == NULL){
                            break;
                        }
                        if(biggerRel(rel1,rel2)){
                            rowidarray=addToArray(rowidarray,res,predicateStructArray[i]->rightRel,predicateStructArray[i]->leftRel);
                        }
                        else
                            rowidarray=addToArray(rowidarray,res,predicateStructArray[i]->leftRel,predicateStructArray[i]->rightRel);

                        relationDelete(rel1);
                        relationDelete(rel2);
                        relationDelete(res);
                    }
                    else if (rowidarray->row_ids[predicateStructArray[i]->rightRel]==NULL && rowidarray->row_ids[predicateStructArray[i]->leftRel]!=NULL){
                        // printf("4\n");
                        //only right is null
                        // printf("4\n");
                        // printf(" 2done %s\n",predicatesArray[i]);
                        // printIntermediate(rowidarray);
                        predicateStructArray[i]->done=1;
                        done_counter++;
                        // printf("predicate done %s\n",predicatesArray[i]);
                        relation *rel1=intermediateToRelation(rowidarray,&relInfo[relationsArray[predicateStructArray[i]->leftRel]],predicateStructArray[i]->leftRelation->payloadList->data,predicateStructArray[i]->leftRel),
                        *rel2=relationInfoToRelation(&relInfo[relationsArray[predicateStructArray[i]->rightRel]],predicateStructArray[i]->rightRelation->payloadList->data);
                        //printf("hello1 rel2->tuples[0].payloadList->data: %d\n",rel2->tuples[0].payloadList->data);
                        // printRelation(rel2);

                        relation *res=PartitionedHashJoin(rel1,rel2);
                        if(res == NULL){
                            // printf("its null\n");
                            relationDelete(rel1);
                            relationDelete(rel2);
                            done_counter=predicateCounter;
                            break;
                        }
                        // printRelation(res);
                        //printf("hello2\n");
                        //printf("rowidarray num relations : %d\n",rowidarray->num_relations);
                        if(biggerRel(rel1,rel2)){
                            //printf("hello3\n");
                            rowidarray=addToArray(rowidarray,res,predicateStructArray[i]->rightRel,predicateStructArray[i]->leftRel);
                        }
                        else    
                            rowidarray=addToArray(rowidarray,res,predicateStructArray[i]->leftRel,predicateStructArray[i]->rightRel);
                        //printf("done with everything\n");
                        //printf("rowidarray num relations : %d\n",rowidarray->num_relations);

                        //printIntermediate(rowidarray);
                        relationDelete(rel1);
                        relationDelete(rel2);
                        relationDelete(res);
                    }
                    else if (rowidarray->row_ids[predicateStructArray[i]->rightRel]!=NULL && rowidarray->row_ids[predicateStructArray[i]->leftRel]!=NULL){
                        // printf("5\n");
                        //none of them is null both of them are in the rowidarray
                        // printf(" done %s\n",predicatesArray[i]);
                        predicateStructArray[i]->done=1;
                        done_counter++;
                        relation *rel1=intermediateToRelation(rowidarray,&relInfo[relationsArray[predicateStructArray[i]->leftRel]],predicateStructArray[i]->leftRelation->payloadList->data,predicateStructArray[i]->leftRel),
                        *rel2=intermediateToRelation(rowidarray,&relInfo[relationsArray[predicateStructArray[i]->rightRel]],predicateStructArray[i]->rightRelation->payloadList->data,predicateStructArray[i]->rightRel);


                        // relation *res=PartitionedHashJoin(rel1,rel2);
                        int count=0;
                        for (int i=0; i<rel1->num_tuples ; i++){

                            if (rel1->tuples[i].payloadList->data==rel2->tuples[i].payloadList->data)
                            count++;
                        }
                        intermediate *newidarray=intermediateCreate(rowidarray->num_relations);

                        newidarray->num_rows=count;
                        for (int i=0 ; i<rowidarray->num_relations ; i++){

                            if (rowidarray->row_ids[i]!=NULL)
                                newidarray->row_ids[i]=malloc(count*sizeof(int));

                        }

                        int insert=0;
                        for( int i=0 ; i<rowidarray->num_rows ; i++){
                            if (rel1->tuples[i].payloadList->data==rel2->tuples[i].payloadList->data){

                                for(int j=0 ; j<newidarray->num_relations ; j++){
                                    if (newidarray->row_ids[j]!=NULL){
                                            newidarray->row_ids[j][insert]=rowidarray->row_ids[j][i];
                                            // printf("%d\n",newidarray->row_ids[j][insert]);
                                    }
    
                                }
                                insert++;
                            }

                        }
                        intermediateDelete(rowidarray);
                        rowidarray=newidarray;

                        relationDelete(rel1);
                        relationDelete(rel2);
                    }
                }
                else{
                    relation *rel1=relationInfoToRelation(&relInfo[relationsArray[predicateStructArray[i]->leftRel]],predicateStructArray[i]->leftRelation->payloadList->data),*rel2=relationInfoToRelation(&relInfo[relationsArray[predicateStructArray[i]->rightRel]],predicateStructArray[i]->rightRelation->payloadList->data);
                    relation *res=PartitionedHashJoin(rel1,rel2);
                    if(res == NULL){
                        break;
                    }
                    if (biggerRel(rel1,rel2)){
                        rowidarray=addToArray(rowidarray,res,predicateStructArray[i]->rightRel,predicateStructArray[i]->leftRel);

                    }
                    else{
                        rowidarray=addToArray(rowidarray,res,predicateStructArray[i]->leftRel,predicateStructArray[i]->rightRel);
                    }
                    // printf("predicate done %s\n",predicatesArray[i]);
                    predicateStructArray[i]->done=1;
                    done_counter++;
                    empty=0;
                    relationDelete(rel1);
                    relationDelete(rel2);
                    relationDelete(res);
                }

            }
        }

        int projRel = 0;
        int projCol = 0;
        int checksum = 0;
        //printIntermediate(rowidarray);
        for(int i = 0; i < projectionCounter; i++){
            projRel = atoi(strtok(projectionsArray[i], "."));
            projCol = atoi(strtok(NULL, "\0"));
            //printf("GET SUM OF COLUMN %d FROM RELATION %d OF FILE r%d\n", projCol, projRel, relationsArray[projRel]);
            relation* result = intermediateToRelation(rowidarray, &relInfo[relationsArray[projRel]], projCol, projRel);
            checksum = getSumRelation(result);
            if(checksum <= 0){
                //printf("NULL ");
                strcat(resultBuffer, "NULL ");
                relationDelete(result);
                continue;
            }
            //printf("%d ", checksum);
             numBuffer[0] = '\0';
             sprintf(numBuffer, "%d ", checksum);
             strcat(resultBuffer, numBuffer);
             relationDelete(result);
        }
        strcat(resultBuffer, "\n");
        //printf("\n");
        //TODO thelo na trexei gia ena pros to paron kai meta tha doume gia perissotera

        intermediateDelete(rowidarray);
        // return 1;
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
            free(predicatesArray[i]);
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

int sameRel(char* predicate){              /*      an einai idio relation epistrefei 1 an einai idio relation kai idio column epistrefei 2 allios epistrefei 0     */

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


