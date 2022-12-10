#include "intermediate.h"

intermediate* intermediateCreate(int numOfRelations ){

    intermediate* array=malloc(sizeof(intermediate));
    array->row_ids=malloc(numOfRelations*sizeof(int*));

    for (int i=0 ; i<numOfRelations ; i++)
        array->row_ids[i]=NULL;


    array->num_relations=numOfRelations;
    array->num_rows=0;

    return array;


}


void applyFilter(relationInfo *r, intermediate *rowidarray,char* filter){         /*        topothetei sto rowidarray to apotelesma tou filter      */
                                                                                    //TODO thelei diorthosi gia perissotera filtra 
    int i=0;
    int rel=0;
    int column=0;
    int filter_num=0;
    int operator=0;
    int dot=0;


    while(filter[i]){

        if (operator!=0)
            filter_num=10*filter_num+filter[i]-'0';    


        if (filter[i]=='>')
            operator='>';
        else if(filter[i]  == '<')
            operator='<';
        else if (filter[i]== '=')
            operator='=';

        if (filter[i]=='.'){
            dot=1;
            i++;
            continue;
        }


        if (operator==0 && dot==0)
            rel=10*rel+filter[i]-'0';

        else if (operator==0 && dot==1)
            column=10*column+filter[i]-'0';


        i++;
    }                                                   /*      if filter is 1.0>3000 then filter_num=3000 and operator = '>'       */
    int count=0;
    for (int i=0 ; i<r->num_tuples ; i++){
        // printf("operator in switch : %d\n",r->columns[column][i]);
        switch (operator)
        {
        case '>':
            if ( r->columns[column][i]>filter_num)  count++;
            break;
        case '<':
            if ( r->columns[column][i]<filter_num)  count++;
            break;
        case '=':
            if ( r->columns[column][i]==filter_num)  count++;
            break;
        default:
            break;
        }
    }
    int total=count;
    



    rowidarray->num_rows=count;
    rowidarray->row_ids[rel]=malloc(count*sizeof(int));


    for (int i=0 ; i<r->num_tuples ; i++){
        // printf("i:%d, value : %d, num tuples:%d, count:%d\n",i,r->columns[column][i],r->num_tuples,count);


        switch (operator)
        {
        case '>':
            if ( r->columns[column][i]>filter_num){
                rowidarray->row_ids[rel][total-count]=i;
                // printf("id added to array: %d, with value: %d\n",i,r->columns[column][i]);
                count--;
            }

            break;
        case '<':
            if ( r->columns[column][i]<filter_num){
                rowidarray->row_ids[rel][total-count]=i;
                // rowidarray->relArray[rel].tuples[total-count].key=i;
                // addToPayloadList(rowidarray->relArray[rel].tuples[total-count].payloadList,r->columns[i][column]);
                count--;
            }
            break;
        case '=':
            if ( r->columns[column][i]==filter_num){
                rowidarray->row_ids[rel][total-count]=i;
                count--;
            }
            break;
        default:
            break;
        }
    }

}


intermediate* addToArray(intermediate *rowidarray, relation *phjRel,int relname1, int relname2){               /*      relname1 relname2 o arithmos tis kathe sxesis (0,1,2...)      */

    printf("just entered add to array\n");
   // printIntermediate(rowidarray);
    intermediate *newidarray;
    if (rowidarray->row_ids[relname1]==NULL && rowidarray->row_ids[relname2]==NULL && rowidarray->num_rows==0){
        int table1[phjRel->num_tuples],table2[phjRel->num_tuples];
        for (int i=0 ; i<phjRel->num_tuples ; i++){
            table1[i]=phjRel->tuples[i].payloadList->data;
            table2[i]=phjRel->tuples[i].payloadList->next->data;
        }

        rowidarray->row_ids[relname1]=table1;
        rowidarray->row_ids[relname2]=table2;
        rowidarray->num_rows=phjRel->num_tuples;
        return rowidarray;
    }
    else if (rowidarray->row_ids[relname1]!=NULL && rowidarray->row_ids[relname2]==NULL){
        printf("hello add to array\n");
        newidarray=intermediateCreate(rowidarray->num_relations);
        newidarray->num_rows=phjRel->num_tuples;
        newidarray->num_relations=rowidarray->num_relations;
        printf("AAAAAAAAAAAAA new id array num relations : %d \n",newidarray->num_relations);
        for( int i=0 ; i<rowidarray->num_relations ; i++){

           if(rowidarray->row_ids[i]!=NULL)
                newidarray->row_ids[i]=malloc(phjRel->num_tuples*sizeof(int));

        }
        printf("2AAAAAAAAAAAAA new id array num relations : %d \n",newidarray->num_relations);
        printf("hello add to array2\n");
        int* table=malloc(phjRel->num_tuples*sizeof(int));
        printf("hello add to array22\n");
        for( int i=0 ; i<phjRel->num_tuples ; i++){

            table[i]=phjRel->tuples[i].payloadList->next->data;     // to kainourgio table gia to null relation ston rowid array 
            // if(table[i]<=0 || table[i]>=100000)
                // printf("HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH table[i]: %d, i: %d\n",table[i],i);
            for(int j=0 ; j<rowidarray->num_relations ; j++){
                if (newidarray->row_ids[j]!=NULL){
                    newidarray->row_ids[j][i]=rowidarray->row_ids[j][phjRel->tuples[i].payloadList->data];

                }
            }


        }
        printf("3AAAAAAAAAAAAA new id array num relations : %d \n",newidarray->num_relations);
        printf("hello add to array3\n");
        newidarray->row_ids[relname2]=table;
        //printIntermediate(newidarray);
        //intermediateDelete(rowidarray);
        //printIntermediate(newidarray);      // auto to print vgainei mia xara
        return newidarray;
        free(table);
    }
    else if (rowidarray->row_ids[relname1]==NULL && rowidarray->row_ids[relname2]!=NULL){
        newidarray=intermediateCreate(rowidarray->num_relations);
        newidarray->num_rows=phjRel->num_tuples;
        newidarray->num_relations=rowidarray->num_relations;
        for( int i=0 ; i<rowidarray->num_relations ; i++){

             if(rowidarray->row_ids[i]!=NULL)
                newidarray->row_ids[i]=malloc(phjRel->num_tuples*sizeof(int));

        }

        int* table=malloc(phjRel->num_tuples*sizeof(int));

        for( int i=0 ; i<phjRel->num_tuples ; i++){

            printf("ADDED VALUE IS %d\n", phjRel->tuples[i].payloadList->data);
            table[i]=phjRel->tuples[i].payloadList->data;     // to kainourgio table gia to null relation ston rowid array 

            for(int j=0 ; j<rowidarray->num_relations ; j++){
                if (newidarray->row_ids[j]!=NULL){
                    newidarray->row_ids[j][i]=rowidarray->row_ids[j][phjRel->tuples[i].payloadList->next->data];

                }
            }


        }
        newidarray->row_ids[relname1]=table;

        //free(table);      //prokalei thema giati eleutherwnei xwro pou xrhsimopoieitai meta
        intermediateDelete(rowidarray);
        //printIntermediate(newidarray); 
        return newidarray;

    }
    else if (  rowidarray->row_ids[relname1]!=NULL && rowidarray->row_ids[relname2]!=NULL)
    {

        newidarray=intermediateCreate(rowidarray->num_relations);
        newidarray->num_rows=phjRel->num_tuples;
        newidarray->num_relations=rowidarray->num_relations;
        for( int i=0 ; i<rowidarray->num_relations ; i++){

            if(rowidarray->row_ids[i]!=NULL)
                newidarray->row_ids[i]=malloc(phjRel->num_tuples*sizeof(int));

        }


        for( int i=0 ; i<phjRel->num_tuples ; i++){


            for(int j=0 ; j<rowidarray->num_relations ; j++){
                if (newidarray->row_ids[j]!=NULL){

                    newidarray->row_ids[j][i]=rowidarray->row_ids[j][phjRel->tuples[i].payloadList->data];

                }
            }


        }
        //printIntermediate(newidarray); 
        return newidarray;
    }
    
    //printIntermediate(newidarray); 
    printf("exiting add to array\n");

}



void intermediateDelete(intermediate* inter){

    for (int i=0; i<inter->num_relations ; i++){
        if (inter->row_ids[i]!=NULL)
            free(inter->row_ids[i]);
    
    }

    free(inter);

    return;
}


void printIntermediate(intermediate *rowidarray){

    printf("printing intermediate \n");

    printf("num relations: %d\n",rowidarray->num_relations);


    for(int i=0 ; i<rowidarray->num_rows ; i++){
        // printf("printing rows\n");
        for (int j=0 ; j<rowidarray->num_relations ; j++){
            if(rowidarray->row_ids[j]!=NULL)
                printf("%-5d    |",rowidarray->row_ids[j][i]);
            else printf("         |");
        }
        printf("\n");
    }
}


relation* intermediateToRelation(intermediate *rowidarray, relationInfo *relInfo,int column,int relname){

    printf("THIS IS INTERMEDIATE TO ARRAY WITH COLUMN: %d RELNAME :%d\n",column,relname);
    relation *rel=createEmptyRelation(rowidarray->num_rows);


    for( int i=0; i<rowidarray->num_rows ; i++){
        rel->tuples[i].key=i;
        relationPayloadList *new=malloc(sizeof(relationPayloadList));
        new->data=relInfo->columns[column][rowidarray->row_ids[relname][i]];
        new->next=NULL;
        rel->tuples[i].payloadList=new;
        // addToPayloadList(rel->tuples[i].payloadList,relInfo->columns[column][rowidarray->row_ids[relname][i]]);

    }
    printf("exiting injtermediate to relation\n");
    return rel;


}

relation* intermediateToRelationSum(intermediate *rowidarray, relationInfo *relInfo,int column,int relname){

    printf("THIS IS INTERMEDIATE TO ARRAY WITH COLUMN FOR SUM: %d RELNAME :%d\n",column,relname);
    relation *rel=createEmptyRelation(rowidarray->num_rows);

    int keyCheck[rowidarray->num_rows];
    for(int j = 0; j< rowidarray->num_rows; j++) {
        keyCheck[j] = -1;
    }

    int exists = 0;
    int keyCounter = 0;

    for( int i=0; i<rowidarray->num_rows ; i++){
        //printf("NUMBER %d WITH VALUE %d\n", rowidarray->row_ids[relname][i], relInfo->columns[column][rowidarray->row_ids[relname][i]]);
        for(int j = 0; j< rowidarray->num_rows; j++){
            if(keyCheck[j] == rowidarray->row_ids[relname][i]){
                exists = 1;
          //      printf("DOUBLE %d WITH VALUE %d\n", rowidarray->row_ids[relname][i], relInfo->columns[column][rowidarray->row_ids[relname][i]]);
                break;
            }
        }

        if(exists == 1){
            exists = 0;
            continue;
        }
        keyCheck[i] = rowidarray->row_ids[relname][i];
        rel->tuples[keyCounter].key=keyCounter;
        relationPayloadList *new=malloc(sizeof(relationPayloadList));
        new->data=relInfo->columns[column][rowidarray->row_ids[relname][i]];
        new->next=NULL;
        rel->tuples[keyCounter++].payloadList=new;
        // addToPayloadList(rel->tuples[i].payloadList,relInfo->columns[column][rowidarray->row_ids[relname][i]]);

    }
    printf("exiting intermediate to relation\n");
    return rel;


}

relation* relationInfoToRelation(relationInfo* relin,int column){           // metatrepei ena relation info se relation (voithitiko gia phj)

    printf("THIS IS RELATIONINFOTOREALTION\n");
    printf("relin->numtuples: %d\n",relin->num_tuples);

    relation* rel=createEmptyRelation(relin->num_tuples);


    printf("rel->numtuples: %d\n",rel->num_tuples);
    for (int i=0 ; i<relin->num_tuples ; i++){
        rel->tuples[i].key=i;
        // printf("relin->columns[column][i] : %d, i:%d, column:%d\n",relin->columns[column][i],i,column);
        relationPayloadList *new=malloc(sizeof(relationPayloadList));
        new->data=relin->columns[column][i];

        new->next=NULL;
        rel->tuples[i].payloadList=new;
        // printf("payload is :%d\n",rel->tuples[i].payloadList->data);
    
    }

    printf("exiting relation info to relation\n");

    return rel;


}