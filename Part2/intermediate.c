#include "intermediate.h"

intermediate* intermediateCreate(int numOfRelations ){

    intermediate* array=malloc(sizeof(intermediate));
    array->relArray=malloc(numOfRelations*sizeof(relation));

    for (int i=0 ; i<numOfRelations ; i++)
    {
        array->relArray[i].tuples=NULL;
        array->relArray[i].num_tuples=0;
        array->num_relations=numOfRelations;
    }

    return array;


}


void applyFilter(relationInfo *r, intermediate *rowidarray,char* filter){         /*        topothetei sto rowidarray to apotelesma tou filter      */

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

    for (int i=0 ; i<r->num_tuples ; i++)
        switch (operator)
        {
        case '>':
            if ( r->columns[i][column]>filter_num)  count++;
            break;
        case '<':
            if ( r->columns[i][column]<filter_num)  count++;
            break;
        case '=':
            if ( r->columns[i][column]==filter_num)  count++;
            break;
        default:
            break;
        }

        int total=count;
        // rowidarray->array[rel]=malloc(count*sizeof(int));
        // rowidarray->num_rows=count;
        rowidarray->relArray[rel].num_tuples=count;
        rowidarray->relArray[rel].tuples=malloc(count*sizeof(tuple));

    for (int i=0 ; i<r->num_tuples ; i++)
        switch (operator)
        {
        case '>':
            if ( r->columns[i][column]>filter_num){
                rowidarray->relArray[rel].tuples[total-count].key=i;
                addToPayloadList(rowidarray->relArray[rel].tuples[total-count].payloadList,r->columns[i][column]);
                count--;
            }

            break;
        case '<':
            if ( r->columns[i][column]<filter_num){
                rowidarray->relArray[rel].tuples[total-count].key=i;
                addToPayloadList(rowidarray->relArray[rel].tuples[total-count].payloadList,r->columns[i][column]);
                count--;
            }
            break;
        case '=':
            if ( r->columns[i][column]==filter_num){
                rowidarray->relArray[rel].tuples[total-count].key=i;
                addToPayloadList(rowidarray->relArray[rel].tuples[total-count].payloadList,r->columns[i][column]);
                count--;
            }
            break;
        default:
            break;
        }

}



void addToArray(intermediate *rowidarray, relation *phjRel,int relname1, int relname2){               /*      rel1 rel2 o arithmos tis kathe sxesis       */


    relation *rel1=malloc(sizeof(relation)),*rel2=malloc(sizeof(relation));

    tuple *tuples_of_rel1=malloc(phjRel->num_tuples*sizeof(tuple)),*tuples_of_rel2=malloc(phjRel->num_tuples*sizeof(tuple));


    // rel1->num_tuples=phjRel->num_tuples;
    // rel2->num_tuples=phjRel->num_tuples;

    // rel1->tuples=malloc(phjRel->num_tuples*sizeof(tuple));
    // rel2->tuples=malloc(phjRel->num_tuples*sizeof(tuple));

    for (int i=0 ; i<phjRel->num_tuples ; i++){

        tuples_of_rel1[i].key=phjRel->tuples[i].payloadList->data;
        tuples_of_rel2[i].key=phjRel->tuples[i].payloadList->next->data;

        addToPayloadList(tuples_of_rel1[i].payloadList,phjRel->tuples[i].key);
        addToPayloadList(tuples_of_rel2[i].payloadList,phjRel->tuples[i].key);
    }

    if(rowidarray->relArray[relname1].tuples==NULL){
        rowidarray->relArray[relname1].tuples=tuples_of_rel1;
        rowidarray->relArray[relname1].num_tuples=phjRel->num_tuples;
    }
    else{
        tuplesDelete(&rowidarray->relArray[relname1]);
        rowidarray->relArray[relname1].tuples=tuples_of_rel1;
        rowidarray->relArray[relname1].num_tuples=phjRel->num_tuples;
    }



    if(rowidarray->relArray[relname2].tuples==NULL){
        rowidarray->relArray[relname2].tuples=tuples_of_rel2;
        rowidarray->relArray[relname2].num_tuples=phjRel->num_tuples;
    }
    else{
        tuplesDelete(&rowidarray->relArray[relname2]);
        rowidarray->relArray[relname2].tuples=tuples_of_rel2;
        rowidarray->relArray[relname2].num_tuples=phjRel->num_tuples;
    }



}

