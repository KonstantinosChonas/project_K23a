#include "intermediate.h"

intermediate* intermediateCreate(int numOfRelations ){

    intermediate* array=malloc(sizeof(intermediate));


    for (int i=0 ; i<numOfRelations ; i++)
    {
        array[i].rel=NULL;
        array->num_relations=0;
    }

    return array;


}


// void applyFilter(relationInfo *r, intermediate *rowidarray,char* filter){           //TODO fix

//     int i=0;
//     int rel=0;
//     int column=0;
//     int filter_num=0;
//     int operator=0;
//     int dot=0;


//     while(filter[i]){

//         if (operator!=0)
//             filter_num=10*filter_num+filter[i]-'0';    


//         if (filter[i]=='>')
//             operator='>';
//         else if(filter[i]  == '<')
//             operator='<';
//         else if (filter[i]== '=')
//             operator='=';

//         if (filter[i]=='.'){
//             dot=1;
//             i++;
//             continue;
//         }


//         if (operator==0 && dot==0)
//             rel=10*rel+filter[i]-'0';

//         else if (operator==0 && dot==1)
//             column=10*column+filter[i]-'0';


//         i++;
//     }                                                   /*      if filter is 1.0>3000 then filter_num=3000 and operator = '>'       */
//     int count=0;

//     for (int i=0 ; i<r->num_tuples ; i++)
//         switch (operator)
//         {
//         case '>':
//             if ( r->columns[i][column]>filter_num)  count++;
//             break;
//         case '<':
//             if ( r->columns[i][column]<filter_num)  count++;
//             break;
//         case '=':
//             if ( r->columns[i][column]==filter_num)  count++;
//             break;
//         default:
//             break;
//         }


//         rowidarray->array[rel]=malloc(count*sizeof(int));
//         rowidarray->num_rows=count;

//     for (int i=0 ; i<r->num_tuples ; i++)
//         switch (operator)
//         {
//         case '>':
//             if ( r->columns[i][column]>filter_num){
//                 rowidarray->array[rel][rowidarray->num_rows-count]=r->columns[i][column];
//                 count--;
//             }

//             break;
//         case '<':
//             if ( r->columns[i][column]<filter_num){
//                 rowidarray->array[rel][rowidarray->num_rows-count]=r->columns[i][column];
//                 count--;
//             }
//             break;
//         case '=':
//             if ( r->columns[i][column]==filter_num){
//                 rowidarray->array[rel][rowidarray->num_rows-count]=r->columns[i][column];
//                 count--;
//             }
//             break;
//         default:
//             break;
//         }

// }



void addToArray(intermediate *rowidarray, relation *phjRel,int relname1, int relname2){               /*      rel1 rel2 o arithmos tis kathe sxesis       */


    relation *rel1=malloc(sizeof(relation)),*rel2=malloc(sizeof(relation));

    rel1->num_tuples=phjRel->num_tuples;
    rel2->num_tuples=phjRel->num_tuples;

    rel1->tuples=malloc(phjRel->num_tuples*sizeof(tuple));
    rel2->tuples=malloc(phjRel->num_tuples*sizeof(tuple));

    for (int i=0 ; i<phjRel->num_tuples ; i++){

        rel1->tuples[i].key=phjRel->tuples[i].payloadList->data;
        rel2->tuples[i].key=phjRel->tuples[i].payloadList->next->data;

        addToPayloadList(rel1->tuples[i].payloadList,phjRel->tuples[i].key);
        addToPayloadList(rel2->tuples[i].payloadList,phjRel->tuples[i].key);
    }

    if(rowidarray[relname1].rel==NULL)
        rowidarray[relname1].rel=rel1;
    else{
        relationDelete(rowidarray[relname1].rel);
        rowidarray[relname1].rel=rel1;
    }



    if(rowidarray[relname2].rel==NULL)
        rowidarray[relname2].rel=rel2;
    else{
        relationDelete(rowidarray[relname2].rel);
        rowidarray[relname2].rel=rel2;
    }



}

