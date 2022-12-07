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


void addToArray(intermediate *rowidarray, relation *phjRel,int relname1, int relname2){               /*      relname1 relname2 o arithmos tis kathe sxesis (0,1,2...)      */

    if (rowidarray->row_ids[relname1]==NULL && rowidarray->row_ids[relname2]==NULL && rowidarray->num_rows==0){
        int table1[phjRel->num_tuples],table2[phjRel->num_tuples];
        for (int i=0 ; i<phjRel->num_tuples ; i++){

            table1[i]=phjRel->tuples[i].payloadList->data;
            table2[i]=phjRel->tuples[i].payloadList->next->data;
        }

        rowidarray->row_ids[relname1]=table1;
        rowidarray->row_ids[relname2]=table2;
        rowidarray->num_rows=phjRel->num_tuples;
        return;
    }
    else if (rowidarray->row_ids[relname1]!=NULL && rowidarray->row_ids[relname2]==NULL){

        intermediate *newidarray=intermediateCreate(rowidarray->num_relations);
        newidarray->num_rows=phjRel->num_tuples;

        for( int i=0 ; i<rowidarray->num_relations ; i++){

            if(rowidarray->row_ids[i]!=NULL)
                newidarray->row_ids[i]=malloc(phjRel->num_tuples*sizeof(int));

        }


        int table[phjRel->num_tuples];

        for( int i=0 ; i<phjRel->num_tuples ; i++){

            table[i]=phjRel->tuples[i].payloadList->next->data;     // to kainourgio table gia to null relation ston rowid array 

            for(int j=0 ; j<rowidarray->num_relations ; j++){
                if (newidarray->row_ids[j]!=NULL){
                    newidarray->row_ids[j][i]=rowidarray->row_ids[j][phjRel->tuples[i].payloadList->data];

                }
            }


        }
        newidarray->row_ids[relname2]=table;


        intermediateDelete(rowidarray);
        return;

    }
    else if (  rowidarray->row_ids[relname1]!=NULL && rowidarray->row_ids[relname2]!=NULL)
    {

        intermediate *newidarray=intermediateCreate(rowidarray->num_relations);
        newidarray->num_rows=phjRel->num_tuples;

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


    }
    

    




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




    for(int i=0 ; i<rowidarray->num_rows ; i++){

        for (int j=0 ; j<rowidarray->num_relations ; j++){
            if(rowidarray->row_ids[j]!=NULL)
                printf("%-5d    |",rowidarray->row_ids[j][i]);
            else printf("         |");
        }
        printf("\n");
    }
}

