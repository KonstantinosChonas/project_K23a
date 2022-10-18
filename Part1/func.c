#include "int.h"


int hashl(int x, int n){

    int i=0,j=1;
    for (i=0;i<n;i++){
        j=j*2;
    }
    j--;

    return x & j;

}


result* PartitionedHashJoin(relation *relR, relation *relS){

    /**     Step 1. Partitioning        **/


    /*##################################################*/

    /* edo prepei na ginei prota i douleia me tin L2  kai tin euresi tou n (gia arxi theoro dedomeno oti to n einai 3)*/

    /*##################################################*/


    /* gia na ginei to partitioning prepei prota na pothikeusoume ta dedomena */
    /* se seira (des ekfonisi sel4 stin arxi to eksigei)*/

    int i,count=0 , n=3 , histSize=1;

    for ( i = 0 ; i < n ; i++ ){

        histSize = 2 * histSize;
    }




    relation *newR;
    newR->tuples=malloc(relR->num_tuples*sizeof(tuple));   /* o R' apo tin ekfonisi */

    newR->num_tuples = 0;




    relation *hist;
    hist->tuples = malloc(histSize * sizeof(tuple));       /* to histogram */

    hist->num_tuples=0;     /*arxika den exei tipota mesa*/

    for ( i = 0 ; i < histSize ; i++ ){

        hist->tuples[i]->payload=0;                 /* midenizo oles tis theseis tou histogram gia na mporo na metriso meta */


    }

    relation *Psum;
    Psum->tuples = malloc(histSize * sizeof(tuple));       /* to Psum */


    for ( i = 0 ; i < relR->num_tuples ; i++ ){             /* etoiamzoume to hist */

        tuple *t;

        if (t = SearchKey(hist,relR->tuples[i]->key)
            t->payload++;
        else{

            hist->tuples[hist->num_tuples]->key=relR->tuples[i]->key;
            hist->num_tuples++;

        }
        

    }

    Psum->num_tuples=hist->num_tuples;


    int position=0;

    for ( i = 0 ; i < hist->num_tuples ; i++ ){             /* etoimazoume to Psum */

        Psum->tuples[i]->key=hist->tuples[i]->key
        Psum->tuples[i]->payload=position;

        position+=hist->tuples[i]->payload;

    }

    

    /* ftiaxnoume to newR */

    int j=0;
    key = Psum->tuples[j]->key;
    positions = Psum->tuples[j]->payload;
    currPos=0;

    i=0;

    while ( currPos != relR->num_tuples) {

        if( relR->tuples[i]->key==key){
            newR->tuples[currPos]->key=key;
            newR->tuples[currPos]->payload=relR->tuples[i]->payload;

            newR->num_tuples++;

            currPos++;
            if (currPos==positions){

                positions=Psum->tuples[++j]->payload;
                key =  Psum->tuples[j]->key;



            }
        }
        if (i==relR->num_tuples)
            i=0;
        i++;
    
    }

    /**     Step 2. Building            **/

    /**     Step 3. Probing             **/
}




tuple* SearchKey(relation *r,int key){              /*psaxnei ena key an den to vrei epistrefei null allios epistrefei deikti sto key*/

    int i;


    for ( i = 0 ; i < r->num_tuples ; i++ ){

        if (r->tuples[i]->key==key){

            return r->tuples[i];

        }
        

    }

    return NULL;

}