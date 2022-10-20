#include "int.h"


int hashl(int key, int n){

    int i=0,j=1;
    for (i=0;i<n;i++){
        j=j*2;
    }
    j--;

    return key & j;

}




int findNumOfBuckets(relation *r){

    int size;

    size = r->num_tuples*sizeof(tuple);

    if (L2>size){       /* no need to partition */


        return 0;

    }
    else{           /* the table does not fit in the L2 cache whole */      //TODO na do gia poliplokotites

        int i,n=1,x,flag=0;

        while(){


            for (i=0;i<n;i++){          /* j = 2^n */
                j=j*2;
            }



            int *count=malloc(j*sizeof(int));                


            for (i = 0 ; i < r->num_tuples ; i++){  /* at the end of this loop we have the number of elements in each bucket */

                count[hashl(r->tuples[i],n)]++;
            }


            flag=1;


            for (i = 0 ; i < j; i++)            
                if(count[i]*sizeof(tuple)>L2){          /* if we cant fit it in the L2 cache we increse n by one and we test again */
                    flag=0;
                    break;
                }




            if(flag) break;

            n++;

            
        }

        return n;


    }


}


result* PartitionedHashJoin(relation *relR, relation *relS){

    /**     Step 1. Partitioning        **/


    /*##################################################*/

    /* edo prepei na ginei prota i douleia me tin L2  kai tin euresi tou n (gia arxi theoro dedomeno oti to n einai 3)*/

    /*##################################################*/


    int nR,nS;

    // nR=findNumOfBuckets(relR);
    // nS=findNumOfBuckets(relS);



    /* gia na ginei to partitioning prepei prota na pothikeusoume ta dedomena */
    /* se seira (des ekfonisi sel4 stin arxi to eksigei)*/

    int i,count=0 , n=1 , histSize=1;

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

        hist->tuples[i].payload=0;                 /* midenizo oles tis theseis tou histogram gia na mporo na metriso meta */


    }

    relation *Psum;
    Psum->tuples = malloc(histSize * sizeof(tuple));       /* to Psum */


    for ( i = 0 ; i < relR->num_tuples ; i++ ){             /* etoiamzoume to hist */

        tuple *t;

        if (t = SearchKey(hist,relR->tuples[i].key,n))        //psaxno to key an to vro epistefo pointer se auto allios NULL
            t->payload++;
        else{

            hist->tuples[hist->num_tuples].key=hashl(relR->tuples[i].key,n);
            hist->num_tuples++;                             //oso to gemizo ayksano to num_tuples

        }
        

    }

    Psum->num_tuples=hist->num_tuples;


    int position=0;

    for ( i = 0 ; i < hist->num_tuples ; i++ ){             /* etoimazoume to Psum */

        Psum->tuples[i].key=hashl(hist->tuples[i].key,n);
        Psum->tuples[i].payload=position;

        position+=hist->tuples[i].payload;

    }

    

    /* ftiaxnoume to newR */

    int j=0,key,positions,currPos;
    key = Psum->tuples[j].key;
    positions = Psum->tuples[j].payload;
    currPos=0;

    i=0;

    while ( currPos != relR->num_tuples) { /*diavazo ena ena stoixeio mexri na mpoun ola*/

        if( hashl(relR->tuples[i].key,n)==hashl(key,n)){         //arxika psaxno mono to proto key molis ta vro ola to epomeno etc
            newR->tuples[currPos].key=key;
            newR->tuples[currPos].payload=relR->tuples[i].payload;

            newR->num_tuples++;

            currPos++;
            if (currPos==positions){            //ama mpoun ola ta stoixeia tou key pao sto epomeno key

                positions=Psum->tuples[++j].payload;
                key =  Psum->tuples[j].key;

            }
        }
        if (i==relR->num_tuples)                //ama ftaso sto telos ksanarxizo 
            i=0;
        i++;
    
    }           
    
        /*tha mporouse olo auto na mpei se mia sinartisi p apla na epistrefei to newR alla tha thela na doume prota pos tha ginei me to L2*/

    /**     Step 2. Building            **/

    /**     Step 3. Probing             **/
}




tuple* SearchKey(relation *r,int key,int n){              /*psaxnei ena key an den to vrei epistrefei null allios epistrefei deikti sto key*/

    int i;


    for ( i = 0 ; i < r->num_tuples ; i++ ){

        if (hashl(r->tuples[i].key,n)==hashl(key,n)){

            return &r->tuples[i];

        }
        

    }

    return NULL;

}