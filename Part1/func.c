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

    printf("i just entered find num of buckets\n");
    int size;

    size = r->num_tuples*sizeof(tuple);

    if (L2>size){       /* no need to partition */


        return 0;

    }
    else{           /* the table does not fit in the L2 cache whole */      //TODO na do gia poliplokotites

        int i,n=1,x,flag=0;

        for(n=1;;n++){

            int j=1;


            for (i=0;i<n;i++){          /* j = 2^n */
                j=j*2;
            }



            int *count=malloc(j*sizeof(int));                
            for (i=0;i<j;i++)
                count[i]=0;

            for (i = 0 ; i < r->num_tuples ; i++){  /* at the end of this loop we have the number of elements in each bucket */

                count[hashl(r->tuples[i].key,n)]++;
            }


            flag=1;


            for (i = 0 ; i < j; i++){
                if(count[i]*sizeof(tuple)>L2){          /* if we cant fit it in the L2 cache we increse n by one and we test again */
                    flag=0;
                    break;
                }
            }
            free(count);


            if(flag) break;

        }

        return n;


    }


}


relation* createPsum(relation* r,int n){


    if(n==0) return NULL; 

    int i,count=0 , histSize=1;

    for ( i = 0 ; i < n ; i++ ){

        histSize = 2 * histSize;
    }




    relation *hist=malloc(sizeof(relation));
    hist->tuples = malloc(histSize * sizeof(tuple));       /* to histogram */

    hist->num_tuples=0;     /*arxika den exei tipota mesa*/


    for ( i = 0 ; i < histSize ; i++ ){

        hist->tuples[i].payload=0;                 /* midenizo oles tis theseis tou histogram gia na mporo na metriso meta */


    }

    relation *Psum=malloc(sizeof(relation));
    Psum->tuples = malloc(histSize * sizeof(tuple));       /* to Psum */



    for ( i = 0 ; i < r->num_tuples ; i++ ){             /* etoiamzoume to hist */

        tuple *t;

        if (t = SearchKey(hist,r->tuples[i].key,n))        //psaxno to key an to vro epistefo pointer se auto allios NULL
            t->payload++;
        else{

            hist->tuples[hist->num_tuples].key=hashl(r->tuples[i].key,n);
            hist->tuples[hist->num_tuples].payload++;
            hist->num_tuples++;                             //oso to gemizo ayksano to num_tuples

        }
        

    }


    //printf("printing hist\n");
    //printRelation(hist);


    Psum->num_tuples=hist->num_tuples;


    int position=0;

    for ( i = 0 ; i < hist->num_tuples ; i++ ){             /* etoimazoume to Psum */

        Psum->tuples[i].key=hashl(hist->tuples[i].key,n);
        Psum->tuples[i].payload=position;

        position+=hist->tuples[i].payload;

    }

    // printf("printing psum\n");
    // printRelation(Psum);
    return Psum;

}



relation* relPartitioned(relation *r, relation *Psum, int n){


    if (n==0) return r;


    printf("i just entered relPartitioned\n");

    
    /* ftiaxnoume to newR */

    int j=0,key,positions,currPos,i;
    key = Psum->tuples[j].key;
    printf("this is the key: %d\n",Psum->tuples[7].key);
    positions = Psum->tuples[j+1].payload;      //i topothesia p vrisketai to epomeno bucket
    currPos=0;          //i topothesia p vriskomaste ston newR
    // printf("positions = %d\n",positions);
    i=0;


    relation *newR=malloc(sizeof(relation));
    newR->tuples=malloc(r->num_tuples*sizeof(tuple));   /* o R' apo tin ekfonisi */

    newR->num_tuples = 0;

    //printf("printing r\n");
    //printRelation(r);

    while ( currPos != r->num_tuples) { /*diavazo ena ena stoixeio mexri na mpoun ola*/

        if( hashl(r->tuples[i].key,n)==key){         //arxika psaxno mono to proto key molis ta vro ola to epomeno etc
            newR->tuples[currPos].key=r->tuples[i].key;          //TODO na to allakso to key gia na mpainei to sosto stoixeio
            newR->tuples[currPos].payload=r->tuples[i].payload;

            newR->num_tuples++;

            currPos++;
            if (currPos==positions){            //ama mpoun ola ta stoixeia tou key pao sto epomeno key
                j++;
                positions=Psum->tuples[j+1].payload;
                key =  Psum->tuples[j].key;

            }
        }
        if (i==r->num_tuples){              //ama ftaso sto telos ksanarxizo
            i=0;
            continue;
        }
        i++;
    
    }           

    printf("printing new r\n");
    printRelation(newR);
    return newR;

}



result* PartitionedHashJoin(relation *relR, relation *relS){

    /**     Step 1. Partitioning        **/


    /*##################################################*/

    /* edo prepei na ginei prota i douleia me tin L2  kai tin euresi tou n (gia arxi theoro dedomeno oti to n einai 3)*/

    /*##################################################*/


    int nR,nS;




    nR=findNumOfBuckets(relR);
    nS=findNumOfBuckets(relS);

    printf("nr:%d  ns:%d  \n",nR,nS);


    relation *newR,*newS,*rPsum,*sPsum;

    rPsum=createPsum(relR,nR);          
    sPsum=createPsum(relS,nS);


    newR=relPartitioned(relR, rPsum, nR);   //na do an xreiazontai na n
    newS=relPartitioned(relS, sPsum, nS);

    return NULL;

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