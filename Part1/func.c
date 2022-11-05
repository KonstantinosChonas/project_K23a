#include "int.h"
#include "hash.h"

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

        for(n=2;n<6;n+=2){              //TODO na to kano na stamataei

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

        return n-2;


    }


}


relation* createPsum(relation* r,int n){


    printf("i just entered psum\n");

    if(n==0) return NULL; 

    int i,count=0 , histSize=1;

    for ( i = 0 ; i < n ; i++ ){

        histSize = 2 * histSize;
    }




    relation *hist=malloc(sizeof(relation));
    hist->tuples = malloc(histSize * sizeof(tuple));       /* to histogram */



    for ( i = 0 ; i < histSize ; i++ ){

        hist->tuples[i].payload=0;                 /* midenizo oles tis theseis tou histogram gia na mporo na metriso meta */
        hist->tuples[i].key=i;

    }

    hist->num_tuples=histSize;


    relation *Psum=malloc(sizeof(relation));
    Psum->tuples = malloc(histSize * sizeof(tuple));       /* to Psum */





    for ( i = 0 ; i < r->num_tuples ; i++ ){             /* etoiamzoume to hist */

        tuple *t;

        if (t = SearchKey(hist,r->tuples[i].key,n))        //psaxno to key an to vro epistefo pointer se auto allios NULL
            t->payload++;
      
    }


    // printf("printing hist\n");
    // printRelation(hist);


    Psum->num_tuples=hist->num_tuples;


    int position=0;

    for ( i = 0 ; i < hist->num_tuples ; i++ ){             /* etoimazoume to Psum */

        Psum->tuples[i].key=hashl(hist->tuples[i].key,n);
        Psum->tuples[i].payload=position;

        position+=hist->tuples[i].payload;

    }

    printf("printing psum\n");
    printRelation(Psum);
    return Psum;

}



relation* relPartitioned(relation *r, relation *Psum, int n){


    if (n==0) return r;


    printf("i just entered relPartitioned\n");

    
    /* ftiaxnoume to newR */

    int j=0,key,positions,currPos,i;
    key = Psum->tuples[j].key;
    //printf("this is the key: %d\n",Psum->tuples[7].key);
    positions = Psum->tuples[j+1].payload;      //i topothesia p vrisketai to epomeno bucket
    currPos=0;          //i topothesia p vriskomaste ston newR
    //printf("positions = %d\n",positions);
    i=0;


    relation *newR=malloc(sizeof(relation));
    newR->tuples=malloc(r->num_tuples*sizeof(tuple));   /* o R' apo tin ekfonisi */

    newR->num_tuples = 0;

    printf("printing r\n");
    printRelation(r);

    while ( currPos != r->num_tuples) { /*diavazo ena ena stoixeio mexri na mpoun ola*/
        if (i==r->num_tuples) i=0;
        if( hashl(r->tuples[i].key,n)==key){         //arxika psaxno mono to proto key molis ta vro ola to epomeno etc

            printf("key: %d payload: %d  i: %d\n",r->tuples[i].key,r->tuples[i].payload,i);


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

hashMap** createHashForBuckets(relation* r, relation* pSum, int n){
    int bucket = 0;
    int* myHash = NULL;
    if(pSum != NULL){
        struct hashMap **hashMapArray = malloc(sizeof(struct hashMap) * pSum->num_tuples);

        for(int i = 0; i < pSum->num_tuples; i++){
            hashMapArray[i] = hashCreate(i);

            if(pSum->tuples[i+1].payload == 0){
                for(int j = pSum->tuples[i].payload; j < r->num_tuples; j++){
                    hashInsert(hashMapArray[i], r->tuples[j].key, r->tuples[j].payload);
                }
            }else
            for(int j = pSum->tuples[i].payload; j < pSum->tuples[i+1].payload; j++){
                hashInsert(hashMapArray[i], r->tuples[j].key, r->tuples[j].payload);
            }
            printf("created hash map for bucket:%d\n", hashMapArray[i]->bucket);
        }

        return hashMapArray;
    }else{
        struct hashMap **hashMapArray = malloc(sizeof(struct hashMap));
        hashMapArray[0] = hashCreate(0);

        for(int i = 0; i < r->num_tuples; i++){
            hashInsert(hashMapArray[0], r->tuples[i].key, r->tuples[i].payload);
        }
        printf("created hash map for bucket:%d\n", hashMapArray[0]->bucket);
        return hashMapArray;
    }
}

relation* joinRelation(struct hashMap** hashMapArray, relation *r, relation *pSum, int n){
    int bucket = 0;
    int exists = 0;
    int nodeCounter = 0;
    struct tuple* newTuple = NULL;

    relation *result = malloc(sizeof(struct relation));
    result->num_tuples = r->num_tuples;
    result->tuples = malloc(sizeof(struct tuple) * result->num_tuples);

    if(pSum != NULL){
        for(int i = 0; i < pSum->num_tuples; i++){
            if(pSum->tuples[i+1].payload == 0){
                for(int j = pSum->tuples[i].payload; j < r->num_tuples; j++){
                    exists = hashSearch(hashMapArray[i], r->tuples[j].key, r->tuples[j].payload, 0);
                    if(exists){
                        newTuple = createTupleFromNode(r->tuples[j].key, r->tuples[j].payload);
                        result->tuples[nodeCounter] = *newTuple;
                        nodeCounter++;
                        free(newTuple);
                    }
                }
            }else
                for(int j = pSum->tuples[i].payload; j < pSum->tuples[i+1].payload; j++){
                    exists = hashSearch(hashMapArray[i], r->tuples[j].key, r->tuples[j].payload, 0);
                    if(exists){
                        newTuple = createTupleFromNode(r->tuples[j].key, r->tuples[j].payload);
                        result->tuples[nodeCounter] = *newTuple;
                        nodeCounter++;
                        free(newTuple);
                    }
                }
        }

        return result;
    }else{
        for(int i = 0; i < r->num_tuples; i++){
           exists = hashSearch(hashMapArray[0], r->tuples[i].key, r->tuples[i].payload, 0);
            if(exists){
                newTuple = createTupleFromNode(r->tuples[i].key, r->tuples[i].payload);
                result->tuples[nodeCounter] = *newTuple;
                nodeCounter++;
                free(newTuple);
            }
        }
        return result;
    }
}

result* PartitionedHashJoin(relation *relR, relation *relS){

    /**     Step 1. Partitioning        **/

    printRelation(relR);


    int nR,nS;


    nR=findNumOfBuckets(relR);
    nS=findNumOfBuckets(relS);

    printf(" nr: %d ns: %d\n",nR,nS);

    relation *newR,*newS,*rPsum,*sPsum;

    rPsum=createPsum(relR,nR);          
    sPsum=createPsum(relS,nS);


    newR=relPartitioned(relR, rPsum, nR);   //na do an xreiazontai na n
    newS=relPartitioned(relS, sPsum, nS);

    printf(" partitioning over\n");
    /* tha epistrefei relation pou tha exei mono ta koina buckets twn 2 relation (dhladh auta pou exoun idio hash)
    de douleuei gia kathe input atm */
    //compareBuckets(newR, newS, rPsum, sPsum, nR, nS);

    hashMap** hashMapArray = NULL;

    hashMapArray = createHashForBuckets(newR, rPsum, nR);
    /*
    hashInsert(hashMapArray[0], 40, 12);
    hashInsert(hashMapArray[0], 41, 122);
    hashInsert(hashMapArray[0], 42, 123);
    hashInsert(hashMapArray[0], 43, 124);
    hashInsert(hashMapArray[0], 120, 125);
    hashInsert(hashMapArray[0], 160, 121);
    hashInsert(hashMapArray[0], 200, 127);

    int y = hashInsert(hashMapArray[0], 80, 15);

    int x = hashSearch(hashMapArray[0],80, 0, 0);
    */

    int j = 0;
    while(hashMapArray[j]){
        printf("h: %d\n", hashMapArray[j]->nodeCount);
        for(int i = 0; i<HASH_TABLE_SIZE; i++){
            if(hashMapArray[j]->hashNodes[i]){
                printf("%d\n", hashMapArray[j]->hashNodes[i]->key);
            }
        }
        j++;
    }

    relation* result = joinRelation(hashMapArray, newS, sPsum, nS);
    printRelation(result);
    //printRelation(newR);
    //printRelation(newS);

    hashDelete(hashMapArray);

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