#include "int.h"
#include "hash.h"
#include <math.h>


relation* inputFromFile(char* s){


    FILE *fp;
    int count=0,num1,num2,num3;
    relation* r;
    char c;

    fp = fopen(s, "r");
 
    // Extract characters from file and store in character c
    for (c = getc(fp); c != EOF; c = getc(fp))
        if (c == '\n') // Increment count if this character is newline
            count = count + 1;
 
    // Close the file
    fclose(fp);



    r=malloc(sizeof(relation));
    r->tuples = malloc(count * sizeof(tuple)); 
    r->num_tuples=count;


    fp = fopen(s, "r");

    
    // Extract characters from file and store in character c
    for(int i = 0 ; i < count ; i++ ){

        fscanf(fp,"%d|%d|%d|\n",&num1,&num2,&num3);
        r->tuples[i].key=num1;
        r->tuples[i].payloadList = createRelationPayloadList(num2);
        //r->tuples[i].payloadList->data=num2;

    }

    // Close the file
    fclose(fp);

    return r;
}


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
    else{           /* the table does not fit in the L2 cache whole */      

        int i,n=1,x,flag=0;

        for(n=2;n<6;n+=2){

            int j=1;


            for (i=0;i<n;i++){          /* j = 2^n */
                j=j*2;
            }



            int *count=malloc(j*sizeof(int));                
            for (i=0;i<j;i++)
                count[i]=0;

            for (i = 0 ; i < r->num_tuples ; i++){  /* at the end of this loop we have the number of elements in each bucket */

                count[hashl(r->tuples[i].payloadList->data,n)]++;
            }


            flag=1;


            for (i = 0 ; i < j; i++){
                if(count[i]*sizeof(tuple)>L2){          /* if we cant fit it in the L2 cache we increse n and we test again */
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



relation* createHist(relation* r,int n)
{

    int i,count=0 , histSize=1;

    for ( i = 0 ; i < n ; i++ ){

        histSize = 2 * histSize;
    }




    relation *hist=malloc(sizeof(relation));
    hist->tuples = malloc(histSize * sizeof(tuple));       /* to histogram */

    for ( i = 0 ; i < histSize ; i++ ){

        hist->tuples[i].payloadList = createRelationPayloadList(0);                 /* midenizo oles tis theseis tou histogram gia na mporo na metriso meta */
        hist->tuples[i].key=i;

    }

    hist->num_tuples=histSize;


    for ( i = 0 ; i < r->num_tuples ; i++ ){             /* etoiamzoume to hist */

        tuple *t;

        if (t = SearchKey(hist,r->tuples[i].payloadList->data,n))        //psaxno to key an to vro epistefo pointer se auto allios NULL
            t->payloadList->data++;

    }


    return hist;

}


relation* createPsum(relation* r,int n,relation* hist){



    if(n==0) return NULL; 

    
    int i;

    relation *Psum=malloc(sizeof(relation));
    Psum->tuples = malloc(hist->num_tuples * sizeof(tuple));       /* to Psum */





    Psum->num_tuples=hist->num_tuples;


    int position=0;

    for ( i = 0 ; i < hist->num_tuples ; i++ ){             /* etoimazoume to Psum */

        Psum->tuples[i].key=hashl(hist->tuples[i].key,n);
        Psum->tuples[i].payloadList = createRelationPayloadList(position);

        position+=hist->tuples[i].payloadList->data;

    }

    return Psum;

}


relation* relPartitioned(relation *r, relation *Psum, int n,relation* hist){


    if (n==0) return r;



    
    /* ftiaxnoume to newR */

    int j=0,key,positions,currPos,i,HistPointer;
    key = Psum->tuples[j].key;
    HistPointer = hist->tuples[j].payloadList->data ;     //i topothesia p vrisketai to epomeno bucket
    int counter=0;          //i topothesia p vriskomaste ston newR
    i=0;


    relation *newR=malloc(sizeof(relation));
    newR->tuples=malloc(r->num_tuples*sizeof(tuple));   /* o R' apo tin ekfonisi */


    newR->num_tuples = 0;


    while(counter!=r->num_tuples)
    {
        key = Psum->tuples[j].key;


        if( hashl(r->tuples[i].payloadList->data,n)==key)         //arxika psaxno mono to proto key molis ta vro ola to epomeno etc
        {



            newR->tuples[counter].key=r->tuples[i].key;          
            newR->tuples[counter].payloadList = createRelationPayloadList(r->tuples[i].payloadList->data);
            newR->num_tuples++;
            counter++;

        }
        i++;
        if (i==r->num_tuples)                       //otan elegksoume olo ton pinaka ksekiname apo tin arxi
        {
            i=0;
            j++;


        }

    }

    //printf("printing newr\n");
    //printRelation(newR);


    return newR;

}


hashMap** createHashForBuckets(relation* r, relation* pSum, int hashmap_size, int neighborhood_size){
    int rehash_check = 0;       //check to see if hashtable needs to be reconstructed with larger size
    struct hashMap** hashMapArray = NULL;

    /* if pSum exists, that means we have to make a hash table for every partition, otherwise just create one hash table */
    if(pSum != NULL){

        /* get number of partitions from pSum, and get memory for this many hash tables */
        hashMapArray = calloc(pSum->num_tuples,sizeof(struct hashMap));

        for(int i = 0; i < pSum->num_tuples; i++){

            /* create the hash table for every partition */
            hashMapArray[i] = hashCreate(hashmap_size,i);

            /* if i + 1 > pSum->num_tuples, that means we have reached the final partition, and we use r->num_tuples to find where it ends */
            if( i + 1 >= pSum->num_tuples){
                for(int j = pSum->tuples[i].payloadList->data; j < r->num_tuples; j++){
                    /* insert every tuple into the appropriate hash table */
                    rehash_check = hashInsert(hashMapArray[i], r->tuples[j].key, r->tuples[j].payloadList->data, neighborhood_size);
                    /* check if hash table needs rehashing */
                    if(rehash_check == -1 && neighborhood_size < 40){
                        hashDelete(hashMapArray);
                        printf("rehashing hash table...\n");
                        hashMapArray = NULL;
                        /* new hash table is created by doubling the size of the original one */
                        hashMapArray = createHashForBuckets(r, pSum, hashmap_size * 2, neighborhood_size * 2);
                        return hashMapArray;
                    }
                }
            }else
                /* here we get the starting and ending point of every partition, by using pSum */
                for(int j = pSum->tuples[i].payloadList->data; j < pSum->tuples[i+1].payloadList->data; j++){
                    rehash_check = hashInsert(hashMapArray[i], r->tuples[j].key, r->tuples[j].payloadList->data, neighborhood_size);
                    if(rehash_check == -1 && neighborhood_size < 40){
                        hashDelete(hashMapArray);
                        printf("rehashing hash table...\n");
                        hashMapArray = NULL;
                        hashMapArray = createHashForBuckets(r, pSum, hashmap_size * 2, neighborhood_size * 2);
                        return hashMapArray;
                    }
                }
                printf("created hash map for bucket:%d\n", hashMapArray[i]->bucket);
        }

        return hashMapArray;
    }else{
        hashMapArray = calloc(1,sizeof(struct hashMap));
        hashMapArray[0] = hashCreate(hashmap_size,0);

        for(int i = 0; i < r->num_tuples; i++){
            rehash_check = hashInsert(hashMapArray[0], r->tuples[i].key, r->tuples[i].payloadList->data, neighborhood_size);
            if(rehash_check == -1 && neighborhood_size < 40){
                hashDelete(hashMapArray);
                printf("rehashing hash table...\n");
                hashMapArray = NULL;
                hashMapArray = createHashForBuckets(r, pSum, hashmap_size * 2, neighborhood_size * 2);
                return hashMapArray;
            }
        }
        printf("created hash map for bucket:%d\n", hashMapArray[0]->bucket);
        return hashMapArray;
    }
}

/* same logic as createHashForBuckets but instead of inserting
 tuples, we search each tuple, and if there's a much, we add the tuple to the result */
relation* joinRelation(struct hashMap** hashMapArray, relation *r, relation *smallerR, relation *pSum){
    int exists = 0;
    int nodeCounter = 0;
    struct tuple* newTuple = NULL;

    relation *result = malloc(sizeof(struct relation));
    result->num_tuples = r->num_tuples * smallerR->num_tuples;
    result->tuples = malloc(sizeof(struct tuple) * result->num_tuples);

    /* we use pSum exactly the same way that we did in createHashForBuckets */
    if(pSum != NULL){
        for(int i = 0; i < pSum->num_tuples; i++){
            if(i+1 >= pSum->num_tuples){
                for(int j = pSum->tuples[i].payloadList->data; j < r->num_tuples; j++){
                    if(hashMapArray[i]){
                        //exists = hashSearch(hashMapArray[i], r->tuples[j].key, r->tuples[j].payloadList->data, 0);
                        //if(exists){
                        printf("looking for %d in bucket %d\n", r->tuples[j].payloadList->data, hashMapArray[i]->bucket);
                        int* rowIdList = getKey(hashMapArray[i], r->tuples[j].payloadList->data, 0);
                            int counter = 0;
                            if(rowIdList != NULL){
                                while(rowIdList[counter] > -1){
                                    newTuple = createTupleFromNode(r->tuples[j].payloadList->data, rowIdList[counter], r->tuples[j].key);
                                    result->tuples[nodeCounter] = *newTuple;
                                    nodeCounter++;
                                    counter++;
                                    free(newTuple);
                                }
                                free(rowIdList);

                            }
                            //newTuple = createTupleFromNode(r->tuples[j].key, r->tuples[j].payloadList->data, newPayload);

                        //}
                    }
                }
            }else
                for(int j = pSum->tuples[i].payloadList->data; j < pSum->tuples[i+1].payloadList->data; j++){
                    if(hashMapArray[i]) {
                        //exists = hashSearch(hashMapArray[i], r->tuples[j].key, r->tuples[j].payloadList->data, 0);
                        //if (exists) {
                            printf("%d looking for %d in bucket %d\n", j, r->tuples[j].payloadList->data, hashMapArray[i]->bucket);
                            int* rowIdList = getKey(hashMapArray[i], r->tuples[j].payloadList->data, 0);
                            int counter = 0;
                            if(rowIdList != NULL){
                                while(rowIdList[counter] > -1){
                                    newTuple = createTupleFromNode(r->tuples[j].payloadList->data, rowIdList[counter], r->tuples[j].key);
                                    result->tuples[nodeCounter] = *newTuple;
                                    nodeCounter++;
                                    counter++;
                                    free(newTuple);
                                }
                                free(rowIdList);
                            }

                        //}
                    }
                }
        }
        result->num_tuples = nodeCounter;
        return result;
    }else{
        /* if there is only 1 partition, check for key in every hash table, top to bottom */
        for(int i = 0; i < r->num_tuples; i++){
            int j = 0;
            while(hashMapArray[j]){
               // exists = hashSearch(hashMapArray[j], r->tuples[i].key, r->tuples[i].payloadList->data, 0);
               // if(exists){
                    int* rowIdList = getKey(hashMapArray[j], r->tuples[i].payloadList->data, 0);
                    int counter = 0;
                    if(rowIdList != NULL){
                        while(rowIdList[counter] > -1){
                            newTuple = createTupleFromNode(r->tuples[i].payloadList->data, rowIdList[counter], r->tuples[i].key);
                            result->tuples[nodeCounter] = *newTuple;
                            nodeCounter++;
                            counter++;
                            free(newTuple);
                        }
                        free(rowIdList);
                    }

              //  }
                j++;
            }
        }
    }
    result->num_tuples = nodeCounter;
    return result;
}


result* PartitionedHashJoin(relation *relR, relation *relS){

    /**     Step 1. Partitioning        **/
    int nR,nS;


    nR=findNumOfBuckets(relR);
    nS=findNumOfBuckets(relS);


    relation *newR,*newS,*rPsum,*sPsum,*rHist,*sHist;


    rHist=createHist(relR,nR);
    sHist=createHist(relS,nS);


    rPsum=createPsum(relR,nR,rHist);          
    sPsum=createPsum(relS,nS,sHist);


    newR=relPartitioned(relR, rPsum, nR,rHist); 
    newS=relPartitioned(relS, sPsum, nS,sHist);


    //printRelation(newR);

    relationDelete(sHist);
    relationDelete(rHist);

    relation* largerR = NULL;
    relation* largerPSum = NULL;
    relation* smallerR = NULL;
    relation* smallerPSum = NULL;

    /* compare the size of the two relations, and use the smaller one to create the hash tables */

    if(newR->num_tuples > newS->num_tuples){
        largerR = newR;
        largerPSum = rPsum;
        smallerR = newS;
        smallerPSum = sPsum;
    }else{
        largerR = newS;
        largerPSum = sPsum;
        smallerR = newR;
        smallerPSum = rPsum;
    }

    hashMap** hashMapArray = NULL;  //array that will hold every hash table
    int hash_map_size = 0;

    if(!rPsum){
        hash_map_size = smallerR->num_tuples;
    }else{
        hash_map_size = smallerR->num_tuples / rPsum->num_tuples;
    }

    double neighborhood_size = log2((double)hash_map_size);

    /* creat hash table for every bucket */
    hashMapArray = createHashForBuckets(smallerR, smallerPSum, hash_map_size, (int)neighborhood_size+1);

    /* use the hash table(s) to create the final result (from join) */
    relation* result = joinRelation(hashMapArray, largerR, smallerR, largerPSum);
    printRelation(result);

    /* freeing the memory from everything */

    if(rPsum){
        relationDelete(rPsum);
        relationDelete(newR);
    }
    if(sPsum){
        relationDelete(sPsum);
        relationDelete(newS);
    }

    relationDelete(result);
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

