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
        r->tuples[i].payloadList.data=num2;

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

        hist->tuples[i].payloadList.data=0;                 /* midenizo oles tis theseis tou histogram gia na mporo na metriso meta */
        hist->tuples[i].key=i;

    }

    hist->num_tuples=histSize;


    relation *Psum=malloc(sizeof(relation));
    Psum->tuples = malloc(histSize * sizeof(tuple));       /* to Psum */





    for ( i = 0 ; i < r->num_tuples ; i++ ){             /* etoiamzoume to hist */

        tuple *t;

        if (t = SearchKey(hist,r->tuples[i].key,n))        //psaxno to key an to vro epistefo pointer se auto allios NULL
            t->payloadList.data++;
      
    }


    // printf("printing hist\n");
    // printRelation(hist);


    Psum->num_tuples=hist->num_tuples;


    int position=0;

    for ( i = 0 ; i < hist->num_tuples ; i++ ){             /* etoimazoume to Psum */

        Psum->tuples[i].key=hashl(hist->tuples[i].key,n);
        Psum->tuples[i].payloadList.data=position;

        position+=hist->tuples[i].payloadList.data;

    }

    relationDelete(hist);

    printf("printing psum\n");
    //printRelation(Psum);
    return Psum;

}


relation* relPartitioned(relation *r, relation *Psum, int n){


    if (n==0) return r;


    printf("i just entered relPartitioned\n");

    
    /* ftiaxnoume to newR */

    int j=0,key,positions,currPos,i;
    key = Psum->tuples[j].key;
    positions = Psum->tuples[j+1].payloadList.data;      //i topothesia p vrisketai to epomeno bucket
    currPos=0;          //i topothesia p vriskomaste ston newR
    i=0;


    relation *newR=malloc(sizeof(relation));
    newR->tuples=malloc(r->num_tuples*sizeof(tuple));   /* o R' apo tin ekfonisi */




    newR->num_tuples = 0;

    printf("printing r\n");
    //printRelation(r);

    while ( currPos != r->num_tuples) { /*diavazo ena ena stoixeio mexri na mpoun ola*/
        if (i==r->num_tuples) i=0;
        if( hashl(r->tuples[i].key,n)==key){         //arxika psaxno mono to proto key molis ta vro ola to epomeno etc



            newR->tuples[currPos].key=r->tuples[i].key;          //TODO na to allakso to key gia na mpainei to sosto stoixeio
            newR->tuples[currPos].payloadList.data=r->tuples[i].payloadList.data;
            newR->num_tuples++;

            currPos++;
            if (currPos==positions){            //ama mpoun ola ta stoixeia tou key pao sto epomeno key
                j++;
                if (j+1 != Psum->num_tuples)        //TODO na exo to nou mou
                    positions=Psum->tuples[j+1].payloadList.data;


                //printf(" num tuples %d kai to j+1 : %d\n",Psum->num_tuples,j+1);
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


hashMap** createHashForBuckets(relation* r, relation* pSum, int hashmap_size, int neighborhood_size){
    int rehash_check = 0;
    int* myHash = NULL;
    struct hashMap** hashMapArray = NULL;
    if(pSum != NULL){
        hashMapArray = calloc(pSum->num_tuples,sizeof(struct hashMap));

        for(int i = 0; i < pSum->num_tuples; i++){
            hashMapArray[i] = hashCreate(hashmap_size,i);

            if( i + 1 >= pSum->num_tuples){
                for(int j = pSum->tuples[i].payloadList.data; j < r->num_tuples; j++){
                    rehash_check = hashInsert(hashMapArray[i], r->tuples[j].key, r->tuples[j].payloadList.data, neighborhood_size);
                    if(rehash_check == -1 && neighborhood_size < 40){
                        hashDelete(hashMapArray);
                        printf("rehashing...\n");
                        hashMapArray = NULL;
                        hashMapArray = createHashForBuckets(r, pSum, hashmap_size * 2, neighborhood_size * 2);
                        return hashMapArray;
                    }
                }
            }else
                for(int j = pSum->tuples[i].payloadList.data; j < pSum->tuples[i+1].payloadList.data; j++){
                    rehash_check = hashInsert(hashMapArray[i], r->tuples[j].key, r->tuples[j].payloadList.data, neighborhood_size);
                    if(rehash_check == -1 && neighborhood_size < 40){
                        hashDelete(hashMapArray);
                        printf("rehashing...\n");
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
            rehash_check = hashInsert(hashMapArray[0], r->tuples[i].key, r->tuples[i].payloadList.data, neighborhood_size);
            if(rehash_check == -1 && neighborhood_size < 40){
                hashDelete(hashMapArray);
                printf("rehashing...\n");
                hashMapArray = NULL;
                hashMapArray = createHashForBuckets(r, pSum, hashmap_size * 2, neighborhood_size * 2);
                return hashMapArray;
            }
        }
        printf("created hash map for bucket:%d\n", hashMapArray[0]->bucket);
        return hashMapArray;
    }
}


relation* joinRelation(struct hashMap** hashMapArray, relation *r, relation *pSum){
    int bucket = 0;
    int exists = 0;
    int nodeCounter = 0;
    struct tuple* newTuple = NULL;

    relation *result = malloc(sizeof(struct relation));
    result->num_tuples = r->num_tuples;
    result->tuples = malloc(sizeof(struct tuple) * result->num_tuples);

    if(pSum != NULL){
        for(int i = 0; i < pSum->num_tuples; i++){
            if(i+1 >= pSum->num_tuples){
                for(int j = pSum->tuples[i].payloadList.data; j < r->num_tuples; j++){
                    if(hashMapArray[i]){
                        exists = hashSearch(hashMapArray[i], r->tuples[j].key, r->tuples[j].payloadList.data, 0);
                        printf("checked key %d exists %d\n", r->tuples[j].key, exists);
                        if(exists){
                            newTuple = createTupleFromNode(r->tuples[j].key, r->tuples[j].payloadList.data);
                            result->tuples[nodeCounter] = *newTuple;
                            nodeCounter++;
                            free(newTuple);
                        }
                    }
                }
            }else
                for(int j = pSum->tuples[i].payloadList.data; j < pSum->tuples[i+1].payloadList.data; j++){
                    if(hashMapArray[i]) {
                        exists = hashSearch(hashMapArray[i], r->tuples[j].key, r->tuples[j].payloadList.data, 0);
                        printf("checked key %d exists %d\n", r->tuples[j].key, exists);
                        if (exists) {
                            newTuple = createTupleFromNode(r->tuples[j].key, r->tuples[j].payloadList.data);
                            result->tuples[nodeCounter] = *newTuple;
                            nodeCounter++;
                            free(newTuple);
                        }
                    }
                }
        }
        result->num_tuples = nodeCounter;
        return result;
    }else{
        for(int i = 0; i < r->num_tuples; i++){
            int j = 0;
            while(hashMapArray[j]){
                exists = hashSearch(hashMapArray[j], r->tuples[i].key, r->tuples[i].payloadList.data, 0);
                //printf("checked key %d exists %d\n", r->tuples[i].key, exists);
                if(exists){
                    newTuple = createTupleFromNode(r->tuples[i].key, r->tuples[i].payloadList.data);
                    result->tuples[nodeCounter] = *newTuple;
                    nodeCounter++;
                    free(newTuple);
                }
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

    printf(" nr: %d ns: %d\n",nR,nS);

    relation *newR,*newS,*rPsum,*sPsum;

    rPsum=createPsum(relR,nR);          
    sPsum=createPsum(relS,nS);


    newR=relPartitioned(relR, rPsum, nR);   //na do an xreiazontai na n
    newS=relPartitioned(relS, sPsum, nS);

    relation* largerR = NULL;
    relation* largerPSum = NULL;
    relation* smallerR = NULL;
    relation* smallerPSum = NULL;

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

    printf(" partitioning over\n");

    hashMap** hashMapArray = NULL;

    int hash_map_size = 0;

    if(!rPsum){
        hash_map_size = largerR->num_tuples;
    }else{
        hash_map_size = largerR->num_tuples / rPsum->num_tuples;
    }

    hashMapArray = createHashForBuckets(largerR, largerPSum, hash_map_size, 4);


    relation* result = joinRelation(hashMapArray, smallerR, smallerPSum);
    printRelation(result);

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

