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

    //printf("AT START size %d  neigh %d\n",hashmap_size, neighborhood_size);

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
                    if(rehash_check == -1 && neighborhood_size < 640){
                        hashDelete(hashMapArray);
                        //printf("rehashing hash table...\n");
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
                    if(rehash_check == -1 && neighborhood_size < 640){
                        hashDelete(hashMapArray);
                        //printf("rehashing hash table...\n");
                        hashMapArray = NULL;
                        hashMapArray = createHashForBuckets(r, pSum, hashmap_size * 2, neighborhood_size * 2);
                        return hashMapArray;
                    }
                }
                //printf("created hash map for bucket:%d\n", hashMapArray[i]->bucket);
        }

        return hashMapArray;
    }else{
        hashMapArray = calloc(r->num_tuples,sizeof(struct hashMap));
        hashMapArray[0] = hashCreate(hashmap_size,0);

        for(int i = 0; i < r->num_tuples; i++){
            rehash_check = hashInsert(hashMapArray[0], r->tuples[i].key, r->tuples[i].payloadList->data, neighborhood_size);
            if(rehash_check == -1 && neighborhood_size < 640){
                hashDelete(hashMapArray);
                hashMapArray = NULL;
                hashMapArray = createHashForBuckets(r, pSum, hashmap_size * 2, neighborhood_size * 2);
                return hashMapArray;
            }
        }
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
    result->num_tuples = r->num_tuples;

    result->tuples = malloc(sizeof(struct tuple) * result->num_tuples);
    if(result->tuples == NULL){
        printf("not enough memory\n");
        return NULL;
    }

    /* we use pSum exactly the same way that we did in createHashForBuckets */
    if(pSum != NULL){
        for(int i = 0; i < pSum->num_tuples; i++){
            if(i+1 >= pSum->num_tuples){
                for(int j = pSum->tuples[i].payloadList->data; j < r->num_tuples; j++){
                    if(hashMapArray[i]){

                        int* rowIdList = getKey(hashMapArray[i], r->tuples[j].payloadList->data, 0);
                            int counter = 0;
                            if(rowIdList != NULL){
                                while(rowIdList[counter] > -1){
                                    newTuple = createTupleFromNode(r->tuples[j].payloadList->data, r->tuples[j].key, rowIdList[counter]);
                                    result->tuples = realloc(result->tuples, sizeof(struct tuple) * (nodeCounter + 1));
                                    result->tuples[nodeCounter] = *newTuple;
                                    nodeCounter++;
                                    counter++;
                                    free(newTuple);
                                }
                                free(rowIdList);

                            }

                    }
                }
            }else
                for(int j = pSum->tuples[i].payloadList->data; j < pSum->tuples[i+1].payloadList->data; j++){
                    if(hashMapArray[i]) {

                        int* rowIdList = getKey(hashMapArray[i], r->tuples[j].payloadList->data, 0);
                        int counter = 0;
                        if(rowIdList != NULL){
                            while(rowIdList[counter] > -1){
                                newTuple = createTupleFromNode(r->tuples[j].payloadList->data, r->tuples[j].key, rowIdList[counter]);
                                result->tuples = realloc(result->tuples, sizeof(struct tuple) * (nodeCounter + 1));
                                result->tuples[nodeCounter] = *newTuple;
                                nodeCounter++;
                                counter++;
                                free(newTuple);
                            }
                            free(rowIdList);
                        }


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

                int* rowIdList = getKey(hashMapArray[j], r->tuples[i].payloadList->data, 0);
                int counter = 0;
                if(rowIdList != NULL){
                    while(rowIdList[counter] > -1){
                        newTuple = createTupleFromNode(r->tuples[i].payloadList->data, r->tuples[i].key, rowIdList[counter]);
                        result->tuples = realloc(result->tuples, sizeof(struct tuple) * (nodeCounter + 1));
                        result->tuples[nodeCounter] = *newTuple;
                        nodeCounter++;
                        counter++;
                        free(newTuple);
                    }
                    free(rowIdList);
                }

                j++;
            }
        }
    }
    result->num_tuples = nodeCounter;
    return result;
}

relation* histArrayToHist(relation **hArray,int size)
{

    int count_size=0;

    for (int i=0 ; i<size ; i++)
    {

        if (hArray[i]->num_tuples>count_size) count_size=hArray[i]->num_tuples;

    }

    relation* hist=createEmptyRelation(count_size);
    for( int i=0 ; i<count_size ; i++)
    {
        hist->tuples[i].key=i;

    }

    for( int i=0 ; i<size ; i++)
    {
        for(int j=0 ; j<hArray[i]->num_tuples ; j++)
        {
            if (hist->tuples[hArray[i]->tuples[j].key].payloadList==NULL){
                relationPayloadList *p=malloc(sizeof(relationPayloadList));

                p->next=NULL;
                p->data=hArray[i]->tuples[j].payloadList->data;
                hist->tuples[hArray[i]->tuples[j].key].payloadList=p;
                continue;
            }
            else
                hist->tuples[hArray[i]->tuples[j].key].payloadList->data+=hArray[i]->tuples[j].payloadList->data;

        }

    }

    return hist;


}


void * histWithThread(void *args)
{
    // printf("entering thread\n");
    histThreadArgs* h= (histThreadArgs*) args;

    h->histR=createHist(h->relR,h->nR);

    h->histS=createHist(h->relS,h->nS);
    // printf("exiting thread\n");
    pthread_exit(NULL);
}


relation* PartitionedHashJoin(relation *relR, relation *relS){

    if(relR->num_tuples <= 0 || relS->num_tuples <= 0){
        return NULL;
    }
    /**     Step 1. Partitioning        **/


    int nR,nS;


    nR=findNumOfBuckets(relR);
    nS=findNumOfBuckets(relS);

/*      threading       */
    int numOfThreads=4;

    threadArray *tarray=malloc(sizeof(threadArray));

    tarray->noThreads=numOfThreads;

    tarray->threads=malloc(numOfThreads*sizeof(pthread_t));

    int indexR=0;
    int indexS=0;

    int Rcounter=relR->num_tuples/numOfThreads;
    int Scounter=relS->num_tuples/numOfThreads;

    relation **threadRelationsR=malloc(numOfThreads*sizeof(relation*));
    relation **threadRelationsS=malloc(numOfThreads*sizeof(relation*));



    for (int i=0 ; i< numOfThreads ; i++)       // just to get everything ready, no thread creation yet
    {


        if (indexR>relR->num_tuples)
            threadRelationsR[i]=createEmptyRelation(Rcounter-(indexR-relR->num_tuples));
        else
            threadRelationsR[i]=createEmptyRelation(Rcounter);


        if (indexS>relS->num_tuples)
            threadRelationsS[i]=createEmptyRelation(Scounter-(indexS-relS->num_tuples));
        else
            threadRelationsS[i]=createEmptyRelation(Scounter);    


        
        for (int j=0 ; j<Rcounter ; j++)
        {
            threadRelationsR[i]->tuples[j].key=relR->tuples[indexR+j].key;
            relationPayloadList *p=malloc(sizeof(relationPayloadList));
            p->data=relR->tuples[indexR+j].payloadList->data;

            p->next=NULL;
            threadRelationsR[i]->tuples[j].payloadList=p;
            if (relR->num_tuples==indexR+j) break;

        }
        for (int j=0 ; j<Scounter ; j++)
        {

            threadRelationsS[i]->tuples[j].key=relS->tuples[indexS+j].key;
            relationPayloadList *p=malloc(sizeof(relationPayloadList));
            p->data=relS->tuples[indexS+j].payloadList->data;
            p->next=NULL;
            threadRelationsS[i]->tuples[j].payloadList=p;
            if (relS->num_tuples==indexS+j) break;
        }
        indexR+=Rcounter;
        indexS+=Scounter;

    }

    relation **histResultR;
    relation **histResultS;


    histResultR=malloc(numOfThreads*sizeof(relation*));
    histResultS=malloc(numOfThreads*sizeof(relation*));

    histThreadArgs *h=malloc(numOfThreads*sizeof(histThreadArgs));
    for (int i=0 ; i<numOfThreads ; i++)
    {
        h[i].relR=threadRelationsR[i];

        h[i].relS=threadRelationsS[i];

        h[i].nR=nR;
        h[i].nS=nS;

        h[i].histR=NULL;
        h[i].histS=NULL;

        pthread_create(&tarray->threads[i] , NULL, histWithThread, (void *)&h[i]);
    }

    for (int i=0 ; i<numOfThreads ; i++)
    pthread_join(tarray->threads[i],NULL);



    /*      end of threading       */


    relation *newR,*newS,*rPsum,*sPsum,*rHist,*sHist;
    for (int i=0 ; i<numOfThreads ; i++){

        histResultR[i]=h[i].histR;
        histResultS[i]=h[i].histS;


    }



    rHist=histArrayToHist(histResultR,numOfThreads);
    sHist=histArrayToHist(histResultS,numOfThreads);


    rPsum=createPsum(relR,nR,rHist);          
    sPsum=createPsum(relS,nS,sHist);


    newR=relPartitioned(relR, rPsum, nR,rHist); 
    newS=relPartitioned(relS, sPsum, nS,sHist);


    //printRelation(newR);
/*              freeing memory for histograms               */
    relationDelete(sHist);
    relationDelete(rHist);
    for (int i=0 ; i<numOfThreads ; i++){
        relationDelete(threadRelationsR[i]);
        relationDelete(threadRelationsS[i]);
        relationDelete(h[i].relR);
        relationDelete(h[i].relS);
        relationDelete(h[i].histR);
        relationDelete(h[i].histS);
    }


    free(h);

    free(threadRelationsR);
    free(threadRelationsS);

    free(tarray->threads);
    free(tarray);


    free(histResultR);
    free(histResultS);

/*                          done                           */
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
        if(rPsum->num_tuples > smallerR->num_tuples){
            hash_map_size = smallerR->num_tuples;
        }
    }

    double neighborhood_size = log2((double)hash_map_size);

    /* creat hash table for every bucket */
    hashMapArray = createHashForBuckets(smallerR, smallerPSum, hash_map_size, (int)neighborhood_size+1);

    /* use the hash table(s) to create the final result (from join) */
    relation* result = joinRelation(hashMapArray, largerR, smallerR, largerPSum);

    /* freeing the memory from everything */

    if(rPsum){
        relationDelete(rPsum);
        relationDelete(newR);
    }
    if(sPsum){
        relationDelete(sPsum);
        relationDelete(newS);
    }

    hashDelete(hashMapArray);

    return result;

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


void addToPayloadList(relationPayloadList* p,int data){             // adds data to list


    relationPayloadList *temp=p;

    if(p==NULL){
        temp=malloc(sizeof(relationPayloadList));
        temp->next=NULL;
        temp->data=data;
        p=temp;
        return;
    }

    while(temp->next!=NULL)
        temp=temp->next;


    temp->next=malloc(sizeof(relationPayloadList));
    temp->next->next=NULL;
    temp->next->data=data;

    return;

}

int getNumRelations(char* str){             // returns number of relations in str

    int count = 1;


	char* token = strtok(str," ");


	while(token!=NULL){
		token = strtok(NULL, " ");
		if(token != NULL){
			count++;
		}
	}
    return count;
}







