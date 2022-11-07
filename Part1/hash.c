#include "hash.h"
#include "int.h"


payloadList* createPayloadList(int data){
    payloadList *temp; 
    temp = malloc(sizeof(payloadList));
    temp->data=data;
    temp->next = NULL;
    return temp;
}

int getHash(int key, int numOfBuckets){
    return (key % numOfBuckets);
}

void addPayload(payloadList* head, int data){
    payloadList *temp,*x;
    temp = createPayloadList(data);
    if(head == NULL){
        head = temp;    
    }
    else{
        x = head;
        while(x->next != NULL){
            x = x->next;
        }
        x->next = temp;
    }
    return;
}

hashMap* hashCreate(int size,int bucket){
    hashMap *newHashMap = malloc(sizeof(struct hashMap));
    newHashMap->hashNodes = malloc(sizeof(struct hashNode)*size);
    newHashMap->bitmap = malloc(sizeof(int)*size);
    newHashMap->hashSize=size;
    newHashMap->nodeCount = 0;
    newHashMap->bucket = bucket;
    for (int i=0; i<newHashMap->hashSize; i++)
        newHashMap->hashNodes[i] = NULL;
    for (int i=0; i<newHashMap->hashSize; i++)
        newHashMap->bitmap[i] = 0;
    return newHashMap;
}

hashNode* hashNodeCreate(int key, int payload, int hop){
    hashNode *newHashNode = malloc(sizeof(struct hashNode));
    newHashNode->key = key;
    newHashNode->payload = createPayloadList(payload);
    newHashNode->hop = hop;
    newHashNode->bitmap = malloc(sizeof(int) * hop);
    for (int i=0; i<hop; i++)
        newHashNode->bitmap[i] = 0;
    return newHashNode;
}

int hashSearch(hashMap* hashTable, int key, int payload, int flag){            //if flag==1 addpayload to payload list
    int keyHash = getHash(key,hashTable->hashSize);
    if(hashTable->hashNodes[keyHash] == NULL)
        return 0;
    int hop=hashTable->hashNodes[keyHash]->hop;

    for(int i=0; i<hop; i++){
        if(keyHash + i < hashTable->hashSize){
            if(hashTable->hashNodes[keyHash+i]){
                if(hashTable->hashNodes[keyHash+i]->key==key){
                    if (flag == 1)
                        addPayload(hashTable->hashNodes[keyHash+1]->payload,payload);
                    return 1;
                }
            }
        }
    }
    return 0;
}


void hashNodeUpdate(hashNode* hashNode, int key, int payload, int hop){
    hashNode->key = key;
    hashNode->payload->data = payload;
    hashNode->hop = hop;
}

void addPayloadToNode(hashNode* hashNode,int payload){
    addPayload(hashNode->payload, payload);
}

int checkNeighborhood(hashMap* hashTable, int keyhash){
    int hop=hashTable->hashNodes[keyhash]->hop;
    for (int i=0; i<hop; i++){
        if (hashTable->hashNodes[keyhash]->bitmap[i] == 0)
            return 0;
    }
    return 1;

}

int hashInsert(hashMap* hashTable, int key, int payload, int neighborhood_size){
    int keyHash, jump, step, index, keyHashAlready;
    int hop = neighborhood_size;

    keyHash = getHash(key, hashTable->hashSize);
    //if the hash is new to the hashTable

    if(hashTable->hashNodes[keyHash] == NULL){
        hashTable->hashNodes[keyHash] = hashNodeCreate(key, payload, hop);
        hashTable->nodeCount++;
        //printf("now hash bitmap is: %d\n", hashTable->bitmap[0]);
        //printf("now node bitmap is: %d\n", hashTable->hashNodes[keyHash]->bitmap[0]);
        updateBitmapInsert(hashTable->hashNodes[keyHash]->bitmap, 0);
        updateBitmapInsert(hashTable->bitmap, keyHash);
        //printf("now node bitmap is: %d\n", hashTable->hashNodes[keyHash]->bitmap[0]);
        //printf("now hash bitmap is: %d\n", hashTable->bitmap[0]);
        return 1;
    }

    //if the same key exists    
    if(hashSearch(hashTable,key,payload,0) == 1){
        return 1;
    }

    //if neighborhood is full
    if(checkNeighborhood(hashTable,keyHash) == 1){
        /* needs rehashing here */
        return -1;
    }
    int nodecheck = 0;
    //find empty node
    for(jump = keyHash+1; jump < hashTable->hashSize; jump++){
        //        if(hashTable->hashNodes[keyHash]->bitmap[jump] ==0)
        if(hashTable->bitmap[jump]==0){
            nodecheck = 1;
            break;
        }
    }
    if (nodecheck == 0){            //if there is no free node
        return -1;
    }
    //if empty node is in the neighborhood
    if((jump-keyHash)% hashTable->hashSize < hop){
        if(hashTable->hashNodes[jump] == NULL){
            hashTable->hashNodes[jump] = hashNodeCreate(key, payload, hop);
            hashTable->nodeCount++;
            //printf("now hash bitmap is: %d\n", hashTable->bitmap[jump]);
            //printf("now node bitmap is: %d\n", hashTable->hashNodes[keyHash]->bitmap[jump-keyHash]);
            updateBitmapInsert(hashTable->hashNodes[keyHash]->bitmap, jump-keyHash);
            updateBitmapInsert(hashTable->bitmap, jump);
            //printf("now node bitmap is: %d\n", hashTable->hashNodes[keyHash]->bitmap[jump-keyHash]);
            //printf("now hash bitmap is: %d\n", hashTable->bitmap[jump]);
            return 1;
        }
    }
    
    int y;
    while((jump-keyHash)% hashTable->hashSize >= hop){
        int flag=0;
        for(y=jump-hop+1; y<jump; y++){
            keyHashAlready = getHash(hashTable->hashNodes[y]->key,hashTable->hashSize);
            if ((jump-keyHashAlready)% hashTable->hashSize < hop){
                if(hashTable->hashNodes[jump] == NULL){
                    hashTable->hashNodes[jump] = hashNodeCreate(hashTable->hashNodes[y]->key, hashTable->hashNodes[y]->payload->data, hop);
                    //hashTable->nodeCount++;
                    updateBitmapInsert(hashTable->hashNodes[keyHashAlready]->bitmap,jump-keyHashAlready);
                    updateBitmapInsert(hashTable->bitmap, jump);
                    updateBitmapRemove(hashTable->hashNodes[keyHashAlready]->bitmap,y-keyHashAlready);
                    updateBitmapInsert(hashTable->bitmap, y);
                    if(hashTable->hashNodes[y] != NULL){
                        free(hashTable->hashNodes[y]->payload);
                        free(hashTable->hashNodes[y]->bitmap);
                        free(hashTable->hashNodes[y]);
                    }
                    hashTable->hashNodes[y] = NULL;
                }
                flag=1;
                break;
            }
        }
        if (flag == 0)
            return -1;

        jump=y;
    }
    if(hashTable->hashNodes[jump] ==NULL){
        hashTable->hashNodes[jump] = hashNodeCreate(key, payload, hop);
        hashTable->nodeCount++;
        //printf("now hash bitmap is: %d\n", hashTable->bitmap[jump]);
        //printf("now node bitmap is: %d\n", hashTable->hashNodes[keyHash]->bitmap[jump-keyHash]);
        updateBitmapInsert(hashTable->hashNodes[keyHash]->bitmap, jump-keyHash);
        updateBitmapInsert(hashTable->bitmap, jump);
        //printf("now node bitmap is: %d\n", hashTable->hashNodes[keyHash]->bitmap[jump-keyHash]);
        //printf("now hash bitmap is: %d\n", hashTable->bitmap[jump]);
        return 1;
    }
 }



void updateBitmapInsert(int* bitmap, int position){
    bitmap[position] = 1;
}

void updateBitmapRemove(int* bitmap, int position){
    bitmap[position] = 0;
}

void hashDelete(hashMap** myHashMap){
    int i = 0;
    int j = 0;
    int counter = 0;

    if(myHashMap){
        while(myHashMap[i] != NULL){
            for(j = 0; j < myHashMap[i]->hashSize; j++){
                if(myHashMap[i]->hashNodes[j] != NULL){
                    //printf("deleted key %d payload %d\n", myHashMap[i]->hashNodes[j]->key, myHashMap[i]->hashNodes[j]->payload->data);
                    free(myHashMap[i]->hashNodes[j]->payload);
                    free(myHashMap[i]->hashNodes[j]->bitmap);
                    free(myHashMap[i]->hashNodes[j]);
                }
            }
            i++;
        }

        i = 0;
        while(myHashMap[i] != NULL){
            free(myHashMap[i]->bitmap);
            free(myHashMap[i]->hashNodes);
            free(myHashMap[i]);
            i++;
        }
        free(myHashMap);
    }
}
