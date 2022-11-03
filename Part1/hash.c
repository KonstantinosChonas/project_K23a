#include "hash.h"
#include "int.h"

payloadList* createPayloadList(int data){
    payloadList *temp; 
    temp = (node)malloc(sizeof(payloadList));
    temp->data=data;
    temp->next = NULL;
    return temp;
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
    return head;
}

hashMap* hashCreate(int bucket){
    hashMap *newHashMap = malloc(sizeof(struct hashMap));
    newHashMap->nodeCount = 0;
    newHashMap->bucket = bucket;
    for (int i=0; i<HASH_TABLE_SIZE; i++)
        newHashMap->hashNodes[i] = NULL;
    for (int i=0; i<HASH_TABLE_SIZE; i++)
        newHashMap->bitmap[i] = 0;
    return newHashMap;
}

hashNode* hashNodeCreate(int key, int payload, int hop){
    hashNode *newHashNode = malloc(sizeof(struct hashNode));
    newHashNode->key = key;
    newHashNode->payload = createPayloadList(payload);
    newHashNode->hop = hop;
    newHashNode->bitmap = malloc(sizeof(hop*sizeof(int)));
    for (int i=0; i<hop; i++)
        newHashNode->bitmap[i] = 0;
    return newHashNode;
}

int hashSearch(hashMap* hashTable, int key, int payload, int n, int flag){            //if flag==1 addpayload to payload list
    int keyHash = hashl(key,n);
    if(hashTable->hashNodes[keyHash] == NULL)
        return 0;
    int hop=hashTable->hashNodes[keyHash]->hop;    
    for(int i=0; i<hop; i++){
        if(hashTable->hashNodes[keyHash+i]->key == key){
            if (flag == 1)
                addPayload(hashTable->hashNodes[keyHash+1]->payload,payload);
            return 1;
        }
    }
    return 0;
}


void hashNodeUpdate(hashNode* hashNode, int key, int payload, int hop){
    hashNode->key = key;
    hashNode->payload = payload;
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

int hashInsert(hashMap* hashTable, int key, int payload, int n){
    int keyHash, jump, step, index, keyHashAlready;
    int hop = 4;

    keyHash = hashl(key,n);
    //if the hash is new to the hashTable
    if(hashTable->hashNodes[keyHash] == NULL){
        hashTable->hashNodes[keyHash] = hashNodeCreate(key, payload, hop);
        printf("now hash bitmap is: %d\n", hashTable->bitmap[0]);
        printf("now node bitmap is: %d\n", hashTable->hashNodes[keyHash]->bitmap[0]);
        updateBitmapInsert(hashTable->hashNodes[keyHash]->bitmap, 0);
        updateBitmapInsert(hashTable->bitmap, 0);
        printf("now node bitmap is: %d\n", hashTable->hashNodes[keyHash]->bitmap[0]);
        printf("now hash bitmap is: %d\n", hashTable->bitmap[0]);
        int x=hashSearch(hashTable,key,n);
        printf("exists: %d\n",x);
        return 1;
    }
    //if the same key exists    
    if(hashSearch(hashTable,key,payload,n,1) == 1){
        return 1;   
    }

    //if neighborhood is full
    if(checkNeighborhood(hashTable,keyHash) == 1){
        return -1;
    }
    //find empty node
    for(jump=keyHash+1; jump<HASH_TABLE_SIZE; jump++){
        if(hashTable->bitmap[jump]==0)
            break;
    }
    //if empty node is in the neighborhood
    if((jump-keyHash)%n < hop){
        if(hashTable->hashNodes[jump] ==NULL){
            hashTable->hashNodes[jump] = hashNodeCreate(key, payload, hop);
    }
        printf("now hash bitmap is: %d\n", hashTable->bitmap[jump]);
        printf("now node bitmap is: %d\n", hashTable->hashNodes[keyHash]->bitmap[jump-keyHash]);
        updateBitmapInsert(hashTable->hashNodes[keyHash]->bitmap, jump-keyHash);
        updateBitmapInsert(hashTable->bitmap, jump);
        printf("now node bitmap is: %d\n", hashTable->hashNodes[keyHash]->bitmap[jump-keyHash]);
        printf("now hash bitmap is: %d\n", hashTable->bitmap[jump]);
        return 1;
    }
    
    int y;
    while((jump-keyHash)%n >= hop){
        int flag=0;
        for(y=jump-hop+1; y<jump; y++){
            keyHashAlready=hashl(hashTable->hashNodes[y]->key,n);
            if ((jump-keyHashAlready)%n < hop){
                if(hashTable->hashNodes[jump] == NULL){
                    hashTable->hashNodes[jump] = hashNodeCreate(hashTable->hashNodes[y]->key, hashTable->hashNodes[y]->payload, hop);
                    updateBitmapInsert(hashTable->hashNodes[keyHashAlready]->bitmap,jump-keyHashAlready);
                    updateBitmapInsert(hashTable->bitmap, jump);
                    updateBitmapRemove(hashTable->hashNodes[keyHashAlready]->bitmap,y-keyHashAlready);
                    updateBitmapInsert(hashTable->bitmap, y);
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
        printf("now hash bitmap is: %d\n", hashTable->bitmap[jump]);
        printf("now node bitmap is: %d\n", hashTable->hashNodes[keyHash]->bitmap[jump-keyHash]);
        updateBitmapInsert(hashTable->hashNodes[keyHash]->bitmap, jump-keyHash);
        updateBitmapInsert(hashTable->bitmap, jump);
        printf("now node bitmap is: %d\n", hashTable->hashNodes[keyHash]->bitmap[jump-keyHash]);
        printf("now hash bitmap is: %d\n", hashTable->bitmap[jump]);
        return 1;
    }
 }



void updateBitmapInsert(int* bitmap, int position){
    bitmap[position] = 1;
}

void updateBitmapRemove(int* bitmap, int position){
    bitmap[position] = 0;
}

