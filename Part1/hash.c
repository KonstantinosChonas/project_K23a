//
// Created by aris on 24/10/2022.
//

#include "hash.h"
#include "int.h"

hashMap* hashCreate(int bucket){
    hashMap *newHashMap = malloc(sizeof(struct hashMap));
    newHashMap->nodeCount = 0;
    newHashMap->bucket = bucket;
    return newHashMap;
}

hashNode* hashNodeCreate(int key, int payload){
    hashNode *newHashNode = malloc(sizeof(struct hashNode));
    newHashNode->key = key;
    newHashNode->payload = payload;

    return newHashNode;
}

int hashInsert(hashMap* hashTable, int key, int payload, int n){
    int keyHash, jump, step, index, keyHashAlready;
    int hop = 4;

    keyHash = hashl(key,n);
    //if the key is new to the hashTable
    if(hashTable->hashNodes[keyHash] == NULL){
        hashTable->hashNodes[keyHash] = hashNodeCreate(key, payload);
        printf("now bitmap is: %d\n", hashTable->hashNodes[keyHash]->bitmap[0]);
        updateBitmapInsert(hashTable->hashNodes[keyHash]->bitmap, 0);
        printf("now bitmap is: %d\n", hashTable->hashNodes[keyHash]->bitmap[0]);
        return 1;
    }else{
        /*
        jump = keyHash + 1;
        while(jump != keyHash && hashTable[jump%HASH_TABLE_SIZE] != 0)
            jump = (jump+1) % 20;

        if(jump == keyHash)
            return 0;

        while(1){
            if(jump < keyHash)
                jump = jump + HASH_TABLE_SIZE;
            if(abs(jump - keyHash) < hop){
                hashTable[jump % HASH_TABLE_SIZE] = key;
                return 1;
            }

            step = 1;
            while(step < hop){
                index = (jump - hop + step) % HASH_TABLE_SIZE;
                if(index <0)
                    index += HASH_TABLE_SIZE;

                keyHashAlready = hashl(hashTable[index], n);
                if(jump > HASH_TABLE_SIZE)
                    jump = jump % HASH_TABLE_SIZE;

                if(abs(jump - keyHashAlready) < hop){
                    hashTable[jump % HASH_TABLE_SIZE] = hashTable[index];
                    hashTable[index] = 0;
                    jump = index;
                    break;
                }
                step++;
            }
            if(step >= hop)
                return 0;
        }
    */}
}

void updateBitmapInsert(int* bitmap, int position){
    bitmap[position] = 1;
}

void updateBitmapRemove(int* bitmap, int position){
    bitmap[position] = 0;
}