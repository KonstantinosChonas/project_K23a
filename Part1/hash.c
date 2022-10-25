//
// Created by aris on 24/10/2022.
//

#include "hash.h"
#include "int.h"

hashMap* hashCreate(int bucket){
    hashMap *newHashMap = malloc(sizeof(struct hashMap));
    newHashMap->nodeCount = 0;
    newHashMap->bucket = bucket;
    newHashMap->nodeList = calloc(HASH_TABLE_SIZE, sizeof(hashNode*));
    return newHashMap;
}

int hashInsert(int* hashTable, int key, int n){
    int keyHash, jump, step, index, keyHashAlready;
    int hop = 3;

    keyHash = hashl(key,n);
    if(hashTable[keyHash] == 0){
        hashTable[keyHash] = key;
        return 1;
    }else{
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
    }
}