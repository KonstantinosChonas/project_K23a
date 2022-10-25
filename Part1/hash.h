#ifndef PROJECTIOAN_HASH_H
#define PROJECTIOAN_HASH_H

#define HASH_TABLE_SIZE 40

typedef struct hashNode{
    int key;
    int payload;
    struct hashNode* next;
    struct hashNode* prev;
}hashNode;

typedef struct hashMap{
    hashNode** nodeList;
    int nodeCount;
    int bucket;     //which bucket is this hashMap for
}hashMap;

hashMap* hashCreate(int bucket);

int hashInsert(int* hashTable, int key, int n);

#endif //PROJECTIOAN_HASH_H
