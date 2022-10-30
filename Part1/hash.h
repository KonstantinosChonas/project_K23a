#ifndef PROJECTIOAN_HASH_H
#define PROJECTIOAN_HASH_H

#define BITMAP_SIZE 4
#define HASH_TABLE_SIZE 40

typedef struct hashNode{
    int key;
    int payload;
    int bitmap[BITMAP_SIZE];
    struct hashNode* next;
    struct hashNode* prev;
}hashNode;

typedef struct hashMap{
    hashNode** nodeList;
    int nodeCount;
    int bucket;     //which bucket is this hashMap for
}hashMap;

hashMap* hashCreate(int bucket);
hashNode* hashNodeCreate(int key, int payload);

int hashInsert(hashMap* hashTable, int key, int payload, int n);
void updateBitmapInsert(int* bitmap, int position);
void updateBitmapRemove(int* bitmap, int position);

#endif //PROJECTIOAN_HASH_H
