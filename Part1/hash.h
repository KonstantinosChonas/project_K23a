#ifndef PROJECTIOAN_HASH_H
#define PROJECTIOAN_HASH_H

#define BITMAP_SIZE 4
#define HASH_TABLE_SIZE 40


typedef struct payloadList{
    int data;
    struct payloadList *next;
 }payloadList;

typedef struct hashNode{
    int key;
    payloadList *payload;
    int hop;
    int *bitmap;
}hashNode;

typedef struct hashMap{
    hashNode* hashNodes[HASH_TABLE_SIZE];
    int bitmap[HASH_TABLE_SIZE];
    int nodeCount;
    int bucket;     //which bucket is this hashMap for
}hashMap;

hashMap* hashCreate(int bucket);
hashNode* hashNodeCreate(int key, int payload, int hop);
void hashDelete(hashMap** myHashMap);

payloadList* createPayloadList(int data);
void addPayload(payloadList* head, int data);
int hashSearch(hashMap* hashTable, int key, int payload, int n, int flag);
int checkNeighborhood(hashMap* hashTable, int keyhash);
void hashNodeUpdate(hashNode* hashNode, int key, int payload, int hop);
int hashInsert(hashMap* hashTable, int key, int payload, int n);
void updateBitmapInsert(int* bitmap, int position);
void updateBitmapRemove(int* bitmap, int position);

#endif //PROJECTIOAN_HASH_H
