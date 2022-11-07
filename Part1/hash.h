#ifndef PROJECTIOAN_HASH_H
#define PROJECTIOAN_HASH_H



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
    int hashSize;
    hashNode** hashNodes;
    int* bitmap;
    int nodeCount;
    int bucket;     //which bucket is this hashMap for
}hashMap;

hashMap* hashCreate(int size,int bucket);
hashNode* hashNodeCreate(int key, int payload, int hop);
void hashDelete(hashMap** myHashMap);

payloadList* createPayloadList(int data);
void addPayload(payloadList* head, int data);
int hashSearch(hashMap* hashTable, int key, int payload, int flag);
int getPayload(hashMap* hashTable, int key, int payload, int flag);
int checkNeighborhood(hashMap* hashTable, int keyhash);
void hashNodeUpdate(hashNode* hashNode, int key, int payload, int hop);
int hashInsert(hashMap* hashTable, int key, int payload, int neighborhood_size);
void updateBitmapInsert(int* bitmap, int position);
void updateBitmapRemove(int* bitmap, int position);
int getHash(int key, int numOfBuckets);

#endif //PROJECTIOAN_HASH_H
