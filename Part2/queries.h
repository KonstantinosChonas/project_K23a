#ifndef SIG18_QUERIES_H
#define SIG18_QUERIES_H
#include "int.h"

typedef struct predicate{
    tuple* leftRelation;
    tuple* rightRelation;
    char operation;     //>,< or =
    int isFilter;          //if isFilter==1, we filter it, otherwise we need to partialHashJoin the two relations
    int value;             //if it's a filter
    int order;
} predicate;

int parseQueries(char* queryFileName, relationInfo* usedRelations);
int isFilter(char* str);
int sameRel(char* predicate);
relation* relationInfoToRelation(relationInfo* relin);

predicate* createPredicate(char* predicateStr, int order);

#endif //SIG18_QUERIES_H
