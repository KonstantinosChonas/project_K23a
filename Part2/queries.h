#ifndef SIG18_QUERIES_H
#define SIG18_QUERIES_H
#include "int.h"

typedef struct predicate{
    tuple* leftRelation;
    tuple* rightRelation;
    int leftRel;        //if predicate is 0.1=3.2 then leftRel=0
    int rightRel;        //if is not filter
    char* predicate;
    char operation;     //>,< or =
    int isFilter;          //if isFilter==1, we filter it, otherwise we need to partialHashJoin the two relations
    int value;             //if it's a filter
    int order;
} predicate;

int parseQueries(char* queryFileName, relationInfo* relInfo, int relationNum);
int isFilter(char* str);
int sameRel(char* predicate);
relation* relationInfoToRelation(relationInfo* relin);

predicate* createPredicate(char* predicateStr, int order);
int returnRelation(char *str);
relation* intermediateToRelation(intermediate *rowidarray, relationInfo *relInfo,int column,int relname);

#endif //SIG18_QUERIES_H
