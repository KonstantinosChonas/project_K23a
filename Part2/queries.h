#ifndef SIG18_QUERIES_H
#define SIG18_QUERIES_H
#include "int.h"
#include "intermediate.h"

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
    int done;
} predicate;

int parseQueries(char* queryFileName, relationInfo* relInfo, int relationNum);
int isFilter(char* str);
int sameRel(char* predicate);
relation* relationInfoToRelation(relationInfo* relin,int column);

predicate* createPredicate(char* predicateStr, int order);
int returnRelation(char *str);
int biggerRel(relation* rel1,relation* rel2);

#endif //SIG18_QUERIES_H
