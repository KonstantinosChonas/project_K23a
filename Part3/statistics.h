#ifndef SIG18_STATISTICS_H
#define SIG18_STATISTICS_H
#include "int.h"

struct relationInfo;
struct predicate;

typedef struct columnStatistics{
    uint64_t min_value;
    uint64_t max_value;
    uint64_t value_count;
    uint64_t discrete_values;

    uint64_t original_min_value;
    uint64_t original_max_value;
    uint64_t original_value_count;
    uint64_t original_discrete_values;
}columnStatistics;

int getFilterStatistics(struct relationInfo* relInfo,struct predicate* curPred, int column, int relName, columnStatistics** statistics);
int getJoinStatistics(struct relationInfo* relInfo,struct predicate* curPred, int relName1, int relName2, columnStatistics** statistics);
int valueExistsInColumn(struct relationInfo* relInfo, int column, int relName, int value);
int joinEnumeration(struct predicate** predicateList, struct relationInfo* relInfo, int predicateNumber, int* relationsArray, int relationNumber);
int getOptimalPredicateOrder(struct predicate** predicateList, struct relationInfo* relInfo, int predicateNumber, int* relationsArray, int relationNumber, int* optimalOrder, columnStatistics** statistics);
void getOriginalStatistics(struct relationInfo* relInfo, int* relationsArray, int relationNumber, columnStatistics** statistics);
struct columnStatistics** copyStatistics(struct relationInfo* relInfo, int relationNumber);
#endif //SIG18_STATISTICS_H
