#include "int.h"
#ifndef SIG18_STATISTICS_H
#define SIG18_STATISTICS_H

struct relationInfo;
struct predicate;

typedef struct columnStatistics{
    uint64_t min_value;
    uint64_t max_value;
    uint64_t value_count;
    uint64_t discrete_values;
}columnStatistics;

int getFilterStatistics(struct relationInfo* relInfo,struct predicate* curPred, int column, int relName);
int getJoinStatistics(struct relationInfo* relInfo,struct predicate* curPred, int relName1, int relName2);
int valueExistsInColumn(struct relationInfo* relInfo, int column, int relName, int value);
int joinEnumeration(struct predicate** predicateList, struct relationInfo* relInfo, int predicateNumber, int* relationsArray);

#endif //SIG18_STATISTICS_H
