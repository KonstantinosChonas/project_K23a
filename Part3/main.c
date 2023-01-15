#include "queries.h"
#include "intermediate.h"
#include "int.h"
#include "statistics.h"
// #include "job.h"
#include <time.h>
#include <stdio.h>
#include <unistd.h>

// sem_t queue_lock;   //lock the queue before accessing 
// sem_t queue_full;   //used for threads to wait until there is a job to be done



int main (int argc, char* argv[]){

    printf("starting main\n");

    int numOfthreads=2;

    sem_init(&queue_lock, 0, 1);
    sem_init(&queue_full, 0, 0);




    JobScheduler* JS=initialize_scheduler(numOfthreads);


    relationInfo* relInfo = NULL;
    int relationNum;

    char* relations = argv[1];
    // char* relations = "workloads/small/small.init";
    if(relations == NULL){
        relations = "workloads/small/small.init";
    }
    relInfo = parseRelations(relations, &relationNum);

    sleep(1);
    /* start of query reading */
    char* queries = argv[2];
    // char* queries = "workloads/small/small.work";
    if(queries == NULL){
        queries = "workloads/small/small.work";
    }

    int i = parseQueries(queries, relInfo, relationNum,JS);
    //int i = parseQueries("workloads/small/small.work", relInfo, relationNum);
    //these should be a function
    // free(rowids->relArray);
    // free(rowids);
    sem_destroy(&queue_lock);
    sem_destroy(&queue_full);

    destroy_scheduler(JS);
    relationInfoDelete(relInfo, relationNum);        //error: command terminated by signal 6 corrupted size vs. prev_size
}