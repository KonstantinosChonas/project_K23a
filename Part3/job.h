#ifndef JOB
#define JOB




#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>
#include <semaphore.h>
#include "int.h"
#include "queries.h"


extern sem_t queue_lock;   //lock the queue before accessing 
extern sem_t queue_full;   //used for threads to wait until there is a job to be done




typedef struct threadPool{

    int numOfThreads;
    pthread_t* threads;


}threadPool;


typedef struct Job{
    void (*function)(void* arg);
	void* arguments;
    struct Job* next;
}Job;

typedef struct Queue{
    Job* front;
    Job* rear;
}Queue;

typedef struct str{
    char data[100];
    struct str* next;

}str;

typedef struct resultQ{
    str* front;
    str* rear;
    sem_t lock;

}resultQ;





typedef struct JobScheduler{
    int numOfThreads;
    Queue* q; // a queue that holds submitted jobs / tasks
    pthread_t* thread_ids; // thread pool
    int isFinished;
}JobScheduler;


typedef struct queryThreadArgs{
    char* line;
    resultQ* q;
    struct relationInfo* relInfo; 
    JobScheduler* sch;
}queryThreadArgs;

JobScheduler* initialize_scheduler(int execution_threads);
int submit_job(JobScheduler* sch, Job* j);
int execute_all_jobs(JobScheduler* sch);
int wait_all_tasks_finish(JobScheduler* sch);
int destroy_scheduler(JobScheduler* sch);
Job* createJob(void (*function)(void* arg),void* arguments);

threadPool* initialize_threadPool(int numOfThreads);

void* threadFun(void* arg);
void* queryFun(void* args);


Queue* createQueue();           //for job queue
void enqueue(Queue* q, Job*);   //for job queue
Job* dequeue(Queue* q);         //for job queue
resultQ* initializeQ();          //for resultQ
void push(resultQ *q,str* new); //for resultQ
str* pop(resultQ *q);           //for resultQ
void deleteQ(Queue *q);
void deleteresultQ(resultQ *q);

#endif