#include "int.h"

typedef struct threadPool{

    int numOfThreads;
    pthread_t* threads;
    int numOfFree; //number of free threads
    int numOfWorkingThreads;
    pthread_mutex_t lock;

}threadPool;


typedef struct Job{
    void (*function)(void* arg);
	void* arguments;
    Job* next;
}Job;

typedef struct Queue{
    Job* front;
    Job* rear;
}Queue;


typedef struct JobScheduler{
int execution_threads; // number of execution threads
Queue* q; // a queue that holds submitted jobs / tasks
// p_thread_t* tids; // execution threads
pthread_mutex_t lock;
pthread_cond_t notEmpty;    //TODO na to ftiaksoume ligo auto

}JobScheduler;

JobScheduler* initialize_scheduler(int execution_threads);
int submit_job(JobScheduler* sch, Job* j);
int execute_all_jobs(JobScheduler* sch);
int wait_all_tasks_finish(JobScheduler* sch);
int destroy_scheduler(JobScheduler* sch);


threadPool* initialize_threadPool(int numOfThreads)





Queue* createQueue();


void enqueue(Queue* q, Job*);

Job* dequeue(Queue* q);