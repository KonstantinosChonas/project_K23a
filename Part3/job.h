#include "int.h"

typedef struct threadPool{

    int numOfThreads;
    pthread_t* threads;


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
    int numOfThreads;
    Queue* q; // a queue that holds submitted jobs / tasks
    pthread_t* thread_ids; // thread pool
    int isFinished;
}JobScheduler;

JobScheduler* initialize_scheduler(int execution_threads);
int submit_job(JobScheduler* sch, Job* j);
int execute_all_jobs(JobScheduler* sch);
int wait_all_tasks_finish(JobScheduler* sch);
int destroy_scheduler(JobScheduler* sch);
Job* createJob(void *(*function)(void* arg),void* arguments);

threadPool* initialize_threadPool(int numOfThreads);





Queue* createQueue();

void enqueue(Queue* q, Job*);

Job* dequeue(Queue* q);