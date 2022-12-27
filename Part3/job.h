#include "int.h"

typedef struct Job{

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
pthread_cond_t notEmpty;    

}JobScheduler;

JobScheduler* initialize_scheduler(int execution_threads);
int submit_job(JobScheduler* sch, Job* j);
int execute_all_jobs(JobScheduler* sch);
int wait_all_tasks_finish(JobScheduler* sch);
int destroy_scheduler(JobScheduler* sch);

Queue* createQueue();


void enqueue(Queue* q, Job*);

Job* dequeue(Queue* q);