#ifndef JOB
#define JOB




#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>
#include <semaphore.h>



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

// typedef struct str{
//     char data[100];
//     struct str* next;

// }str;
typedef struct {
    int priority;
    char data[100];
} Element;

typedef struct {
    Element *heap;
    int size;
    int capacity;
    sem_t lock;
    sem_t full;
} resultQ;
// typedef struct resultQ{
//     str* front;
//     str* rear;
//     sem_t lock;
//     sem_t full;
// }resultQ;





typedef struct JobScheduler{
    int numOfThreads;
    Queue* q; // a queue that holds submitted jobs / tasks
    pthread_t* thread_ids; // thread pool
    int isFinished;
}JobScheduler;


typedef struct queryThreadArgs queryThreadArgs;
struct queryThreadArgs {
    JobScheduler* sch;
    resultQ* q;
    char line[100];
    struct relationInfo* relInfo;
    int priority;
    queryThreadArgs* next;
};

typedef struct argsList {
    queryThreadArgs* head;
    int size;
} argsList;




// typedef struct queryThreadArgs{
//     char line[100];
//     resultQ* q;
//     struct relationInfo* relInfo; 
//     JobScheduler* sch;
//     int priority;
// }queryThreadArgs;

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
void push(resultQ *q,Element e); //for resultQ
void increaseCapacity(resultQ *q);//for resultQ
Element pop(resultQ *q);           //for resultQ
void moveUp(resultQ *q, int i);
void moveDown(resultQ *q, int i);
void swap(Element *heap, int i, int j);
int compare(Element a, Element b);
void deleteQ(Queue *q);
void deleteresultQ(resultQ *q);

argsList* initializeArgsList();
void addToArgsList(argsList* list, JobScheduler* sch, resultQ* q, char* line, struct relationInfo* relInfo, int priority);
void freeArgsList(argsList* list);

#endif