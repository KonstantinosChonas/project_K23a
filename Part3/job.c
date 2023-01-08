#include "job.h"


JobScheduler* initialize_scheduler(int execution_threads){
	JobScheduler* sch = malloc(sizeof(JobScheduler));
	sch->q=createQueue();

	sch->numOfThreads = execution_threads;
	sch->thread_ids=malloc(execution_threads*sizeof(pthread_t));
	sch->isFinished=0;
	for(int i=0 ; i<execution_threads ; i++){

		pthread_create(&sch->thread_ids[i], NULL, threadFun, sch);

	}


	return sch;
}

Job* createJob(void *(*function)(void* arg),void* arguments){

	Job *j=malloc(sizeof(Job));

	j->function=function;
	j->arguments=arguments;
	j->next=NULL;
	return j;
}

int destroy_scheduler(JobScheduler* sch){
	Job* currentJob = sch->q->front;
	Job* tempJob = NULL;
	while(currentJob != NULL){
		tempJob = currentJob;
		currentJob = currentJob->next;
		free(tempJob->arguments);
		free(tempJob);
	}
	sch->q->front = NULL;
	free(sch);
	sch = NULL;
    return 1;
}


int submit_job(JobScheduler* sch, Job* j){
	// pthread_mutex_lock(&(sch->lock));
	sem_wait(&queue_lock);
	enqueue(sch->q,j);
	sem_post(&queue_full);
	sem_post(&queue_lock);
	// pthread_cond_signal(&sch->notEmpty);
	// pthread_mutex_unlock(&(sch->lock));
	return EXIT_SUCCESS;
}


int wait_all_tasks_finish(JobScheduler* sch){

    while(sch->q!=NULL)
        execute_all_jobs(sch);


	// for(int i=0 ; i<sch->execution_threads ; i++){
	// 	if(!pthread_join())

	// }


    return EXIT_SUCCESS;

}

int execute_all_jobs(JobScheduler* sch){		//agnoise 

	Job *j;

	while(1){
		j=dequeue(sch->q);

		if (j){		//returned non-NULL
			j->function(j->arguments);
		}
		else return EXIT_SUCCESS;	//nothing to execute
	}
}


void threadFun(JobScheduler* sch){
	while (1) {
		// Wait until there is a job in the queue
		sem_wait(&queue_full);

		// Acquire lock on the queue
		sem_wait(&queue_lock);

		// Remove a job from the queue
		Job *j = dequeue(sch->q);

		// Release lock on the queue
		sem_post(&queue_lock);


		//Execute job
		j->function(j->arguments);

		if (sch->isFinished && sch->q==NULL) pthread_exit(NULL);
	}
}


Queue* createQueue() {
    Queue* q = (Queue*)malloc(sizeof(Queue));
    q->front = q->rear = NULL;
    return q;
}

void enqueue(struct Queue* q, Job* job) {
    Job* temp = job;
    // temp->data = data;
    temp->next = NULL;
    if (q->rear == NULL) {
        q->front = q->rear = temp;
        return;
    }
    q->rear->next = temp;
    q->rear = temp;
}

Job* dequeue(Queue* q) {
    if (q->front == NULL)
        return NULL;
    Job* temp = q->front;
    q->front = q->front->next;
    if (q->front == NULL)
        q->rear = NULL;
    return temp;
}




// threadPool* initialize_threadPool(int numOfThreads){
	

// 	if(numOfThreads==0)
// 		return NULL;


// 	threadPool* pool = malloc(sizeof(threadPool));

// 	pool->numOfThreads=numOfThreads;
// 	pool->numOfFree=0;
// 	pool->numOfWorkingThreads=0;


// 	pool->threads=malloc(numOfThreads*sizeof(pthread_t));

// 	if(pthread_mutex_init(&pool->lock, NULL) != 0){
// 		deleteThreadPool(&pool);
// 		return NULL;
// 	}

// 	for(int i=0; i < numOfThreads; i++){
// 		pthread_create(&pool->threads[i], NULL, (void *) execute_all_jobs, &pool->threads[i]);
// 		pthread_detach(pool->threads[i]);
// 	}

// 	return pool;

// }

// void deleteThreadPool(threadPool* pool){

// 	pthread_mutex_destroy(&pool->lock);
// 	free(pool);

// }