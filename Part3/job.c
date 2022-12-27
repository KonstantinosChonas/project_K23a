#include "job.h"


JobScheduler* initialize_scheduler(int execution_threads){
	JobScheduler* sch = malloc(sizeof(JobScheduler));
	sch->q->front = NULL;
	sch->q->rear = NULL;
	sch->execution_threads = 0;
	if(pthread_mutex_init(&(sch->lock),NULL)!=0){   //if error destroy
		destroyJobPool(&sch);
		return NULL;
	}
	if(pthread_cond_init(&(sch->notEmpty),NULL)!=0){    //if error destroy
		destroy_scheduler(&sch);
		return NULL;
	}
	return sch;
}



int destroyJobPool(JobScheduler* sch){
	Job* currentJob = sch->q->front;
	Job* tempJob = NULL;
	while(currentJob != NULL){
		tempJob = currentJob;
		currentJob = currentJob->next;
		// free(tempJob->arg);         //TODO na doume ti tha mpei edo
		free(tempJob);
	}
	sch->q->front = NULL;
	sch->execution_threads = 0;
	pthread_mutex_destroy(&(sch->lock));
    pthread_cond_destroy(&(sch->notEmpty));
	free(sch);
	sch = NULL;
    return 1;
}


int submit_job(JobScheduler* sch, Job* j){
// Lock sch because it's going to change
	pthread_mutex_lock(&(sch->lock));
	enqueue(sch->q,j);
	sch->execution_threads;
	pthread_cond_signal(&sch->notEmpty);
	pthread_mutex_unlock(&(sch->lock));
}


// void* executeJob(thread* th){
// 	threadPool* thPool = th->thPool;
// 	// Set thread as alive
// 	pthread_mutex_lock(&thPool->lockThreadPool);
// 	thPool->noAlive += 1;
// 	pthread_mutex_unlock(&thPool->lockThreadPool);

// 	while(keepAlive){
// 		// wait until job pool has jobs
// 		pthread_mutex_lock(&(thPool->sch->lockJobPool));
// 		if(thPool->sch->size <= 0) {
// 			pthread_cond_wait(&(thPool->sch->notEmpty), &(thPool->sch->lockJobPool));
// 		}
// 		pthread_mutex_unlock(&(thPool->sch->lockJobPool));
// 		// If threads are kept alive
// 		if (keepAlive && thPool->sch->size > 0){
// 			pthread_mutex_lock(&thPool->lockThreadPool);
// 			// Set current thread as working
// 			thPool->noWorking++;
// 			pthread_mutex_unlock(&thPool->lockThreadPool);
// 			// Pop job
// 			Job* job = getJob(thPool->sch);
// 			// Execute job
// 			job->function(job->arg);
// 			// Free job object
// 			free(job);
// 			pthread_mutex_lock(&thPool->lockThreadPool);
// 			thPool->noWorking--;
// 			pthread_mutex_unlock(&thPool->lockThreadPool);
// 			/*pthread_mutex_lock(&thPool->lockThreadPool);
// 			if (thPool->noWorking <= 0) {
// 				// Signal that all threads are not working
// 				pthread_cond_signal(&thPool->allNotWorking);
// 			}
// 			pthread_mutex_unlock(&thPool->lockThreadPool);*/
// 			// Stop and wait for all in order for main to get the rightful chunks
// 			pthread_barrier_wait(&thPool->barrier);
// 		}
// 	}

// 	pthread_mutex_lock(&thPool->lockThreadPool);
// 	// Decrement number of threads being alive
// 	thPool->noAlive--;
// 	pthread_mutex_unlock(&thPool->lockThreadPool);
// 	return NULL;
// }






Queue* createQueue() {
    Queue* q = (Queue*)malloc(sizeof(Queue));
    q->front = q->rear = NULL;
    return q;
}

enqueue(struct Queue* q, Job* job) {
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