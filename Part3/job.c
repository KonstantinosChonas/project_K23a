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
	sch->execution_threads = 0;
	pthread_mutex_destroy(&(sch->lock));
    pthread_cond_destroy(&(sch->notEmpty));
	free(sch);
	sch = NULL;
    return 1;
}


int submit_job(JobScheduler* sch, Job* j){
	pthread_mutex_lock(&(sch->lock));
	enqueue(sch->q,j);
	sch->execution_threads++;
	pthread_cond_signal(&sch->notEmpty);
	pthread_mutex_unlock(&(sch->lock));
}


int wait_all_tasks_finish(JobScheduler* sch){

    while(sch->q!=NULL)
        execute_all_jobs(sch);

    return 0;

}

int execute_all_jobs(JobScheduler* sch){
    
}



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