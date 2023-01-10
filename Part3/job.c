
#include "job.h"
#include "queries.h"
#include "intermediate.h"

sem_t queue_lock;   //lock the queue before accessing 
sem_t queue_full;   //used for threads to wait until there is a job to be done

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

Job* createJob(void (*function)(void* arg),void* arguments){

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


void* threadFun(void* arg){
	JobScheduler* sch=(JobScheduler*)arg;
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

		free(j);

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


resultQ* initializeQ() {
    resultQ* q = (resultQ*)malloc(sizeof(Queue));
    q->front = q->rear = NULL;
	sem_init(&q->lock, 0, 1);
    return q;
}

void push(resultQ* q,str* new){
    str* temp=new;
	
    temp->next = NULL;
    if (q->rear == NULL) {
        q->front = q->rear = temp;
        return;
    }
    q->rear->next = temp;
    q->rear = temp;
}

str* pop(resultQ* q) {
    if (q->front == NULL)
        return NULL;
    str* temp = q->front;
    q->front = q->front->next;
    if (q->front == NULL)
        q->rear = NULL;
    return temp;
}


void* queryFun(void* args){

	queryThreadArgs* temp=(queryThreadArgs*) args;

	char* line;


	strcpy(line,temp->line);

	resultQ *q=temp->q;

	relationInfo* relInfo=temp->relInfo;
	JobScheduler* sch=temp->sch;

	char *token;
    char *endCheck;
    FILE *fp;
    char resultBuffer[1000] = "";
    char numBuffer[20] = "";

    char* relations;
    char* predicates;
    char* projections;

	
	token = strtok(line, "|");
	relations = token;

	token = strtok(NULL, "|");
	predicates = token;

	token = strtok(NULL, "\n");
	projections = token;


	/* code for relation handling */

	// printf("Start of relation handling\n");


	int relationCounter = 0;
	char tempRelations[20];
	strcpy(tempRelations, relations);
	// printf("%s\n", tempRelations);
	token = strtok(tempRelations, " \t");
	relationCounter++;
	while(token != NULL){
		token = strtok(NULL, " \t");
		if(token == NULL){
			break;
		}
		relationCounter++;
	}
	int relationsArray[relationCounter];

	token = strtok(relations, " \t");
	relationsArray[0] = atoi(token);

	int i = 1;
	while(token != NULL){
		token = strtok(NULL, " \t");
		if(token == NULL){
			break;
		}
		relationsArray[i] = atoi(token);
		i++;
	}

	relationInfo usedRelations[relationCounter];

	//get appropriate relations from relInfo and put them in usedRelations in correct order
	for(i = 0; i < relationCounter; i++){
		usedRelations[i] = relInfo[relationsArray[i]];
	}

	/* code for predicates handling */

	// printf("Start of predicate handling\n");

	int predicateCounter = 0;
	char tempPredicates[50];
	strcpy(tempPredicates, predicates);


	token = strtok(tempPredicates, "&");
	predicateCounter++;
	while(token != NULL){
		
		token = strtok(NULL, "&");

		if(token == NULL){
			break;
		}
		predicateCounter++;
	}

	char* predicatesArray[predicateCounter+1];
	for(int j = 0; j < predicateCounter; j++){
		predicatesArray[j] = malloc(50 * sizeof(char));
	}

	i = 0;

	token = strtok(predicates, "&");
	strcpy(predicatesArray[0], token);

	while(token != NULL){
		
		token = strtok(NULL, "&");

		if(token == NULL){
			break;
		}
		else{
			i++;
			strcpy(predicatesArray[i], token);
//                printf("PREDICATESARRAY[%d]= %s \n",i,token);
//                if(isFilter(predicatesArray[i])){
//                    printf("predicate: %s, is filter\n", predicatesArray[i]);
//                }else
//                    printf("predicate: %s, is not filter\n", predicatesArray[i]);
		}
	}
	

	predicate* predicateStructArray[predicateCounter];

	for(i = 0; i < predicateCounter; i++){
		predicateStructArray[i] = createPredicate(predicatesArray[i], i);

	}



	/* code for projection handling */

	// printf("Start of projection handling\n");

	int projectionCounter = 0;
	char tempProjections[50];
	strcpy(tempProjections, projections);
	token = strtok(tempProjections, " \t");
	projectionCounter++;
	while(token != NULL){
		token = strtok(NULL, " \t");
		if(token == NULL){
			break;
		}
		projectionCounter++;
	}
	char* projectionsArray[projectionCounter];

	token = strtok(projections, " \t");
	projectionsArray[0] = malloc(50 * sizeof(char));
	strcpy(projectionsArray[0], token);

	// printf("%s\n", projectionsArray[0]);


	i = 1;
	while(token != NULL){
		token = strtok(NULL, " \t");
		if(token == NULL){
			break;
		}
		projectionsArray[i] = malloc(50 * sizeof(char));
		strcpy(projectionsArray[i], token);
		// printf("%s\n", projectionsArray[i]);
		i++;
	}
	/*          adding to intermediate           */
/*----------------------------------------------------------------*/
	intermediate *rowidarray=intermediateCreate(relationCounter);

	//printf("NUM OF RELATIONS=%d\n",relationCounter);
	int empty=1;

	int done_counter=0;


	for(int i=0 ; i<predicateCounter ; i++)                 // apply filter first
	{
		if ( predicateStructArray[i]->isFilter==1)
		{
			empty=0;
			applyFilter(&relInfo[relationsArray[predicateStructArray[i]->leftRel]],rowidarray,predicateStructArray[i]->predicate);
			predicateStructArray[i]->done=1;
			done_counter++;
		}
	}

	//printf("key %d, payload %d\n",predicateStructArray[0]->leftRelation->key,predicateStructArray[0]->leftRelation->payloadList->data);

	while(done_counter!=predicateCounter){

		// printf("hello\n");
		for(int i=0 ; i<predicateCounter ; i++){
			// printf("CURRENT PREDICATE %s predicate counter %d, done counter %d \n", predicateStructArray[i]->predicate, predicateCounter,done_counter);
			if (predicateStructArray[i]->done==1) continue;
			//printf("first\n");
			if (empty==0){

				if(rowidarray->row_ids[predicateStructArray[i]->leftRel]==NULL && (rowidarray->row_ids[predicateStructArray[i]->rightRel]!=NULL)){
				//    printf("3\n");
					if (rowidarray->row_ids[predicateStructArray[i]->rightRel]==NULL){
						continue;
					}
					//only left is null

					predicateStructArray[i]->done=1;
					done_counter++;
					relation *rel1=relationInfoToRelation(&relInfo[relationsArray[predicateStructArray[i]->leftRel]],predicateStructArray[i]->leftRelation->payloadList->data),
					*rel2=intermediateToRelation(rowidarray,&relInfo[relationsArray[predicateStructArray[i]->rightRel]],predicateStructArray[i]->rightRelation->payloadList->data,predicateStructArray[i]->rightRel);
					
					relation *res=PartitionedHashJoin(rel1,rel2,sch);
					if(res == NULL){
						break;
					}
					if(biggerRel(rel1,rel2)){
						rowidarray=addToArray(rowidarray,res,predicateStructArray[i]->rightRel,predicateStructArray[i]->leftRel);
					}
					else
						rowidarray=addToArray(rowidarray,res,predicateStructArray[i]->leftRel,predicateStructArray[i]->rightRel);

					relationDelete(rel1);
					relationDelete(rel2);
					relationDelete(res);
				}
				else if (rowidarray->row_ids[predicateStructArray[i]->rightRel]==NULL && rowidarray->row_ids[predicateStructArray[i]->leftRel]!=NULL){
					// printf("4\n");
					//only right is null

					predicateStructArray[i]->done=1;
					done_counter++;
					// printf("predicate done %s\n",predicatesArray[i]);
					relation *rel1=intermediateToRelation(rowidarray,&relInfo[relationsArray[predicateStructArray[i]->leftRel]],predicateStructArray[i]->leftRelation->payloadList->data,predicateStructArray[i]->leftRel),
					*rel2=relationInfoToRelation(&relInfo[relationsArray[predicateStructArray[i]->rightRel]],predicateStructArray[i]->rightRelation->payloadList->data);
					//printf("hello1 rel2->tuples[0].payloadList->data: %d\n",rel2->tuples[0].payloadList->data);
					// printRelation(rel2);
					relation *res=PartitionedHashJoin(rel1,rel2,sch);
					if(res == NULL){
						relationDelete(rel1);
						relationDelete(rel2);
						done_counter=predicateCounter;
						break;
					}

					if(biggerRel(rel1,rel2)){
						// printf("hello3\n");
						rowidarray=addToArray(rowidarray,res,predicateStructArray[i]->rightRel,predicateStructArray[i]->leftRel);
					}
					else    
						rowidarray=addToArray(rowidarray,res,predicateStructArray[i]->leftRel,predicateStructArray[i]->rightRel);

					relationDelete(rel1);
					relationDelete(rel2);
					relationDelete(res);
				}
				else if (rowidarray->row_ids[predicateStructArray[i]->rightRel]!=NULL && rowidarray->row_ids[predicateStructArray[i]->leftRel]!=NULL){

					//none of them is null both of them are in the rowidarray

					predicateStructArray[i]->done=1;
					done_counter++;
					relation *rel1=intermediateToRelation(rowidarray,&relInfo[relationsArray[predicateStructArray[i]->leftRel]],predicateStructArray[i]->leftRelation->payloadList->data,predicateStructArray[i]->leftRel),
					*rel2=intermediateToRelation(rowidarray,&relInfo[relationsArray[predicateStructArray[i]->rightRel]],predicateStructArray[i]->rightRelation->payloadList->data,predicateStructArray[i]->rightRel);



					int count=0;
					for (int i=0; i<rel1->num_tuples ; i++){

						if (rel1->tuples[i].payloadList->data==rel2->tuples[i].payloadList->data)
						count++;
					}
					intermediate *newidarray=intermediateCreate(rowidarray->num_relations);

					newidarray->num_rows=count;
					for (int i=0 ; i<rowidarray->num_relations ; i++){

						if (rowidarray->row_ids[i]!=NULL)
							newidarray->row_ids[i]=malloc(count*sizeof(int));

					}

					int insert=0;
					for( int i=0 ; i<rowidarray->num_rows ; i++){
						if (rel1->tuples[i].payloadList->data==rel2->tuples[i].payloadList->data){

							for(int j=0 ; j<newidarray->num_relations ; j++){
								if (newidarray->row_ids[j]!=NULL){
										newidarray->row_ids[j][insert]=rowidarray->row_ids[j][i];
										// printf("%d\n",newidarray->row_ids[j][insert]);
								}

							}
							insert++;
						}

					}
					intermediateDelete(rowidarray);
					rowidarray=newidarray;

					relationDelete(rel1);
					relationDelete(rel2);
				}
			}
			else{
				relation *rel1=relationInfoToRelation(&relInfo[relationsArray[predicateStructArray[i]->leftRel]],predicateStructArray[i]->leftRelation->payloadList->data),*rel2=relationInfoToRelation(&relInfo[relationsArray[predicateStructArray[i]->rightRel]],predicateStructArray[i]->rightRelation->payloadList->data);
				relation *res=PartitionedHashJoin(rel1,rel2,sch);
				if(res == NULL){
					break;
				}
				if (biggerRel(rel1,rel2)){
					rowidarray=addToArray(rowidarray,res,predicateStructArray[i]->rightRel,predicateStructArray[i]->leftRel);

				}
				else{
					rowidarray=addToArray(rowidarray,res,predicateStructArray[i]->leftRel,predicateStructArray[i]->rightRel);
				}

				predicateStructArray[i]->done=1;
				done_counter++;
				empty=0;
				relationDelete(rel1);
				relationDelete(rel2);
				relationDelete(res);
			}

		}
	}

	int projRel = 0;
	int projCol = 0;
	int checksum = 0;

	for(int i = 0; i < projectionCounter; i++){
		projRel = atoi(strtok(projectionsArray[i], "."));
		projCol = atoi(strtok(NULL, "\0"));
		relation* result = intermediateToRelation(rowidarray, &relInfo[relationsArray[projRel]], projCol, projRel);
		checksum = getSumRelation(result);
		if(checksum <= 0){
			strcat(resultBuffer, "NULL ");
			relationDelete(result);
			continue;
		}
		numBuffer[0] = '\0';
		sprintf(numBuffer, "%d ", checksum);
		strcat(resultBuffer, numBuffer);
		relationDelete(result);
	}
	str *s=malloc(sizeof(str));
	strcpy(s->data,resultBuffer);
	s->next=NULL;
	sem_wait(&q->lock);
	push(q,s);
	sem_post(&q->lock);
	//printf("\n");
	//TODO thelo na trexei gia ena pros to paron kai meta tha doume gia perissotera

	intermediateDelete(rowidarray);
	// return 1;
/*----------------------------------------------------------------*/
	/*            end of  intermediate          */
	//freeing memory used in query
	if(projectionsArray != NULL){
		for(int j = 0; j < i; j++){
			free(projectionsArray[j]);
		}
	}

	for(i = 0; i < predicateCounter; i++){
		if(predicateStructArray[i]->rightRelation != NULL){
			tupleDelete(predicateStructArray[i]->rightRelation);
		}

		if(predicateStructArray[i]->leftRelation != NULL){
			tupleDelete(predicateStructArray[i]->leftRelation);
		}
		free(predicateStructArray[i]);
		free(predicatesArray[i]);
	}

	return NULL;

}

void deleteQ(Queue *q){


	Job* temp;

	while(q->front!=NULL)
	{
		temp=q->front;
		q->front=temp->next;
		free(temp);
	}

	return;

}


void deleteresultQ(resultQ* q){

	str* temp;

	while(q->front!=NULL)
	{
		temp=q->front;
		q->front=temp->next;
		free(temp);
	}

	return;


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

