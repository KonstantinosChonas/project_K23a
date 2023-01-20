
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
	free(sch->q);
	free(sch->thread_ids);
	free(sch);
	sch = NULL;
    return 1;
}


int submit_job(JobScheduler* sch, Job* j){
	sem_wait(&queue_lock);
	enqueue(sch->q,j);
	sem_post(&queue_full);
	sem_post(&queue_lock);
	return EXIT_SUCCESS;
}







void* threadFun(void* arg){
	JobScheduler* sch=(JobScheduler*)arg;
	while (1) {
		// Wait until there is a job in the queue
		// printf("before sem wait\n");
		sem_wait(&queue_full);
		// printf("finished: %d \n",sch->isFinished);
		if (sch->isFinished==1 && sch->q->front==NULL) pthread_exit(NULL);
		// Acquire lock on the queue
		sem_wait(&queue_lock);

		// Remove a job from the queue
		Job *j = dequeue(sch->q);

		// Release lock on the queue
		sem_post(&queue_lock);


		//Execute job
		j->function(j->arguments);

		free(j);

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


resultQ* initializeQ()  {
	resultQ* q = (resultQ*)malloc(sizeof(resultQ));
    q->size = 0;
	q->capacity=10;
	q->heap = (Element *)malloc(q->capacity * sizeof(Element));
	sem_init(&q->lock, 0, 1);
	sem_init(&q->full,0,0);
	return q;
}




void push(resultQ* q,Element e){
    if (q->size == q->capacity) {
        increaseCapacity(q);
    }
    q->heap[q->size] = e;
    moveUp(q, q->size);
    q->size++;
}





Element pop(resultQ* q) {
    Element top = q->heap[0];
    q->size--;
    q->heap[0] = q->heap[q->size];
    moveDown(q, 0);
    if((q->capacity > 1) && (q->size <= q->capacity/4)) {
        q->capacity /= 2;
        Element *new_heap = (Element *)realloc(q->heap, q->capacity * sizeof(Element));
        q->heap = new_heap;
    }
    return top;
}
int compare(Element a, Element b) {
    return a.priority < b.priority;
}

// Swap elements at index i and j in the heap array
void swap(Element *heap, int i, int j) {
    Element temp = heap[i];
    heap[i] = heap[j];
    heap[j] = temp;
}

void increaseCapacity(resultQ *q) {
    q->capacity *= 2;
    Element *new_heap = (Element *)realloc(q->heap, q->capacity * sizeof(Element));
    q->heap = new_heap;
}

void moveUp(resultQ *q, int i) {
    int parent = (i - 1) / 2;
    if (parent >= 0 && compare(q->heap[i], q->heap[parent])) {
        swap(q->heap, i, parent);
        moveUp(q, parent);
    }
}

void moveDown(resultQ *q, int i) {
    int left = 2 * i + 1;
    int right = 2 * i + 2;
    int smallest = i;

    if (left < q->size && compare(q->heap[left], q->heap[i])) {
        smallest = left;
    }
    if (right < q->size && compare(q->heap[right], q->heap[smallest])) {
        smallest = right;
    }
    if (smallest != i) {
        swap(q->heap, i, smallest);
        moveDown(q, smallest);
    }
}






void* queryFun(void* args){

	queryThreadArgs* temp=(queryThreadArgs*) args;

	char* line;




	line=temp->line;

	if(strcmp(line,"F")==0){return NULL;}

	resultQ *q=temp->q;

	relationInfo* relInfo=temp->relInfo;
	JobScheduler* sch=temp->sch;
	int priority=temp->priority;

	char *token;
	char *endCheck;
	FILE *fp;
	char resultBuffer[200] = "";
	char numBuffer[50] = "";

	char* relations;
	char* predicates;
	char* projections;
	char *saveptr;

	token = strtok_r(line, "|", &saveptr);
	relations = token;

	token = strtok_r(NULL, "|", &saveptr);
	predicates = token;

	token = strtok_r(NULL, "\n", &saveptr);
	projections = token;
	

	int relationCounter = 0;
	char tempRelations[20];
	strcpy(tempRelations, relations);
	token = strtok_r(tempRelations, " \t", &saveptr);
	relationCounter++;
	while(token != NULL){
		token = strtok_r(NULL, " \t", &saveptr);
		if(token == NULL){
			break;
		}
		relationCounter++;
	}
	int relationsArray[relationCounter];

	token = strtok_r(relations, " \t", &saveptr);
	relationsArray[0] = atoi(token);

	int i = 1;
	while(token != NULL){
		token = strtok_r(NULL, " \t", &saveptr);
		if(token == NULL){
			break;
		}
		relationsArray[i] = atoi(token);
		i++;
	}
	

	/* code for predicates handling */

	// printf("Start of predicate handling\n");
	int predicateCounter = 0;
	char tempPredicates[50];
	strcpy(tempPredicates, predicates);

	token = strtok_r(tempPredicates, "&", &saveptr);
	predicateCounter++;
	while(token != NULL){

		token = strtok_r(NULL, "&", &saveptr);

		if(token == NULL){
			break;
		}
		predicateCounter++;
	}

	char* predicatesArray[predicateCounter+1];
	for(int j = 0; j < predicateCounter; j++){
		predicatesArray[j] = malloc(100 * sizeof(char));
	}

	i = 0;

	token = strtok_r(predicates, "&", &saveptr);
	strcpy(predicatesArray[0], token);
	while(token != NULL){

		token = strtok_r(NULL, "&", &saveptr);

		if(token == NULL){
			break;
		}
		else{
			i++;
			strcpy(predicatesArray[i], token);
		}
	}

	predicate* predicateStructArray[predicateCounter];
	for(i = 0; i < predicateCounter; i++){
		predicateStructArray[i] = createPredicate(predicatesArray[i], i);

	}



	int error = joinEnumeration(predicateStructArray, relInfo, predicateCounter, relationsArray, relationCounter);




	/* code for projection handling */

	// printf("Start of projection handling\n");
	int projectionCounter = 0;
	char tempProjections[50];
	strcpy(tempProjections, projections);
	token = strtok_r(tempProjections, " \t", &saveptr);
	projectionCounter++;
	while(token != NULL){
		token = strtok_r(NULL, " \t", &saveptr);
		if(token == NULL){
			break;
		}
		projectionCounter++;
	}
	char* projectionsArray[projectionCounter];
	token = strtok_r(projections, " \t", &saveptr);
	projectionsArray[0] = malloc(50 * sizeof(char));
	strcpy(projectionsArray[0], token);


	i = 1;
	while(token != NULL){
		token = strtok_r(NULL, " \t", &saveptr);
		if(token == NULL){
			break;
		}
		projectionsArray[i] = malloc(50 * sizeof(char));
		strcpy(projectionsArray[i], token);
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

	while(done_counter!=predicateCounter){

		for(int i=0 ; i<predicateCounter ; i++){
			if (predicateStructArray[i]->done==1) continue;
			if (empty==0){

				if(rowidarray->row_ids[predicateStructArray[i]->leftRel]==NULL && (rowidarray->row_ids[predicateStructArray[i]->rightRel]!=NULL)){
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
						relationDelete(rel1);
						relationDelete(rel2);
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
					//only right is null

					predicateStructArray[i]->done=1;
					done_counter++;
					relation *rel1=intermediateToRelation(rowidarray,&relInfo[relationsArray[predicateStructArray[i]->leftRel]],predicateStructArray[i]->leftRelation->payloadList->data,predicateStructArray[i]->leftRel);
					relation* rel2=relationInfoToRelation(&relInfo[relationsArray[predicateStructArray[i]->rightRel]],predicateStructArray[i]->rightRelation->payloadList->data);
					relation *res=PartitionedHashJoin(rel1,rel2,sch);
					if(res == NULL){
						relationDelete(rel1);
						relationDelete(rel2);
						done_counter=predicateCounter;
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
	unsigned long long int checksum = 0;
	for(int i = 0; i < projectionCounter; i++){
		sscanf(projectionsArray[i], "%d.%d", &projRel, &projCol);
		relation* result = intermediateToRelation(rowidarray, &relInfo[relationsArray[projRel]], projCol, projRel);
		checksum = getSumRelation(result);
		if(checksum <= 0){
			strcat(resultBuffer, "NULL ");
			relationDelete(result);
			continue;
		}
		numBuffer[0] = '\0';
		sprintf(numBuffer, "%lld ", checksum);
		strcat(resultBuffer, numBuffer);
		relationDelete(result);
	}
	
	Element e;
	strcpy(e.data,resultBuffer);
	e.priority=priority;
	sem_wait(&q->lock);
	push(q,e);
	sem_post(&q->full);
	sem_post(&q->lock);
	intermediateDelete(rowidarray);
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




argsList* initializeArgsList() {
    argsList* list = (argsList*)malloc(sizeof(argsList));
    list->head = NULL;
    list->size = 0;
    return list;
}

void addToArgsList(argsList* list, JobScheduler* sch, resultQ* q, char* line, relationInfo* relInfo, int priority) {
    queryThreadArgs* newArgs = (queryThreadArgs*)malloc(sizeof(queryThreadArgs));
    newArgs->sch = sch;
    newArgs->q = q;
    strcpy(newArgs->line, line);
    newArgs->relInfo = relInfo;
    newArgs->priority = priority;
    newArgs->next = NULL;


	newArgs->next=list->head;
	list->head=newArgs;

    list->size++;
}

void freeArgsList(argsList* list) {
    queryThreadArgs* current = list->head;
    queryThreadArgs* next;
    while (current != NULL) {
        next = current->next;
        free(current);
        current = next;
    }
    free(list);
}
