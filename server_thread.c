#include "request.h"
#include "server_thread.h"
#include "common.h"
#include <assert.h>
#include <stdbool.h>

#define TABLE_SIZE 1000003
#define AVAILABLE 1
#define TAKEN 0

struct server {
	int nr_threads;
	int max_requests;
	int max_cache_size;
	int exiting;
	/* add any other parameters you need */
	int *conn_buf;
	pthread_t *threads;
	int request_head;
	int request_tail;
	pthread_mutex_t mutex;
	pthread_cond_t prod_cond;
	pthread_cond_t cons_cond;	
};

struct Cache{
	int maxSize;
	int currSize;
	struct CacheNode **cacheArray; // array of pointers
	struct Node *head;
	struct Node *tail;
	// struct file_data *dataSend;
	pthread_mutex_t cacheMutex1;
	pthread_mutex_t cacheMutex2;
	pthread_mutex_t cacheMutex3;
	pthread_mutex_t cacheMutex4;
	pthread_mutex_t cacheMutex5;
};

struct CacheNode{
	struct file_data *data;
	struct CacheNode *next;
	struct Node *queue;
};

struct Node{ // for doubly linked list
    char *file_name;
    int size;
    struct Node *next;
	struct Node *prev;
};

/* static functions */
int cache_evict(int evictSize);
bool cache_lookup(struct file_data *data);
void cache_insert(struct file_data *data);
int hash(char *file_name);
void enqueue(struct CacheNode* cnPtr);
void dequeue(struct CacheNode* cnPtr);
void removeAnyNode(struct CacheNode* cnPtr);
void freeNodes(void);
void freeCacheNodes(void);
void freeCacheObj(void);
void printNodes(void);

struct Cache *cPtr; // the cache pointer

/* initialize file data */
static struct file_data *
file_data_init(void)
{
	struct file_data *data;

	data = Malloc(sizeof(struct file_data));
	data->file_name = NULL;
	data->file_buf = NULL;
	data->file_size = 0;
	return data;
}

/* free all file data */
static void
file_data_free(struct file_data *data)
{
	free(data->file_name);
	free(data->file_buf);
	free(data);
}

static void
do_server_request(struct server *sv, int connfd)
{
	int ret;
	struct request *rq;
	struct file_data *data;

	data = file_data_init();

	/* fill data->file_name with name of the file being requested */
	// NOT rq->data, but just data
	rq = request_init(connfd, data); // this function links rq & data together (rq-data = data)
	if (!rq) {
		file_data_free(data);
		return;
	}
	/* read file, 
	 * fills data->file_buf with the file contents,
	 * data->file_size with file size. */
		
	// in the beginning, only have the file_name
	// if (data->file_size <= sv->max_cache_size) { // file is small enough to cache
	// file is small enough to cache
	// *NOTE: if cache_lookup() returns true, then data has been linked with the file in cache
	
	pthread_mutex_lock(&(cPtr->cacheMutex1));	
	bool inCache = cache_lookup(data);

	if(!inCache){
		pthread_mutex_unlock(&(cPtr->cacheMutex1));	

		// DON'T LOCK THIS
		ret = request_readfile(rq); // reading file from disk
		if (ret == 0){ /* couldn't read file */
			request_set_data(rq, data); // rq->data = data;
			goto out;
		}
		pthread_mutex_lock(&(cPtr->cacheMutex1));	

		if (data->file_size <= sv->max_cache_size) { 
			// file is small enough to cache
			if (!cache_lookup(data)){
				cache_insert(data);
			}
		}

	}
	// At this point, the file is in the cache
	else {
		// cache HIT
		request_set_data(rq, data); // rq->data = data;
		// filling in the request (rq) data
	}
	/* send file to client */
	// broken pipe may be caused by not having a deep copy of the data
	
	pthread_mutex_unlock(&(cPtr->cacheMutex1));	
	request_sendfile(rq);
out:
	request_destroy(rq); // doesn't free the data, only closes the fd
	// file_data_free(data);
}

// Evicts files in the cache based on LRU
// Returns the amount of bytes evicted
int cache_evict(int evictSize)
{
    int size = 0;

    while (cPtr->maxSize - cPtr->currSize + size < evictSize){ // (amount of free space) < evictSize

		// tptr MAY NOT BE POINTING TO THE CORRECT CacheNode IF THERE ARE COLLISIONS
		int index = hash(cPtr->tail->file_name);
		
        // removing file from cache
        struct CacheNode *tptr = cPtr->cacheArray[index];
        struct CacheNode *prevPtr = tptr;
		bool flagFirst = true;

		char *file_name = cPtr->tail->file_name;
		size += cPtr->tail->size;
        
        while (tptr != NULL){
            
            if (strcmp(tptr->data->file_name, file_name) == 0){
				dequeue(tptr);	
                struct CacheNode *nextPtr = tptr->next;
                
				// freeing CacheNode
				file_data_free(tptr->data);
				tptr->data = NULL;
				tptr->next = NULL;
				// *NOTE: tptr->queue is alreay NULL in dequeue()
                free(tptr);
                
				if (flagFirst){ // if no collisions
					cPtr->cacheArray[index] = NULL;	
				}
				else {
					// relinking 
					prevPtr->next = nextPtr;
					nextPtr = NULL;
				}
				break;
            }
			// if there is collisions
			flagFirst = false;
            prevPtr = tptr;
            tptr = tptr->next;
        }
		tptr = NULL;
		prevPtr = NULL;
    }
    
    return size;
}

void printNodes(void){
	struct Node *tptr = cPtr->tail;
	printf(">>>printNodes:\n");
	
	if (tptr == NULL){
		printf("queue is empty\n");
		printf(">>>\n");
		return;
	}

	printf("tail -> ");
	while (tptr != NULL){
		printf("[%s, %d] -> ", tptr->file_name, tptr->size);
		tptr = tptr->next;
	}
	printf("head\n>>>\n");
}

// adds a Node into the HEAD (most recently used) of the queue
// also links the Node to the given CacheNode
void enqueue(struct CacheNode* cnPtr)
{
	struct Node *nodePtr = (struct Node*) malloc(sizeof(struct Node));
	nodePtr->file_name = (char*) malloc(sizeof(char));

	strcpy(nodePtr->file_name, cnPtr->data->file_name);
	nodePtr->size = cnPtr->data->file_size;

	if (cPtr->head == NULL){ 
		// if the queue is EMPTY
		cPtr->head = nodePtr;
		cPtr->tail = nodePtr;

		// initalizing the node object
		nodePtr->prev = NULL;
		nodePtr->next = NULL;
	}
	else {
		// if the queue is NOT empty
		// initializing Node
		// attaching new Node at the head of the queue
		cPtr->head->next = nodePtr;
		nodePtr->prev = cPtr->head;
		nodePtr->next = NULL;
		cPtr->head = nodePtr;
	}
	cnPtr->queue = nodePtr; // linking CacheNode to Node

	nodePtr = NULL;
}

// Removes a Node from the tail of the queue (least recently used)
void dequeue(struct CacheNode* cnPtr)
{
	// resetting the tail
	if (cPtr->tail == cPtr->head && cPtr->tail != NULL){
		// edge case: if there's only 1 file in cache
		cPtr->tail = NULL;			
		cPtr->head = NULL;
	}
	else {
		cPtr->tail->next->prev = NULL;
		cPtr->tail = cPtr->tail->next;
	}

	// freeing the Node
	cnPtr->queue->next = NULL;
	cnPtr->queue->file_name = NULL;
	free(cnPtr->queue);
	cnPtr->queue = NULL;
}

// removes any Node 
void removeAnyNode(struct CacheNode* cnPtr)
{
	if (cPtr->head == NULL){
		assert(0); // this should not happen
	}

	if (cPtr->tail == cPtr->head && cPtr->head == cnPtr->queue){ // if there's only one Node
		cPtr->tail = NULL;
		cPtr->head = NULL;		
	}	
	else if (cnPtr->queue == cPtr->tail){ // else if Node is at tail
		cnPtr->queue->next->prev = NULL;
		cPtr->tail = cnPtr->queue->next;
	}
	else if (cnPtr->queue == cPtr->head){ // else if Node is at head
		cnPtr->queue->prev->next = NULL;
		cPtr->head = cnPtr->queue->prev;
	}
	else { // if NOde is somewhere in the middle
		cnPtr->queue->prev->next = cnPtr->queue->next;
		cnPtr->queue->next->prev = cnPtr->queue->prev;
	}

	cnPtr->queue->prev = NULL;
	cnPtr->queue->next = NULL;
	free(cnPtr->queue->file_name);
	cnPtr->queue->file_name = NULL;
	free(cnPtr->queue);
	cnPtr->queue = NULL;
}

// Inserts a given file into the cache
// If there is no more room, will evict files 
void cache_insert(struct file_data *data)
{
    if (cPtr->currSize + data->file_size > cPtr->maxSize){
        // need to evict

	int amountEvicted = cache_evict(data->file_size);
        cPtr->currSize -= amountEvicted;
    }
    cPtr->currSize += data->file_size;
	
	// inserting new CacheNode into cache (hashMap)
	int index = hash(data->file_name);
	struct CacheNode *tptr = cPtr->cacheArray[index];

	if (tptr == NULL){
		// no collisions
		cPtr->cacheArray[index] = (struct CacheNode*) malloc(sizeof(struct CacheNode));
		tptr = cPtr->cacheArray[index]; // DON'T DELETE
	}
	else {
		// if there are collisions
		struct CacheNode *prevPtr;
		
		while (tptr != NULL){ 
			prevPtr = tptr;
			tptr = tptr->next;
		}

		// linking previous CacheNode to new one
		prevPtr->next = (struct CacheNode*) malloc(sizeof(struct CacheNode));
		tptr = prevPtr;
		prevPtr = NULL;
    }

	// at this point, tptr is pointing to the newly created CacheNode
    tptr->data = file_data_init();

	memcpy(tptr->data, data, sizeof(struct file_data));
	tptr->next = NULL;
	tptr->queue = NULL;
    enqueue(tptr); // placing this new file as most recently used
    tptr = NULL;
}

// Determines if the file is in cache
// Returns true if file is in cache and imports info into data
// Returns false otherwise
bool cache_lookup(struct file_data *data)
{
	struct CacheNode *tptr = cPtr->cacheArray[hash(data->file_name)];

	while (tptr != NULL){

		if (strcmp(tptr->data->file_name, data->file_name) == 0){
			// cache HIT
			memcpy(data, tptr->data, sizeof(struct file_data));

			// updating the LRU queue
			removeAnyNode(tptr);
			enqueue(tptr);

			tptr = NULL;
			return true;
		}	

		tptr = tptr->next;
	}
	// cache MISS
	tptr = NULL;
	return false;
}


static void *
do_server_thread(void *arg)
{
	struct server *sv = (struct server *)arg;
	int connfd;

	while (1) {
		pthread_mutex_lock(&sv->mutex);
		while (sv->request_head == sv->request_tail) {
			/* buffer is empty */
			if (sv->exiting) {
				pthread_mutex_unlock(&sv->mutex);
				goto out;
			}
			pthread_cond_wait(&sv->cons_cond, &sv->mutex);
		}
		/* get request from tail */
		connfd = sv->conn_buf[sv->request_tail];
		/* consume request */
		sv->conn_buf[sv->request_tail] = -1;
		sv->request_tail = (sv->request_tail + 1) % sv->max_requests;
		
		pthread_cond_signal(&sv->prod_cond);
		pthread_mutex_unlock(&sv->mutex);
		/* now serve request */
		do_server_request(sv, connfd);
	}
out:
	return NULL;
}

/* entry point functions */

int hash(char *file_name){

    int hashValue = 0;
	int length = strlen(file_name);

    for (int i=0; i<length; i++){
        hashValue += file_name[i];
        hashValue = (hashValue * file_name[i]) % TABLE_SIZE;
    }

    if (hashValue < 0)
		hashValue *= -1;

    return hashValue;
}


struct server *
server_init(int nr_threads, int max_requests, int max_cache_size)
{
	struct server *sv;
	int i;

	sv = Malloc(sizeof(struct server));
	sv->nr_threads = nr_threads;
	/* we add 1 because we queue at most max_request - 1 requests */
	sv->max_requests = max_requests + 1;
	sv->max_cache_size = max_cache_size;
	sv->exiting = 0;

	/* Lab 4: create queue of max_request size when max_requests > 0 */
	sv->conn_buf = Malloc(sizeof(*sv->conn_buf) * sv->max_requests);
	for (i = 0; i < sv->max_requests; i++) {
		sv->conn_buf[i] = -1;
	}
	sv->request_head = 0;
	sv->request_tail = 0;

	/* Lab 5: init server cache and limit its size to max_cache_size */
	cPtr = (struct Cache*) malloc(sizeof(struct Cache));
	cPtr->maxSize = max_cache_size;
	cPtr->currSize = 0;
	cPtr->head = NULL;
	cPtr->tail = NULL;
	pthread_mutex_init(&(cPtr->cacheMutex1), NULL);
	pthread_mutex_init(&(cPtr->cacheMutex2), NULL);
	pthread_mutex_init(&(cPtr->cacheMutex3), NULL);
	pthread_mutex_init(&(cPtr->cacheMutex4), NULL);
	pthread_mutex_init(&(cPtr->cacheMutex5), NULL);
	cPtr->cacheArray = (struct CacheNode**) malloc(sizeof(struct CacheNode*) * TABLE_SIZE);

	for (int i=0; i<TABLE_SIZE; i++){
		cPtr->cacheArray[i] = NULL;
	}

	/* Lab 4: create worker threads when nr_threads > 0 */
	pthread_mutex_init(&sv->mutex, NULL);
	pthread_cond_init(&sv->prod_cond, NULL);
	pthread_cond_init(&sv->cons_cond, NULL);	
	sv->threads = Malloc(sizeof(pthread_t) * nr_threads);
	for (i = 0; i < nr_threads; i++) {
		SYS(pthread_create(&(sv->threads[i]), NULL, do_server_thread,
				   (void *)sv));
	}
	return sv;
}

void
server_request(struct server *sv, int connfd)
{
	if (sv->nr_threads == 0) { /* no worker threads */
		do_server_request(sv, connfd);
	} else {
		/*  Save the relevant info in a buffer and have one of the
		 *  worker threads do the work. */

		pthread_mutex_lock(&sv->mutex);
		while (((sv->request_head - sv->request_tail + sv->max_requests)
			% sv->max_requests) == (sv->max_requests - 1)) {
			/* buffer is full */
			pthread_cond_wait(&sv->prod_cond, &sv->mutex);
		}
		/* fill conn_buf with this request */
		assert(sv->conn_buf[sv->request_head] == -1);
		sv->conn_buf[sv->request_head] = connfd;
		sv->request_head = (sv->request_head + 1) % sv->max_requests;
		pthread_cond_signal(&sv->cons_cond);
		pthread_mutex_unlock(&sv->mutex);
	}
}

void
server_exit(struct server *sv)
{
	int i;
	/* when using one or more worker threads, use sv->exiting to indicate to
	 * these threads that the server is exiting. make sure to call
	 * pthread_join in this function so that the main server thread waits
	 * for all the worker threads to exit before exiting. */
	pthread_mutex_lock(&sv->mutex);
	sv->exiting = 1;
	pthread_cond_broadcast(&sv->cons_cond);
	pthread_mutex_unlock(&sv->mutex);
	for (i = 0; i < sv->nr_threads; i++) {
		pthread_join(sv->threads[i], NULL);
	}

	/* make sure to free any allocated resources */
	pthread_mutex_destroy(&(cPtr->cacheMutex1));
	pthread_mutex_destroy(&(cPtr->cacheMutex2));
	pthread_mutex_destroy(&(cPtr->cacheMutex3));
	pthread_mutex_destroy(&(cPtr->cacheMutex4));
	pthread_mutex_destroy(&(cPtr->cacheMutex5));
	freeNodes();
	freeCacheNodes();
	freeCacheObj();
	cPtr = NULL;

	free(sv->conn_buf);
	free(sv->threads);
	free(sv);
}

// return 1 if sucessful, 0 otherwise
void freeNodes(void)
{
	if (cPtr->tail == NULL) // queue is empty
		return; 

	struct Node *tptr = cPtr->tail;	
	struct Node *prevPtr = tptr;
	
	while (tptr != NULL){
		tptr = tptr->next;

		prevPtr->file_name = NULL;
		prevPtr->prev = NULL;
		prevPtr->next = NULL;
		free(prevPtr);
		
		prevPtr = tptr;
	}
	
	tptr = NULL;
	prevPtr = NULL;
}

// return 1 if sucessful, 0 otherwise
void freeCacheNodes(void)
{
	for (int i=0; i<TABLE_SIZE; i++){
		struct CacheNode *tptr = cPtr->cacheArray[i];
		struct CacheNode *prevPtr = tptr;

		while (tptr != NULL){
			tptr = tptr->next;
			
			prevPtr->queue = NULL;
			file_data_free(prevPtr->data);
			prevPtr->data = NULL;
			prevPtr->next = NULL;
			free(prevPtr);
			
			prevPtr = tptr;
		}		
		cPtr->cacheArray[i] = NULL;
	}
}

void freeCacheObj(void)
{
	cPtr->head = NULL;
	cPtr->tail = NULL;
	cPtr->cacheArray = NULL;
}

