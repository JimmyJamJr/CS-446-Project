/* Jimson Huang
 * CS446
 * sumsq.c
 *
 * CS 446.646 Project 5 (Pthreads)
 *
 * Compile with --std=c99
 */

#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>

// Max Worker Count
#define MAX_WORKER_COUNT 9999

// aggregate variables
long sum = 0;
long odd = 0;
long min = INT_MAX;
long max = INT_MIN;
volatile bool done = false;
volatile int finished_threads = 0;

// Mutex Locks
pthread_mutex_t variables_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond_stop_idle = PTHREAD_COND_INITIALIZER;
pthread_cond_t cond_thread_finished = PTHREAD_COND_INITIALIZER;
pthread_cond_t cond_dequeue = PTHREAD_COND_INITIALIZER;

// Queue Node
struct TaskNode {
  char action;
  long num;
  struct TaskNode * next;
};

// Create new TaskNode given an action and number
struct TaskNode * new_node(char action, long num) {
  struct TaskNode * n = (struct TaskNode*) malloc(sizeof(struct TaskNode));
  n->action = action;
  n->num = num;
  return n;
}

// Task Queue
struct TaskQueue {
  struct TaskNode * front;
  struct TaskNode * back;
};

volatile struct TaskQueue * create_queue() {
  struct TaskQueue * q = (struct TaskQueue*) malloc(sizeof(struct TaskQueue));
  q->front = NULL;
  q->back = NULL;
  return q;
}

void enqueue(volatile struct TaskQueue * q, char action, long num) {
  struct TaskNode* n = new_node(action, num);
  n->next = NULL;
  if (q->back == NULL) {
    q->front = n;
    q->back = n;
  }
  else {
    q->back->next = n;
    q->back = q->back->next;
  }
}

void dequeue(volatile struct TaskQueue * q) {
  if (q->front == NULL) return;
  struct TaskNode* n = q->front;
  q->front = q->front->next;
  if (q->front == NULL) q->back = NULL;
  free(n);
}

volatile struct TaskQueue * taskQueue;

// function prototypes
void calculate_square(long number);

/*
 * update global aggregate variables given a number
 */
void calculate_square(long number)
{

  // calculate the square
  long the_square = number * number;

  // ok that was not so hard, but let's pretend it was
  // simulate how hard it is to square this number!
  sleep(number);

  // LOCK GLOBAL VARIABLES WITH MUTEX
  pthread_mutex_lock(&variables_mutex);

  // let's add this to our (global) sum
  sum += the_square;

  // now we also tabulate some (meaningless) statistics
  if (number % 2 == 1) {
    // how many of our numbers were odd?
    odd++;
  }

  // what was the smallest one we had to deal with?
  if (number < min) {
    min = number;
  }

  // and what was the biggest one?
  if (number > max) {
    max = number;
  }

  // UNLOCK GLOBAL VARIABLES WITH MUTEX
  pthread_mutex_unlock(&variables_mutex);
}

bool processing_done() {
  pthread_mutex_lock(&variables_mutex);
  bool is_done = done;
  pthread_mutex_unlock(&variables_mutex);
  return is_done;
}

// Worker Function
void * process_task(void * data) {
  volatile long num;
  // long id = *((long *) data);

  pthread_mutex_lock(&queue_mutex);
  while (!processing_done()) {
    if (taskQueue->front != NULL) {
      num = taskQueue->front->num;
      dequeue(taskQueue);
      pthread_cond_signal(&cond_dequeue);
      pthread_mutex_unlock(&queue_mutex);
      // printf("[Thread %d] starting task p %ld\n", id, num);
      calculate_square(num);
      // printf("[Thread %d] finished task p %ld\n", id, num);
    }
    else {
      // printf("[Thread %d] idle\n", id);
      pthread_cond_wait(&cond_stop_idle, &queue_mutex);
    }
  }

  pthread_mutex_unlock(&queue_mutex);

  pthread_mutex_lock(&variables_mutex);
  finished_threads++;
  pthread_mutex_unlock(&variables_mutex);

  // printf("[Thread %d] done, total %d threads done\n", id, finished_threads);
  pthread_cond_signal(&cond_thread_finished);

  return NULL;
}


int main(int argc, char* argv[])
{
  // check and parse command line options
  if (argc != 3) {
    printf("Usage: sumsq <infile> <worker thread count>\n");
    exit(EXIT_FAILURE);
  }
  char *fn = argv[1];
  int thread_ct = strtol(argv[2], NULL, 10);
  if (thread_ct < 0) {
    printf("Tread count must not be negative.\n");
    exit(EXIT_FAILURE);
  }
  if (thread_ct > MAX_WORKER_COUNT) {
    printf("Maximum Thread count is %d.\n", MAX_WORKER_COUNT);
    exit(EXIT_FAILURE);
  }

  // Creation of Task Queue
  taskQueue = create_queue();

  // Creation of Worker Threads
  pthread_t threads[thread_ct];
  int thread_ids[thread_ct];
  for (int i = 0; i < thread_ct; i++) {
    thread_ids[i] = i;
    pthread_create(&threads[i], NULL, process_task, (void *)(&thread_ids[i]));
  }
  
  // load numbers and add them to the queue
  FILE* fin = fopen(fn, "r");
  char action;
  long num;

  while (fscanf(fin, "%c %ld\n", &action, &num) == 2) {
    if (action == 'p') {
      if (thread_ct > 0) {
        pthread_mutex_lock(&queue_mutex);
        enqueue(taskQueue, action, num);
        pthread_mutex_unlock(&queue_mutex);
        // printf("[main] Task p %d added to queue\n", num);
        pthread_cond_signal(&cond_stop_idle);
      }
      else {
        calculate_square(num);
      }
    }
    else if (action == 'w') {
      // printf("[main] Waiting\n");
      sleep(num);
    }
    else {
      printf("ERROR: Unrecognized action: '%c'\n", action);
      exit(EXIT_FAILURE);
    }
  }
  fclose(fin);

  // Wait for queue to empty
  pthread_mutex_lock(&queue_mutex);
  while (taskQueue->front != NULL) {
    pthread_cond_wait(&cond_dequeue, &queue_mutex);
  }
  pthread_mutex_unlock(&queue_mutex);
  // printf("[main] Task queue empty\n");

  pthread_mutex_lock(&variables_mutex);
  done = true;
  pthread_mutex_unlock(&variables_mutex);
  pthread_cond_broadcast(&cond_stop_idle);
  // printf("[main] Stop idle for all threads\n");

  // Wait for threads to finish
  pthread_mutex_lock(&variables_mutex);
  while (finished_threads < thread_ct) {
    pthread_cond_broadcast(&cond_stop_idle);
    pthread_cond_wait(&cond_thread_finished, &variables_mutex);
  }
  pthread_mutex_unlock(&variables_mutex);
  // print results
  printf("%ld %ld %ld %ld\n", sum, odd, min, max);
  
  // clean up and return
  return (EXIT_SUCCESS);
}

