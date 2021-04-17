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
volatile bool queue_finished = false;  // whether or not all tasks have been assigned to a worker
volatile int finished_threads = 0;  // number of worker threads finished

// Mutex Locks
pthread_mutex_t variables_mutex = PTHREAD_MUTEX_INITIALIZER;  // Aggregate Variables Mutex
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;  // Task Queue Mutex
// Condition
pthread_cond_t cond_stop_idle = PTHREAD_COND_INITIALIZER; // Worker Thread Stop Idle Condition

// Queue Node
struct TaskNode {
  volatile char action;
  volatile long num;
  volatile struct TaskNode * next;
};

// Create new TaskNode given an action and number
volatile struct TaskNode * new_node(char action, long num) {
  struct TaskNode * n = (struct TaskNode*) malloc(sizeof(struct TaskNode));
  n->action = action;
  n->num = num;
  return n;
}

// Task Queue struct
struct TaskQueue {
  volatile struct TaskNode * front;
  volatile struct TaskNode * back;
};

// Task Queue variable
volatile struct TaskQueue * taskQueue;

// Create a new Task Queue
volatile struct TaskQueue * create_queue() {
  struct TaskQueue * q = (struct TaskQueue*) malloc(sizeof(struct TaskQueue));
  q->front = NULL;
  q->back = NULL;
  return q;
}

// Add task to the back of a Task Queue
void enqueue(volatile struct TaskQueue * q, char action, long num) {
  volatile struct TaskNode* n = new_node(action, num);
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

// Remove item at the front of Task Queue
void dequeue(volatile struct TaskQueue * q) {
  if (q->front == NULL || q == NULL) return;
  volatile struct TaskNode* n = q->front;
  q->front = q->front->next;
  if (q->front == NULL) q->back = NULL;
  // Prevent double free errors
  if (n != NULL) {
    free((void *) n);
    n = NULL;
  }
}

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

// Check if queue_finished variable is true, uses variables Mutex
bool check_finished() {
  pthread_mutex_lock(&variables_mutex);
  bool finished = queue_finished;
  pthread_mutex_unlock(&variables_mutex);
  return finished;
}

// Worker Function
void * process_task(void * data) {
  volatile long num;
  // long id = *((long *) data);

  pthread_mutex_lock(&queue_mutex);
  // While task queue not finished, check for tasks
  while (!check_finished()) {
    // If queue not empty, dequeue and process
    if (taskQueue->front != NULL) {
      num = taskQueue->front->num;
      dequeue(taskQueue);
      pthread_mutex_unlock(&queue_mutex);
      // printf("[Thread %d] starting task p %ld\n", id, num);
      calculate_square(num);
      // printf("[Thread %d] finished task p %ld\n", id, num);
    }
    else {
      // printf("[Thread %d] idle\n", id);
      // If queue empty, wait for stop idle condition from main
      pthread_cond_wait(&cond_stop_idle, &queue_mutex);
    }
  }

  pthread_mutex_unlock(&queue_mutex);
  // Add 1 to finished_threads once done
  pthread_mutex_lock(&variables_mutex);
  finished_threads++;
  pthread_mutex_unlock(&variables_mutex);

  // printf("[Thread %d] done, total %d threads done\n", id, finished_threads);
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
        // Add task to queue
        pthread_mutex_lock(&queue_mutex);
        enqueue(taskQueue, action, num);
        pthread_cond_signal(&cond_stop_idle);
        pthread_mutex_unlock(&queue_mutex);
        // printf("[main] Task p %d added to queue\n", num);
      }
      else {
        calculate_square(num);
      }
    }
    else if (action == 'w') {
      sleep(num);
    }
    else {
      printf("ERROR: Unrecognized action: '%c'\n", action);
      exit(EXIT_FAILURE);
    }
  }
  fclose(fin);

  // Wait for queue to empty, singals idle workers to finish tasks in queue if needed
  while (taskQueue->front != NULL) {
    pthread_cond_signal(&cond_stop_idle);
  }

  // Set queue_finished to true
  pthread_mutex_lock(&variables_mutex);
  queue_finished = true;
  pthread_mutex_unlock(&variables_mutex);

  // Wait for threads to finish, and singals all idling threads to quit (in while loop to prevent rare lockup when delays are removed)
  while (finished_threads < thread_ct) {
      pthread_cond_broadcast(&cond_stop_idle);
  }
  // print results
  printf("%ld %ld %ld %ld\n", sum, odd, min, max);

  // Clean up taskQueue and return
  free((void *) taskQueue);
  return (EXIT_SUCCESS);
}