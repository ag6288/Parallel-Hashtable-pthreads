#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>
#include <sys/time.h>

#define NUM_BUCKETS 5     // Buckets in hash table
#define NUM_KEYS 100000   // Number of keys inserted per thread
int num_threads = 1;      // Number of threads (configurable)
int keys[NUM_KEYS];
pthread_spinlock_t spinlock[NUM_BUCKETS]; // declare lock (spin)

typedef struct _bucket_entry {
    int key;
    int val;
    struct _bucket_entry *next;
} bucket_entry;

bucket_entry *table[NUM_BUCKETS];

void panic(char *msg) {
    printf("%s\n", msg);
    exit(1);
}

double now() {
    struct timeval tv;
    gettimeofday(&tv, 0);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

// Inserts a key-value pair into the table
void insert(int key, int val) {
    
    int i = key % NUM_BUCKETS;
    bucket_entry *e = (bucket_entry *) malloc(sizeof(bucket_entry));
    if (!e) panic("No memory to allocate bucket!");
    // acquire state of lock (spin) 
    pthread_spin_lock(&spinlock[i]);
    e->next = table[i];
    e->key = key;
    e->val = val;
    table[i] = e;
    pthread_spin_unlock(&spinlock[i]);
}

// Retrieves an entry from the hash table by key
// Returns NULL if the key isn't found in the table
bucket_entry * retrieve(int key) {
    bucket_entry *b;
    for (b = table[key % NUM_BUCKETS]; b != NULL; b = b->next) {
        if (b->key == key) return b;
    }
    return NULL;
}

void * put_phase(void *arg) {
    long tid = (long) arg;
    int key = 0;

    // If there are k threads, thread i inserts
    //      (i, i), (i+k, i), (i+k*2)
    for (key = tid ; key < NUM_KEYS; key += num_threads) {
        insert(keys[key], tid); 
    }

    pthread_exit(NULL);
}

void * get_phase(void *arg) {
    long tid = (long) arg;
    int key = 0;
    long lost = 0;

    for (key = tid ; key < NUM_KEYS; key += num_threads) {
        if (retrieve(keys[key]) == NULL) lost++; 
    }
    printf("[thread %ld] %ld keys lost!\n", tid, lost);

    pthread_exit((void *)lost);
}

int main(int argc, char **argv) {
    long i;
    pthread_t *threads;
    double start, end;

     //initialize the lock (spin)
    for (i = 0; i < NUM_BUCKETS; i ++)
        pthread_spin_init(&spinlock[i], 0);


    if (argc != 2) {
        panic("usage: ./parallel_hashtable <num_threads>");
    }
    if ((num_threads = atoi(argv[1])) <= 0) {
        panic("must enter a valid number of threads to run");
    }
    
    srandom(time(NULL));
    for (i = 0; i < NUM_KEYS; i++)
        keys[i] = random();

   
    threads = (pthread_t *) malloc(sizeof(pthread_t)*num_threads);
    if (!threads) {
        panic("out of memory allocating thread handles");
    }

    // Insert keys in parallel
    start = now();
    for (i = 0; i < num_threads; i++) {
        pthread_create(&threads[i], NULL, put_phase, (void *)i);
    }
    
    // Barrier
    for (i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }
    end = now();
    
    printf("[main] Inserted %d keys in %f seconds\n", NUM_KEYS, end - start);
    
    // Reset the thread array
    memset(threads, 0, sizeof(pthread_t)*num_threads);

    // Retrieve keys in parallel
    start = now();
    for (i = 0; i < num_threads; i++) {
        pthread_create(&threads[i], NULL, get_phase, (void *)i);
    }

    // Collect count of lost keys
    long total_lost = 0;
    long *lost_keys = (long *) malloc(sizeof(long) * num_threads);
    for (i = 0; i < num_threads; i++) {
        pthread_join(threads[i], (void **)&lost_keys[i]);
        total_lost += lost_keys[i];
    }
    end = now();

    printf("[main] Retrieved %ld/%d keys in %f seconds\n", NUM_KEYS - total_lost, NUM_KEYS, end - start);

    return 0;
}

//Answers:

//Part1: 
//While inserting only entry loss can happen. Locks are implemented only in the insert fuction so, there so no code required to handle the retieval process.

//Part2: 
//We do not lose keys when using mutex and spinlock because we set the insertion part as a critical section in order to prevent the race condition when threads are competing for resources which are shared or can say is in the bucket. The comparison of either mutex or spinlock is faster is quite variable.
//In this case, spinlock took longer time for insertion as compared to mutex lock. The mutex is faster here because it entered a busy waiting while loop if it failed to acquire the lock and used up the CPU quantum assigned, whereas mutex just yielded the CPU if it failed to acquire the lock. It is always better to put threads to sleep and immediately swap rather than waiting for a lock until it is released.  
// Mutex:   1 Threads = 18.5951 sec; 2 Threads = 14.0615sec; 10 Threads = 8.9526sec; 20 Threads = 6.0843sec; 
// Spinlock:1 Threads = 18.6533 sec; 2 Threads = 14.6468sec; 10 Threads = 9.1738sec; 20 Threads = 6.4787sec;


//Part 3: 
//Lock is not required for retrieval of an item from hash table. Insert was failing in the unmodified code because of race condition.The implementation of locks and mutexes were done only for the insert part in the program and the retrieve still runs paralelly loosing not keys. Everything is done in more or less one step for the retrieving inorder to avoid race conditions.

// Part 4: 
//The code changes have been made and now insert operation can run in parallel. The locks are only set when the value of i is same as key % NUM_BUCKETS. In case of a race condition, if we try to access the same bucket, we acquire the lock first making the thread wait. In case of accessing a different bucket, the lock will unlock itself and get acquired by a new thread. The shared resource is the parallel hashtable in particular to a bucket specifically.
