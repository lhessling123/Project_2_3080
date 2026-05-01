/*
 * kv.h -- Mini-KV server: shared declarations
 *
 * Project 2, CprE 3080, Spring 2026
 *
 * You may modify this file. It is provided as a starting point, not a rigid
 * interface. If your design benefits from additional fields or types, add them.
 */
#ifndef KV_H
#define KV_H

#include <stddef.h>
#include <stdint.h>
#include <time.h>
#include <pthread.h>
#include <stdatomic.h>

/* -------- Protocol constants (do NOT change) ---------------------------- */

#define MAX_KEY_LEN    256
#define MAX_VAL_LEN    256
#define MAX_LINE_LEN   (MAX_KEY_LEN + MAX_VAL_LEN + 64)  /* + command + ttl */

/* Response strings. Each response is one line ending in '\n'. */
#define RESP_OK        "OK\n"
#define RESP_BYE       "BYE\n"
#define RESP_NOTFOUND  "NOT_FOUND\n"

/* -------- Your types go here -------------------------------------------- */

/*
 * TODO (Stage 1): Define your hash-table entry and bucket types.
 *
 * TODO (Stage 2): Define your work-queue type (bounded FIFO of int fds).
 *
 * TODO (Stage 3): Add an rwlock to your table type.
 *
 * TODO (Stage 4): Add expiration timestamp to entries; declare the sweeper
 *                 thread function.
*/

typedef struct kv_stats {
    atomic_int keys;
    atomic_int hits;
    atomic_int misses;
    atomic_int puts;
    atomic_int dels;
    time_t start_time;
    atomic_int active_conns;
} kv_stats_t;

//I am predefining the arrays but could change for better memory management
typedef struct kv_entry {
    char key[MAX_KEY_LEN];
    char value[MAX_VAL_LEN];
    struct kv_entry *next;
} kv_entry_t;

typedef struct kv_table {
    int num_buckets;
    kv_entry_t **buckets;
    kv_stats_t *stats;
    pthread_rwlock_t rwlock;
} kv_table_t;

typedef struct queue_element {
    int fd;
    struct queue_element *next;
} queue_element_t;

typedef struct work_queue {
    queue_element_t *head;
    queue_element_t *tail;
    int capacity;
    int count;
    pthread_mutex_t lock;
    pthread_cond_t enqueued; //element was enqueued
    pthread_cond_t dequeued;//element was dequeued
} work_queue_t;

typedef struct worker_args {
    int thread_id;
    kv_table_t *table;
    work_queue_t *queue;
} worker_args_t;


/* -------- Function prototypes you will likely want ---------------------- */

/* Protocol / connection handling (Stage 1) */
void handle_client(int conn_fd, kv_table_t *table);        /* loop: read line, parse, reply */


/* Hash-table operations (Stage 1, made thread-safe in Stage 3) */
/*   Return 0 on success, -1 on not-found / error. */
/*   You design the full signatures -- these are just suggestions. */
/* int  kv_get(const char *key, char *out_val, size_t out_cap); */
/* int  kv_put(const char *key, const char *val, int ttl_seconds); */
/* int  kv_del(const char *key); */

kv_table_t *kv_init(int num_buckets);
int kv_get(kv_table_t *table, const char *key, char *out_val);
int kv_put(kv_table_t *table, const char *key, const char *val, int ttl_seconds);
int kv_del(kv_table_t *table, const char *key);
void kv_free(kv_table_t *table);

work_queue_t *queue_init(int capacity);
void enqueue(work_queue_t *queue, int fd);
int dequeue(work_queue_t *queue);
void queue_free(work_queue_t *queue);

#endif /* KV_H */
