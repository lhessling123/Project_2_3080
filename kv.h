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

/* -------- Protocol constants (do NOT change) ---------------------------- */

#define MAX_KEY_LEN    256
#define MAX_VAL_LEN    256
#define MAX_LINE_LEN   (MAX_KEY_LEN + MAX_VAL_LEN + 64)  /* + command + ttl */

/* Response strings. Each response is one line ending in '\n'. */
#define RESP_OK        "OK\n"
#define RESP_BYE       "BYE\n"
#define RESP_NOTFOUND  "NOT_FOUND\n"

/* -------- Your types go here -------------------------------------------- */

//I am predefining the arrays but could change for better memory management
typedef struct kv_entry {
    char key[MAX_KEY_LEN];
    char value[MAX_VAL_LEN];
    struct kv_entry *next;
} kv_entry_t;

typedef struct kv_table {
    int num_buckets;
    kv_entry_t **buckets;
} kv_table_t;

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

#endif /* KV_H */
