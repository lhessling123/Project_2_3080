#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kv.h"

unsigned int hash(const char *key, int num_buckets) {
  unsigned long hash = 5381;
  int i = 0;
  while (key[i] != '\0') {
    hash = ((hash << 5) + hash) + key[i];
    i++;
  }
  return (unsigned int)(hash % num_buckets);
}

kv_table_t *kv_init(int num_buckets) {
  kv_table_t *table = malloc(sizeof(kv_table_t));
  table->num_buckets = num_buckets;
  table->buckets = calloc(num_buckets, sizeof(kv_entry_t *));
  table->stats = malloc(sizeof(kv_stats_t));

  table->stats->keys = 0;
  table->stats->hits = 0;
  table->stats->misses = 0;
  table->stats->puts = 0;
  table->stats->dels = 0;
  table->stats->start_time = time(NULL);
  table->stats->active_conns = 0;

  pthread_rwlock_init(&table->rwlock, NULL);

  return table;
}

int kv_put(kv_table_t *table, const char *key, const char *val,
           int ttl_seconds) {
  unsigned int bucket = hash(key, table->num_buckets);
  kv_entry_t *curr = table->buckets[bucket];

  atomic_fetch_add(&table->stats->puts, 1);

  while (curr != NULL) {
    if (strcmp(curr->key, key) == 0) {
      strncpy(curr->value, val, MAX_VAL_LEN - 1);
      curr->value[MAX_VAL_LEN - 1] = '\0';
      curr->expire_at = (ttl_seconds > 0) ? time(NULL) + ttl_seconds : 0; 
      return 0;
    }
    curr = curr->next;
  }

  kv_entry_t *new_entry = malloc(sizeof(kv_entry_t));
  if (!new_entry) {
    return -1;
  }
  strncpy(new_entry->key, key, MAX_KEY_LEN - 1);
  new_entry->key[MAX_KEY_LEN - 1] = '\0';
  strncpy(new_entry->value, val, MAX_VAL_LEN - 1);
  new_entry->value[MAX_VAL_LEN - 1] = '\0';
  new_entry->expire_at = (ttl_seconds > 0) ? time(NULL) + ttl_seconds : 0;
  new_entry->next = table->buckets[bucket];
  table->buckets[bucket] = new_entry;
  atomic_fetch_add(&table->stats->keys, 1);
  return 1;
}

int kv_get(kv_table_t *table, const char *key, char *out_val) {
  unsigned int bucket = hash(key, table->num_buckets);
  kv_entry_t *curr = table->buckets[bucket];
  while (curr != NULL) {
    if (strcmp(curr->key, key) == 0 && (curr->expire_at == 0 || curr->expire_at > time(NULL))) {
      strncpy(out_val, curr->value, MAX_VAL_LEN);
      atomic_fetch_add(&table->stats->hits, 1);
      return 0;
    }
    curr = curr->next;
  }
  atomic_fetch_add(&table->stats->misses, 1);
  return -1;
}

int kv_del(kv_table_t *table, const char *key) {
  unsigned int bucket = hash(key, table->num_buckets);
  kv_entry_t *curr = table->buckets[bucket];
  kv_entry_t *prev = NULL;
  while (curr != NULL) {
    if (strcmp(curr->key, key) == 0) {
      if (prev) {
        prev->next = curr->next;
      } else {
        table->buckets[bucket] = curr->next;
      }
      free(curr);
      atomic_fetch_add(&table->stats->dels, 1);
      atomic_fetch_sub(&table->stats->keys, 1);
      return 0;
    }
    prev = curr;
    curr = curr->next;
  }
  return -1;
}

void kv_free(kv_table_t *table) {
  for (int i = 0; i < table->num_buckets; i++) {
    kv_entry_t *curr = table->buckets[i];
    while (curr != NULL) {
      kv_entry_t *next = curr->next;
      free(curr);
      curr = next;
    }
  }
  pthread_rwlock_destroy(&table->rwlock);
  free(table->stats);
  free(table->buckets);
  free(table);
}