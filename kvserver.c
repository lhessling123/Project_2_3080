/*
 * kvserver.c -- Mini-KV server entry point
 *
 * Project 2, CprE 3080, Spring 2026
 *
 * Starter scaffolding: this file gives you a working TCP listener and an
 * argument parser. Everything else -- accept loop, protocol, hash table,
 * thread pool, RW locking, TTL sweeper -- is yours to write.
 *
 * Build: run `make` in this directory. See the provided Makefile.
 */

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "kv.h"

/* -------- Globals ------------------------------------------------------- */

static volatile sig_atomic_t g_shutdown = 0;

static void sigint_handler(int sig) {
    (void)sig;
    g_shutdown = 1;
}

/* -------- Socket helpers ------------------------------------------------ */

/* Create a listening TCP socket bound to the given port. Returns fd or -1. */
static int make_listen_socket(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }
    int opt = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt");
        close(fd);
        return -1;
    }
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons((uint16_t)port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(fd);
        return -1;
    }
    if (listen(fd, 64) < 0) {
        perror("listen");
        close(fd);
        return -1;
    }
    return fd;
}

/* -------- Entry point --------------------------------------------------- */

static void usage(const char *prog) {
    fprintf(stderr,
        "usage: %s <port> <num_workers> <num_buckets> [sweeper_interval_ms]\n"
        "   port                TCP port to listen on (1-65535)\n"
        "   num_workers         number of worker threads (>=1)\n"
        "   num_buckets         hash-table bucket count (>=1)\n"
        "   sweeper_interval_ms default 500\n",
        prog);
}

void handle_client(int conn_fd, kv_table_t *table){
    char line[MAX_LINE_LEN];
    FILE *in = fdopen(conn_fd, "r");
    FILE *out = fdopen(dup(conn_fd), "w");

    if(!in || !out){
        if (in) fclose(in);
        if (out) fclose(out);
        return;
    }
    
    while (fgets(line, sizeof(line), in)){
        char cmd[16], key[MAX_KEY_LEN], val[MAX_VAL_LEN];
        int ttl = 0;

        int parsed = sscanf(line, "%15s %255s %255s %d", cmd, key, val, &ttl);
        if (parsed < 1) continue;

        if (strcmp(cmd, "QUIT") == 0){
            fprintf(out, RESP_BYE);
            break;
        } else if (strcmp(cmd, "GET") == 0){

        } else if (strcmp(cmd, "PUT") == 0){

        } else if (strcmp(cmd, "DEL") == 0){

        } else {
            fprintf(out, "ERR unknown command\n");
        }

        fflush(out);
    }

    fclose(in);
    fclose(out);
}

kv_table_t *kv_init(int num_buckets){
    kv_table_t *return_table = malloc(sizeof(kv_table_t));
    return_table->num_buckets = num_buckets;
    return_table->buckets = calloc(num_buckets, sizeof(kv_entry_t *));
    return return_table;
}

int kv_put(kv_table_t *table, const char *key, const char *val, int ttl_seconds){
    unsigned int buclet = hash(key, table->num_buckets);

    kv_entry_t *curr = table->buckets[bucket];
    while (curr != NUL){
        if (srtcmp(curr->key, key) == 0){
            strncpy(curr->value, val, MAC_VAL_LEN - 1);
            curr->value[MAX_VAL_LEN - 1] = '\0';
            return 0;
        }
        curr = curr->next;
    }

    kv_entry_t *new_entry = malloc(sizeof(kv_entry_t));
    if (!new_entry) return -1;
    strncpy(new_entry->key, key, MAX_KEY_LEN - 1);
    new_entry->key[MAX_KEY_LEN - 1] = '\0';
    strncpy(new_entry->value, val, MAX_VAL_LEN - 1);
    new_entry->value[MAX_VAL_LEN - 1] = '\0';
    new_entry->next = table->buckets[bucket];

    new_entry->next = table->buckets[bucket];
    table->buckets[bucket] = new_entry;

    return 0;
}

int kv_get(kv_table_t *table, const char *key, char *out_val){
    
}

int main(int argc, char **argv) {
    if (argc < 4 || argc > 5) {
        usage(argv[0]);
        return 1;
    }
    int port         = atoi(argv[1]);
    int num_workers  = atoi(argv[2]);
    int num_buckets  = atoi(argv[3]);
    int sweeper_ms   = (argc == 5) ? atoi(argv[4]) : 500;

    if (port <= 0 || port > 65535 || num_workers < 1 ||
        num_buckets < 1 || sweeper_ms <= 0) {
        usage(argv[0]);
        return 1;
    }

    /* Install Ctrl-C handler for clean shutdown. */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigint_handler;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* Ignore SIGPIPE: writes to closed sockets should fail with EPIPE, not
     * kill the server. */
    signal(SIGPIPE, SIG_IGN);

    int listen_fd = make_listen_socket(port);
    if (listen_fd < 0) return 1;

    fprintf(stderr,
        "kvserver: listening on port %d "
        "(workers=%d, buckets=%d, sweeper=%dms)\n",
        port, num_workers, num_buckets, sweeper_ms);

    /* ================================================================
     * TODO (Stage 1): Sequential accept loop.
     *   while (!g_shutdown) {
     *       int conn = accept(listen_fd, NULL, NULL);
     *       if (conn < 0) { ...handle EINTR on signal, else perror... }
     *       handle_client(conn);
     *       close(conn);
     *   }
     *
     * TODO (Stage 2): Initialize work queue + spawn worker threads.
     *                 The accept loop now enqueues conn fds instead of
     *                 calling handle_client directly.
     *
     * TODO (Stage 3): Initialize the hash table's rwlock before the accept
     *                 loop starts.
     *
     * TODO (Stage 4): Spawn the sweeper thread; join it on shutdown.
     *
     * TODO (shutdown): drain queue, join all threads, free everything.
     * ================================================================ */

     kv_table_t *server_table = kv_init(num_buckets);

     while(!g_shutdown) {
        int conn = accept(listen_fd, NULL, NULL);
        if (conn < 0){
            if(errno == EINTR){
                continue;
            } else {
                perror("accept");
                break;
            }
        }
        handle_client(conn, server_table);
        close(conn);
    }

    close(listen_fd);
    return 0;
}
