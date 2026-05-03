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

        key[0] = '\0';
        val[0] = '\0';

        int parsed = sscanf(line, "%15s %255s %255s %d", cmd, key, val, &ttl);
        if (parsed < 1) continue;

        if (strcmp(cmd, "QUIT") == 0){
            fprintf(out, RESP_BYE);
            break;
        } else if (strcmp(cmd, "GET") == 0){
            if(parsed >= 2){
                char out_val[MAX_VAL_LEN];
                if (kv_get(table, key, out_val) == 0){
                    fprintf(out, "VALUE %s\n", out_val);
                } else {
                    fprintf(out, RESP_NOTFOUND);
                }
            }
        } else if (strcmp(cmd, "PUT") == 0){
             if(parsed >= 3){
                kv_put(table, key, val, ttl);
                fprintf(out, RESP_OK);
             } else {
                fprintf(out, "ERROR\n");
            }
        } else if (strcmp(cmd, "DEL") == 0){
            if(parsed >= 2){
                if (kv_del(table, key) == 0){
                    fprintf(out, RESP_OK);
                } else {
                    fprintf(out, RESP_NOTFOUND);
                }
            }
        } else if (strcmp(cmd, "STATS") == 0){
            time_t now = time(NULL);
            double uptime = difftime(now, table->stats->start_time);

            fprintf(out, "STATS keys=%d hits=%d misses=%d puts=%d dels=%d active_conns=%d uptime=%.2f\n",
                table->stats->keys, table->stats->hits, table->stats->misses,
                table->stats->puts, table->stats->dels, atomic_load(&table->stats->active_conns), 
                uptime);

        } else {
            fprintf(out, "ERR unknown command\n");
        }

        fflush(out);
    }

    fclose(in);
    fclose(out);
}

void worker_enqueue(work_queue_t *queue, int fd){
    pthread_mutex_lock(&queue->lock);
    while(queue->count == queue->capacity && !g_shutdown){
        pthread_cond_wait(&queue->dequeued, &queue->lock);
    }
    if(g_shutdown){
        pthread_mutex_unlock(&queue->lock);
        return;
    }
    enqueue(queue, fd);
    pthread_cond_signal(&queue->enqueued);
    pthread_mutex_unlock(&queue->lock);
}

int worker_dequeue(work_queue_t *queue){
    pthread_mutex_lock(&queue->lock);
    while(queue->count == 0 && !g_shutdown){
        pthread_cond_wait(&queue->enqueued, &queue->lock);
    }
    if(g_shutdown && queue->count == 0){
        pthread_mutex_unlock(&queue->lock);
        return -1;
    }
    int item = dequeue(queue);
    pthread_cond_signal(&queue->dequeued);
    pthread_mutex_unlock(&queue->lock);
    return item;
}

void *worker_main(void *arg){
    worker_args_t *worker_args = (worker_args_t *)arg;
    int id = worker_args->thread_id;
    kv_table_t *table = worker_args->table;
    work_queue_t *queue = worker_args->queue;

    printf("Worker %d: started\n", id);

    while(1){
        int client_fd = worker_dequeue(queue);

        fprintf(stderr, "kvserver: recieved command, using worker %d with shutdown %d. client_fd: %d\n", id, g_shutdown, client_fd);

        if(client_fd >= 0){
            atomic_fetch_add(&table->stats->active_conns, 1);
            handle_client(client_fd, table);
            atomic_fetch_sub(&table->stats->active_conns, 1);
        }else {
            if(g_shutdown)
                break;
            
        }
    }
    return NULL;
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

     work_queue_t *queue = queue_init(num_workers * 2);
     kv_table_t *server_table = kv_init(num_buckets);
     pthread_t workers[num_workers];
     worker_args_t worker_args[num_workers];

     for (int i = 0; i < num_workers; i++){
        worker_args[i].thread_id = i;
        worker_args[i].table = server_table;
        worker_args[i].queue = queue;
        if(pthread_create(&workers[i], NULL, worker_main, &worker_args[i]) != 0){
            perror("pthread_create");
            return 1;
        }
     }

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
        worker_enqueue(queue, conn);
    }

    fprintf(stderr, "main: exited accept loop\n");

    pthread_mutex_lock(&queue->lock);
    pthread_cond_broadcast(&queue->enqueued);
    pthread_mutex_unlock(&queue->lock);


    fprintf(stderr, "main: broadcast sent\n");

   
    for(int i = 0; i < num_workers; i++){
        fprintf(stderr, "main: joining worker %d\n", i);
        pthread_join(workers[i], NULL);
        fprintf(stderr, "main: worker %d joined\n", i);
    }


    close(listen_fd);
    kv_free(server_table);
    queue_free(queue);
    return 0;
}
