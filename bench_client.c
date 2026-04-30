/*
 * bench_client.c -- Mini-KV concurrent benchmark client
 *
 * Project 2, CprE 3080, Spring 2026
 *
 * YOU write this file. The scaffolding here is intentionally minimal:
 * an argument parser and nothing else. Your job is to fill in the rest.
 *
 * Usage:
 *   ./bench_client <host> <port> <num_clients> <ops_per_client> <read_pct>
 *
 * Requirements (from the spec):
 *   - Spawn <num_clients> threads.
 *   - Each thread opens its own TCP connection to <host>:<port>.
 *   - Each thread issues <ops_per_client> operations.
 *   - <read_pct> percent of ops are GETs; the rest are PUTs.
 *   - Keys drawn from a small pool (~1000 keys) so GETs actually hit.
 *   - Report total wall-clock time and overall throughput (ops/sec).
 *
 * Hints:
 *   - Use clock_gettime(CLOCK_MONOTONIC, ...) to measure elapsed time.
 *   - Each thread needs its own rand_r() seed to avoid serialization on
 *     the global rand() lock.
 *   - Read the server's reply line-by-line; don't assume one TCP packet
 *     per command.
 */

#define _POSIX_C_SOURCE 200112L

#include <errno.h>
#include <netdb.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

typedef struct {
    int thread_id;
    const char *host;
    int port;
    int ops_per_client;
    int read_pct;
    unsigned int seed;
} worker_args_t;

static int connect_to_server(const char *host, int port){
    struct addrinfo hints, *res;
    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", port);

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(host, port_str, &hints, &res) != 0){
        perror("getaddrinfo");
        return -1;
    }
    int sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (sockfd < 0){
        perror("socket");
        freeaddrinfo(res);
        return -1;
    }

    if(connect(sockfd, res->ai_addr, res->ai_addrlen) < 0){
        perror("connect");
        close(sockfd);
        freeaddrinfo(res);
        return -1;
    }

    freeaddrinfo(res);
    return sockfd;
}

void* client_thread(void *arg) {
    worker_args_t *args = (worker_args_t*)arg;



    int sockfd = connect_to_server(args->host, args->port);
    if (sockfd < 0) {
        fprintf(stderr, "thread %d: failed to connect\n", args->thread_id);
        return NULL;
    }

    FILE *in = fdopen(sockfd, "r");
    FILE *out = fdopen(dup(sockfd), "w");
    char line[256];

    for(int i = 0; i < args->ops_per_client; i++){
        int is_get = (rand_r(&args->seed) % 100) < args->read_pct;
        int key_id = rand_r(&args->seed) % 1000;

        if(is_get){
            fprintf(out, "GET key%d\n", key_id);
        } else {
            fprintf(out, "PUT key%d value%d\n", key_id, key_id);
        }
        fflush(out);

        if (fgets(line, sizeof(line), in) == NULL){
            break;
        }
    }

    fprintf(out, "QUIT\n");
    fflush(out);
    fclose(in);
    fclose(out);
    return NULL;

}

static void usage(const char *prog) {
    fprintf(stderr,
        "usage: %s <host> <port> <num_clients> <ops_per_client> <read_pct>\n",
        prog);
}

int main(int argc, char **argv) {
    if (argc != 6) {
        usage(argv[0]);
        return 1;
    }

    const char *host      = argv[1];
    int port              = atoi(argv[2]);
    int num_clients       = atoi(argv[3]);
    int ops_per_client    = atoi(argv[4]);
    int read_pct          = atoi(argv[5]);

    if (port <= 0 || num_clients < 1 || ops_per_client < 1 ||
        read_pct < 0 || read_pct > 100) {
        usage(argv[0]);
        return 1;
    }

    struct timespec start_time, end_time;
    pthread_t *threads = malloc((size_t)num_clients * sizeof(*threads));
    worker_args_t *args = malloc((size_t)num_clients * sizeof(*args));

    if (!threads || !args) {
        perror("malloc");
        free(threads);
        free(args);
        return 1;
    }

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    for (int i = 0; i < num_clients; i++) {
        args[i].thread_id = i;
        args[i].host = host;
        args[i].port = port;
        args[i].ops_per_client = ops_per_client;
        args[i].read_pct = read_pct;
        args[i].seed = (unsigned int)time(NULL) ^ (unsigned int)(i * 2654435761u);
        if (pthread_create(&threads[i], NULL, client_thread, &args[i]) != 0) {
            perror("pthread_create");
            for (int j = 0; j < i; j++) {
                pthread_join(threads[j], NULL);
            }
            free(threads);
            free(args);
            return 1;
        }
    }

    for (int i = 0; i < num_clients; i++) {
        pthread_join(threads[i], NULL);
    }

    clock_gettime(CLOCK_MONOTONIC, &end_time);
    double elapsed_sec = (end_time.tv_sec - start_time.tv_sec) +
                         (end_time.tv_nsec - start_time.tv_nsec) / 1e9;
    long long total_ops = (long long)num_clients * ops_per_client;
    double throughput = elapsed_sec > 0.0 ? total_ops / elapsed_sec : 0.0;

    printf("\n--- Benchmark Complete ---\n");
    printf("Total Elapsed Time: %.4f seconds\n", elapsed_sec);
    printf("Total Operations:   %lld\n", total_ops);
    printf("Throughput:         %.2f ops/sec\n", throughput);

    free(threads);
    free(args);
    return 0;
}
