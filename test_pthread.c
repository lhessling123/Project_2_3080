#include <pthread.h>
#include <stdio.h>

pthread_rwlock_t lock = PTHREAD_RWLOCK_INITIALIZER;

void* thread_func(void* arg) {
    pthread_rwlock_rdlock(&lock);
    printf("Thread has acquired the lock.\n");
    pthread_rwlock_unlock(&lock);
    return NULL;
}

int main() {
    pthread_t thread;
    pthread_create(&thread, NULL, thread_func, NULL);
    pthread_join(thread, NULL);
    return 0;
}