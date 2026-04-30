#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kv.h"

work_queue_t *queue_init(int capacity){
    work_queue_t *queue = malloc(sizeof(work_queue_t));
    queue->head = NULL;
    queue->tail = NULL;
    queue->capacity = capacity;
    queue->count = 0;
    pthread_mutex_init(&queue->lock, NULL);
    pthread_cond_init(&queue->enqueued, NULL);
    pthread_cond_init(&queue->dequeued, NULL);
    return queue;
}

void enqueue(work_queue_t *queue, int fd){
    queue_element_t *new_element = malloc(sizeof(queue_element_t));
    if (!new_element) return;
    new_element->fd = fd;
    new_element->next = NULL;
    if(queue->count == 0){
        queue->head = new_element;
        queue->tail = new_element;
    }else{
        queue->tail->next = new_element;
        queue->tail = new_element;
    }
    queue->count++;
}

int dequeue(work_queue_t *queue){
    queue_element_t *element = queue->head;
    queue->head = element->next;
    if(queue->head == NULL){
        queue->tail = NULL;
    }
    queue->count--;
    int fd = element->fd;
    free(element);
    return fd;
}

void queue_free(work_queue_t *queue){
    queue_element_t *current = queue->head;
    while(current != NULL){
        queue_element_t *next = current->next;
        free(current);
        current = next;
    }
    pthread_mutex_destroy(&queue->lock);
    pthread_cond_destroy(&queue->enqueued);
    pthread_cond_destroy(&queue->dequeued);
    free(queue);
}