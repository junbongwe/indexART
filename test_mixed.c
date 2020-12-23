#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include <limits.h>

#include "radix-tree.h"

#define WRITE_THREAD_CNT 16
#define READ_THREAD_CNT 16

#define LEAF_LENGTH 0xFFFFFFFFFFULL //1TB

struct radix_tree_node *root = NULL;

volatile bool write_ended = false;

void *read_thread_main(void *aux) {
    struct radix_tree_leaf *leaf = NULL;
    while (!write_ended) {
        unsigned long long key = ((((unsigned long long)rand()) << 32) | ((unsigned long long)rand())) & 0xFFFFFFFFFF;
        switch (radix_tree_lookup(&root, key, &leaf)) {
            case RET_PREV_NODE:
            case RET_MATCH_NODE:
            case RET_NEXT_NODE:
                if (leaf->node.offset != (unsigned long long)leaf->log_addr) {
                    printf("consistency check failed\n");
                    fflush(NULL);
                    exit(-1);
                }
                continue;
            case ENOEXIST_RADIX:
                printf("ENOEXIST_RADIX\n");
                break;
            case EFAULT_RADIX:
                printf("EFAULT_RADIX\n");
                break;
            exit(-1);
        }
    }
    return NULL;
}

void *write_thread_main(void *aux) {
    long long cur_ops, end_ops = (long long)aux;
    
    for (cur_ops = 0; cur_ops < end_ops; cur_ops++) {
        unsigned long long key = ((((unsigned long long)rand()) << 32) | ((unsigned long long)rand())) & 0xFFFFFFFFFF;
        radix_tree_insert(&root, key, LEAF_LENGTH, (void *)key, 0);
    }

    return NULL;
}

int main(int argc, char *argv[]) {
    pthread_t read_threads[READ_THREAD_CNT], write_threads[WRITE_THREAD_CNT];
    long long total_write, write_per_thread;
    int i = 0;
    void *ret;

    if (argc < 2) {
        printf("input total ops\n");
        return 0;
    }
    else {
        total_write = atoll(argv[1]);
        if (total_write < 0) {
            printf("wrong input\n");
            return 0;
        }
        write_per_thread = total_write / WRITE_THREAD_CNT;
    }

    unsigned int seed = (unsigned int)time(NULL);
    printf("seed: %u\n", seed);
    srand(seed);

    radix_tree_insert(&root, 0xFFFFFFFFFFULL, LEAF_LENGTH, (void *)0xFFFFFFFFFFULL, 0);

    while (i < WRITE_THREAD_CNT) {
        if (pthread_create(&write_threads[i++], NULL, &write_thread_main, (void *)write_per_thread) != 0) {
            printf("write thread creation failed\n");
            exit(-1);
        }
    }
    i = 0;
    while (i < READ_THREAD_CNT) {
        if (pthread_create(&read_threads[i++], NULL, &read_thread_main, NULL) != 0) {
            printf("read thread creation failed\n");
            exit(-1);
        }
    }
    
    for (i = 0; i < WRITE_THREAD_CNT; i++)
        pthread_join(write_threads[i], &ret);
    write_ended = true;
    for (i = 0; i < READ_THREAD_CNT; i++)
        pthread_join(read_threads[i], &ret);

    return 0;
}
