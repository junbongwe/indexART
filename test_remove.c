#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>

#include "radix_tree.h"

#define THREADS_CNT 16

#define LEAF_LENGTH 0xFFFFFFFFFFULL //1TB

struct radix_tree_root root;

unsigned long long total_ops, ops_per_thread;
unsigned long long total_key;
unsigned long long *key;

volatile int created = 0;
volatile int inserted = 0;

unsigned long long total_insert = 0;
unsigned long long total_delete = 0;

static inline unsigned long long rand_ull(void) {
    return ((((unsigned long long)rand()) << 32) | ((unsigned long long)rand())) & 0xFFFFFFFFFF;
}

void *thread_main(void *aux) {
    unsigned long long tid = (unsigned long long)aux;
    unsigned long long i, j, start, end, stat_insert = 0, stat_delete = 0;
    struct radix_tree_leaf *leaf, *tmp;
    __sync_fetch_and_add(&created, 0x1);
    while (created < THREADS_CNT);

    start = tid * (total_key / (THREADS_CNT * 2));
    end = (tid + 1) * (total_key / (THREADS_CNT * 2));
    for (i = start; i < end; i++) {
        radix_tree_insert(&root, key[i], LEAF_LENGTH, (void *)key[i], 0);
        if (radix_tree_lookup(&root, key[i], &leaf) != RET_MATCH_NODE) {
            printf("failed to lookup inserted key\n");
            exit(-1);
        }
        if (leaf->log_addr != (void *)key[i]) {
            printf("consistency check failed - insert\n");
            exit(-1);
        }
    }

    __sync_fetch_and_add(&inserted, 0x1);
    while(inserted < THREADS_CNT);

    for (i = 0; i < ops_per_thread; i++) {
        if (rand() % 2) { // Try to insert.
            j = rand_ull();
            if (radix_tree_lookup(&root, j, &leaf) == RET_MATCH_NODE)
                goto remove;
insert:
            radix_tree_insert(&root, j, LEAF_LENGTH, (void *)j, 0);
            if (radix_tree_lookup(&root, j, &leaf) != RET_MATCH_NODE)
                goto insert;
            stat_insert++;
        }
        else { // Try to remove.
            j = rand_ull();
            if (radix_tree_lookup(&root, j, &leaf) != RET_MATCH_NODE) {
                if (leaf == NULL)
                    goto insert;
                j = leaf->node.offset;
            } 
remove:
            radix_tree_remove(&root, leaf);
            if (radix_tree_lookup(&root, j, &tmp) == RET_MATCH_NODE) {
                if (leaf == tmp) {
                    printf("failed to delete\n");
                    exit(-1);
                }
            }
            stat_delete++;
        }
    }
    __sync_fetch_and_add(&total_insert, stat_insert);
    __sync_fetch_and_add(&total_delete, stat_delete);
    return NULL;
}

int main(int argv, char *argc[]) {
    pthread_t threads[THREADS_CNT];
    unsigned long long i = 0;

    if (argv != 3) {
        printf("input total keys and ops\n");
        return 0;
    }
    else {
        total_key = atoll(argc[1]);
        total_ops = atoll(argc[2]);
        if ((total_key > 0xFFFFFFFFFFULL) || (total_ops > 0xFFFFFFFFFFULL)) {
            printf("too large input\n");
            return -1;
        }
        total_key -= total_key % 2;
        ops_per_thread = total_ops / THREADS_CNT;
    }

    key = (unsigned long long *)malloc((total_key / 2) * sizeof(unsigned long long));
    
    unsigned int seed = (unsigned int)time(NULL);
    printf("seed: %u\n", seed);
    srand(seed);

    radix_tree_init();
    radix_tree_create(&root);
    
    for (i = 0; i < (total_key / 2); i++)
        key[i] = rand_ull();

    for (i = 0; i < THREADS_CNT; i++) {
        if (pthread_create(&threads[i], NULL, thread_main, (void *)i) != 0) {
            printf("failed to create thread: %llu\n", i);
            exit(-1);
        }
    }
    for (i = 0; i < THREADS_CNT; i++)
        pthread_join(threads[i], NULL);
    printf("total insert: %llu, delete: %llu\n", total_insert, total_delete);
    return 0;
}
