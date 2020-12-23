#include "radix-tree.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>

struct radix_tree_node *root = NULL;

int quiet = 1;
unsigned int seed;

#define BREAK_OPS (-1)
#define THREAD_CNT 32

void bk_fn(void) {
    printf("seed: %u\n", seed);
}

void *thread_main(void *aux) {
    long long total_ops = (long long)aux, cur_ops;
    struct radix_tree_leaf *leaf = NULL;
    unsigned long long *arr = (unsigned long long *)malloc(total_ops * sizeof(unsigned long long));
    unsigned long long i = 0, j;

    for (cur_ops = 0; cur_ops < total_ops; cur_ops++) {
        if ((i == 0) || rand() % 2) {
            //insert
            j = i++;
            arr[j] = ((((unsigned long long)rand()) << 32) | ((unsigned long long)rand())) & 0xFFFFFFFFFF;
            if (!quiet)
                printf("(%lld)\tinsert key %llx\n", cur_ops, arr[j]);
            if (cur_ops == BREAK_OPS)
                bk_fn();
            radix_tree_insert(&root, arr[j], 0, (void *)arr[j], 0);
        }
        else {
            //lookup
            j = ((((unsigned long long)rand()) << 32) | ((unsigned long long)rand())) % i;
            if (!quiet) {
                printf("(%lld)\tLookup for key %llx", cur_ops, arr[j]);
                fflush(NULL);
            }
            if (cur_ops == BREAK_OPS)
                bk_fn();
            leaf = NULL;
            enum lookup_results r;
            if ((r = radix_tree_lookup(&root, arr[j], &leaf)) != RET_MATCH_NODE) {
                bk_fn();
                if (leaf != NULL) {
                    printf("(%lld)lookup fail, expected key : %p, actual key: %p, actual value: %p, lookup: %d\n", cur_ops, (void *)arr[j], (void *)leaf->node.offset, leaf->log_addr, r);
                }
                else{
                    printf("(%lld)NULL leaf: %d, %llu\n", cur_ops, r, j);
                    continue;
                }
            }
            if (leaf->log_addr != (void *)arr[j]) {
                printf("seed: %u\n", seed);
                printf(" failed(consistency check failed), %llu\n", j);
                return NULL;
            }
            if (!quiet)
                printf(" succeed: %llx\n", (unsigned long long)leaf->log_addr);
        }
    }
    return NULL;
}

int main(int argc, char *argv[]) {
    pthread_t threads[THREAD_CNT];
    int thread_cnt = 0;
    long long total_ops, ops_per_thread;

    if (argc < 2) {
        printf("input total ops\n");
        return 0;
    }
    else {
        total_ops = atoll(argv[1]);
        ops_per_thread = total_ops / THREAD_CNT;
    }

    if (argc != 2)
        quiet = 0;

    seed = (unsigned int)time(NULL);
    printf("seed: %u\n", seed);
    srand(seed);

    radix_tree_init();
    while (thread_cnt < THREAD_CNT)
        pthread_create(&threads[thread_cnt++], NULL, &thread_main, (void *)ops_per_thread);
    for (thread_cnt = 0; thread_cnt < THREAD_CNT; thread_cnt++) {
        pthread_join(threads[thread_cnt], NULL);
    }

    thread_main((void *)total_ops);
    return 0;
}
