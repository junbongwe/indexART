#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include <limits.h>

#include "radix_tree.h"

#define THREAD_CNT 16

#define OFS_MASK 0xFFFFFFFFULL // 4GB
#define LEN_MASK 0xFFFFFULL // 1MB

struct radix_tree_root root;

void *thread_main(void *aux) {
	unsigned long long ops = (unsigned long long)aux, i, ofs, len;
	
	for (i = 0; i < ops; i++) {
        	ofs = ((((unsigned long long)rand()) << 32) | ((unsigned long long)rand())) & OFS_MASK;
        	len = (((((unsigned long long)rand()) << 32) | ((unsigned long long)rand())) & LEN_MASK) + 1;
		radix_tree_insert(&root, ofs, len, (void *)ofs, 0);
	}
	return NULL;
}

unsigned long long check_inserted_leaf() {
    struct radix_tree_leaf *leaf, *next_leaf;
    unsigned long long cnt = 0;

    leaf = root.head.next;

    while (leaf) {
	    cnt++;
	    next_leaf = leaf->next;
	    if (next_leaf && (leaf->node.offset >= next_leaf->node.offset))
		    return -1;
	    leaf = next_leaf;
    }
    return cnt;
}

int main(int argc, char *argv[]) {
	pthread_t threads[THREAD_CNT];
	long long total_ops, ops_per_thread;
	void *ret;
	int i;

	if (argc < 2) {
		printf("input total ops\n");
		return -1;
	}
	else {
		total_ops = atoll(argv[1]);
		if (total_ops < 0) {
			printf("wrong input\n");
			return -1;
		}
		ops_per_thread = total_ops / THREAD_CNT;
	}

	unsigned int seed = (unsigned int)time(NULL);
	printf("seed: %u\n", seed);
	fflush(stdout);
	srand(seed);

	radix_tree_init();
	radix_tree_create(&root);

	for (i = 0; i < THREAD_CNT; i++) {
		if (pthread_create(&threads[i], NULL, &thread_main, (void *)ops_per_thread)) {
			printf("thread creation failed\n");
			return -1;
		}
	}
	for (i = 0; i < THREAD_CNT; i++)
		pthread_join(threads[i], &ret);

	printf("total leaf: %lld\n", check_inserted_leaf());
}

