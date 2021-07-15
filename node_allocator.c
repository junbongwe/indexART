#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "radix_tree.h"

// TID Allocator
#define MAX_ARR_SIZE 211
struct pthread_elem {
	pthread_t tid;
	int idx;
};
struct pthread_elem pthread_arr[MAX_ARR_SIZE] = {0, };
int pthread_arr_idx = 0;

static inline int hash_function (pthread_t tid) {
	return (tid >> 8) % MAX_ARR_SIZE;
}

/* Get unique thread id, starting from 0. */
int get_tid (void) {
	pthread_t tid = pthread_self ();
	int h = hash_function (tid);
	int i = h;

	do {
		if (pthread_arr[i].tid == 0) {
			if (__sync_val_compare_and_swap (&pthread_arr[i].tid, 0, tid) == 0) {
					pthread_arr[i].idx = __sync_fetch_and_add (&pthread_arr_idx, 0x1);
					return pthread_arr[i].idx;
			}
		}
		else if (pthread_equal (pthread_arr[i].tid, tid))
			return pthread_arr[i].idx;
		i = ((i >= MAX_ARR_SIZE - 1) ? 0 : i + 1);
	} while (i != h);

	return -1;
}


// Node allocator
struct leaf_pthread_elem {
	struct radix_tree_leaf *head;
	unsigned long long alloc;
	unsigned long long free;
};
struct N4_pthread_elem {
	struct N4 *head;
	unsigned long long alloc;
	unsigned long long free;
};
struct N16_pthread_elem {
	struct N16 *head;
	unsigned long long alloc;
	unsigned long long free;
};
struct N48_pthread_elem {
	struct N48 *head;
	unsigned long long alloc;
	unsigned long long free;
};
struct N256_pthread_elem {
	struct N256 *head;
	unsigned long long alloc;
	unsigned long long free;
};

static struct leaf_pthread_elem leaf_pthread_arr[MAX_TRANSACTION] = {0, };
static struct radix_tree_leaf *leaf_pool_head = NULL;
static pthread_mutex_t leaf_pool_lock = PTHREAD_MUTEX_INITIALIZER;
static unsigned long long leaf_malloc_cnt = 0;
static unsigned long long leaf_pool_free_cnt = 0;

static struct N4_pthread_elem N4_pthread_arr[MAX_TRANSACTION] = {0, };
static struct N4 *N4_pool_head = NULL;
static pthread_mutex_t N4_pool_lock = PTHREAD_MUTEX_INITIALIZER;
static unsigned long long N4_malloc_cnt = 0;
static unsigned long long N4_pool_free_cnt = 0;

static struct N16_pthread_elem N16_pthread_arr[MAX_TRANSACTION] = {0, };
static struct N16 *N16_pool_head = NULL;
static pthread_mutex_t N16_pool_lock = PTHREAD_MUTEX_INITIALIZER;
static unsigned long long N16_malloc_cnt = 0;
static unsigned long long N16_pool_free_cnt = 0;

static struct N48_pthread_elem N48_pthread_arr[MAX_TRANSACTION] = {0, };
static struct N48 *N48_pool_head = NULL;
static pthread_mutex_t N48_pool_lock = PTHREAD_MUTEX_INITIALIZER;
static unsigned long long N48_malloc_cnt = 0;
static unsigned long long N48_pool_free_cnt = 0;

static struct N256_pthread_elem N256_pthread_arr[MAX_TRANSACTION] = {0, };
static struct N256 *N256_pool_head = NULL;
static pthread_mutex_t N256_pool_lock = PTHREAD_MUTEX_INITIALIZER;
static unsigned long long N256_malloc_cnt = 0;
static unsigned long long N256_pool_free_cnt = 0;

static inline void *return_node_to_pool (void *node, unsigned long long length, enum node_types type){
	void *ret_head;
	unsigned long long i;

	switch(type) {
		{
		struct radix_tree_leaf *append_tail;
		case LEAF_NODE:
			append_tail = (struct radix_tree_leaf *)node;
			for (i = 0; i < length - 1; i++)
				append_tail = append_tail->next;
			ret_head = append_tail->next;
			pthread_mutex_lock (&leaf_pool_lock);
			append_tail->next = leaf_pool_head;
			leaf_pool_head = (struct radix_tree_leaf *)node;
			leaf_pool_free_cnt += length;
			pthread_mutex_unlock (&leaf_pool_lock);
			return ret_head;
		}
		{
		struct N4 *append_tail;
		case N4:
			append_tail = (struct N4 *)node;
			for (i = 0; i < length - 1; i++)
				append_tail = append_tail->slots[0];
			ret_head = append_tail->slots[0];
			pthread_mutex_lock (&N4_pool_lock);
			append_tail->slots[0] = N4_pool_head;
			N4_pool_head = (struct N4 *)node;
			N4_pool_free_cnt += length;
			pthread_mutex_unlock (&N4_pool_lock);
			return ret_head;
		}
		{
		struct N16 *append_tail;
		case N16:
			append_tail = (struct N16 *)node;
			for (i = 0; i < length - 1; i++)
				append_tail = append_tail->slots[0];
			ret_head = append_tail->slots[0];
			pthread_mutex_lock (&N16_pool_lock);
			append_tail->slots[0] = N16_pool_head;
			N16_pool_head = (struct N16 *)node;
			N16_pool_free_cnt += length;
			pthread_mutex_unlock (&N16_pool_lock);
			return ret_head;
		}
		{
		struct N48 *append_tail;
		case N48:
			append_tail = (struct N48 *)node;
			for (i = 0; i < length - 1; i++)
				append_tail = append_tail->slots[0];
			ret_head = append_tail->slots[0];
			pthread_mutex_lock (&N48_pool_lock);
			append_tail->slots[0] = N48_pool_head;
			N48_pool_head = (struct N48 *)node;
			N48_pool_free_cnt += length;
			pthread_mutex_unlock (&N48_pool_lock);
			return ret_head;
		}
		{
		struct N256 *append_tail;
		case N256:
			append_tail = (struct N256 *)node;
			for (i = 0; i < length - 1; i++)
				append_tail = append_tail->slots[0];
			ret_head = append_tail->slots[0];
			pthread_mutex_lock (&N256_pool_lock);
			append_tail->slots[0] = N256_pool_head;
			N256_pool_head = (struct N256 *)node;
			N256_pool_free_cnt += length;
			pthread_mutex_unlock (&N256_pool_lock);
			return ret_head;
		}
	}
	radix_unreachable();
}

void return_node(struct radix_tree_node *new_node){
	free(new_node);
	return;

	pid_t tid = get_tid ();
	switch(new_node->type){
		{
		struct leaf_pthread_elem *elem;
		struct radix_tree_leaf *leaf;
		case LEAF_NODE:
			elem = &leaf_pthread_arr[tid];
			leaf = (struct radix_tree_leaf *)new_node;
			leaf->next = elem->head;
			elem->head = leaf;
			elem->alloc--;
			elem->free++;
			if ((elem->free > (elem->alloc * 2)) && (elem->alloc > 0)) {
				elem->head = return_node_to_pool (elem->head, elem->alloc, LEAF_NODE);
				elem->free -= elem->alloc;
			}
			return;
		}
		{
		struct N4_pthread_elem *elem;
		struct N4 *n4;
		case N4:
			elem = &N4_pthread_arr[tid];
			n4 = (struct N4 *)new_node;
			n4->slots[0] = elem->head;
			elem->head = n4;
			elem->alloc--;
			elem->free++;
			if ((elem->free > (elem->alloc * 2)) && (elem->alloc > 0)) {
				elem->head = return_node_to_pool (elem->head, elem->alloc, N4);
				elem->free -= elem->alloc;
			}
			return;
		}
		{
		struct N16_pthread_elem *elem;
		struct N16 *n16;
		case N16:
			elem = &N16_pthread_arr[tid];
			n16 = (struct N16 *)new_node;
			n16->slots[0] = elem->head;
			elem->head = n16;
			elem->alloc--;
			elem->free++;
			if ((elem->free > (elem->alloc * 2)) && (elem->alloc > 0)) {
				elem->head = return_node_to_pool (elem->head, elem->alloc, N16);
				elem->free -= elem->alloc;
			}
			return;
		}
		{
		struct N48_pthread_elem *elem;
		struct N48 *n48;
		case N48:
			elem = &N48_pthread_arr[tid];
			n48 = (struct N48 *)new_node;
			n48->slots[0] = elem->head;
			elem->head = n48;
			elem->alloc--;
			elem->free++;
			if ((elem->free > (elem->alloc * 2)) && (elem->alloc > 0)) {
				elem->head = return_node_to_pool (elem->head, elem->alloc, N48);
				elem->free -= elem->alloc;
			}
			return;
		}
		{
		struct N256_pthread_elem *elem;
		struct N256 *n256;
		case N256:
			elem = &N256_pthread_arr[tid];
			n256 = (struct N256 *)new_node;
			n256->slots[0] = elem->head;
			elem->head = n256;
			elem->alloc--;
			elem->free++;
			if ((elem->free > (elem->alloc * 2)) && (elem->alloc > 0)) {
				elem->head = return_node_to_pool (elem->head, elem->alloc, N256);
				elem->free -= elem->alloc;
			}
			return;
		}
	}
}

int build_node(unsigned long long n, enum node_types type){
	return 0;
	unsigned long long i;
	switch(type){
		{
		struct radix_tree_leaf *leaf;
		case LEAF_NODE:
			leaf = (struct radix_tree_leaf *)malloc(sizeof(struct radix_tree_leaf) * n);
			radix_assert(leaf != NULL);
			if (leaf == NULL)
				return -1;
			for (i = 0; i < n - 1; i++)
				leaf[i].next = &leaf[i + 1];
			leaf[n - 1].next = leaf_pool_head;
			leaf_pool_head = leaf;
			leaf_pool_free_cnt += n;
			leaf_malloc_cnt += n;
			return 0;
		}
		{
		struct N4 *n4;
		case N4:
			n4 = (struct N4 *)malloc(sizeof(struct N4) * n);
			radix_assert(n4 != NULL);
			if (n4 == NULL)
				return -1;
			for (i = 0; i < n - 1; i++)
				n4[i].slots[0] = &n4[i+1];
			n4[n - 1].slots[0] = N4_pool_head;
			N4_pool_head = n4;
			N4_pool_free_cnt += n;
			N4_malloc_cnt += n;
			return 0;
		}
		{
		struct N16 *n16;
		case N16:
			n16 = (struct N16 *)malloc(sizeof(struct N16) * n);
			radix_assert(n16 != NULL);
			if (n16 == NULL)
				return -1;
			for (i = 0; i < n - 1; i++)
				n16[i].slots[0] = &n16[i+1];
			n16[n - 1].slots[0] = N16_pool_head;
			N16_pool_head = n16;
			N16_pool_free_cnt += n;
			N16_malloc_cnt += n;
			return 0;
		}
		{
		struct N48 *n48;
		case N48:
			n48 = (struct N48 *)malloc(sizeof(struct N48) * n);
			radix_assert(n48 != NULL);
			if (n48 == NULL)
				return -1;
			for (i = 0; i < n - 1; i++)
				n48[i].slots[0] = &n48[i+1];
			n48[n - 1].slots[0] = N48_pool_head;
			N48_pool_head = n48;
			N48_pool_free_cnt += n;
			N48_malloc_cnt += n;
			return 0;
		}
		{
		struct N256 *n256;
		case N256:
			n256 = (struct N256 *)malloc(sizeof(struct N256) * n);
			radix_assert(n256 != NULL);
			if (n256 == NULL)
				return -1;
			for (i = 0; i < n - 1; i++)
				n256[i].slots[0] = &n256[i+1];
			n256[n - 1].slots[0] = N256_pool_head;
			N256_pool_head = n256;
			N256_pool_free_cnt += n;
			N256_malloc_cnt += n;
			return 0;
		}
	}
	return -1;
}

static inline void *get_node_from_pool (unsigned long long length, enum node_types type) {
	unsigned long long i;
	void *ret_head;

	switch(type) {
		{
		struct radix_tree_leaf *ret_tail;
		case LEAF_NODE:
			pthread_mutex_lock (&leaf_pool_lock);
		leaf_node_begin:
			if (leaf_pool_free_cnt >= length) {
				ret_head = (ret_tail = leaf_pool_head);
				for (i = 0; i < length - 1; i++)
					ret_tail = ret_tail->next;
				leaf_pool_head = ret_tail->next;
				leaf_pool_free_cnt -= length;
				pthread_mutex_unlock (&leaf_pool_lock);
				ret_tail->next = NULL;

				return ret_head;
			}
			else {
				if (build_node ((leaf_malloc_cnt > 5) ? leaf_malloc_cnt : 5, type) != 0)
					return NULL;
				goto leaf_node_begin;
			}
		}
		{
		struct N4 *ret_tail;
		case N4:
			pthread_mutex_lock (&N4_pool_lock);
		n4_node_begin:
			if (N4_pool_free_cnt >= length) {
				ret_head = (ret_tail = N4_pool_head);
				for (i = 0; i < length - 1; i++)
					ret_tail = ret_tail->slots[0];
				N4_pool_head = ret_tail->slots[0];
				N4_pool_free_cnt -= length;
				pthread_mutex_unlock (&N4_pool_lock);
				ret_tail->slots[0] = NULL;

				return ret_head;
			}
			else {
				if (build_node ((N4_malloc_cnt > 5) ? N4_malloc_cnt : 5, type) != 0)
					return NULL;
				goto n4_node_begin;
			}
		}
		{
		struct N16 *ret_tail;
		case N16:
			pthread_mutex_lock (&N16_pool_lock);
		n16_node_begin:
			if (N16_pool_free_cnt >= length) {
				ret_head = (ret_tail = N16_pool_head);
				for (i = 0; i < length - 1; i++)
					ret_tail = ret_tail->slots[0];
				N16_pool_head = ret_tail->slots[0];
				N16_pool_free_cnt -= length;
				pthread_mutex_unlock (&N16_pool_lock);
				ret_tail->slots[0] = NULL;

				return ret_head;
			}
			else {
				if (build_node ((N16_malloc_cnt > 5) ? N16_malloc_cnt : 5, type) != 0)
					return NULL;
				goto n16_node_begin;
			}
		}
		{
		struct N48 *ret_tail;
		case N48:
			pthread_mutex_lock (&N48_pool_lock);
		n48_node_begin:
			if (N48_pool_free_cnt >= length) {
				ret_head = (ret_tail = N48_pool_head);
				for (i = 0; i < length - 1; i++)
					ret_tail = ret_tail->slots[0];
				N48_pool_head = ret_tail->slots[0];
				N48_pool_free_cnt -= length;
				pthread_mutex_unlock (&N48_pool_lock);
				ret_tail->slots[0] = NULL;

				return ret_head;
			}
			else {
				//printf("build N48\n");
				if (build_node ((N48_malloc_cnt > 5) ? N48_malloc_cnt : 5, type) != 0)
					return NULL;
				goto n48_node_begin;
			}
		}
		{
		struct N256 *ret_tail;
		case N256:
			pthread_mutex_lock (&N256_pool_lock);
		n256_node_begin:
			if (N256_pool_free_cnt >= length) {
				ret_head = (ret_tail = N256_pool_head);
				for (i = 0; i < length - 1; i++)
					ret_tail = ret_tail->slots[0];
				N256_pool_head = ret_tail->slots[0];
				N256_pool_free_cnt -= length;
				pthread_mutex_unlock (&N256_pool_lock);
				ret_tail->slots[0] = NULL;

				return ret_head;
			}
			else {
				//printf("build N256\n");
				if (build_node ((N256_malloc_cnt > 5) ? N256_malloc_cnt : 5, type) != 0)
					return NULL;
				goto n256_node_begin;
			}
		}
	}
	radix_unreachable();
}

/* Allocate node of type TYPE. Caller should fill zero if necessary. */
struct radix_tree_node *get_node(enum node_types type){
	pid_t tid = get_tid ();
	struct radix_tree_node *node;
	unsigned long long pool_request_size;
	switch(type){
		{
		struct radix_tree_leaf *leaf;
		struct leaf_pthread_elem *elem;
		case LEAF_NODE:
			node = (struct radix_tree_node *)calloc(1, sizeof(struct radix_tree_leaf));
			node->type = LEAF_NODE;
			return node;

			elem = &leaf_pthread_arr[tid];
			if (elem->head != NULL) {
				leaf = elem->head;
				elem->head = leaf->next;
				elem->alloc++;
				elem->free--;
			}
			else {
				pool_request_size = (elem->alloc) ? elem->alloc*2 : 100;
				leaf = get_node_from_pool (pool_request_size, type);
				if (leaf != NULL) {
					elem->head = leaf->next;
					elem->alloc++;
					elem->free = pool_request_size - 1;
				}
			}
			leaf->node.type = LEAF_NODE;
			return (struct radix_tree_node*)leaf;
		}
		{
		struct N4 *n4;
		struct N4_pthread_elem *elem;
		case N4:
			node = (struct radix_tree_node *)calloc(1, sizeof(struct N4));
			node->type = N4;
			return node;

			elem = &N4_pthread_arr[tid];
			if (elem->head != NULL) {
				n4 = elem->head;
				elem->head = n4->slots[0];
				elem->alloc++;
				elem->free--;
			}
			else {
				pool_request_size = (elem->alloc) ? elem->alloc*2: 100;
				n4 = get_node_from_pool (pool_request_size, type);
				if (n4 != NULL) {
					elem->head = n4->slots[0];
					elem->alloc++;
					elem->free = pool_request_size - 1;
				}
			}
			n4->node.type = N4;
			return (struct radix_tree_node*)n4;
		}
		{
		struct N16 *n16;
		struct N16_pthread_elem *elem;
		case N16:
			node = (struct radix_tree_node *)calloc(1, sizeof(struct N16));
			node->type = N16;
			return node;

			elem = &N16_pthread_arr[tid];
			if (elem->head != NULL) {
				n16 = elem->head;
				elem->head = n16->slots[0];
				elem->alloc++;
				elem->free--;
			}
			else {
				pool_request_size = (elem->alloc) ? elem->alloc*2: 100;
				n16 = get_node_from_pool (pool_request_size, type);
				if (n16 != NULL) {
					elem->head = n16->slots[0];
					elem->alloc++;
					elem->free = pool_request_size - 1;
				}
			}
			n16->node.type = N16;
			return (struct radix_tree_node*)n16;
		}
		{
		struct N48 *n48;
		struct N48_pthread_elem *elem;
		case N48:
			node = (struct radix_tree_node *)calloc(1, sizeof(struct N48));
			node->type = N48;
			return node;

			elem = &N48_pthread_arr[tid];
			if (elem->head != NULL) {
				n48 = elem->head;
				elem->head = n48->slots[0];
				elem->alloc++;
				elem->free--;
			}
			else {
				pool_request_size = (elem->alloc) ? elem->alloc*2: 100;
				n48 = get_node_from_pool (pool_request_size, type);
				if (n48 != NULL) {
					elem->head = n48->slots[0];
					elem->alloc++;
					elem->free = pool_request_size - 1;
				}
			}
			n48->node.type = N48;
			return (struct radix_tree_node*)n48;
		}
		{
		struct N256 *n256;
		struct N256_pthread_elem *elem;
		case N256:
			node = (struct radix_tree_node *)calloc(1, sizeof(struct N256));
			node->type = N256;
			return node;

			elem = &N256_pthread_arr[tid];
			if (elem->head != NULL) {
				n256 = elem->head;
				elem->head = n256->slots[0];
				elem->alloc++;
				elem->free--;
			}
			else {
				pool_request_size = (elem->alloc) ? elem->alloc*2: 100;
				n256 = get_node_from_pool (pool_request_size, type);
				if (n256 != NULL) {
					elem->head = n256->slots[0];
					elem->alloc++;
					elem->free = pool_request_size - 1;
				}
			}
			n256->node.type = N256;
			return (struct radix_tree_node*)n256;
		}
	}
	radix_unreachable();
}

