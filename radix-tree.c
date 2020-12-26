#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdatomic.h>
#include <immintrin.h>

#include "radix-tree.h"

#define MAX_TRANSACTION 32

#ifdef NVTX_DEBUG
#define radix_assert(EXP) assert(EXP)
#else
#define radix_assert(EXP) {}
#endif

#define barrier() asm volatile("": : :"memory")

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

// Node Allocator
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
	radix_assert(false);
	__builtin_unreachable();
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

static inline int build_node(unsigned long long n, enum node_types type){
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
	radix_assert(false);
	__builtin_unreachable();
}

/* Allocate node of type TYPE. Caller should fill zero if necessary. */
static inline struct radix_tree_node *get_node(enum node_types type){
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
	radix_assert(false);
	__builtin_unreachable();
}

// Mutex implememtation for Radix tree nodes
#define get_version(NODE) (atomic_load(&(NODE)->lock_n_obsolete))
#define is_locked(VERSION) (((VERSION) & 0b10) == 0b10)
#define is_obsolete(VERSION) ((VERSION) & 1)
#define write_unlock(NODE) (atomic_fetch_add(&(NODE)->lock_n_obsolete, 0b10))
#define write_unlock_obsolete(NODE) (atomic_fetch_add(&(NODE)->lock_n_obsolete, 0b11))
#define read_unlock_or_restart(NODE, START_READ) ((START_READ) != atomic_load(&(NODE)->lock_n_obsolete))
#define check_or_restart(NODE, START_READ) (read_unlock_or_restart(NODE, START_READ))


/* Return true for restart needed, false for success. */
static inline bool write_lock_or_restart(struct radix_tree_node *node) {
	unsigned long long version;
	do {
		version = get_version(node);
		while (is_locked(version)) {
			_mm_pause();
			version = get_version(node);
		}
		if (is_obsolete(version))
			return true;
	} while (!atomic_compare_exchange_weak(&node->lock_n_obsolete, &version, version + 0b10));
	return false;
}

static inline bool lock_version_or_restart(struct radix_tree_node *node, unsigned long long *version) {
	if (is_locked(*version) || is_obsolete(*version))
		return true;
	if (atomic_compare_exchange_strong(&node->lock_n_obsolete, version, *version + 0b10)) {
		*version += 0b10;
		return false;
	}
	else
		return true;
}

// Radix tree node grabage collector.
static void return_node_to_gc(struct radix_tree_node *node) {
	//printf("return node\n");
	return;
}

// Radix tree ops
#define is_leaf(NODE) ((NODE)->type == LEAF_NODE)
#define is_fault_node(NODE, KEY, PARENT_LEVEL) \
		((KEY) != (((NODE)->offset >> ((((NODE)->level - (PARENT_LEVEL) - 1) * RADIX_TREE_ENTRY_BIT_SIZE))) & RADIX_TREE_MAP_MASK))

int radix_tree_init() {
	build_node(10000, LEAF_NODE);
	build_node(10000, N4);
	build_node(10000, N16);
	build_node(10000, N48);
	build_node(10000, N256);
	return 0;
}

void radix_tree_destroy(radix_tree_root root) {
	return;
}

static inline enum lookup_results get_child_range(struct radix_tree_node *parent_, struct radix_tree_node **nodep, unsigned char key, unsigned char level) {
	struct radix_tree_node *i_node, *ret_node;
	unsigned char index_idx, index_pos, bit_idx;
	unsigned char ret_key, i_key, idx;
	unsigned long long bitfield;
	int i, i_diff, ret_diff, count;

	switch (parent_->type) {
		{
		struct N4 *parent;
		case N4:
			parent = (struct N4 *)parent_;
n4_begin:
			count = parent->node.count;
			ret_diff = INT_MAX;
			ret_node = NULL;
			barrier();
			for (i = count - 1; i >= 0; i--) {
				i_key = parent->key[i];
				i_diff = (int)i_key - (int)key;
				barrier();
				if (i_diff == 0) {
					if ((ret_node = parent->slots[i]) != NULL) {
						if (is_fault_node(ret_node, key, level))
							goto n4_begin;
						*nodep = ret_node;
						return RET_MATCH_NODE;
					}
				}
				else if (i_diff < 0) {
					if (ret_diff > 0 || ret_diff < i_diff) {
						if ((i_node = parent->slots[i]) != NULL) {
							ret_diff = i_diff;
							ret_node = i_node;
							ret_key = i_key;
						}
					}
				}
				else {
					if (ret_diff > 0 && ret_diff > i_diff) {
						if ((i_node = parent->slots[i]) != NULL) {
							ret_diff = i_diff;
							ret_node = i_node;
							ret_key = i_key;
						}
					}
				}
			}
			radix_assert(ret_node != NULL);
			*nodep = ret_node;
			if (is_fault_node(ret_node, ret_key, level))
				goto n4_begin;
			return (ret_key < key) ? RET_PREV_NODE : RET_NEXT_NODE;
		}
		{
		struct N16 *parent;
		case N16:
			parent = (struct N16 *)parent_;
n16_begin:
			count = parent->node.count;
			ret_diff = INT_MAX;
			ret_node = NULL;
			barrier();
			for (i = count - 1; i >= 0; i--) {
				i_key = parent->key[i];
				i_diff = (int)i_key - (int)key;
				barrier();
				if (i_diff == 0) {
					if ((ret_node = parent->slots[i]) != NULL) {
						if (is_fault_node(ret_node, key, level))
							goto n16_begin;
						*nodep = ret_node;
						return RET_MATCH_NODE;
					}
				}
				else if (i_diff < 0) {
					if (ret_diff > 0 || ret_diff < i_diff) {
						if ((i_node = parent->slots[i]) != NULL) {
							ret_diff = i_diff;
							ret_node = i_node;
							ret_key = i_key;
						}
					}
				}
				else {
					if (ret_diff > 0 && ret_diff > i_diff) {
						if ((i_node = parent->slots[i]) != NULL) {
							ret_diff = i_diff;
							ret_node = i_node;
							ret_key = i_key;
						}
					}
				}
			}
			radix_assert(ret_node != NULL);
			*nodep = ret_node;
			if (ret_key != ((ret_node->offset >> (((ret_node->level - level - 1) * RADIX_TREE_ENTRY_BIT_SIZE))) & RADIX_TREE_MAP_MASK))
				goto n16_begin;
			return (ret_key < key) ? RET_PREV_NODE : RET_NEXT_NODE;
		}
		{
		struct N48 *parent;
		case N48:
			parent = (struct N48 *)parent_;
n48_begin:

			if ((idx = parent->key[key]) != N48_NO_ENT) {
				if ((ret_node = parent->slots[idx]) != NULL) {
					if (is_fault_node(ret_node, key, level))
						goto n48_begin;
					*nodep = ret_node;
					return RET_MATCH_NODE;
				}
			}
			
			unsigned long long index[RADIX_TREE_INDEX_SIZE];
			__m256i index_;
			for (i = 0; i < RADIX_TREE_INDEX_SIZE; i++)
				index[i] = parent->index[i];
			while (true) {
				barrier();
				index_ = _mm256_loadu_si256((__m256i *)parent->index);
				if (!_mm256_cmpneq_epi64_mask(_mm256_loadu_si256((__m256i *)index), index_))
					break;
				barrier();
				for (i = 0; i < RADIX_TREE_INDEX_SIZE; i++)
					index[i] = parent->index[i];
				if (!_mm256_cmpneq_epi64_mask(_mm256_loadu_si256((__m256i *)index), index_))
					break;
			}

			index_idx = key / BITS_PER_INDEX;
			index_pos = key % BITS_PER_INDEX;

			bitfield = INDEX_LE(index[index_idx], index_pos);
			while (bitfield) {
				bit_idx = __builtin_ctzll(bitfield);
				ret_key = (index_idx * BITS_PER_INDEX) + (BITS_PER_INDEX - 1) - bit_idx;
				idx = parent->key[ret_key];
				if (idx != N48_NO_ENT) {
					if ((ret_node = parent->slots[idx]) != NULL) {
						if (is_fault_node(ret_node, ret_key, level))
							goto n48_begin;
						*nodep = ret_node;
						return RET_PREV_NODE;
					}
				}
				bitfield ^= (1ULL << bit_idx);
			}
			for (i = index_idx - 1; i >= 0; i--) {
				bitfield = index[i];
				while (bitfield) {
					bit_idx = __builtin_ctzll(bitfield);
					ret_key = (i * BITS_PER_INDEX) + (BITS_PER_INDEX - 1) - bit_idx;
					idx = parent->key[ret_key];
					if (idx != N48_NO_ENT) {
						if ((ret_node = parent->slots[idx]) != NULL) {
							if (is_fault_node(ret_node, ret_key, level))
								goto n48_begin;
							*nodep = ret_node;
							return RET_PREV_NODE;
						}
					}
					bitfield ^= (1ULL << bit_idx);
				}
			}

			bitfield = INDEX_GE(index[index_idx], index_pos);
			while (bitfield) {
				bit_idx = __builtin_clzll(bitfield);
				ret_key = (index_idx * BITS_PER_INDEX) + bit_idx;
				idx = parent->key[ret_key];
				if (idx != N48_NO_ENT) {
					if ((ret_node = parent->slots[idx]) != NULL) {
						if (is_fault_node(ret_node, ret_key, level))
							goto n48_begin;
						*nodep = ret_node;
						return RET_NEXT_NODE;
					}
				}
				bitfield ^= (1ULL << ((BITS_PER_INDEX - 1) - bit_idx));
			}
			for (i = index_idx + 1; i < RADIX_TREE_INDEX_SIZE; i++) {
				bitfield = index[i];
				while (bitfield) {
					bit_idx = __builtin_clzll(bitfield);
					ret_key = (i * BITS_PER_INDEX) + bit_idx;
					idx = parent->key[ret_key];
					if (idx != N48_NO_ENT) {
						if ((ret_node = parent->slots[idx]) != NULL) {
							if (is_fault_node(ret_node, ret_key, level))
								goto n48_begin;
							*nodep = ret_node;
							return RET_NEXT_NODE;
						}
					}
					bitfield ^= (1ULL << ((BITS_PER_INDEX - 1) - bit_idx));
				}
			}
			radix_assert(false);
		}
		{
		struct N256 *parent;
		default:
			parent = (struct N256 *)parent_;

			radix_assert(parent_->type == N256);

			if ((ret_node = parent->slots[key]) != NULL) {
				*nodep = ret_node;
				return RET_MATCH_NODE;
			}

			unsigned long long index[RADIX_TREE_INDEX_SIZE];
			__m256i index_;
			for (i = 0; i < RADIX_TREE_INDEX_SIZE; i++)
				index[i] = parent->index[i];
			while (true) {
				barrier();
				index_ = _mm256_loadu_si256((__m256i *)parent->index);
				if (!_mm256_cmpneq_epi64_mask(_mm256_loadu_si256((__m256i *)index), index_))
					break;
				barrier();
				for (i = 0; i < RADIX_TREE_INDEX_SIZE; i++)
					index[i] = parent->index[i];
				if (!_mm256_cmpneq_epi64_mask(_mm256_loadu_si256((__m256i *)index), index_))
					break;
			}

			index_idx = key / BITS_PER_INDEX;
			index_pos = key % BITS_PER_INDEX;

			bitfield = INDEX_LE(index[index_idx], index_pos);
			while (bitfield) {
				idx = __builtin_ctzll(bitfield);
				if ((ret_node = parent->slots[(index_idx * BITS_PER_INDEX) + (BITS_PER_INDEX - 1) - idx]) != NULL) {
					*nodep = ret_node;
					return RET_PREV_NODE;
				}
				bitfield ^= (1ULL << idx);
			}
			for (i = index_idx - 1; i >= 0; i--) {
				bitfield = index[i];
				while (bitfield) {
					idx = __builtin_ctzll(bitfield);
					if ((ret_node = parent->slots[(i * BITS_PER_INDEX) + (BITS_PER_INDEX - 1) - idx]) != NULL) {
						*nodep = ret_node;
						return RET_PREV_NODE;
					}
					bitfield ^= (1ULL << idx);
				}
			}

			bitfield = INDEX_GE(index[index_idx], index_pos);
			while (bitfield) {
				idx = __builtin_clzll(bitfield);
				if ((ret_node = parent->slots[(index_idx * BITS_PER_INDEX) + idx]) != NULL) {
					*nodep = ret_node;
					return RET_NEXT_NODE;
				}
				bitfield ^= (1ULL << ((BITS_PER_INDEX - 1) - idx));
			}
			for (i = index_idx + 1; i < 4; i++) {
				bitfield = index[i];
				while (bitfield) {
					idx = __builtin_clzll(bitfield);
					if ((ret_node = parent->slots[(i * BITS_PER_INDEX) + idx]) != NULL) {
						*nodep = ret_node;
						return RET_NEXT_NODE;
					}
					bitfield ^= (1ULL << ((BITS_PER_INDEX - 1) - idx));
				}
			}
		}
	}
	radix_assert(false);
	__builtin_unreachable();
}

static inline struct radix_tree_node *get_child_remain(struct radix_tree_node *parent_, unsigned char key) {
	int idx, index_idx, index_pos;
	switch (parent_->type) {
		{
		struct N4 *parent;
		case N4:
			parent = (struct N4 *)parent_;
			return ((parent->key[0] != key) ? parent->slots[0] : parent->slots[1]);
		}
		{
		struct N16 *parent;
		case N16:
			parent = (struct N16 *)parent_;
			return ((parent->key[0] != key) ? parent->slots[0] : parent->slots[1]);
		}
		{
		struct N48 *parent;
		case N48:
			parent = (struct N48 *)parent_;
			for (index_idx = 0; index_idx < 4; index_idx++) {
				unsigned long long bitfield = parent->index[index_idx];
				while (bitfield) {
					index_pos = __builtin_ctzll(bitfield);
					idx = (index_idx * BITS_PER_INDEX) + (BITS_PER_INDEX - 1) - index_pos;
					if (idx != key) {
						radix_assert(parent->key[idx] != N48_NO_ENT);
						radix_assert(parent->slots[parent->key[idx]] != NULL);
						return parent->slots[parent->key[idx]];
					}
					bitfield ^= (1ULL << index_pos);
				}
			}
			radix_assert(false);
			__builtin_unreachable();
		}
		{
		struct N256 *parent;
		case N256:
			parent = (struct N256 *)parent_;
			for (index_idx = 0; index_idx < 4; index_idx++) {
				unsigned long long bitfield = parent->index[index_idx];
				while (bitfield) {
					index_pos = __builtin_ctzll(bitfield);
					idx = (index_idx * BITS_PER_INDEX) + (BITS_PER_INDEX - 1) - index_pos;
					if (idx != key) {
						radix_assert(parent->slots[idx] != NULL);
						return parent->slots[idx];
					}
					bitfield ^= (1ULL << index_pos);
				}
			}
			radix_assert(false);
			__builtin_unreachable();
		}
		default:
			radix_assert(false);
			__builtin_unreachable();
	}
}

static struct radix_tree_node *get_child(struct radix_tree_node *parent_, unsigned char key, unsigned char level) {
	struct radix_tree_node *child;
	int idx, count;
	switch (parent_->type) {
		{
		struct N4 *parent;
		case N4:
			parent = (struct N4 *)parent_;
n4_begin:
			count = parent->node.count;
			barrier();
			for (idx = count - 1; idx >= 0; idx--) {
				if (parent->key[idx] == key) {
					if ((child = parent->slots[idx]) != NULL) {
						if (is_fault_node(child, key, level))
							goto n4_begin;
						return child;
					}
				}
			}
			return NULL;
		}
		{
		struct N16 *parent;
		case N16:
			parent = (struct N16 *)parent_;
n16_begin:
			count = parent->node.count;
			barrier();
			unsigned short bitfield = (_mm_cmpeq_epi8_mask(_mm_set1_epi8(key), _mm_loadu_si128((__m128i *)parent->key))) & ((1 << count) - 1);
			while (bitfield) {
				unsigned char pos = 31 - __builtin_clz(bitfield);
				if ((child = parent->slots[pos]) != NULL) {
					if (is_fault_node(child, key, level))
						goto n16_begin;
					return child;
				}
				bitfield ^= (1 << pos);
			}
			return NULL;
		}
		{
		struct N48 *parent;
		case N48:
			parent = (struct N48 *)parent_;
n48_begin:
			if ((idx = parent->key[key]) == N48_NO_ENT)
				return NULL;
			child = parent->slots[idx];
			if (child == NULL)
				return NULL;
			if (is_fault_node(child, key, level))
				goto n48_begin;
			return child;
		}
		{
		struct N256 *parent;
		case N256:
			parent = (struct N256 *)parent_;
			return parent->slots[key];
		}
		default:
			radix_assert(false);
			__builtin_unreachable();
	}
}

static inline bool insert_child(struct radix_tree_node *parent_, unsigned char key, struct radix_tree_node *child) {
	int idx;
	switch (parent_->type) {
		{
		struct N4 *parent;
		case N4:
			parent = (struct N4 *)parent_;
			if ((idx = parent->node.count) == 4)
				return false;
			parent->key[idx] = key;
			barrier();
			parent->slots[idx] = child;
			barrier();
			parent->node.count = idx + 1;
			return true;
		}
		{
		struct N16 *parent;
		case N16:
			parent = (struct N16 *)parent_;
			if ((idx = parent->node.count) == 16)
				return false;
			parent->key[idx] = key;
			barrier();
			parent->slots[idx] = child;
			barrier();
			parent->node.count = idx + 1;
			return true;
		}
		{
		struct N48 *parent;
		case N48:
			parent = (struct N48 *)parent_;
			unsigned char index_idx;
			unsigned long long bitfield, bitmask;
			int idx;
			if (parent->key[key] != N48_NO_ENT) {
				radix_assert(parent->slots[parent->key[key]] == NULL);
				parent->slots[parent->key[key]] = child;
				return true;
			}
			if ((idx = parent->node.count) == 48)
				return false;
			index_idx = key / BITS_PER_INDEX;
			bitmask = 1ULL << (BITS_PER_INDEX - 1 - (key % BITS_PER_INDEX));
			bitfield = parent->index[index_idx];
			radix_assert((bitfield & bitmask) == 0);
			parent->slots[idx] = child;
			barrier();
			parent->key[key] = idx;
			barrier();
			parent->index[index_idx] = (bitfield | bitmask);
			parent->node.count = idx + 1;
			return true;
		}
		{
		struct N256 *parent;
		case N256:
			parent = (struct N256 *)parent_;
			unsigned char index_idx = key / BITS_PER_INDEX;
			unsigned long long bitfield = parent->index[index_idx];
			unsigned long long bitmask = (1ULL << (BITS_PER_INDEX - 1 - (key % BITS_PER_INDEX)));
			radix_assert(parent->slots[key] == NULL);
			radix_assert((bitfield & bitmask) == 0);
			parent->slots[key] = child;
			barrier();
			parent->index[index_idx] = (bitfield | bitmask);
			parent_->count++;
			return true;
		}
		default:
			radix_assert(false);
			__builtin_unreachable();
	}
}

static inline void insert_child_force(struct radix_tree_node *parent_, unsigned char key, struct radix_tree_node *child) {
	switch (parent_->type) {
		{
		struct N4 *parent;
		case N4:
			parent = (struct N4 *)parent_;
			int idx = parent_->count++;
			radix_assert(idx < 4);
			parent->key[idx] = key;
			parent->slots[idx] = child;
			break;
		}
		{
		struct N16 *parent;
		case N16:
			parent = (struct N16 *)parent_;
			int idx = parent_->count++;
			radix_assert(idx < 16);
			parent->key[idx] = key;
			parent->slots[idx] = child;
			break;
		}
		{
		struct N48 *parent;
		case N48:
			parent = (struct N48 *)parent_;
			int idx = parent_->count++;
			radix_assert(idx < 48);
			parent->key[key] = idx;
			parent->slots[idx] = child;
			parent->index[key / BITS_PER_INDEX] |= (1ULL << (BITS_PER_INDEX - 1 - (key % BITS_PER_INDEX)));
			break;
		}
		{
		struct N256 *parent;
		case N256:
			parent = (struct N256 *)parent_;
			parent_->count++;
			parent->slots[key] = child;
			parent->index[key / BITS_PER_INDEX] |= (1ULL << (BITS_PER_INDEX - 1 - (key % BITS_PER_INDEX)));
			break;
		}
		default:
			radix_assert(false);
			__builtin_unreachable();
	}
}

static inline void delete_child(struct radix_tree_node *parent_, unsigned char key) {
	unsigned char last_idx, idx;
	switch (parent_->type) {
		{
		struct N4 *parent;
		case N4:
			parent = (struct N4 *)parent_;
			last_idx = parent_->count - 1;
			if (parent->key[last_idx] == key) {
				parent_->count--;
				parent->slots[last_idx] = NULL;
				barrier();
				return;
			}
			for (idx = 0; idx < last_idx; idx++) {
				if (parent->key[idx] == key)
					break;
			}
			radix_assert(idx < last_idx);
			parent->slots[idx] = NULL;
			barrier();
			parent->key[idx] = parent->key[last_idx];
			barrier();
			parent->slots[idx] = parent->slots[last_idx];
			barrier();
			parent_->count = last_idx;
			parent->slots[last_idx] = NULL;
			barrier();
			return;
		}
		{
		struct N16 *parent;
		case N16:
			parent = (struct N16 *)parent_;
			last_idx = parent_->count - 1;
			if (parent->key[last_idx] == key) {
				parent_->count--;
				parent->slots[last_idx] = NULL;
				barrier();
				return;
			}
			short cmp = _mm_cmpeq_epi8_mask(_mm_set1_epi8(key), _mm_loadu_si128((__m128i *)parent->key));
			idx = __builtin_ctz(cmp);
			radix_assert(idx < last_idx);
			parent->slots[idx] = NULL;
			barrier();
			parent->key[idx] = parent->key[last_idx];
			barrier();
			parent->slots[idx] = parent->slots[last_idx];
			barrier();
			parent_->count = last_idx;
			parent->slots[last_idx] = NULL;
			barrier();
			return;
		}
		{
		struct N48 *parent;
		case N48:
			parent = (struct N48 *)parent_;
			unsigned char index_idx = key / BITS_PER_INDEX;
			unsigned char index_pos = key % BITS_PER_INDEX;
			idx = parent->key[key];
			last_idx = parent_->count - 1;
			radix_assert(idx != N48_NO_ENT);
			parent->index[index_idx] &= (~(1ULL << (BITS_PER_INDEX - 1 - index_pos)));
			if (idx == last_idx) {
				parent->slots[idx] = NULL;
				parent->key[key] = N48_NO_ENT;
				parent_->count--;
				return;
			}
			
			unsigned char i, last_key = 0;
			unsigned long long bitfield;
			for (i = 0; i < 4; i++) {
				bitfield = _mm512_cmpeq_epi8_mask(_mm512_set1_epi8(last_idx), _mm512_loadu_si512((__m512i *)&parent->key[64 * i]));
				if (bitfield) {
					last_key = (64 * i) + __builtin_ctzll(bitfield);
					break;
				}
			}
			radix_assert(parent->key[last_key] == last_idx);

			parent->slots[idx] = NULL;
			parent->key[key] = N48_NO_ENT;
			barrier();
			parent->slots[idx] = parent->slots[last_idx];
			parent->key[last_key] = idx;
			parent_->count--;
			return;
		}
		{
		struct N256 *parent;
		case N256:
			parent = (struct N256 *)parent_;
			unsigned char index_idx = key / BITS_PER_INDEX;
			unsigned char index_pos = key % BITS_PER_INDEX;
			radix_assert(parent->slots[key] != NULL);
			parent->index[index_idx] &= (~(1ULL << (BITS_PER_INDEX - 1 - index_pos)));
			parent->slots[key] = NULL;
			parent_->count--;
			return;
		}
		default:
			radix_assert(false);
			__builtin_unreachable();
	}
}

static inline void update_child(struct radix_tree_node *parent_, unsigned char key, struct radix_tree_node *new_child) {
	switch (parent_->type) {
		{
		struct N4 *parent;
		case N4:
			parent = (struct N4 *)parent_;
			int i;
			for (i = 0; i < parent->node.count; i++) {
				if (parent->key[i] == key) {
					radix_assert(parent->slots[i] != NULL);
					parent->slots[i] = new_child;
					return;
				}
			}
			radix_assert(false);
			__builtin_unreachable();
		}
		{
		struct N16 *parent;
		case N16:
			parent = (struct N16 *)parent_;
			__m128i cmp = _mm_cmpeq_epi8(_mm_set1_epi8(key), _mm_loadu_si128((__m128i *)parent->key));
			unsigned bitfield = _mm_movemask_epi8(cmp);
			radix_assert((bitfield >> 16) == 0);
			radix_assert(bitfield);
			radix_assert(parent->slots[__builtin_ctz(bitfield)] != NULL);
			radix_assert(__builtin_ctz(bitfield) < parent->node.count);
			parent->slots[__builtin_ctz(bitfield)] = new_child;
			return;
		}
		{
		struct N48 *parent;
		case N48:
			parent = (struct N48 *)parent_;
			radix_assert(parent->key[key] != N48_NO_ENT);
			radix_assert(parent->slots[parent->key[key]] != NULL);
			parent->slots[parent->key[key]] = new_child;
			return;
		}
		{
		struct N256 *parent;
		case N256:
			parent = (struct N256 *)parent_;
			radix_assert(parent->slots[key] != NULL);
			parent->slots[key] = new_child;
			return;
		}
		default:
			radix_assert(false);
			__builtin_unreachable();
	}
}

static inline void init_node(struct radix_tree_node *node, unsigned char level, unsigned char count, unsigned long long offset) {
	node->level = level;
	node->count = count;
	node->offset = offset;
	node->lock_n_obsolete = 0;
}

/* Trim deleted node to make free slots. If there is no free slots, expand node
   and copy all the child. Allways return new node to provide read consistency.
   *** Write Operation, WRITE LOCK SHOULD BE ACQUIRED BY CALLER. */
static inline struct radix_tree_node *radix_node_expand(struct radix_tree_node *node_) {
	switch (node_->type) {
		{
		struct N4 *node;
		struct N16 *new_node;
		case N4:
			node = (struct N4 *)node_;
			int i;
			radix_assert(node->node.count == 4);
			new_node = (struct N16 *)get_node(N16);
			init_node(&new_node->node, node_->level, 4, node_->offset);
			for (i = 0; i < 4; i++) {
				radix_assert(node->slots[i] != NULL);
				new_node->key[i] = node->key[i];
				new_node->slots[i] = node->slots[i];
			}
			return (struct radix_tree_node *)new_node;
		}
		{
		struct N16 *node;
		struct N48 *new_node;
		case N16:
			node = (struct N16 *)node_;
			int i;
			radix_assert(node->node.count == 16);
			new_node = (struct N48 *)get_node(N48);
			unsigned char key;
			init_node(&new_node->node, node_->level, 16, node_->offset);
			memset(new_node->key, N48_NO_ENT, sizeof(new_node->key));
			memset(new_node->index, 0, sizeof(new_node->index));
			for (i = 0; i < 16; i++) {
				radix_assert(node->slots[i] != NULL);
				key = node->key[i];
				new_node->key[key] = i;
				new_node->slots[i] = node->slots[i];
				new_node->index[key / BITS_PER_INDEX] |= (1ULL << (BITS_PER_INDEX - 1 - (key % BITS_PER_INDEX)));
			}
			return (struct radix_tree_node *)new_node;
		}
		{
		struct N48 *node;
		struct N256 *new_node;
		case N48:
			node = (struct N48 *)node_;
			unsigned long long bitfield;
			unsigned char key;
			int i, bit_idx, entry_idx;
			radix_assert(node->node.count == 48);
			new_node = (struct N256 *)get_node(N256);
			init_node(&new_node->node, node_->level, 48, node_->offset);
			memset(new_node->slots, 0, sizeof(new_node->slots));
			memset(new_node->index, 0, sizeof(new_node->index));
			for (i = 0; i < 4; i++) {
				bitfield = node->index[i];
				while (bitfield) {
					bit_idx = __builtin_ctzll(bitfield);
					key = (BITS_PER_INDEX * i) + (BITS_PER_INDEX - 1) - bit_idx;
					entry_idx = node->key[key];
					radix_assert(entry_idx != N48_NO_ENT);
					if (node->slots[entry_idx] != NULL) {
						new_node->slots[key] = node->slots[entry_idx];
						new_node->index[key / BITS_PER_INDEX] |= (1ULL << (BITS_PER_INDEX - 1 - (key % BITS_PER_INDEX)));
					}
					bitfield ^= (1ULL << bit_idx);
				}
			}
			return (struct radix_tree_node *)new_node;
		}
		default:
			radix_assert(false);
			__builtin_unreachable();
	}
}

enum check_prefix_result {PREFIX_PREV, PREFIX_MATCH, PREFIX_NEXT};

static inline enum check_prefix_result check_prefix(unsigned long long cur_index, unsigned long long target_prefix_,
		unsigned char cur_level, unsigned char target_level) {
	unsigned long long cur_prefix = cur_index >> ((RADIX_TREE_HEIGHT + 1 - target_level) * RADIX_TREE_ENTRY_BIT_SIZE);
	unsigned long long target_prefix = INDEX_GE(target_prefix_, 64 - (target_level - cur_level) * 8);

	if (cur_prefix == target_prefix)
		return PREFIX_MATCH;
	else if (cur_prefix > target_prefix)
		return PREFIX_PREV;
	else
		return PREFIX_NEXT;
}

static inline struct radix_tree_node *radix_tree_do_lookup(struct radix_tree_node *root, unsigned long long index) {
	struct radix_tree_node *node = root;
	unsigned long long cur_index = index;
	unsigned char level = 0;

	while (node != NULL) {
		if (is_leaf(node))
			return node;
		if (level != node->level) {
			radix_assert (level < node->level);
			switch (check_prefix(cur_index, node->offset, level, node->level)) {
				case PREFIX_PREV:
					cur_index = INDEX_GE(ULLONG_MAX, 24 + (8 * node->level));
					break;
				case PREFIX_MATCH:
					cur_index = INDEX_GE(cur_index, 24 + (8 * node->level));
					break;
				case PREFIX_NEXT:
					cur_index = 0;
					break;
			}
			level = node->level;
		}
		switch (get_child_range(node, &node, (unsigned char)(cur_index >> ((RADIX_TREE_HEIGHT - level) * RADIX_TREE_ENTRY_BIT_SIZE)), level)) {
			case RET_PREV_NODE:
				level++;
				cur_index = INDEX_GE(ULLONG_MAX, 24 + (8 * level));
				break;
			case RET_NEXT_NODE:
				cur_index = 0;
				level++;
				break;
			case RET_MATCH_NODE:
				level++;
				cur_index = INDEX_GE(cur_index, 24 + (8 * level));
				break;
			default:
				//TODO: If there is empty node, this point is reachable.
				assert(false);
		}
	}

	return NULL;
}

enum lookup_results radix_tree_lookup(radix_tree_root root, unsigned long long index, struct radix_tree_leaf **leaf) {
	unsigned long long ret_index;
	struct radix_tree_leaf *ret_leaf;

	if (index >> (RADIX_TREE_ENTRY_BIT_SIZE * OFFSET_SIZE)) {
		*leaf = NULL;
		return EFAULT_RADIX;
	}
	
	ret_leaf = (struct radix_tree_leaf *)radix_tree_do_lookup(*root, index);
	if (ret_leaf == NULL) {
		*leaf = NULL;
		return ENOEXIST_RADIX;
	}
	else
		radix_assert(ret_leaf->node.type == LEAF_NODE);
	
	ret_index = ret_leaf->node.offset;
	if (ret_index == index) {
		*leaf = ret_leaf;
		return RET_MATCH_NODE;
	}
	else if (ret_index > index) {
		*leaf = ret_leaf;
		return RET_NEXT_NODE;
	}
	else {
		if (ret_index + ret_leaf->length > index) {
			*leaf = ret_leaf;
			return RET_PREV_NODE;
		}
		else {
			if ((*leaf = ret_leaf->next) != NULL)
				return RET_NEXT_NODE;
			else
				return ENOEXIST_RADIX;
		}
	}
}

static inline struct radix_tree_node *alloc_init_leaf(unsigned long long index, unsigned long long length, void *log_addr, int tx_id) {
	struct radix_tree_node *node = get_node(LEAF_NODE);
	struct radix_tree_leaf *leaf = (struct radix_tree_leaf *)node;

	node->level = 5;
	node->offset = index;
	node->lock_n_obsolete = 0;
	leaf->length = length;
	leaf->tx_id = tx_id;
	leaf->log_addr = log_addr;
	leaf->prev = NULL;
	leaf->next = NULL;

	return node;
}

void radix_tree_insert(radix_tree_root root, unsigned long long index, unsigned long long length, void *log_addr, int tx_id) {
	radix_assert((index >> 40) == 0);
	struct radix_tree_node *node, *child_node, *parent_node;
	unsigned char parent_key, node_key, level;
	unsigned long long parent_version, node_version = 0;
	unsigned long long cur_index;
restart:
	parent_node = NULL;
	node = NULL;
	child_node = *root;
	cur_index = index;
	node_key = 0;
	level = 0;

	if (child_node == NULL) {
		node = alloc_init_leaf(index, length, log_addr, tx_id);
		barrier();
		if (__sync_val_compare_and_swap(root, NULL, node) == NULL)
			return;
		else {
			return_node(node);
			goto restart;
		}
	}

	while (true) {
		parent_node = node;
		parent_version = node_version;
		parent_key = node_key;
		node = child_node;
		node_version = get_version(node);

		if (level != node->level) {
			radix_assert(level < node->level);
			unsigned long long node_prefix = node->offset;
			switch (check_prefix(cur_index, node_prefix, level, node->level)) {
				case PREFIX_MATCH:
					level = node->level;
					cur_index = INDEX_GE(cur_index, 24 + (RADIX_TREE_ENTRY_BIT_SIZE * level));
					break;
				default:
					if (lock_version_or_restart(node, &node_version))
						goto restart;
					
					struct radix_tree_node *new_node, *new_leaf;
					unsigned long long cur_prefix = cur_index >> ((RADIX_TREE_HEIGHT + 1 - node->level) * RADIX_TREE_ENTRY_BIT_SIZE);
					unsigned char match_len, level_diff = node->level - level;
					node_prefix = INDEX_GE(node_prefix, BITS_PER_INDEX - (level_diff * 8));
					for (match_len = level_diff - 1; match_len > 0; match_len--) {
						if (cur_prefix >> ((level_diff - match_len) * RADIX_TREE_ENTRY_BIT_SIZE) ==
								node_prefix >> ((level_diff - match_len) * RADIX_TREE_ENTRY_BIT_SIZE)) {
							break;
						}
					}

					new_node = get_node(N4);
					new_node->level = level + match_len;
					new_node->count = 0;
					new_node->offset = index >> ((RADIX_TREE_HEIGHT + 1 - new_node->level) * RADIX_TREE_ENTRY_BIT_SIZE);
					new_node->lock_n_obsolete = 0;
					new_leaf = alloc_init_leaf(index, length, log_addr, tx_id);
					insert_child_force(new_node, (node_prefix >> ((level_diff - 1 - match_len) * RADIX_TREE_ENTRY_BIT_SIZE)) & RADIX_TREE_MAP_MASK, node);
					insert_child_force(new_node, (cur_prefix >> ((level_diff - 1 - match_len) * RADIX_TREE_ENTRY_BIT_SIZE)) & RADIX_TREE_MAP_MASK, new_leaf);
					barrier();

					if (parent_node == NULL) {
						if (__sync_val_compare_and_swap(root, node, new_node) == node) {
							write_unlock(node);
							return;
						}
						else {
							write_unlock(node);
							return_node(new_node);
							return_node(new_leaf);
							goto restart;
						}
					}
					else {
						if (lock_version_or_restart(parent_node, &parent_version)) {
							write_unlock(node);
							return_node(new_node);
							return_node(new_leaf);
							goto restart;
						}
						update_child(parent_node, parent_key, new_node);
						write_unlock(parent_node);
					}
					write_unlock(node);
					return;
			}
		}
		if (is_leaf(node)) {
			// Duplicate key.
			radix_assert(node->offset == index);
			return;
		}

		node_key = cur_index >> ((RADIX_TREE_HEIGHT - level) * RADIX_TREE_ENTRY_BIT_SIZE);
		child_node = get_child(node, node_key, level);

		if (child_node == NULL) {
			if (lock_version_or_restart(node, &node_version))
				goto restart;
			struct radix_tree_node *leaf = alloc_init_leaf(index, length, log_addr, tx_id), *new_node;
			if (insert_child(node, node_key, leaf)) {
				write_unlock(node);
				return;
			}
			new_node = radix_node_expand(node);
			insert_child_force(new_node, node_key, leaf);
			barrier();

			if (parent_node == NULL) {
				if (__sync_val_compare_and_swap(root, node, new_node) == node) {
					write_unlock_obsolete(node);
					return;
				}
				else {
					write_unlock(node);
					return_node(new_node);
					return_node(leaf);
					goto restart;
				}
			}
			else {
				if (lock_version_or_restart(parent_node, &parent_version)) {
					write_unlock(node);
					return_node(new_node);
					return_node(leaf);
					goto restart;
				}
				update_child(parent_node, parent_key, new_node);
				write_unlock(parent_node);
				write_unlock_obsolete(node);
				return_node_to_gc(node);
				return;
			}
		}

		level++;
		cur_index = INDEX_GE(cur_index, 24 + (8 * level));
	}
}

void radix_tree_remove(radix_tree_root root, struct radix_tree_leaf *leaf) {
	struct radix_tree_node *node, *child_node, *parent_node, *leaf_node = (struct radix_tree_node *)leaf;
	unsigned long long parent_version, node_version = 0;
	unsigned char parent_key, node_key, level;
	unsigned long long cur_index;
restart:
	parent_node = NULL;
	node = NULL;
	child_node = *root;
	cur_index = leaf->node.offset;
	node_key = 0;
	level = 0;

	radix_assert(child_node != NULL);
	if (child_node == leaf_node) {
		if (__sync_val_compare_and_swap(root, leaf_node, NULL) == leaf_node) {
			return_node_to_gc(leaf_node);
			return;
		}
		else
			goto restart;
	}
	if (is_leaf(child_node))
		return;

	while (true) {
		parent_node = node;
		parent_version = node_version;
		parent_key = node_key;
		node = child_node;
		node_version = get_version(node);

		if (level != node->level) {
			radix_assert(level < node->level);
			unsigned long long node_prefix = node->offset;
			switch (check_prefix(cur_index, node_prefix, level, node->level)) {
				case PREFIX_MATCH:
					level = node->level;
					cur_index = INDEX_GE(cur_index, 24 + (RADIX_TREE_ENTRY_BIT_SIZE * level));
					break;
				default:
					// This point is reachable only when leaf has already been removed.
					return;
			}
		}
		node_key = cur_index >> ((RADIX_TREE_HEIGHT - level) * RADIX_TREE_ENTRY_BIT_SIZE);
		child_node = get_child(node, node_key, level);

		if (child_node == NULL) {
			if (is_obsolete(node_version) || node_version != get_version(node))
				goto restart;
			// This point is reachable only when leaf has already been removed.
			return;
		}

		if (is_leaf(child_node)) {
			if (child_node != leaf_node) {
				// This point is reachable only when leaf has already been removed.
				return;
			}
			if (lock_version_or_restart(node, &node_version))
				goto restart;
			radix_assert(node->count != 1);
			if (node->count == 2) {
				struct radix_tree_node *remaining_child = get_child_remain(node, node_key);
				if (parent_node == NULL) {
					if (__sync_val_compare_and_swap(root, node, remaining_child) == node) {
						write_unlock_obsolete(node);
						return_node_to_gc(node);
						return_node_to_gc(child_node);
						return;
					}
					else {
						write_unlock(node);
						goto restart;
					}
				}
				else {
					if (lock_version_or_restart(parent_node, &parent_version)) {
						write_unlock(node);
						goto restart;
					}
					update_child(parent_node, parent_key, remaining_child);
					write_unlock(parent_node);
					write_unlock_obsolete(node);
					return_node_to_gc(node);
					return_node_to_gc(child_node);
					return;
				}
			}
			else {
				delete_child(node, node_key);
				write_unlock(node);
				return_node_to_gc(child_node);
				return;
			}
		}
	}
}
