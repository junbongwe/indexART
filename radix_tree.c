#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdatomic.h>
#include <immintrin.h>

#include "radix_tree.h"

// Mutex implementation for Radix tree root
#define ROOT_LOCK_BIT (1ULL << 63)
#define get_root_node(ROOT) \
	((typeof((ROOT)->root_node))(((unsigned long long)atomic_load(&(ROOT)->root_node)) & (ROOT_LOCK_BIT - 1)))
#define root_write_unlock(ROOT, NEW_ROOT_NODE) (atomic_store(&(ROOT)->root_node, (NEW_ROOT_NODE)))
#define root_write_lock_or_restart(ROOT, ROOT_NODE) \
	(__sync_val_compare_and_swap(&(ROOT)->root_node, (ROOT_NODE), \
				     (typeof((ROOT)->root_node))(((unsigned long long)(ROOT_NODE)) | ROOT_LOCK_BIT)) != (ROOT_NODE))
#define ROOT_END_OFS (1ULL << 40)

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
#define test_leaf_range_or_restart(PREV, NEXT, IDX) \
	((((PREV)->node.offset != ROOT_END_OFS) && ((PREV)->node.offset >= (IDX))) || ((NEXT)->node.offset <= (IDX)))

int radix_tree_init() {
	build_node(10000, LEAF_NODE);
	build_node(10000, N4);
	build_node(10000, N16);
	build_node(10000, N48);
	build_node(10000, N256);
	return 0;
}

void radix_tree_destroy(struct radix_tree_root *root) {
	return;
}

void radix_tree_create(struct radix_tree_root *root) {
	memset(root, 0x0, sizeof(*root));
	root->head.node.offset = ROOT_END_OFS;
	root->tail.node.offset = ROOT_END_OFS;
	root->head.next = &root->tail;
	root->tail.prev = &root->head;
	pthread_mutex_init(&root->head.lock, NULL);
	pthread_mutex_init(&root->tail.lock, NULL);
}

static inline void radix_tree_do_insert(struct radix_tree_root *root, unsigned long long index, unsigned long long length, void *log_addr, int tx_id, bool lock_leaf_);
static inline void radix_tree_do_remove(struct radix_tree_root *root, struct radix_tree_leaf *leaf, bool lock_leaf);

/* Get child with KEY from PARENT_ node and store child node to NODEP. Parent LEVEL should be given to check child key again.
   If there is child with KEY, return that child. If there is no child with KEY, look for closest previous node and return if
   exist. If there is no previous node, look for closest next node and return. */
static inline enum radix_tree_lookup_results get_child_range(struct radix_tree_node *parent_, struct radix_tree_node **nodep, unsigned char key, unsigned char level) {
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
			ret_key = 0;
			for (i = count - 1; i >= 0; i--) {
				i_key = parent->key[i];
				barrier();
				i_diff = (int)i_key - (int)key;
				if (i_diff == 0) {
					if ((i_node = parent->slots[i]) != NULL) {
						if (is_fault_node(i_node, key, level))
							goto n4_begin;
						*nodep = i_node;
						return RET_MATCH_NODE;
					}
				}
				else if (i_diff < 0) {
					if ((ret_diff > 0) || (ret_diff < i_diff)) {
						if ((i_node = parent->slots[i]) != NULL) {
							ret_diff = i_diff;
							ret_node = i_node;
							ret_key = i_key;
						}
					}
				}
				else {
					if ((ret_diff > 0) && (ret_diff > i_diff)) {
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
			barrier();
			ret_diff = INT_MAX;
			ret_node = NULL;
			ret_key = 0;
			for (i = count - 1; i >= 0; i--) {
				i_key = parent->key[i];
				barrier();
				i_diff = (int)i_key - (int)key;
				if (i_diff == 0) {
					if ((i_node = parent->slots[i]) != NULL) {
						if (is_fault_node(i_node, key, level))
							goto n16_begin;
						*nodep = i_node;
						return RET_MATCH_NODE;
					}
				}
				else if (i_diff < 0) {
					if ((ret_diff > 0) || (ret_diff < i_diff)) {
						if ((i_node = parent->slots[i]) != NULL) {
							ret_diff = i_diff;
							ret_node = i_node;
							ret_key = i_key;
						}
					}
				}
				else {
					if ((ret_diff > 0) && (ret_diff > i_diff)) {
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
			radix_unreachable();
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
	radix_unreachable();
}

/* Get ramaining child in PARENT_ node whose key is not equal to KEY.
   This function should be called after acquiring PARENT_ lock. */
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
			for (index_idx = 0; index_idx < RADIX_TREE_INDEX_SIZE; index_idx++) {
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
			radix_unreachable();
		}
		{
		struct N256 *parent;
		case N256:
			parent = (struct N256 *)parent_;
			for (index_idx = 0; index_idx < RADIX_TREE_INDEX_SIZE; index_idx++) {
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
			radix_unreachable();
		}
		default:
			radix_unreachable();
	}
}

/* Get child with exact KEY from PARENT_ node. Parent LEVEL should be given to check child key again. */
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
			unsigned short bitfield = _mm_cmpeq_epi8_mask(_mm_set1_epi8(key), _mm_loadu_si128((__m128i *)parent->key)) & ((1 << count) - 1);
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
			radix_unreachable();
	}
}

/* Insert CHILD node to PARENT_ node with KEY. Return true for success, false for failure(need to expand node).
   WRITE OPERATION, PARENT_ lock should be acquired by caller. */
static inline bool insert_child(struct radix_tree_node *parent_, unsigned char key, struct radix_tree_node *child) {
	int idx;
	unsigned char index_idx;
	unsigned long long bitfield, bitmask;
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
			parent_->count = idx + 1;
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
			parent_->count = idx + 1;
			return true;
		}
		{
		struct N48 *parent;
		case N48:
			parent = (struct N48 *)parent_;
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
			parent_->count = idx + 1;
			return true;
		}
		{
		struct N256 *parent;
		case N256:
			parent = (struct N256 *)parent_;
			index_idx = key / BITS_PER_INDEX;
			bitfield = parent->index[index_idx];
			bitmask = (1ULL << (BITS_PER_INDEX - 1 - (key % BITS_PER_INDEX)));
			radix_assert(parent->slots[key] == NULL);
			radix_assert((bitfield & bitmask) == 0);
			parent->slots[key] = child;
			barrier();
			parent->index[index_idx] = (bitfield | bitmask);
			parent_->count++;
			return true;
		}
		default:
			radix_unreachable();
	}
}

/* Insert CHILD node to PARENT_ node with KEY. This function must succeed. 
   No internal synchronization, PARENT_ should not be inserted to tree yet. */
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
			radix_unreachable();
	}
}

/* Delete child with KEY from PARENT_ node. This function must succeed. */
static inline void delete_child(struct radix_tree_node *parent_, unsigned char key) {
	unsigned char idx, last_idx, index_idx, index_pos;
	switch (parent_->type) {
		{
		struct N4 *parent;
		case N4:
			parent = (struct N4 *)parent_;
			last_idx = parent_->count - 1;
			if (parent->key[last_idx] == key) {
				parent_->count--;
				barrier();
				parent->slots[last_idx] = NULL;
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
				barrier();
				parent->slots[last_idx] = NULL;
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
			idx = parent->key[key];
			last_idx = parent_->count - 1;
			index_idx = key / BITS_PER_INDEX;
			index_pos = key % BITS_PER_INDEX;
			radix_assert(idx != N48_NO_ENT);
			parent->index[index_idx] &= (~(1ULL << (BITS_PER_INDEX - 1 - index_pos)));
			barrier();
			if (idx == last_idx) {
				parent->slots[idx] = NULL;
				parent->key[key] = N48_NO_ENT;
				parent_->count--;
				return;
			}
			
			unsigned char i, last_key = N48_NO_ENT;
			unsigned long long bitfield;
			for (i = 0; i < RADIX_TREE_INDEX_SIZE; i++) {
				bitfield = _mm512_cmpeq_epi8_mask(_mm512_set1_epi8(last_idx), _mm512_loadu_si512((__m512i *)&parent->key[64 * i]));
				if (bitfield) {
					last_key = (64 * i) + __builtin_ctzll(bitfield);
					break;
				}
			}
			radix_assert(last_idx != N48_NO_ENT);
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
			index_idx = key / BITS_PER_INDEX;
			index_pos = key % BITS_PER_INDEX;
			radix_assert(parent->slots[key] != NULL);
			parent->index[index_idx] &= (~(1ULL << (BITS_PER_INDEX - 1 - index_pos)));
			barrier();
			parent->slots[key] = NULL;
			parent_->count--;
			return;
		}
		default:
			radix_unreachable();
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
			radix_unreachable();
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
			radix_unreachable();
	}
}

/* Init NODE with given LEVEL, COUNT, OFFSET. */
static inline void init_node(struct radix_tree_node *node, unsigned char level, unsigned char count, unsigned long long offset) {
	node->level = level;
	node->count = count;
	node->offset = offset;
	node->lock_n_obsolete = 0;
}

static inline bool radix_node_need_expand(struct radix_tree_node *node) {
	switch (node->type) {
		case N4:
			return (node->count == 4);
		case N16:
			return (node->count == 16);
		case N48:
			return (node->count == 48);
		default:
			return false;
	}
}

/* Expand NODE_ to larger node type. Allocate new node, copy all the child and return the new node.
   WRITE OPERATION, NODE_ lock should be acquired by caller. */
static inline struct radix_tree_node *radix_node_expand(struct radix_tree_node *node_) {
	int i;
	unsigned char key;
	switch (node_->type) {
		{
		struct N4 *node;
		struct N16 *new_node;
		case N4:
			node = (struct N4 *)node_;
			new_node = (struct N16 *)get_node(N16);
			radix_assert(node->node.count == 4);
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
			new_node = (struct N48 *)get_node(N48);
			radix_assert(node->node.count == 16);
			init_node(&new_node->node, node_->level, 16, node_->offset);
			memset(new_node->key, N48_NO_ENT, sizeof(new_node->key));
			memset(new_node->index, 0, sizeof(new_node->index));
			memset(new_node->slots, 0, sizeof(new_node->slots));
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
			new_node = (struct N256 *)get_node(N256);
			radix_assert(node->node.count == 48);
			init_node(&new_node->node, node_->level, 48, node_->offset);
			memset(new_node->slots, 0, sizeof(new_node->slots));
			memset(new_node->index, 0, sizeof(new_node->index));
			for (i = 0; i < 4; i++) {
				unsigned long long bitfield = node->index[i];
				int bit_idx, entry_idx;
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
			radix_unreachable();
	}
}

/* Check prefix between CUR_INDEX and TARGET_PREFIX_ with CUR_LEVEL and TARGET_LEVEL.
   If prefix match, return PREFIX_MATCH. Otherwise, return PREFIX_PREV or PREFIX_NEXT accordingly. */
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

/* Do lookup with given ROOT and INDEX. */
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

/* Lookup operation entry point. Find leaf with INDEX from ROOT and store the leaf to LEAF. */
enum radix_tree_lookup_results radix_tree_lookup(struct radix_tree_root *root, unsigned long long index, struct radix_tree_leaf **leaf) {
	unsigned long long ret_index;
	struct radix_tree_leaf *ret_leaf;

	if (index >> (RADIX_TREE_ENTRY_BIT_SIZE * OFFSET_SIZE)) {
		*leaf = NULL;
		return EFAULT_RADIX;
	}
	
	ret_leaf = (struct radix_tree_leaf *)radix_tree_do_lookup(get_root_node(root), index);
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

/* Allocate new leaf and initialize with given INDEX, LENGTH, LOG_ADDR, TX_ID, and return the new leaf. */
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
	pthread_mutex_init(&leaf->lock, NULL);
	pthread_mutex_lock(&leaf->lock);

	return node;
}

static inline struct radix_tree_leaf *get_left_most_leaf(struct radix_tree_node *start) {
	struct radix_tree_node *node = start;
	while (!is_leaf(node)) {
		get_child_range(node, &node, 0, node->level);
	}
	return (struct radix_tree_leaf *)node;
}

static inline struct radix_tree_leaf *get_right_most_leaf(struct radix_tree_node *start) {
	struct radix_tree_node *node = start;
	while (!is_leaf(node)) {
		get_child_range(node, &node, 0xFF, node->level);
	}
	return (struct radix_tree_leaf *)node;
}

static inline void link_and_remove_leaf(struct radix_tree_root *root, unsigned long long index, unsigned long long length, struct radix_tree_leaf *new_leaf, struct radix_tree_leaf *prev_leaf, struct radix_tree_leaf *next_leaf) {
	struct radix_tree_leaf *leaf = new_leaf->next, *next = NULL;
	unsigned long long end = index + length;

	radix_assert(prev_leaf && next_leaf);

	if (prev_leaf->node.offset != ROOT_END_OFS) {
		unsigned long long prev_end = prev_leaf->node.offset + prev_leaf->length;
		if (prev_end > end) {
			next_leaf->prev = new_leaf;
			prev_leaf->length = index - prev_leaf->node.offset;
			radix_tree_do_insert(root, end, prev_end - end, prev_leaf->log_addr + (end - prev_leaf->node.offset), prev_leaf->tx_id, false);
			return;
		}
		if ((prev_leaf->node.offset + prev_leaf->length) > index)
			prev_leaf->length = index - prev_leaf->node.offset;
	}

	while (true) {
		if (leaf == NULL)
			return;
		if (leaf->node.offset + leaf->length <= end) {
			next = leaf->next;
			radix_tree_do_remove(root, leaf, false);
			pthread_mutex_unlock(&leaf->lock);
			leaf = next;
		}
		else if (leaf->node.offset < end) {
			radix_tree_do_insert(root, end, leaf->length + leaf->node.offset - end, leaf->log_addr + end - leaf->node.offset, leaf->tx_id, false);
			radix_tree_do_remove(root, leaf, false);
			pthread_mutex_unlock(&leaf->lock);
			return;
		}
		else
			return;
	}
}


/* Unlock leaf sequentially. */
static inline void unlock_leaf_seq(struct radix_tree_leaf *begin, unsigned long long end) {
	struct radix_tree_leaf *cur = begin, *next;

	radix_assert(cur != NULL);
	do {
		next = cur->next;
		barrier();
		pthread_mutex_unlock(&cur->lock);
		cur = next;
	} while ((cur->node.offset + cur->length) != end);
	pthread_mutex_unlock(&cur->lock);
}

/* Lock leaf sequentially. Either PREV_LEAF or NEXT_LEAF should be non-NULL.
 * We can check transaction conflict at this point. */
static inline unsigned long long lock_leaf_seq_or_restart(struct radix_tree_leaf *prev_leaf, struct radix_tree_leaf *next_leaf, unsigned long long index, unsigned long long length) {
	struct radix_tree_leaf *cur = next_leaf;
	unsigned long long end = index + length;

	radix_assert(prev_leaf && next_leaf);

	pthread_mutex_lock(&prev_leaf->lock);
	if ((prev_leaf->next != next_leaf) || (next_leaf->prev != prev_leaf)) {
		radix_assert((prev_leaf->node.offset == ROOT_END_OFS) || (prev_leaf->node.offset < index));
		pthread_mutex_unlock(&prev_leaf->lock);
		return ULLONG_MAX;
	}

	pthread_mutex_lock(&next_leaf->lock);
	radix_assert((next_leaf->prev == prev_leaf) && (next_leaf->node.offset >= index));

	while (true) {
		if (cur->node.offset >= end)
			return cur->node.offset + cur->length;
		cur = cur->next;
		pthread_mutex_lock(&cur->lock);
	}
}

/* Insert operation entry point. Insert leaf with INDEX to ROOT. Initialize leaf with given INDEX, LENGTH, LOG_ADDR, TX_ID. */
void radix_tree_insert(struct radix_tree_root *root, unsigned long long index, unsigned long long length, void *log_addr, int tx_id) {
	radix_tree_do_insert(root, index, length, log_addr, tx_id, true);
}

static inline void radix_tree_do_insert(struct radix_tree_root *root, unsigned long long index, unsigned long long length, void *log_addr, int tx_id, bool lock_leaf_) {
	radix_assert((index >> 40) == 0);
	struct radix_tree_node *node, *child_node, *parent_node, *new_node, *new_leaf;
	struct radix_tree_leaf *prev_leaf, *next_leaf, *new_leaf_;
	unsigned char parent_key, node_key, level;
	unsigned long long parent_version, node_version = 0;
	unsigned long long cur_index, lock_end_idx;
	bool lock_leaf = lock_leaf_, unlock_leaf = false;

	new_leaf_ = (struct radix_tree_leaf *)(new_leaf = alloc_init_leaf(index, length, log_addr, tx_id));
restart:
	parent_node = NULL;
	node = NULL;
	child_node = get_root_node(root);
	cur_index = index;
	node_key = 0;
	level = 0;

	if (child_node == NULL) {
		new_leaf_->prev = &root->head;
		new_leaf_->next = &root->tail;
		if (lock_leaf) {
			pthread_mutex_lock(&root->head.lock);
			if (root->head.next != &root->tail) {
				pthread_mutex_unlock(&root->head.lock);
				goto restart;
			}
			pthread_mutex_lock(&root->tail.lock);
			radix_assert(root->tail.prev == &root->head);
			lock_leaf = false;
			unlock_leaf = true;
		}
		barrier();
		root->head.next = new_leaf_;
		root->tail.prev = new_leaf_;
		if (__sync_val_compare_and_swap(&root->root_node, NULL, new_leaf) == NULL) {
			if (unlock_leaf) {
				pthread_mutex_unlock(&root->head.lock);
				pthread_mutex_unlock(&new_leaf_->lock);
				pthread_mutex_unlock(&root->tail.lock);
			}
			return;
		}
		else
			assert(false);
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
			enum check_prefix_result result;
			switch (result = check_prefix(cur_index, node_prefix, level, node->level)) {
				case PREFIX_MATCH:
					level = node->level;
					cur_index = INDEX_GE(cur_index, 24 + (RADIX_TREE_ENTRY_BIT_SIZE * level));
					break;
				default:
					if (result == PREFIX_PREV) {
						prev_leaf = get_right_most_leaf(node);
						if (prev_leaf == NULL)
							goto restart;
						next_leaf = prev_leaf->next;
					}
					else {
						next_leaf = get_left_most_leaf(node);
						if (next_leaf == NULL)
							goto restart;
						prev_leaf = next_leaf->prev;
					}
					if (test_leaf_range_or_restart(prev_leaf, next_leaf, index))
						goto restart;

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
					insert_child_force(new_node, (node_prefix >> ((level_diff - 1 - match_len) * RADIX_TREE_ENTRY_BIT_SIZE)) & RADIX_TREE_MAP_MASK, node);
					insert_child_force(new_node, (cur_prefix >> ((level_diff - 1 - match_len) * RADIX_TREE_ENTRY_BIT_SIZE)) & RADIX_TREE_MAP_MASK, new_leaf);

					barrier();

					if (lock_leaf) {
						if ((lock_end_idx = lock_leaf_seq_or_restart(prev_leaf, next_leaf, index, length)) == ULLONG_MAX) {
							return_node(new_node);
							goto restart;
						}
						lock_leaf = false;
						unlock_leaf = true;
					}

					if (lock_version_or_restart(node, &node_version)) {
						return_node(new_node);
						goto restart;
					}

					if (parent_node == NULL) {
						if (root_write_lock_or_restart(root, node)) {
							write_unlock(node);
							return_node(new_node);
							goto restart;
						}
					}
					else {
						if (lock_version_or_restart(parent_node, &parent_version)) {
							write_unlock(node);
							return_node(new_node);
							goto restart;
						}
					}

					new_leaf_->prev = prev_leaf;
					new_leaf_->next = next_leaf;
					barrier();
					prev_leaf->next = new_leaf_;
					next_leaf->prev = new_leaf_;

					if (parent_node == NULL)
						root_write_unlock(root, new_node);
					else {
						update_child(parent_node, parent_key, new_node);
						write_unlock(parent_node);
					}
					write_unlock(node);
					link_and_remove_leaf(root, index, length, new_leaf_, prev_leaf, next_leaf);
					if (unlock_leaf)
						unlock_leaf_seq(prev_leaf, lock_end_idx);
					return;
			}
		}
		if (is_leaf(node)) {
			// Overwrite
			radix_assert(node->offset == index);

			next_leaf = (struct radix_tree_leaf *)node;
			prev_leaf = next_leaf->prev;
			if (lock_leaf) {
				if ((lock_end_idx = lock_leaf_seq_or_restart(prev_leaf, next_leaf, index, length)) == ULLONG_MAX)
					goto restart;
				lock_leaf = false;
				unlock_leaf = true;
			}

			if (parent_node == NULL) {
				if (root_write_lock_or_restart(root, node))
					goto restart;
			}
			else {
				if (lock_version_or_restart(parent_node, &parent_version))
					goto restart;
			}

			new_leaf_->prev = prev_leaf;
			new_leaf_->next = next_leaf;
			barrier();
			prev_leaf->next = new_leaf_;
			next_leaf->prev = new_leaf_;
			barrier();
			if (parent_node == NULL)
				root_write_unlock(root, new_leaf);
			else {
				update_child(parent_node, parent_key, new_leaf);
				write_unlock(parent_node);
			}
			barrier();
			new_leaf_->next = next_leaf->next;
			next_leaf->next->prev = new_leaf_;
			barrier();
			if (unlock_leaf)
				pthread_mutex_unlock(&next_leaf->lock);
			return_node_to_gc(node);
			next_leaf = new_leaf_->next;

			link_and_remove_leaf(root, index, length, new_leaf_, prev_leaf, next_leaf);
			if (unlock_leaf)
				unlock_leaf_seq(prev_leaf, lock_end_idx);
			return;

		}

		node_key = cur_index >> ((RADIX_TREE_HEIGHT - level) * RADIX_TREE_ENTRY_BIT_SIZE);
		child_node = get_child(node, node_key, level);

		if (child_node == NULL) {
			switch (get_child_range(node, &child_node, node_key, level)) {
				case RET_PREV_NODE:
					prev_leaf = get_right_most_leaf(child_node);
					if (prev_leaf == NULL)
						goto restart;
					next_leaf = prev_leaf->next;
					break;
				case RET_NEXT_NODE:
					next_leaf = get_left_most_leaf(child_node);
					if (next_leaf == NULL)
						goto restart;
					prev_leaf = next_leaf->prev;
					break;
				default:
					goto restart;
			}
			if (test_leaf_range_or_restart(prev_leaf, next_leaf, index))
				goto restart;
			bool need_expand = radix_node_need_expand(node);

			if (lock_leaf) {
				if ((lock_end_idx = lock_leaf_seq_or_restart(prev_leaf, next_leaf, index, length)) == ULLONG_MAX)
					goto restart;
				lock_leaf = false;
				unlock_leaf = true;
			}

			if (lock_version_or_restart(node, &node_version))
				goto restart;

			if (need_expand) {
				if (parent_node == NULL) {
					if (root_write_lock_or_restart(root, node)) {
						write_unlock(node);
						goto restart;
					}
				}
				else {
					if (lock_version_or_restart(parent_node, &parent_version)) {
						write_unlock(node);
						goto restart;
					}
				}
			}

			new_leaf_->prev = prev_leaf;
			new_leaf_->next = next_leaf;
			barrier();
			prev_leaf->next = new_leaf_;
			next_leaf->prev = new_leaf_;

			if (insert_child(node, node_key, new_leaf)) {
				radix_assert(!need_expand);
				write_unlock(node);
				link_and_remove_leaf(root, index, length, new_leaf_, prev_leaf, next_leaf);
				if (unlock_leaf)
					unlock_leaf_seq(prev_leaf, lock_end_idx);
				return;
			}

			radix_assert(need_expand);
			new_node = radix_node_expand(node);
			insert_child_force(new_node, node_key, new_leaf);
			barrier();

			if (parent_node == NULL)
				root_write_unlock(root, new_node);
			else {
				update_child(parent_node, parent_key, new_node);
				write_unlock(parent_node);
			}
			write_unlock_obsolete(node);
			return_node_to_gc(node);
			link_and_remove_leaf(root, index, length, new_leaf_, prev_leaf, next_leaf);
			if (unlock_leaf)
				unlock_leaf_seq(prev_leaf, lock_end_idx);
			return;
		}

		level++;
		cur_index = INDEX_GE(cur_index, 24 + (8 * level));
	}
}

static inline void remove_leaf_unlock(struct radix_tree_leaf *leaf, struct radix_tree_leaf *prev, struct radix_tree_leaf *next) {
	pthread_mutex_unlock(&prev->lock);
	pthread_mutex_unlock(&leaf->lock);
	pthread_mutex_unlock(&next->lock);
}

/* Remove operation entry point. Remove LEAF from ROOT. */
void radix_tree_remove(struct radix_tree_root *root, struct radix_tree_leaf *leaf) {
	radix_tree_do_remove(root, leaf, true);
}

static inline void radix_tree_do_remove(struct radix_tree_root *root, struct radix_tree_leaf *leaf, bool lock_leaf) {
	struct radix_tree_node *node, *child_node, *parent_node, *leaf_node = (struct radix_tree_node *)leaf;
	struct radix_tree_leaf *prev_leaf = leaf->prev, *next_leaf = leaf->next;
	unsigned long long parent_version, node_version = 0;
	unsigned char parent_key, node_key, level;
	unsigned long long cur_index;
	bool unlock_leaf = false;

	if (lock_leaf) {
lock_restart:
		pthread_mutex_lock(&prev_leaf->lock);
		if ((prev_leaf->next != leaf) || (leaf->prev != prev_leaf)) {
			pthread_mutex_unlock(&prev_leaf->lock);
			prev_leaf = leaf->prev;
			goto lock_restart;
		}
		pthread_mutex_lock(&leaf->lock);
		next_leaf = leaf->next;
		pthread_mutex_lock(&next_leaf->lock);
		unlock_leaf = true;
	}
restart:
	parent_node = NULL;
	node = NULL;
	child_node = get_root_node(root);
	cur_index = leaf->node.offset;
	node_key = 0;
	level = 0;

	radix_assert(child_node != NULL);
	if (child_node == leaf_node) {
		root->head.next = &root->tail;
		root->tail.prev = &root->head;
		if (__sync_val_compare_and_swap(&root->root_node, leaf_node, NULL) == leaf_node) {
			if (unlock_leaf)
				remove_leaf_unlock(prev_leaf, leaf, next_leaf);
			return_node_to_gc(leaf_node);
			return;
		}
		else
			goto restart;
	}
	if (is_leaf(child_node)) {
		// This point is reachable only when leaf has already been removed.
		if (unlock_leaf)
			remove_leaf_unlock(prev_leaf, leaf, next_leaf);
		return;
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
					// This point is reachable only when leaf has already been removed.
					if (unlock_leaf)
						remove_leaf_unlock(prev_leaf, leaf, next_leaf);
					return;
			}
		}
		node_key = cur_index >> ((RADIX_TREE_HEIGHT - level) * RADIX_TREE_ENTRY_BIT_SIZE);
		child_node = get_child(node, node_key, level);

		if (child_node == NULL) {
			if (is_obsolete(node_version) || node_version != get_version(node))
				goto restart;
			// This point is reachable only when leaf has already been removed.
			if (unlock_leaf)
				remove_leaf_unlock(prev_leaf, leaf, next_leaf);
			return;
		}

		if (is_leaf(child_node)) {
			if (child_node != leaf_node) {
				// This point is reachable only when leaf has already been removed.
				radix_assert(child_node->offset == leaf->node.offset);
				if (unlock_leaf)
					remove_leaf_unlock(prev_leaf, leaf, next_leaf);
				return;
			}
			if (lock_version_or_restart(node, &node_version))
				goto restart;
			radix_assert(node->count != 1);

			if (node->count == 2) {
				struct radix_tree_node *remaining_child = get_child_remain(node, node_key);
				if (parent_node == NULL) {
					if (__sync_val_compare_and_swap(&root->root_node, node, remaining_child) != node) {
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
				}
				write_unlock_obsolete(node);
				return_node_to_gc(node);
			}
			else {
				delete_child(node, node_key);
				write_unlock(node);
			}
			// Change link between leaves.
			prev_leaf->next = next_leaf;
			next_leaf->prev = prev_leaf;

			if (unlock_leaf)
				remove_leaf_unlock(prev_leaf, leaf, next_leaf);
			return_node_to_gc(child_node);
			return;
		}
	}
}

