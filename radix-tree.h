#ifdef __cplusplus
extern "C"
{
#endif

#include <stdbool.h>
#include <limits.h>

#define RADIX_TREE_ENTRY_BIT_SIZE 8
#define RADIX_TREE_HEIGHT 4 /* Starts from 0 */
#define RADIX_TREE_MAP_SIZE (1ULL<<RADIX_TREE_ENTRY_BIT_SIZE)
#define RADIX_TREE_INDEX_SIZE (RADIX_TREE_MAP_SIZE / (sizeof(unsigned long long) * 8))
#define RADIX_TREE_MAP_MASK (RADIX_TREE_MAP_SIZE - 1)
#define OFFSET_SIZE (RADIX_TREE_HEIGHT + 1)


#define MOVE_BLOCK_SIZE (1UL<<12)

enum lookup_results {
	RET_MATCH_NODE, /* Node with requested offset found. */
	RET_PREV_NODE, /* Node offset is smaller than request but the node contains requested offset*/
	RET_NEXT_NODE, /* Look up node does not exist, return next. */
	ENOEXIST_RADIX, /* Look up node does not exist. */
	EFAULT_RADIX /* Offset out of 40 bits boundary. */
};
enum node_types {LEAF_NODE, N4, N16, N48, N256};

#define N48_NO_ENT (50)
#define BITS_PER_INDEX (sizeof(unsigned long long) * 8)
#define INDEX_LE(INDEX, POS) (((INDEX) >> ((BITS_PER_INDEX - 1) - (POS)) << ((BITS_PER_INDEX - 1) - (POS))))
#define INDEX_GE(INDEX, POS) (((INDEX) << (POS)) >> (POS))

struct radix_tree_node {
	unsigned char type;
	unsigned char level;
	unsigned char count;
	unsigned long long offset;
	unsigned long long lock_n_obsolete;
};

struct radix_tree_leaf {
	struct radix_tree_node node;
	unsigned long long length;
	int tx_id;
	void *log_addr;
	struct radix_tree_leaf *prev;
	struct radix_tree_leaf *next;
};

struct N4 {
	struct radix_tree_node node;
	unsigned char key[4];
	void* slots[4];
};

struct N16 {
	struct radix_tree_node node;
	unsigned char key[16];
	void* slots[16];
};

struct N48 {
	struct radix_tree_node node;
	unsigned char key[256];
	void* slots[48];
	unsigned long long index[RADIX_TREE_INDEX_SIZE];
};

struct N256 {
	struct radix_tree_node node;
	void* slots[256];
	unsigned long long index[RADIX_TREE_INDEX_SIZE];
};

typedef struct radix_tree_node **radix_tree_root;

struct radix_tree_node_list {
	struct radix_tree_node *node;
};

struct radix_tree_leaf_list {
	struct radix_tree_leaf *leaf;
};

int radix_tree_init();
void radix_tree_destroy(radix_tree_root root);
struct radix_tree_node *radix_tree_create(void);
enum lookup_results radix_tree_lookup(radix_tree_root root, unsigned long long index, struct radix_tree_leaf **leaf);
void radix_tree_insert(radix_tree_root root, unsigned long long index, unsigned long long length, void *log_addr, int tx_id);
void radix_tree_remove(radix_tree_root root, struct radix_tree_leaf *leaf);

#ifdef __cplusplus
}
#endif