CFLAGS = -Wall -march=native -O3
CFLAGS += -g -DRADIX_DEBUG

all: radix_tree node_allocator test_isolated test_mixed test_remove test_overlap

radix_tree:
	gcc -c radix_tree.c $(CFLAGS)

node_allocator:
	gcc -c node_allocator.c $(CFLAGS)

test_isolated:
	gcc test_isolated.c radix_tree.o node_allocator.o -o isolated -lpthread $(CFLAGS)

test_mixed:
	gcc test_mixed.c radix_tree.o node_allocator.o -o mixed -lpthread $(CFLAGS)

test_remove:
	gcc test_remove.c radix_tree.o node_allocator.o -o remove -lpthread $(CFLAGS)

test_overlap:
	gcc test_overlap.c radix_tree.o node_allocator.o -o overlap -lpthread $(CFLAGS)

clean:
	rm -rf isolated mixed remove overlap radix_tree.o node_allocator.o *.out
