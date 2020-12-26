CFLAGS = -Wall -march=native -O3
CFLAGS += -g -DNVTX_DEBUG

all: radix_tree test_isolated test_mixed test_remove

radix_tree:
	gcc -c radix-tree.c $(CFLAGS)

test_isolated:
	gcc test_isolated.c radix-tree.c -o isolated -lpthread $(CFLAGS)

test_mixed:
	gcc test_mixed.c radix-tree.c -o mixed -lpthread $(CFLAGS)

test_remove:
	gcc test_remove.c radix-tree.c -o remove -lpthread $(CFLAGS)

clean:
	rm -rf isolated mixed radix-tree.o *.txt
