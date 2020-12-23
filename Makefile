all: radix_tree test_isolated test_mixed

radix_tree:
	gcc -c radix-tree.c -Wall -march=native -O3 

test_isolated:
	gcc test_isolated.c radix-tree.c -o isolated -lpthread -Wall -march=native -O3

test_mixed:
	gcc test_mixed.c radix-tree.c -o mixed -lpthread -Wall -march=native -O3

clean:
	rm -rf isolated mixed radix-tree.o
