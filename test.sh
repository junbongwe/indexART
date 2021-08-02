#!/bin/bash

for i in {1..5000}
do
	./isolated 1000000 >> isolated.out
	./mixed 1000000 >> mixed.out
	./remove 10000000 100000000 >> remove.out
	./overlap 10000000 >> overlap.out
done
