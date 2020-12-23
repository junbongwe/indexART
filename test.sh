#!/bin/bash

for i in {1..5000}
do
	./isolated 1000000 >> isolated.txt
	./mixed 1000000 >> mixed.txt
done
