.PHONY: build

build:
	gcc -Wall -dynamic -fno-common -std=gnu99 -c -o rlist.o rlist.c
	ld -o rlist.so rlist.o -bundle -undefined dynamic_lookup -lc
