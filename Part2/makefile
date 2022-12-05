main: main.c input.c func.c hash.c queries.c intermediate.c parser.c
	gcc  -o main main.c input.c func.c hash.c queries.c intermediate.c parser.c -I.  -g -lm

all: run

run: main
	time ./main

.PHONY: all run