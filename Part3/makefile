main: main.c input.c func.c hash.c queries.c intermediate.c parser.c statistics.c job.c
	gcc  -o main main.c input.c func.c hash.c queries.c intermediate.c parser.c statistics.c job.c -I. -pthread -g -lm 

all: run

run: main
	time ./main workloads/small/small.init workloads/small/small.work

.PHONY: all run

.PHONY: clean

clean:
	rm -f main