CC     ?= gcc
CFLAGS ?= -O2 -g -Wall -Wextra -std=gnu11 -pthread

all: blast_sender blast_receiver

blast_sender: sender.c garbler.c common.h garbler.h
	$(CC) $(CFLAGS) -o $@ sender.c garbler.c

blast_receiver: receiver.c garbler.c common.h garbler.h
	$(CC) $(CFLAGS) -o $@ receiver.c garbler.c

test: all
	./run_tests.sh

experiments: all
	./run_experiments.sh

clean:
	rm -f blast_sender blast_receiver

.PHONY: all test experiments clean
