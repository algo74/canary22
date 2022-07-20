all: canary

canary: main.c
	gcc -g -O2 -std=gnu99 -Wall -o canary main.c -llustreapi
clean:
	rm -f core canary *.o
