#CFLAGS = -std=c11
#CCFLAGS = -std=c11
all: canary

canary: CFLAGS +=-g -O2 -std=gnu99 -Wall -llustreapi
canary: CCFLAGS += -g -O2 -std=gnu99 -Wall -llustreapi

canary: main.o simple_server/cJSON.o simple_server/client.o
	$(COMPILE.c) -g -O2 -std=gnu99 -Wall main.c -llustreapi
	$(LINK.c) -o canary main.o simple_server/client.o simple_server/cJSON.o

simple_server/%.o:
    cd simple_server && make %.o

clean:
	rm -f core canary *.o
	cd simple_server && $(MAKE) clean
