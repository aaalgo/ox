CC = g++
CXXFLAGS += -std=c++1y -g
LDFLAGS += -std=c++1y
LDLIBS += -lglog -lgflags -lboost_system -lboost_thread -lpthread -lrt
COMMON = ox.o

.PHONY:	all clean

all:	test

test:	test.o $(COMMON)

clean:
	rm *.o
