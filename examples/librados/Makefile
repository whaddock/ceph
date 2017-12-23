CXXFLAGS=-std=c++0x -I../../src/include
CFLAGS=-I../../src/include
LDFLAGS=-lrados -lradosstriper
all: librados_hello_world librados_hello_world_c my_radosstriper

librados_hello_world: hello_world.cc
	g++ -g -c hello_world.cc -o hello_world.o $(CXXFLAGS)
	g++ -g hello_world.o  -o librados_hello_world $(LDFLAGS)

librados_hello_world_c: hello_world_c.c
	cc -g -c hello_world_c.c -o hello_world_c.o $(CFLAGS)
	cc -g hello_world_c.o  -o librados_hello_world_c $(LDFLAGS)

my_radosstriper: my_hello_radosstriper.cc
	g++ -g -c my_hello_radosstriper.cc -o my_hello_radosstriper.o $(CXXFLAGS)
	g++ -g my_hello_radosstriper.o   -o my_hello_radosstriper $(LDFLAGS)

my_radosstriper_aio: my_hello_radosstriper_aio.cc
	g++ -g -c my_hello_radosstriper_aio.cc -o my_hello_radosstriper_aio.o $(CXXFLAGS)
	g++ -g my_hello_radosstriper_aio.o   -o my_hello_radosstriper_aio $(LDFLAGS)

clean:
	rm hello_world.o librados_hello_world
	rm hello_world_c.o librados_hello_world_c
	rm my_hello_radosstriper.o  my_hello_radosstriper
	rm my_hello_radosstriper_aio.o  my_hello_radosstriper_aio
