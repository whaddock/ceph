CXXFLAGS=-std=c++11 -I../../src/include  -I../../src
CFLAGS=-I../../src/include
LDFLAGS=-lrados -lradosstriper
all: aio_write_test_queue aio_read_test_queue rados_threaded_read_test rados_threaded_write_test machine_threads 

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

librados_my_hello_world: my_hello_world.cc
	g++ -g -c my_hello_world.cc -o my_hello_world.o $(CXXFLAGS)
	g++ -g my_hello_world.o  -o librados_my_hello_world $(LDFLAGS)

aio_write_test: rados_aio_write_test.cc
	g++ -g -c rados_aio_write_test.cc -o rados_aio_write_test.o $(CXXFLAGS)
	g++ -g rados_aio_write_test.o  -o rados_aio_write_test $(LDFLAGS)

aio_write_test_queue: rados_aio_write_test_queue.cc
	g++ -g -c rados_aio_write_test_queue.cc -o rados_aio_write_test_queue.o $(CXXFLAGS)
	g++ -g rados_aio_write_test_queue.o  -o rados_aio_write_test_queue $(LDFLAGS)

aio_read_test_queue: rados_aio_read_test_queue.cc
	g++ -g -c rados_aio_read_test_queue.cc -o rados_aio_read_test_queue.o $(CXXFLAGS)
	g++ -g rados_aio_read_test_queue.o  -o rados_aio_read_test_queue $(LDFLAGS)

rados_threaded_write_test: rados_threaded_write_test.cc
	g++ -g -c rados_threaded_write_test.cc -o rados_threaded_write_test.o $(CXXFLAGS)
	g++ -g rados_threaded_write_test.o  -o rados_threaded_write_test $(LDFLAGS)

rados_threaded_read_test: rados_threaded_read_test.cc
	g++ -g -c rados_threaded_read_test.cc -o rados_threaded_read_test.o $(CXXFLAGS)
	g++ -g rados_threaded_read_test.o  -o rados_threaded_read_test $(LDFLAGS)

machine_threads: machine_threads.cc
	g++ -g -c machine_threads.cc -o machine_threads.o $(CXXFLAGS)
	g++ -g machine_threads.o  -o machine_threads $(LDFLAGS)

clean:
	rm hello_world.o librados_hello_world
	rm hello_world_c.o librados_hello_world_c
	rm my_hello_radosstriper.o  my_hello_radosstriper
	rm my_hello_radosstriper_aio.o  my_hello_radosstriper_aio
	rm my_hello_world.o my_librados_hello_world
	rm rados_aio_write_test.o rados_aio_write_test
	rm rados_aio_write_test_queue.o rados_aio_write_test_queue
	rm rados_aio_read_test_queue.o rados_aio_read_test_queue
	rm rados_threaded_write_test.o rados_threaded_write_test
	rm rados_threaded_read_test.o rados_threaded_read_test
	rm machine_threads.o machine_threads
