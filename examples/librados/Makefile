ceph_src = ../../src
ceph_libs = $(ceph_src)/.libs
CXXFLAGS=-std=c++11 -I$(ceph_src)/include  -I$(ceph_src)
CFLAGS=-I$(ceph_src)/include -I$(ceph_src)
LDFLAGS=-L/lib64 -L$(ceph_src) -L$(ceph_libs) -lrados -lradosstriper
BOOST=-L/usr/lib64 -lboost_program_options-mt

ceph_erasure_code_benchmark_LDADD =  \
        $(LIBOSD) $(LIBCOMMON) \
        $(BOOST_PROGRAM_OPTIONS_LIBS) \
        $(CEPH_GLOBAL) \
        $(LIBERASURE_CODE) \
        $(am__append_175)

am__append_175 = -ldl
CEPH_GLOBAL = $(LIBGLOBAL) $(LIBCOMMON) $(PTHREAD_LIBS) -lm $(CRYPTO_LIBS) $(EXTRALIBS)
BOOST_PROGRAM_OPTIONS_LIBS = -lboost_program_options-mt 
LIBGLOBAL = $(ceph_libs)/libglobal.a
LIBCOMMON = $(ceph_libs)/libcommon.a -luuid
LIBOSD = $(ceph_src)/libosd.a $(am__append_23) $(LIBOSDC) $(LIBOS) $(LIBPERFGLUE) \
        $(LIBOSD_TYPES) $(LIBOS_TYPES)
LIBOSD_TYPES = $(ceph_libs)/libosd_types.a
PTHREAD_LIBS =  -lpthread
CRYPTO_LIBS = -lssl3 -lsmime3 -lnss3 -lnssutil3 -lplds4 -lplc4 -lnspr4 -lpthread -ldl  
EXTRALIBS = -lm $(am__append_14) $(am__append_15) $(am__append_16) \
        $(am__append_27)
#am__append_23 = -ljemalloc
LIBOSDC = $(ceph_libs)/libosdc.a
LIBOS = $(ceph_src)/libos.a $(am__append_17) $(am__append_18) $(am__append_19) \
        $(LIBOS_TYPES) $(LIBKV) $(LIBFUSE_LIBS)
LIBOS_TYPES = $(ceph_src)/libos_types.a
LIBPERFGLUE = $(ceph_libs)/libperfglue.a $(am__append_20) $(am__append_21)
#am__append_14 = -lexecinfo
am__append_15 = -lrt
#am__append_16 = -lprofiler
#am__append_27 = -lgcov
LIBKV = $(ceph_src)/libkv.a $(am__append_28) $(am__append_29) $(am__append_30) -lz \
        -lleveldb -lsnappy
LIBFUSE_LIBS = -pthread -lfuse  
#am__append_20 = -ltcmalloc_minimal
am__append_21 = -ltcmalloc
am__append_28 = $(ceph_src)/rocksdb/librocksdb.a
am__append_29 = -lbz2
#am__append_30 = -llz4
LIBERASURE_CODE = $(ceph_libs)/liberasure_code.a

all: aio_write_test_queue aio_read_test_queue rados_threaded_read_test rados_threaded_write_test \
machine_threads ceph_gibraltar_bench ceph_gibraltar_bench_threaded \
ceph_gibraltar_bench_aio_read

ceph_gibraltar_bench_aio_read: ceph_gibraltar_bench_aio_read.cc
	g++ -g -c ceph_gibraltar_bench_aio_read.cc -o ceph_gibraltar_bench_aio_read.o $(CXXFLAGS)
	g++ -g ceph_gibraltar_bench_aio_read.o  -o ceph_gibraltar_bench_aio_read $(LDFLAGS)

ceph_gibraltar_bench: ceph_gibraltar_bench.cc
	g++ -g -c ceph_gibraltar_bench.cc -o ceph_gibraltar_bench.o $(CXXFLAGS)
	g++ -g ceph_gibraltar_bench.o  -o ceph_gibraltar_bench $(LDFLAGS)

ceph_gibraltar_bench_threaded: ceph_gibraltar_bench_threaded.cc
	g++ -g -c ceph_gibraltar_bench_threaded.cc -o ceph_gibraltar_bench_threaded.o $(CXXFLAGS)
	g++ -g ceph_gibraltar_bench_threaded.o  -o ceph_gibraltar_bench_threaded $(LDFLAGS)

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

rados_threaded_ec_write_test: rados_threaded_ec_write_test.cc
	g++ -g -c rados_threaded_ec_write_test.cc -o rados_threaded_ec_write_test.o $(CXXFLAGS)
	g++ -g rados_threaded_ec_write_test.o  -o rados_threaded_ec_write_test $(LDFLAGS) $(ceph_erasure_code_benchmark_LDADD)

rados_threaded_read_test: rados_threaded_read_test.cc
	g++ -g -c rados_threaded_read_test.cc -o rados_threaded_read_test.o $(CXXFLAGS)
	g++ -g rados_threaded_read_test.o  -o rados_threaded_read_test $(LDFLAGS)

bogus: $@.cc
#	echo "$@"
#	echo "$^"
#	echo "$<"
#	echo "$(CXXFLAGS)
#	echo "$(LDFLAGS)

machine_threads: machine_threads.cc
	g++ -g -c machine_threads.cc -o machine_threads.o $(CXXFLAGS)
	g++ -g machine_threads.o  -o machine_threads $(LDFLAGS)

ceph_native_write_benchmark: ceph_native_write_benchmark.cc
	g++ -g -c ceph_native_write_benchmark.cc  -o ceph_native_write_benchmark.o $(CXXFLAGS)
	g++ -g ceph_native_write_benchmark.o  -o ceph_native_write_benchmark $(LDFLAGS) $(BOOST) \
           $(CEPH_GLOBAL)

test: test.cc
	g++ -g -c test.cc  -o test.o $(CXXFLAGS)
	g++ -g test.o  -o test $(LDFLAGS) $(BOOST)

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
	rm rados_threaded_ec_write_test.o rados_threaded_ec_write_test
	rm rados_threaded_read_test.o rados_threaded_read_test
	rm machine_threads.o machine_threads
