# gibraltar plugin
noinst_HEADERS += \
  erasure-code/gibraltar/libgibraltar/include/dynamic_fp.h \
  erasure-code/gibraltar/libgibraltar/include/dynamic_fp.h \
  erasure-code/gibraltar/libgibraltar/include/gib_cpu_funcs.h \
  erasure-code/gibraltar/libgibraltar/include/gib_cuda_checksum.h \
  erasure-code/gibraltar/libgibraltar/include/gib_galois.h \
  erasure-code/gibraltar/libgibraltar/include/gibraltar.h \
  erasure-code/gibraltar/ErasureCodeGibraltar.h

gibraltar_sources = \
  erasure-code/ErasureCode.cc \
  erasure-code/gibraltar/libgibraltar/src/gib_cpu_funcs.c \
  erasure-code/gibraltar/libgibraltar/src/gib_cuda_driver.c \
  erasure-code/gibraltar/libgibraltar/src/gibraltar.c \
  erasure-code/gibraltar/libgibraltar/src/gib_galois.c \
  erasure-code/gibraltar/libgibraltar/src/gibraltar_cpu.c \
  erasure-code/gibraltar/ErasureCodePluginGibraltar.cc \
  erasure-code/gibraltar/ErasureCodeGibraltar.cc

erasure-code/gibraltar/ErasureCodePluginGibraltar.cc: ./ceph_ver.h

libec_gibraltar_generic_la_SOURCES = ${gibraltar_sources}
libec_gibraltar_generic_la_CFLAGS = ${AM_CFLAGS}  \
	-DGIB_USE_MMAP=0 \
	-I/usr/local/cuda/include \
	-L/usr/local/cuda/lib \
	-I$(srcdir)/erasure-code/gibraltar/libgibraltar/include \
	-lcuda

libec_gibraltar_generic_la_CXXFLAGS= ${AM_CXXFLAGS} \
	-DGIB_USE_MMAP=0 \
	-I/usr/local/cuda/include \
	-L/usr/local/cuda/lib \
	-I$(srcdir)/erasure-code/gibraltar/libgibraltar/include \
	-lcuda

libec_gibraltar_generic_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS) 
libec_gibraltar_generic_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared 

if LINUX
libec_gibraltar_generic_la_LDFLAGS += -export-symbols-regex '.*__erasure_code_.*'
endif

erasure_codelib_LTLIBRARIES += libec_gibraltar_generic.la

libec_gibraltar_la_SOURCES = \
	erasure-code/gibraltar/ErasureCodePluginSelectGibraltar.cc
libec_gibraltar_la_CFLAGS = ${AM_CFLAGS} \
	-I/usr/local/cuda/include \
	-L/usr/local/cuda/lib \
	-I$(srcdir)/erasure-code/gibraltar/libgibraltar/include \
	-lcuda

libec_gibraltar_la_CXXFLAGS= ${AM_CXXFLAGS} \
	-I/usr/local/cuda/include \
	-L/usr/local/cuda/lib \
	-I$(srcdir)/erasure-code/gibraltar/libgibraltar/include \
	-lcuda

libec_gibraltar_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
libec_gibraltar_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared
if LINUX
libec_gibraltar_la_LDFLAGS += -export-symbols-regex '.*__erasure_code_.*'
endif

erasure-code/gibraltar/ErasureCodePluginSelectGibraltar.cc: ./ceph_ver.h

erasure_codelib_LTLIBRARIES += libec_gibraltar.la
