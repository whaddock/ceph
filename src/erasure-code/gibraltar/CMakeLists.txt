# gibraltar plugin

set(gibraltar_utils_srcs 
  ErasureCodePluginGibraltar.cc
  ErasureCodeGibraltar.cc)

add_library(gibraltar_utils OBJECT ${gibraltar_utils_srcs})
add_dependencies(gibraltar_utils ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)

# Set the CFLAGS correctly for libgibraltar based on CUDA
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -I/usr/local/cuda/include -L/usr/local/cuda/lib -Ilibgibraltar/include -lcuda")
set(CMAKE_C_DEFINES "${CMAKE_C_DEFINES} -DGIB_USE_MMAP=0")

set(libgibraltar_srcs
  libgibraltar/src/gib_cpu_funcs.c
  libgibraltar/src/gib_cuda_driver.c
  libgibraltar/src/gibraltar.c
  libgibraltar/src/gib_galois.c
  libgibraltar/src/gibraltar_cpu.c)

add_library(libgibraltar_objs OBJECT ${libgibraltar_srcs})

set(ec_gibraltar_objs
  $<TARGET_OBJECTS:libgibraltar_objs>
  $<TARGET_OBJECTS:gibraltar_utils>
  $<TARGET_OBJECTS:erasure_code_objs>)

add_library(ec_gibraltar SHARED ${ec_gibraltar_objs})
target_link_libraries(ec_gibraltar ${EXTRALIBS})
set_target_properties(ec_gibraltar PROPERTIES VERSION 1.0.0 SOVERSION 1)
install(TARGETS ec_gibraltar DESTINATION ${erasure_plugin_dir})

if(WITH_EMBEDDED)
  add_library(cephd_ec_gibraltar STATIC ${gibraltar_utils_srcs} ${libgibraltar_srcs})
  list(APPEND COMPILE_DEFINITIONS BUILDING_FOR_EMBEDDED)
  set_target_properties(cephd_ec_gibraltar PROPERTIES LINKER_LANGUAGE CXX)
  set_target_properties(cephd_ec_gibraltar PROPERTIES COMPILE_DEFINITIONS BUILDING_FOR_EMBEDDED)
endif()
