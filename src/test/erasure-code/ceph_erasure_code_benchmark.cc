// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */


#include <boost/scoped_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/program_options/option.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/algorithm/string.hpp>

#include "global/global_context.h"
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/Clock.h"
#include "include/utime.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "erasure-code/ErasureCode.h"
#include "ceph_erasure_code_benchmark.h"
#include "rados_shard.h"

#include <include/rados/librados.hpp>
#include <iostream>
#include <string>
#include <sstream>
#include <climits>
#include <ctime>
#include <stdio.h>
#include <deque>
#include <queue>
#include <map>
#include <thread>
#include <mutex>
#include <cassert>
#include <chrono>

//#define TRACE 
//#define VERBOSITY_1 */
#define RADOS_THREADS 1

#ifndef EC_THREADS
#define EC_THREADS 10
#endif
#define MAX_DECODE_QUEUE_SIZE 3
#define COMPLETION_WAIT_COUNT 500
#define STRIPE_QUEUE_FACTOR 2
#define BLCTHREADS 10
#define REPORT_SLEEP_DURATION 10000 // in milliseconds
#define SHUTDOWN_SLEEP_DURATION 100 // in milliseconds
#define THREAD_SLEEP_DURATION 1 // in milliseconds
#define READ_SLEEP_DURATION 2 // in milliseconds
#define THREAD_ID << ceph_clock_now(g_ceph_context)  << " Thread: " << std::this_thread::get_id() << " | "
#define START_TIMER begin_time = ceph_clock_now(g_ceph_context);
#define FINISH_TIMER end_time = ceph_clock_now(g_ceph_context);	\
  total_time_s = (end_time - begin_time);
#define REPORT_TIMING output_lock.lock();\
  std::cout << total_time_s  << " s\t" << end_time << std::endl;	\
  cout.flush();\
  output_lock.unlock();
#define REPORT_BENCHMARK output_lock.lock();\
  std::cout << total_time_s  << " s\t" \
  << ((double)stripe_size * (double)shard_size) / (1024*1024) << " MB\t" \
  << ((double)stripe_size * (double)shard_size) / (double)(1024*total_time_s) \
  << " MB/s\t" << std::endl; \
  total_run_time_s += total_time_s;\
  cout.flush();\
  output_lock.unlock();

// Globals for program
int iterations = 0;
int in_size = 0;
int queue_size = 0;
int shard_size = 0;
int stripe_size = 0;
long long int object_sets = 0;
std::string obj_name;
std::string pool_name;
int ret = 0;
bool failed = false; // Used as a flag to indicate that a failure has occurred
int rados_mode = 1; // Enable extended rados benchmarks. If false, only original erasure code benchmarks.
int _argc; // Global argc for threads to use
const char** _argv; // Global argv for threads to use
const std::chrono::milliseconds report_sleep_duration(REPORT_SLEEP_DURATION);
const std::chrono::milliseconds read_sleep_duration(READ_SLEEP_DURATION);
const std::chrono::milliseconds thread_sleep_duration(THREAD_SLEEP_DURATION);
const std::chrono::milliseconds shutdown_sleep_duration(SHUTDOWN_SLEEP_DURATION);
librados::Rados rados;
librados::IoCtx io_ctx;

// Create queues, maps and iterators used by the main and thread functions.
std::queue<int> stripes;
std::queue<map<int,Shard>> stripes_decode_queue;
list<librados::AioCompletion *> completions, finishing;
std::queue<Shard> pending_buffers_queue;
std::map<int,Shard> shards;
std::vector<std::thread> rados_threads;
std::vector<std::thread> v_ec_threads;
std::vector<std::thread> v_blc_threads;
std::thread report_thread;
std::thread bsThread;
std::thread ecThread;
std::thread prThread;
bool g_is_encoding = false;
bool reporting_done = false; // Becomes true at end of shutdown.
bool aio_done = false; //Becomes true at the end when we all aio operations are finished
bool completions_done = false; //Becomes true at the end when we all aio operations are completed
bool ec_done = false; //Becomes true at the end when we all ec operations are completed
bool reading_done = false; //Becomes true at the end when we all stripes in the stripes_decode queue are repaired.
bool writing_done = false; //Becomes true at the end when we all objects in pending_buffer are written
int stripes_that_remain = 0;
int concurrentios = RADOS_THREADS;
int ec_threads = EC_THREADS;
int K = 0; // Number of data shards
int M = 0; // Number of erasure code shards
uint32_t buffers_created_count = 0;
std::vector<int> v_erased;

// Locks for containers sharee with the handleAioCompletions thread.
std::mutex output_lock;
std::mutex stripes_lock;
std::mutex stripes_read_lock;
std::mutex stripes_decode_queue_lock;
std::mutex completions_lock;
std::mutex shards_lock;
std::mutex pending_buffers_queue_lock;
std::mutex write_buffers_waiting_lock;
std::mutex pending_ops_lock;
std::mutex cout_lock;
std::mutex objs_lock;

// Object information. We stripe over these objects.
struct obj_info {
  string name;
  size_t len;
};
map<int, obj_info> objs;

struct CompletionOp {
  int id;
  std::string name;
  librados::AioCompletion *completion;
  librados::bufferlist bl;
  Shard shard;

  explicit CompletionOp(std::string _name) : id(0), name(_name), completion(NULL) {}
};

std::map<int,CompletionOp *> pending_ops;

// guarded queue accessor functions
void print_message(std::string message) {
  std::lock_guard<std::mutex> guard(cout_lock);
  std::cout << message << std::endl;
  std::cout.flush();
}

int get_objs_size() { 
  std::lock_guard<std::mutex> guard(objs_lock);
  return objs.size();
}

void insert_objs(int _index, obj_info _info) {
  std::lock_guard<std::mutex> guard(objs_lock);
  objs[_index] = _info;
  return;
}

obj_info get_obj_info(int _index) {
  std::lock_guard<std::mutex> guard(objs_lock);
  return objs[_index];
}

int get_pending_ops_size() { 
  std::lock_guard<std::mutex> guard(pending_ops_lock);
  return pending_ops.size();
}

void insert_pending_op(int index, CompletionOp *op) {
  std::lock_guard<std::mutex> guard(pending_ops_lock);
  pending_ops.insert(std::pair<int,CompletionOp *>(index,op));
  return;
}

int get_stripes_queue_size() { 
  std::lock_guard<std::mutex> guard(stripes_lock);
  return stripes.size();
}

int get_stripe() {
  int stripe;
  std::lock_guard<std::mutex> guard(stripes_lock);
  if (!stripes.empty()) {
    stripe = stripes.front();
    stripes.pop();
  }
  else 
    stripe = INT_MIN;
  return stripe;
}

bool is_stripes_queue_empty() {
  std::lock_guard<std::mutex> guard(stripes_lock);
  return stripes.empty();
}

/* Getting a shard is a two step call and the postRead thread
 * is the only consumer. Because the read callback mechanism
 * is inserting shards into the map after they have been read,
 * we need to make this a mutex. It is possible for one operation
 * to interfere with the other and corrupt the map.
 *
 * First, the call to is_shard_available is made. Once this returns
 * true, then the second call to get_shard_and_erase is made. The second
 * call will get the shard that has already been determined to be
 * available and erase it from the map.
 *
 * We have implemented this queue as a map because we need to organize
 * the shards into stripes for further processing. The read callback
 * mechanism knows which slot the shard goes into which makes this a
 * convenient way to process shards that have been read as stripes
 * for the erasure code processing.
 */
bool is_shard_available(int index) {
  std::lock_guard<std::mutex> guard(shards_lock);
  std::map<int, Shard>::iterator it = shards.find(index);
  bool status = true;
  if (it == shards.end()) 
    status = false;
  return status;
}

Shard get_shard_and_erase(int index) {
  std::lock_guard<std::mutex> guard(shards_lock);
  std::map<int, Shard>::iterator it = shards.find(index);
  Shard shard = it->second;
  shards.erase(it);
  return shard;
}

int get_shards_map_size() {
  std::lock_guard<std::mutex> guard(shards_lock);
  return shards.size();
}

bool is_shards_map_empty() {
  std::lock_guard<std::mutex> guard(shards_lock);
  return shards.empty();
}

void insert_shard(int index,Shard shard) {
  std::lock_guard<std::mutex> guard(shards_lock);
  shards.insert(pair<int,Shard>(index,shard));
  return;
}

bool is_stripes_decode_queue_empty() {
  std::lock_guard<std::mutex> guard(stripes_decode_queue_lock);
  return stripes_decode_queue.empty();
}

std::map<int,Shard> get_stripe_decode(bool status) {
  std::map<int,Shard> stripe;
  std::lock_guard<std::mutex> guard(stripes_decode_queue_lock);
  if (stripes_decode_queue.empty())
    status = false;
  else {
    stripe = stripes_decode_queue.front();
    stripes_decode_queue.pop();
  }
  return stripe;
}

void insert_stripe_decode(map<int,Shard> stripe) {
  std::lock_guard<std::mutex> guard(stripes_decode_queue_lock);
  stripes_decode_queue.push(stripe);
  return;
}


int get_stripes_decode_queue_size() {
  std::lock_guard<std::mutex> guard(stripes_decode_queue_lock);
  return stripes_decode_queue.size();
}

int get_pending_buffers_queue_size() {
  std::lock_guard<std::mutex> guard(pending_buffers_queue_lock);
  return pending_buffers_queue.size();
}

bool is_pending_buffers_queue_empty() {
  std::lock_guard<std::mutex> guard(pending_buffers_queue_lock);
  return pending_buffers_queue.empty();
}

void pending_buffers_queue_push(Shard shard) {
  std::lock_guard<std::mutex> guard(pending_buffers_queue_lock);
  pending_buffers_queue.push(shard);
  return;
}

Shard pending_buffers_queue_pop() {
  std::lock_guard<std::mutex> guard(pending_buffers_queue_lock);
  Shard shard = pending_buffers_queue.front();
  pending_buffers_queue.pop();
  return shard;
}

void io_cb(librados::completion_t c, CompletionOp *op) {
  std::lock_guard<std::mutex> guard(pending_ops_lock);

  std::map<int, CompletionOp *>::iterator iter = pending_ops.find(op->id);
  if (iter != pending_ops.end())
    pending_ops.erase(iter);

  op->completion->release();
  delete op;
  //  std::cout << "-";
}

static void _completion_cb(librados::completion_t c, void *param)
{
  CompletionOp *op = (CompletionOp *)param;
  io_cb(c, op);
}

void read_cb(librados::completion_t c, CompletionOp *op) {
  std::lock_guard<std::mutex> guard(pending_ops_lock);

  std::map<int, CompletionOp *>::iterator iter = pending_ops.find(op->id);
  if (iter != pending_ops.end())
    pending_ops.erase(iter);

  op->completion->release();

  // For reads we need to keep the shard object for erasure coding
  insert_shard(op->id,op->shard);
  delete op;
  //  std::cout << "-";
}

static void _read_completion_cb(librados::completion_t c, void *param)
{
  CompletionOp *op = (CompletionOp *)param;
  read_cb(c, op);
}

/* Function to get the IO Context */
void initRadosIO() {

  // first, we create a Rados object and initialize it
  ret = rados.init("admin"); // just use the client.admin keyring
  if (ret < 0) { // let's handle any error that might have come back
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "couldn't initialize rados! error " << ret << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
    ret = EXIT_FAILURE;
  } else {
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "we just set up a rados cluster object" << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
  }

  /*
   * Now we need to get the rados object its config info. It can
   * parse argv for us to find the id, monitors, etc, so let's just
   * use that.
   */
  {
    ret = rados.conf_parse_argv(_argc, _argv);
    if (ret < 0) {
      // This really can't happen, but we need to check to be a good citizen.
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "failed to parse config options! error " << ret << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      ret = EXIT_FAILURE;
    } else {
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "we just parsed our config options" << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      // We also want to apply the config file if the user specified
      // one, and conf_parse_argv won't do that for us.
      for (int i = 0; i < _argc; ++i) {
	if ((strcmp(_argv[i], "-c") == 0) || (strcmp(_argv[i], "--conf") == 0)) {
	  ret = rados.conf_read_file(_argv[i+1]);
	  if (ret < 0) {
	    // This could fail if the config file is malformed, but it'd be hard.
#ifdef TRACE
	    output_lock.lock();
	    std::cerr THREAD_ID << "failed to parse config file " << _argv[i+1]
				<< "! error" << ret << std::endl;
	    std::cerr.flush();
	    output_lock.unlock();
#endif
	    ret = EXIT_FAILURE;
	  }
	  break;
	}
      }
    }
  }

  /*
   * next, we actually connect to the cluster
   */
  {
    ret = rados.connect();
    if (ret < 0) {
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "couldn't connect to cluster! error " << ret << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      ret = EXIT_FAILURE;
    } else {
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "we just connected to the rados cluster" << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
    }
  }

  /*
   * create an "IoCtx" which is used to do IO to a pool
   */
  {
    ret = rados.ioctx_create(pool_name.c_str(), io_ctx);
    if (ret < 0) {
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "couldn't set up ioctx! error " << ret << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      ret = EXIT_FAILURE;
    } 
  }

  return;
}

/* Function used for Report Thread
 */
void reportThread()
{
  bool started = false;

  if(!started) {
    started = true;
    output_lock.lock();
    std::cout THREAD_ID << "Starting reportThread()" << std::endl;
    std::cout.flush();
    output_lock.unlock();
  }

  while (!reporting_done)
    {
      std::this_thread::sleep_for(report_sleep_duration);
      output_lock.lock();
      std::cout THREAD_ID << "objs\tPendingO\tstripes\tshards\tdecode\t"
			  << "PendingQ" << std::endl;
      std::cout THREAD_ID << get_objs_size() << "\t" 
			  << get_pending_ops_size() << "\t"
			  << get_stripes_queue_size() << "\t"
			  << get_shards_map_size() << "\t"
			  << get_stripes_decode_queue_size() << "\t"
			  << get_pending_buffers_queue_size() << "\t"
			  << std::endl;
      std::cout.flush();
      output_lock.unlock();
    }
  return;
}

/* Function used for bootstrap thread
 */
void bootstrapThread()
{
  /* Object Sets: Ceph objects can contain about 80 MB by design so we have to 
   * store our stripes in Object Sets that consist of 80 MB objects. If we are
   * writing 8 MB shards, then we can do about 10 stripes per Object Set.
   * The logic in the bootstrapThread will produce enough Object Sets for our
   * benchmark according to the shard size and the number of stripes to write.
   * The read and write threads will need to apply the same logic in order to
   * use the appropriate Object Set for their operation.
   * int iterations: a global that contains a value indicating the number of stripes
   * to be processed.
   * int shard_size: a global int that contains a value indicating the size of
   * the shards.
   * int object_set: an int that selects the Object Set, part of the object name.
   * The object_set value is computed with the following formula:
   * floor(stripe_number * shard_size) / in_size )
   * The bootstrapThread will create all of the Object Sets using the following formula:
   * ceil(iterations * shard_size) / in_size )
   * shard_size must be a factor of in_size. Specifically, shard_size * stripes_per_object_set = in_size.
   */

#ifdef TRACE
  output_lock.lock();
  std::cerr THREAD_ID << "Started bootstrapThread()" << std::endl;
  std::cerr THREAD_ID << "iterations: " << iterations << std::endl;
  std::cerr THREAD_ID << "shard_size: " << shard_size << std::endl;
  std::cerr THREAD_ID << "in_size: " << in_size << std::endl;
  std::cerr.flush();
  output_lock.unlock();
#endif

  int buf_len = 1;
  int index = 0;

#ifdef TRACE
  output_lock.lock();
  std::cerr THREAD_ID << "object_sets: " << object_sets << std::endl;
  std::cerr THREAD_ID << "Starting to init objects for writing..."
		      << std::endl;
  std::cerr.flush();
  output_lock.unlock();
#endif
  for (int object_set = 0; object_set < object_sets; object_set++) {
    for (int shard = 0; shard < stripe_size; shard++) {

    // Create the data structure for the objects we will use
      obj_info info;
      std::stringstream object_name;
      object_name << obj_name << "." << object_set << "." << shard;
      info.name = object_name.str();
      info.len = in_size;
      index = object_set * stripe_size + shard;
      insert_objs(index,info);
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Creating object: " << info.name
			  << std::endl;
      std::cerr THREAD_ID << "index:: " << index << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      bufferptr p = buffer::create(buf_len);
      bufferlist bl;
      memset(p.c_str(), 0, buf_len);
      bl.push_back(p);

      CompletionOp *op = new CompletionOp(info.name);
      op->completion = rados.aio_create_completion(op, _completion_cb, NULL);
      op->id = index;
      op->bl = bl;

      // generate object
      ret = io_ctx.aio_write(info.name, op->completion, op->bl, buf_len, info.len - buf_len);
      if (ret < 0) {
	cerr << "couldn't write obj: " << info.name << " ret=" << ret << std::endl;
      }
    }
  }
#ifdef TRACE
  output_lock.lock();
  std::cerr THREAD_ID << "Finished bootstrapThread(), exiting." << std::endl;
  std::cerr.flush();
  output_lock.unlock();
#endif
}

void radosWriteThread() {
  bool started = false;
  int ret;
  uint32_t stripe = 0;
  int obj_index = 0;
  uint32_t shard_index = 0;
  uint32_t offset = 0;
  int object_set = 0;

  if(!started) {
    started = true;
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Starting radosWriteThread()" << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
  }

  // Write loop 
  while (!writing_done) {
    // wait for the request to complete, and check that it succeeded.
    Shard a_shard;

    if (is_pending_buffers_queue_empty()) {
      std::this_thread::sleep_for(shutdown_sleep_duration);
    }
    else {
      a_shard = pending_buffers_queue_pop();
      stripe = a_shard.get_stripe();
      object_set = stripe * shard_size / in_size;
      offset = stripe * shard_size % in_size;
      obj_index = object_set * stripe_size + a_shard.get_shard();
      obj_info info = get_obj_info(obj_index);
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID  << "pbit: pending_buffers size " << get_pending_buffers_queue_size() << std::endl;
      std::cerr THREAD_ID  << "pbit: Writing Object Name " << info.name << " to storage." << std::endl;
      std::cerr THREAD_ID  << "Shard Object stripe position: " << a_shard.get_shard() << std::endl;
      std::cerr THREAD_ID  << "Shard Object stripe number: " << a_shard.get_stripe() << std::endl;
      std::cerr THREAD_ID  << "stripe: " << stripe << std::endl;
      std::cerr THREAD_ID  << "offset: " << offset << std::endl;
      std::cerr THREAD_ID  << "shard_index: " << shard_index << std::endl;
      std::cerr THREAD_ID  << "obj_index: " << obj_index << std::endl;
      std::cerr THREAD_ID  << "object_set: " << object_set << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif

      CompletionOp *op = new CompletionOp(info.name);
      op->completion = rados.aio_create_completion(op, _completion_cb, NULL);
      op->id = shard_index++;
      op->bl = a_shard.get_bufferlist();

      ret = io_ctx.aio_write(info.name, op->completion, op->bl, shard_size, offset);
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Write called."
			  << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      if (ret < 0) {
#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "couldn't start write object! error at index "
			    << op->id << std::endl;
	std::cerr.flush();
	output_lock.unlock();
#endif
	ret = EXIT_FAILURE;
	failed = true; 
	// We have had a failure, so do not execute any further, 
	// fall through.
      }
      insert_pending_op(op->id,op);

#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "buffer: " << op->id
			  << " pushed to write_buffers_waiting queue in radosWriteThread()"
			  << std::endl;
      std::cerr THREAD_ID << "we wrote object "
			  << info.name
			  << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif

      /* throttle...
       * This block causes the write thread to wait until the number
       * of outstanding AIO completions is below the number of 
       * concurrentios that was set in the configuration. Since
       * the completions queue and the shards_in_flight queue are
       * a bijection, we are assured of dereferencing the corresponding
       * buffer once the write has completed.
       */
	while (get_pending_ops_size() > concurrentios) {
	  output_lock.lock();
	  std::cerr THREAD_ID << "pending_ops_size "
			      << get_pending_ops_size() << " > " << concurrentios
			      << std::endl;
	  std::cerr.flush();
	  output_lock.unlock();
	  std::this_thread::sleep_for(thread_sleep_duration);
	}

    }   // End Write procedure
  } // End of write loop

#ifdef TRACE
  output_lock.lock();
  std::cerr THREAD_ID << "Write thread exiting now." << std::endl;
  std::cerr.flush();
  output_lock.unlock();
#endif

  return; // Thread terminates
}
/* @writing_done means that the rados has finished writing shards/objects.
 */

void radosReadThread(ErasureCodeBench ecbench) {
  bool started = false;

  int ret;
  int shard_index = 0;
  int obj_index = 0;
  int stripe = 0;
  // For this thread
  uint32_t offset = 0;
  Shard shard;
  int object_set = 0;

  if(!started) {
    started = true;
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Starting radosReadThread()" << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
  }

  // Read loop 
  while (!reading_done) {
    // wait for the request to complete, and check that it succeeded.
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID  << "In radosReadThread() outer while loop." << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif

    while (!is_stripes_queue_empty()) {
      if ( (stripe = get_stripe()) < 0) {
	/* When the stripes queue is empty, we have submitted all of the
	 * work to be done. If by chance another thread grabbed the last
	 * work item from the stripes queue, then this test will exit now.
	 */
	break;
      }

#ifdef VERBOSITY_1
      std::cout THREAD_ID << "Processing stripe " << stripe << std::endl;
#endif

      object_set = stripe * shard_size / in_size;
      offset = stripe * shard_size % in_size;

      /* On the read benchmark, we only read K shards from Ceph.
       * We do not use the bootstrap here because we create the obj_info
       * in this thread.
       */
      for (int i_shard=0;i_shard<K;i_shard++) {

	std::stringstream object_name;
	obj_info info;
	object_name << obj_name << "." << object_set << "." << i_shard;
	info.name = object_name.str();
	info.len = in_size;
	obj_index = object_set * stripe_size + i_shard;
	insert_objs(obj_index,info);
	Shard data(stripe,i_shard,stripe_size,info.name);
	librados::bufferlist bl = librados::bufferlist();
	// We create the buffer with data that will be overwritten.
	bl.append(std::string(shard_size,(char)shard_index%26+97)); // start with 'a'
#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "Created a Shard for encoding: "
			    << info.name << std::endl;
	std::cerr THREAD_ID << "iterations: " << iterations << std::endl;
	std::cerr THREAD_ID << "shard_size: " << shard_size << std::endl;
	std::cerr THREAD_ID << "in_size: " << in_size << std::endl;
	std::cerr THREAD_ID << "shard: " << i_shard << std::endl;
	std::cerr THREAD_ID << "stripe: " << stripe << std::endl;
	std::cerr THREAD_ID << "offset: " << offset << std::endl;
	std::cerr THREAD_ID << "object_set: " << object_set << std::endl;
	std::cerr.flush();
	output_lock.unlock();
#endif

	CompletionOp *op = new CompletionOp(info.name);
	op->completion = rados.aio_create_completion(op, _read_completion_cb, NULL);
	op->id = shard_index++;
	op->bl = bl;

	ret = io_ctx.aio_read(op->name, op->completion, &op->bl, (uint64_t)shard_size, 
			      (uint64_t)offset);
	data.set_bufferlist(op->bl);
	op->shard = data;

#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "Read called."
			    << std::endl;
	std::cerr.flush();
	output_lock.unlock();
#endif
	if (ret < 0) {
#ifdef TRACE
	  output_lock.lock();
	  std::cerr THREAD_ID << "couldn't start read object! error at index "
			      << shard_index << std::endl;
	  std::cerr.flush();
	  output_lock.unlock();
#endif
	  ret = EXIT_FAILURE;
	  failed = true; 
	  // We have had a failure, so do not execute any further, 
	  // fall through.
	}
	insert_pending_op(op->id,op);

#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "pending_ops_size "
			    << get_pending_ops_size() << " > " << concurrentios
			    << std::endl;
	std::cerr.flush();
	output_lock.unlock();
#endif
	
	/* throttle...
	 * This block causes the read thread to wait on
	 * the read ops so we don't over demand
	 * the IO system.
	 */
	while (get_pending_ops_size() > concurrentios) {
	  std::this_thread::sleep_for(read_sleep_duration);
	}

      } // Finished a stripe.
      /* throttle...
       * This block causes the read thread to wait on
       * the erasure decoding so we don't over demand
       * memory.
       */
      while (get_shards_map_size() > queue_size ) {
	std::this_thread::sleep_for(read_sleep_duration);
      }

    } // While loop over stripes queue
  }  // While loop waiting for reading to be done.
#ifdef TRACE
  output_lock.lock();
  std::cerr THREAD_ID << "Read thread exiting now." << std::endl;
  std::cerr.flush();
  output_lock.unlock();
#endif

  return; // Thread terminates
}

void postReadThread() {
  bool started = false;
  int index = 0;

  if(!started) {
    started = true;
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Starting postReadThread()" << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
  }

  /* In the postReadThread we assmeble stripes for erasure coding with
   * Shards from the shards map. There are K Shards for each stripe in
   * the shards map. The stripes are laid out in a single dimension so
   * the first stripe has an index of 0..K-1, the second stripe has indices
   * of K..2K-1, etc.
   */

  /* We have to guard everything with aio_done in case it occurs after
   * the routine has started. There is a good possibility of this 
   * condition occuring when we are running multiple erasure coding
   * threads. The logic will not set aio_done to true until the
   * shards map is empty and all of the IO has completed. If we
   * are building the last stripe, aio_done will remain false
   * until some time after the last share is taken.
   */
  for (int stripe = 0;stripe < iterations;stripe++) {
    map<int, Shard> a_stripe;
    for ( int i_shard = 0; i_shard < K;i_shard++) {
      index = stripe * K + i_shard;
      while (!aio_done && !is_shard_available(index)) {
	std::this_thread::sleep_for(read_sleep_duration);
      }
      if (aio_done)
	break;
      Shard a_shard = get_shard_and_erase(index);
      a_stripe.insert(std::pair<int, Shard>(i_shard,a_shard));
    }

    if (!aio_done) {
      // Limit the number of stripes waiting for decode to a reasonable number
      while (get_stripes_decode_queue_size() > MAX_DECODE_QUEUE_SIZE) {
	std::this_thread::sleep_for(read_sleep_duration);
      }

      insert_stripe_decode(a_stripe);
#ifdef VERBOSITY_1
      output_lock.lock();
      std::cerr THREAD_ID << "shards_map_size is " << get_shards_map_size() << std::endl;
      std::cerr THREAD_ID << "stripes_decode_size is " << get_stripes_decode_queue_size() 
			  << " in postReadThread, done reading a stripe." 
			  << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
    }
  }
#ifdef TRACE
  output_lock.lock();
  std::cerr THREAD_ID << "Post Read thread exiting now." << std::endl;
  std::cerr.flush();
  output_lock.unlock();
#endif

  return; // Thread terminates
}

/* Function used to perform erasure encoding.
 */
void erasureEncodeThread(ErasureCodeBench ecbench) {
  bool started = false;
  int stripe = 0;
  int buffers_created_count = 0;
  int object_set = 0;

  if(!started) {
    started = true;
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Starting erasureEncodeThread()" << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
  }
  while (!ec_done) {
    while (!is_stripes_queue_empty()) {
      if ((stripe = get_stripe()) < 0) {
	/* When the stripes queue is empty, we have submitted all of the
	 * work to be done. If by chance another thread grabbed the last
	 * work item from the stripes queue, then this test will exit now.
	 */
	break;
      }

#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "iterations: " << iterations << std::endl;
      std::cerr THREAD_ID << "shard_size: " << shard_size << std::endl;
      std::cerr THREAD_ID << "in_size: " << in_size << std::endl;
      std::cerr THREAD_ID << "object_sets: " << object_sets << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      int obj_index = 0;
      object_set = stripe * shard_size / in_size;
      map<int,Shard> a_stripe;
      map<int,librados::bufferlist> encoded;
      for (int i_shard=0;i_shard<stripe_size;i_shard++) {
	// Create the data structure for the objects we will use
	obj_index = object_set * stripe_size + i_shard;
	obj_info info = get_obj_info(obj_index);

	Shard data(stripe,i_shard,stripe_size,info.name);
	librados::bufferlist bl = librados::bufferlist();
	bl.append(std::string(shard_size,(char)buffers_created_count++%26+97)); // start with 'a'
	data.set_bufferlist(bl);
	a_stripe.insert( std::pair<int,Shard>(i_shard,data));
#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "iterations: " << iterations << std::endl;
	std::cerr THREAD_ID << "object name: " << info.name << std::endl;
	std::cerr THREAD_ID << "shard_size: " << shard_size << std::endl;
	std::cerr THREAD_ID << "in_size: " << in_size << std::endl;
	std::cerr THREAD_ID << "Created a Shard for encoding [object_set.shard] " << object_set << "." << i_shard << std::endl;
	std::cerr THREAD_ID << "object_sets: " << object_sets << std::endl;
	std::cerr.flush();
	output_lock.unlock();
#endif
      } 

#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Encoding a stripe" << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      for (map<int,Shard>::iterator stripe_it=a_stripe.begin();
	   stripe_it!=a_stripe.end();stripe_it++) {
	encoded.insert(pair<int,librados::bufferlist>(stripe_it->second.get_shard(),stripe_it->second.get_bufferlist()));
      }
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Stripe " <<  a_stripe.begin()->second.get_stripe() << " calling encode in erasureCodeThread()" << 
	std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      ret = ecbench.encode(&encoded);
      if (ret < 0) {
	output_lock.lock();
	std::cerr << "Error in erasure code call to ecbench. " << ret << std::endl;
	std::cerr.flush();
	output_lock.unlock();
      } 

      for (map<int,Shard>::iterator stripe_it=a_stripe.begin();stripe_it!=a_stripe.end();stripe_it++) {
	pending_buffers_queue_push(stripe_it->second);
#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "Pushed buffer " << stripe_it->second.get_hash() <<
	  " to pending_buffer_queue."  << std::endl;
	std::cerr.flush();
	output_lock.unlock();
#endif
      }

#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Encoding done, buffers inserted, in erasureCodeThread()." << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      encoded.clear();
      a_stripe.clear();

      // Throttle, don't use too many buffers.
      while (get_pending_buffers_queue_size() > ecbench.queue_size) {
	std::this_thread::sleep_for(thread_sleep_duration);
      }
    }
  }

#ifdef TRACE
  output_lock.lock();
  std::cerr THREAD_ID << "erasureEncodeThread exiting now." << std::endl;
  std::cerr.flush();
  output_lock.unlock();
#endif

  return; // Thread terminates
}

void erasureDecodeThread(ErasureCodeBench ecbench) {
  bool started = false;
  bool got_stripe = false;
  Shard shard;
  map<int,Shard> stripe;
  map<int,Shard>::iterator stripe_it;
  map<int,librados::bufferlist>::iterator encoded_it;
  map<int,librados::bufferlist> encoded;

  if(!started) {
    started = true;
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Starting erasureDecodeThread()" << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
  }
  while (!ec_done) {
    got_stripe = true; // Test for getting a stripe from the decode queue.
    if (!is_stripes_decode_queue_empty()) {
      stripe = get_stripe_decode(got_stripe);
      if (got_stripe) {

	// We have a stripe to repair. There are K shards in stripe.
	for (stripe_it=stripe.begin();
	     stripe_it!=stripe.end();stripe_it++) {
	  encoded.insert(pair<int,librados::bufferlist>(stripe_it->second.get_shard(),stripe_it->second.get_bufferlist()));
	}

	// Add the M parity buffers.
	for (int m = 0; m < M; m++) {
	  librados::bufferlist bl = librados::bufferlist();
	  bl.append(std::string(shard_size,(char)20)); // fill with space
	  encoded.insert(pair<int,librados::bufferlist>(K+m,bl));
	}

	ret = ecbench.encode(&encoded);
	// For now, we are just recreating the M parity shards, worst case. 
	if (ret < 0) {
	  output_lock.lock();
	  std::cerr THREAD_ID << "Error in erasure code call to ecbench. " << ret << std::endl;
	  std::cerr.flush();
	  output_lock.unlock();
	} 

#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "Stripe " <<  stripe.begin()->second.get_stripe() 
			    << " decoded in erasureDecodeThread()" << std::endl;
	std::cerr THREAD_ID << "Decoding done, buffers discarded, in erasureDecodeThread()."
			    << std::endl;
	std::cerr THREAD_ID << "stripes_decode_size: " << get_stripes_decode_queue_size() << std::endl;
	std::cerr.flush();
	output_lock.unlock();
#endif
	encoded.clear();
	/* Need to release the stripe memory. We are just measuring the time to repair
	 * the stripe. Now we can release the resources*/
	for (stripe_it=stripe.begin();
	     stripe_it!=stripe.end();stripe_it++) {
	  stripe_it->second.dereference_bufferlist();
	  //Shard * p_shard = stripe_it->second.get_pointer();
	  stripe.erase(stripe_it);
	  //p_shard->~Shard();
	}
    
	for (encoded_it=encoded.begin();
	     encoded_it!=encoded.end();encoded_it++) 
	  encoded.erase(encoded_it);
      }
    }
    else 
      std::this_thread::sleep_for(shutdown_sleep_duration);

  }
#ifdef VERBOSITY_1
  output_lock.lock();
  std::cerr THREAD_ID << "erasureDecodeThread exiting now." << std::endl;
  std::cerr.flush();
  output_lock.unlock();
#endif

  return; // Thread terminates
}

/* @writing_done means that the rados has finished writing shards/objects.
 */
namespace po = boost::program_options;

int ErasureCodeBench::setup(int argc, const char** argv) {
  std::cerr << "Entering ErasureCodeBench::setup()" << std::endl;
  std::cerr.flush();

  po::options_description desc("Allowed options");
  desc.add_options()
    ("help,h", "produce help message")
    ("verbose,v", "explain what happens")
    ("name,n", po::value<string>()->default_value("test"),
     "Prefix of object name: i.e. test123")
    ;
  std::cerr << "Added help,verbose,name" << std::endl;
  std::cerr.flush();
  desc.add_options()
    ("rados_mode,r", po::value<int>()->default_value(1),
     "Enables rados benchmarks. If false, original EC benchmarks.") 
    ("size,s", po::value<int>()->default_value(1024 * 1024),
     "size of the buffer to be encoded")
    ("threads,t", po::value<int>()->default_value(RADOS_THREADS),
     "Number of reader/writer threads to run.") 
    ("ecthreads", po::value<int>()->default_value(EC_THREADS),
     "Number of erasure coding threads to run.")
    ;
  std::cerr << "Added rados,size,threads" << std::endl;
  std::cerr.flush();
  desc.add_options()
    ("queuesize,q", po::value<int>()->default_value(1024),
     "size of the buffer queue")
    ("shard_size,x", po::value<int>()->default_value(1024 * 1024 * 8),
     "size of the objects/shards to be encoded") 
    ("iterations,i", po::value<int>()->default_value(1),
     "number of encode/decode runs")
    ;
  std::cerr << "Added queuesize,shard_size,iterations" << std::endl;
  std::cerr.flush();
  desc.add_options()
    ("pool,y", po::value<string>()->default_value("stripe"),
     "pool name")
    ("plugin,p", po::value<string>()->default_value("jerasure"),
     "erasure code plugin name")
    ("workload,w", po::value<string>()->default_value("encode"),
     "run either encode or decode")
    ("erasures,e", po::value<int>()->default_value(1),
     "number of erasures when decoding")
    ;
  std::cerr << "Added pool,plugin,workload,erasures" << std::endl;
  std::cerr.flush();
  desc.add_options()
    ("erased", po::value<vector<int> >(),
     "erased chunk (repeat if more than one chunk is erased)")
    ("erasures-generation,E", po::value<string>()->default_value("random"),
     "If set to 'random', pick the number of chunks to recover (as specified by "
     " --erasures) at random. If set to 'exhaustive' try all combinations of erasures "
     " (i.e. k=4,m=3 with one erasure will try to recover from the erasure of "
     " the first chunk, then the second etc.)")
    ("parameter,P", po::value<vector<string> >(),
     "add a parameter to the erasure code profile")
    ;
  std::cerr << "Added erased,erased chunk,parameter" << std::endl;
  std::cerr.flush();


  po::variables_map vm;
  po::parsed_options parsed =
    po::command_line_parser(argc, argv).options(desc).allow_unregistered().run();
  po::store(
	    parsed,
	    vm);
  po::notify(vm);
  std::cerr << "Returned from parser" << std::endl;
  std::cerr.flush();

  vector<const char *> ceph_options, def_args;
  vector<string> ceph_option_strings = po::collect_unrecognized(
								parsed.options, po::include_positional);
  std::cerr << "Returned from po::collect_unrecognized. " << ceph_option_strings.size() << std::endl;
  std::cerr.flush();

  ceph_options.reserve(ceph_option_strings.size());
  std::cerr << "Returned from ceph_options.reserve()" << std::endl;
  std::cerr.flush();

  for (vector<string>::iterator i = ceph_option_strings.begin();
       i != ceph_option_strings.end();
       ++i) {
    std::cerr << "Pushing " << i->c_str() << std::endl;
    std::cerr.flush();
    ceph_options.push_back(i->c_str());
    std::cerr << "Returned from ceph_options. End of init function." << std::endl;
    std::cerr.flush();
  }

  cct = global_init(
		    &def_args, ceph_options, CEPH_ENTITY_TYPE_CLIENT,
		    CODE_ENVIRONMENT_UTILITY,
		    CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);

  if (vm.count("help")) {
    cout << desc << std::endl;
    return 1;
  }

  if (vm.count("parameter")) {
    const vector<string> &p = vm["parameter"].as< vector<string> >();
    for (vector<string>::const_iterator i = p.begin();
	 i != p.end();
	 ++i) {
      std::vector<std::string> strs;
      boost::split(strs, *i, boost::is_any_of("="));
      if (strs.size() != 2) {
	cerr << "--parameter " << *i << " ignored because it does not contain exactly one =" << endl;
      } else {
	profile[strs[0]] = strs[1];
      }
    }
  }

  in_size = vm["size"].as<int>();
  rados_mode = vm["rados_mode"].as<int>();
  concurrentios = vm["threads"].as<int>();
  ec_threads = vm["ecthreads"].as<int>();
  queue_size = vm["queuesize"].as<int>();
  shard_size = vm["shard_size"].as<int>(); 
  max_iterations = vm["iterations"].as<int>();
  obj_name = vm["name"].as<string>();
  pool_name = vm["pool"].as<string>();
  plugin = vm["plugin"].as<string>();
  workload = vm["workload"].as<string>();
  erasures = vm["erasures"].as<int>();
  if (vm.count("erasures-generation") > 0 &&
      vm["erasures-generation"].as<string>() == "exhaustive")
    exhaustive_erasures = true;
  else
    exhaustive_erasures = false;
  if (vm.count("erased") > 0)
    erased = vm["erased"].as<vector<int> >();

  k = atoi(profile["k"].c_str());
  m = atoi(profile["m"].c_str());
  
  if (k <= 0) {
    cout << "parameter k is " << k << ". But k needs to be > 0." << endl;
    return -EINVAL;
  } else if ( m < 0 ) {
    cout << "parameter m is " << m << ". But m needs to be >= 0." << endl;
    return -EINVAL;
  } 
  stripe_size = k + m;
  verbose = vm.count("verbose") > 0 ? true : false;

  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  instance.disable_dlclose = true;
  stringstream messages;
  int code = instance.factory(plugin,
			      g_conf->erasure_code_dir,
			      profile, &erasure_code, &messages);
  if (code) {
    cerr << messages.str() << endl;
    return code;
  }
  if (erasure_code->get_data_chunk_count() != (unsigned int)k ||
      (erasure_code->get_chunk_count() - erasure_code->get_data_chunk_count()
       != (unsigned int)m)) {
    cout << "parameter k is " << k << "/m is " << m << ". But data chunk count is "
	 << erasure_code->get_data_chunk_count() <<"/parity chunk count is "
	 << erasure_code->get_chunk_count() - erasure_code->get_data_chunk_count() << endl;
    return -EINVAL;
  }

  return 0;
}

int ErasureCodeBench::run() {
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  instance.disable_dlclose = true;

  if (workload == "encode")
    return encode();
  else
    return decode();
}

int ErasureCodeBench::encode()
{
  bufferlist in;
  in.append(string(in_size, 'X'));
  in.rebuild_aligned(ErasureCode::SIMD_ALIGN);
  set<int> want_to_encode;
  for (int i = 0; i < k + m; i++) {
    want_to_encode.insert(i);
  }
  utime_t begin_time = ceph_clock_now(g_ceph_context);
  for (int i = 0; i < max_iterations; i++) {
    map<int,bufferlist> encoded;
    int code = erasure_code->encode(want_to_encode, in, &encoded);
    if (code)
      return code;
  }
  utime_t end_time = ceph_clock_now(g_ceph_context);
  output_lock.lock();
  cout << (end_time - begin_time) << "\t" << (max_iterations * (in_size / 1024)) << endl;
  cout.flush();
  output_lock.unlock();
  return 0;
}

int ErasureCodeBench::encode(map<int, bufferlist> *encoded)
{
  set<int> want_to_encode;
  for (int i = 0; i < k + m; i++) {
    want_to_encode.insert(i);
  }
  utime_t begin_time = ceph_clock_now(g_ceph_context);
  int code = erasure_code->encode_chunks(want_to_encode,encoded);
  if (code)
    return code;
  utime_t end_time = ceph_clock_now(g_ceph_context);
  output_lock.lock();
  cout << (end_time - begin_time) << "\t" << ((in_size / 1024)) 
       << "\t" << begin_time << "\t" << end_time << endl;
  cout.flush();
  output_lock.unlock();
  return 0;
}

static void display_chunks(const map<int,bufferlist> &chunks,
			   unsigned int chunk_count) {
  cout << "chunks ";
  for (unsigned int chunk = 0; chunk < chunk_count; chunk++) {
    if (chunks.count(chunk) == 0) {
      cout << "(" << chunk << ")";
    } else {
      cout << " " << chunk << " ";
    }
    cout << " ";
  }
  cout << "(X) is an erased chunk" << endl;
}

int ErasureCodeBench::decode_erasures(const map<int,bufferlist> &all_chunks,
				      const map<int,bufferlist> &chunks,
				      unsigned i,
				      unsigned want_erasures,
				      ErasureCodeInterfaceRef erasure_code)
{
  int code = 0;

  if (want_erasures == 0) {
    if (verbose)
      display_chunks(chunks, erasure_code->get_chunk_count());
    set<int> want_to_read;
    for (unsigned int chunk = 0; chunk < erasure_code->get_chunk_count(); chunk++)
      if (chunks.count(chunk) == 0)
	want_to_read.insert(chunk);

    map<int,bufferlist> decoded;
    code = erasure_code->decode(want_to_read, chunks, &decoded);
    if (code)
      return code;
    for (set<int>::iterator chunk = want_to_read.begin();
	 chunk != want_to_read.end();
	 ++chunk) {
      if (all_chunks.find(*chunk)->second.length() != decoded[*chunk].length()) {
	cerr << "chunk " << *chunk << " length=" << all_chunks.find(*chunk)->second.length()
	     << " decoded with length=" << decoded[*chunk].length() << endl;
	return -1;
      }
      bufferlist tmp = all_chunks.find(*chunk)->second;
      if (!tmp.contents_equal(decoded[*chunk])) {
	cerr << "chunk " << *chunk
	     << " content and recovered content are different" << endl;
	return -1;
      }
    }
    return 0;
  }

  for (; i < erasure_code->get_chunk_count(); i++) {
    map<int,bufferlist> one_less = chunks;
    one_less.erase(i);
    code = decode_erasures(all_chunks, one_less, i + 1, want_erasures - 1, erasure_code);
    if (code)
      return code;
  }

  return 0;
}

int ErasureCodeBench::decode_erasures(const map<int,bufferlist> &chunks)
{
  if (verbose)
    display_chunks(chunks, erasure_code->get_chunk_count());
  set<int> want_to_read;
  for (unsigned int chunk = 0; chunk < erasure_code->get_chunk_count(); chunk++)
    if (chunks.count(chunk) == 0)
      want_to_read.insert(chunk);

  map<int,bufferlist> decoded;
  int code = erasure_code->decode(want_to_read, chunks, &decoded);
  if (code)
    return code;
  return 0;
}

int ErasureCodeBench::decode()
{
  bufferlist in;
  in.append(string(in_size, 'X'));
  in.rebuild_aligned(ErasureCode::SIMD_ALIGN);

  set<int> want_to_encode;
  for (int i = 0; i < k + m; i++) {
    want_to_encode.insert(i);
  }

  map<int,bufferlist> encoded;
  int code = erasure_code->encode(want_to_encode, in, &encoded);
  if (code)
    return code;

  set<int> want_to_read = want_to_encode;

  if (erased.size() > 0) {
    for (vector<int>::const_iterator i = erased.begin();
	 i != erased.end();
	 ++i)
      encoded.erase(*i);
    display_chunks(encoded, erasure_code->get_chunk_count());
  }

  utime_t begin_time = ceph_clock_now(g_ceph_context);
  for (int i = 0; i < max_iterations; i++) {
    if (exhaustive_erasures) {
      code = decode_erasures(encoded, encoded, 0, erasures, erasure_code);
      if (code)
	return code;
    } else if (erased.size() > 0) {
      map<int,bufferlist> decoded;
      code = erasure_code->decode(want_to_read, encoded, &decoded);
      if (code)
	return code;
    } else {
      map<int,bufferlist> chunks = encoded;
      for (int j = 0; j < erasures; j++) {
	int erasure;
	do {
	  erasure = rand() % ( k + m );
	} while(chunks.count(erasure) == 0);
	chunks.erase(erasure);
      }
      map<int,bufferlist> decoded;
      code = erasure_code->decode(want_to_read, chunks, &decoded);
      if (code)
	return code;
    }
  }
  utime_t end_time = ceph_clock_now(g_ceph_context);
  output_lock.lock();
  cout << (end_time - begin_time) << "\t" << (max_iterations * (in_size / 1024)) << endl;
  cout.flush();
  output_lock.unlock();
  return 0;
}

int main(int argc, const char** argv) {
  ErasureCodeBench ecbench;

  // variables used for timing
  utime_t end_time;
  utime_t begin_time;
  utime_t total_time_s;
  utime_t total_run_time_s;

  try {
    int err = ecbench.setup(argc, argv);
    if (err)
      return err;
    //    return ecbench.run();
  } catch(po::error &e) {
    std::cerr THREAD_ID << e.what() << std::endl; 
    return 1;
  }
  START_TIMER; // Code for the begin_time
  if (rados_mode == 0) {
    iterations = ecbench.max_iterations;
    stripe_size = ecbench.stripe_size;
    queue_size = ecbench.queue_size;
    shard_size = ecbench.shard_size;
    in_size = ecbench.in_size;
    obj_name = ecbench.obj_name;
    pool_name = ecbench.pool_name;
    K = ecbench.k;
    M = ecbench.m;
    v_erased = ecbench.erased;
    object_sets = (long long int)iterations * (long long int)shard_size / (long long int)in_size;

    std::cout THREAD_ID << "Iterations = " << iterations << std::endl;
    std::cout THREAD_ID << "Stripe Size = " << stripe_size << std::endl;
    std::cout THREAD_ID << "Queue Size = " << queue_size << std::endl;
    std::cout THREAD_ID << "Object Size = " << in_size << std::endl;
    std::cout THREAD_ID << "Shard Size = " << shard_size << std::endl;
    std::cout THREAD_ID << "Object Sets = " << object_sets << std::endl;
    std::cout THREAD_ID << "K = " << K << std::endl;
    std::cout THREAD_ID << "M = " << M << std::endl;
    std::cout THREAD_ID << "Object Name Prefix = " << obj_name << std::endl;
    std::cout THREAD_ID << "Pool Name = " << pool_name << std::endl;

    // store the program inputs in the global vars for access by threads
    _argc = argc;
    _argv = argv;

    // Initialize the stripes list
    // Single thread here, no locking required.
    for (int i = 0;i<iterations;i++)
      stripes.push(i);
 
    // Initialize rados
    initRadosIO();
    report_thread = std::thread (reportThread);

    // Locking required now, starting threads.
    // Start the radosWriteThread
    if (ecbench.workload == "encode") {
      // Run the object bootstrap
      g_is_encoding = true;
      // Bootstrap the objects in the object store
      bsThread = std::thread (bootstrapThread);
      // Wait until there are stripe_size objects created
      while (get_objs_size() < stripe_size)
	std::this_thread::sleep_for(thread_sleep_duration);
      // Start the erasureEncodeThread. Only do one thread with Gibraltar
      for (int i = 0;i<ec_threads;i++) {
	v_ec_threads.push_back(std::thread (erasureEncodeThread, ecbench));
      }
      // Start the rados writer thread
      rados_threads.push_back(std::thread (radosWriteThread));
    } else {
      // Start the radosReadThread
      rados_threads.push_back(std::thread (radosReadThread, ecbench));
      prThread = std::thread(postReadThread);
      // Start the erasureEncodeThread. Only do one thread with Gibraltar
      for (int i = 0;i<ec_threads;i++) {
	v_ec_threads.push_back(std::thread (erasureDecodeThread, ecbench));
      }
    }

    // We should be finished with the bootstrapThread
    if (g_is_encoding)
      bsThread.join(); // This needs to finish before we start writing.

    // RADOS IO Test Here
    /*
     * Here we do write all of the objects and then wait for completion
     * after they have been dispatched.
     * We create a queue of bufferlists so we can reuse them.
     * We iterate over the procedure writing stripes of data.
     */

    FINISH_TIMER; // Compute total time since START_TIMER
    std::cout << "Program startup done." << std::endl;
    REPORT_TIMING;

    START_TIMER; // Code for the begin_time
    // the iteration loop begins here
    utime_t begin_time_final = ceph_clock_now(g_ceph_context);

    FINISH_TIMER; // Compute total time since START_TIMER
    REPORT_TIMING;

    START_TIMER; // Code for the begin_time

    /* Test for work to finish. When encoding/writing, encoding will finish first, then
     *  the writing will finish. Encoding is done when the stripes map is empty. 
     * Set the ec_done flag to true. Writing is done when the pending_buffers is empty. 
     * Set the writing_done flag to true. This causes the threads to return so they
     * can be joined by this main thread.
     * When reading/repairing, reading will finish first, then the erasure repair will
     * finish. Reading is done when the pending_buffers map is empty, set the reading_don
     * flag. Erasure repair is finished when the stripes map is empty, set the ec_done
     * flag. 
     * Shutdown logic begins here.
     */
    //    const std::chrono::milliseconds debug_sleep_duration(1000);
    //    std::this_thread::sleep_for(debug_sleep_duration);
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Shutdown: Starting test for done." << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif

    if (ecbench.workload == "encode") { // encoding/writing case

      while (!ec_done) {
	stripes_lock.lock(); // *** stripes_lock acquired ***
	if (stripes.empty())
	  ec_done = true;
	stripes_lock.unlock(); // !!! stripes_lock released !!!
	if (!ec_done)
	  std::this_thread::sleep_for(shutdown_sleep_duration);
      }
      std::vector<std::thread>::iterator ecit;
      for (ecit=v_ec_threads.begin();ecit!=v_ec_threads.end();ecit++)
	ecit->join();  // Wait for the ecThread to finish.

#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Shutdown: Done with erasure codiding." << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      /* If we get here, then all of the erasure coded buffers will be in the
       * pending_buffers map. We should not set writing_done until all of the
       * buffers have been written to the object store.
       */
      while (!writing_done) {
	if (is_pending_buffers_queue_empty())
	  writing_done = true;
#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "Shutdown: pending_buffers.size() is " 
			    << get_pending_buffers_queue_size() 
			    <<  " in Shutdown writing routine." << std::endl;
	std::cerr.flush();
	output_lock.unlock();
#endif

	if (!writing_done)
	  std::this_thread::sleep_for(shutdown_sleep_duration);
      }

#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Shutdown: Done with writing." << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      while (get_pending_ops_size() > 0) {
#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "Shutdown: completions_size is " << get_pending_ops_size() << std::endl;
	std::cerr.flush();
	output_lock.unlock();
#endif
	std::this_thread::sleep_for(shutdown_sleep_duration);
      }
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Shutdown: Done with completions." << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      completions_done = true;

      aio_done = true; // this should stop the finishing collection thread

      // These threads must wait until the ccThread and the finishingThread have terminated.
      std::vector<std::thread>::iterator rit;
      for (rit=rados_threads.begin();rit!=rados_threads.end();rit++)
	rit->join(); // wait for the rados threads to finish.
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Shutdown: Done with aio." << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif

    }
    else { 
      /* READING:
       * buffers are created in the rados reading thread, when the
       * callbacks return, the shards are put into the shards map,
       * the post read thread assembles the shards into stripes and
       * puts them into the stripes_decode_queue. Last, the EC queu
       * decodes the stripes.
       */
      while (!reading_done) {
	if (is_stripes_queue_empty())
	  reading_done = true;
	else {
#ifdef TRACE
	  output_lock.lock();
	  std::cerr THREAD_ID << "Shutdown: stripes() is " << get_stripes_queue_size() <<
	    " in reading routine." << std::endl;
	  std::cerr.flush();
	  output_lock.unlock();
#endif
	  std::this_thread::sleep_for(shutdown_sleep_duration);
	}
      }
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Shutdown: Done with reading." << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif

      // Join rados thread after everything else has stopped.
      std::vector<std::thread>::iterator rit;
      for (rit=rados_threads.begin();rit!=rados_threads.end();rit++)
	rit->join(); // wait for the rados threads to finish.

      while (get_pending_ops_size() > 0) 
	std::this_thread::sleep_for(shutdown_sleep_duration);

#ifdef VERBOSITY_1
      std::cout << "Shutdown: Pending Ops Size is: " << get_pending_ops_size() << std::endl;
#endif

      while (!is_shards_map_empty()) {
#ifdef VERBOSITY_1
	std::cout << "Shutdown: Shards Map Size is: " << get_shards_map_size() << std::endl;
#endif
	std::this_thread::sleep_for(shutdown_sleep_duration);
      }
#ifdef VERBOSITY_1
      std::cout << "Shutdown: Shards Map is empty. Setting aio_done." << std::endl;
#endif

      aio_done = true; // this should stop the post read thread
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Shutdown: Done with aio." << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif

      prThread.join();

      while (!ec_done) {
	if (is_stripes_decode_queue_empty())
	  ec_done = true;
	else {
#ifdef VERBOSITY_1
	  std::cout << "Shutdown: Stripes Decode Queue Size is: " << get_stripes_decode_queue_size() << std::endl;
#endif
	  std::this_thread::sleep_for(shutdown_sleep_duration);
	}
      }
      std::vector<std::thread>::iterator ecit;
      for (ecit=v_ec_threads.begin();ecit!=v_ec_threads.end();ecit++)
	ecit->join();  // Wait for the ecThread to finish.

#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Shutdown: Done with erasure codiding." << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
    }
    reporting_done = true;
    report_thread.join();

    // Locking not required after this point.
    std::cerr THREAD_ID << "Shutdown: Wait for all writes to flush." << std::endl;
    std::cerr.flush();
    rados.shutdown();
    utime_t end_time_final = ceph_clock_now(g_ceph_context);
    long long int total_data_processed = iterations*(ecbench.k+ecbench.m)*(ecbench.shard_size/1024);
#ifdef TRACE
    std::cout << "*** Tracing is on, output is to STDERR. ***" << std::endl;
    std::cout << "Factors for computing size: iterations: " << iterations
	      << " max_iterations: " << ecbench.max_iterations << std::endl
	      << " stripe_size: " << stripe_size << std::endl
	      << " K: " << K << std::endl
	      << " M: " << M << std::endl
	      << " Threads: " << ec_threads << std::endl
	      << " shard_size: " << shard_size << std::endl
	      << " total data processed: " << total_data_processed << " KiB"
	      << std::endl;
#endif
    std::cout << (end_time_final - begin_time_final) << "\t" << total_data_processed << " KiB\t"
	      << total_data_processed/(double)(end_time_final - begin_time_final) 
	      << " KiB/s" << std::endl;
    std::cout.flush();

    FINISH_TIMER; // Compute total time since START_TIMER
    std::cout << "Cleanup." << std::endl;
    REPORT_TIMING;
    REPORT_BENCHMARK; // Print out the elapsed time for this section
    std::cout << "Total run time " << total_run_time_s << " s" << std::endl;
  } else {
    ecbench.run();
  }
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make -j4 ceph_erasure_code_benchmark &&
 *   valgrind --tool=memcheck --leak-check=full \
 *      ./ceph_erasure_code_benchmark \
 *      --plugin jerasure \
 *      --parameter directory=.libs \
 *      --parameter technique=reed_sol_van \
 *      --parameter k=2 \
 *      --parameter m=2 \
 *      --iterations 1
 * "
 * End:
 */
