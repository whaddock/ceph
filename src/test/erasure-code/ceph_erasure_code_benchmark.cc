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

#define TRACE
#define RADOS_THREADS 1

#ifndef EC_THREADS
#define EC_THREADS 10
#endif
#define MAX_STRIPE_QUEUE_SIZE 10
#define COMPLETION_WAIT_COUNT 500
#define STRIPE_QUEUE_FACTOR 2
#define BLCTHREADS 10
#define SHUTDOWN_SLEEP_DURATION 100 // in millisecinds
#define BLQ_SLEEP_DURATION 50 // in millisecinds
#define THREAD_SLEEP_DURATION 3 // in millisecinds
#define CC_THREAD_SLEEP_DURATION 30 // in millisecinds
#define READ_SLEEP_DURATION 100 // in millisecinds
#define THREAD_ID << ceph_clock_now(g_ceph_context)  << "Thread: " << std::this_thread::get_id() << " | "
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
int queue_size = 0;
int shard_size = 0;
int stripe_size = 0;
std::string obj_name;
std::string pool_name;
int ret = 0;
bool failed = false; // Used as a flag to indicate that a failure has occurred
int rados_mode = 1; // Enable extended rados benchmarks. If false, only original erasure code benchmarks.
int _argc; // Global argc for threads to use
const char** _argv; // Global argv for threads to use
const std::chrono::milliseconds read_sleep_duration(READ_SLEEP_DURATION);
const std::chrono::milliseconds thread_sleep_duration(THREAD_SLEEP_DURATION);
const std::chrono::milliseconds cc_thread_sleep_duration(CC_THREAD_SLEEP_DURATION);
const std::chrono::milliseconds blq_sleep_duration(BLQ_SLEEP_DURATION);
const std::chrono::milliseconds shutdown_sleep_duration(SHUTDOWN_SLEEP_DURATION);
librados::Rados rados;

// Create queues, maps and iterators used by the main and thread functions.
std::queue<librados::bufferlist> blq;
std::queue<map<int,Shard>> stripes, stripes_read, stripes_decode;
list<librados::AioCompletion *> completions, finishing;
std::queue<Shard> pending_buffers,  write_buffers_waiting, finishing_buffers_waiting;
std::map<int,Shard> shards;
std::vector<std::thread> rados_threads;
std::vector<std::thread> v_ec_threads;
std::vector<std::thread> v_blc_threads;
std::thread bsThread;
std::thread ecThread;
std::thread blThread;
std::thread ccThread;
std::thread finishingThread;
bool g_is_encoding = false;
bool aio_done = false; //Becomes true at the end when we all aio operations are finished
bool completions_done = false; //Becomes true at the end when we all aio operations are completed
bool ec_done = false; //Becomes true at the end when we all ec operations are completed
bool blq_done = false; //Becomes true when all work has been queued.
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
std::mutex blq_lock;
std::mutex stripes_lock;
std::mutex stripes_read_lock;
std::mutex stripes_decode_lock;
std::mutex completions_lock;
std::mutex shards_lock;
std::mutex pending_buffers_lock;
std::mutex write_buffers_waiting_lock;
std::mutex objs_lock;
std::mutex finishing_buffers_waiting_lock;
std::mutex finishing_lock;

// Object information. We stripe over these objects.
struct obj_info {
  string name;
  size_t len;
};
map<int, obj_info> objs;

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

  return;
}

/* Function used for bootstrap thread
 */
void bootstrapThread()
{
#ifdef TRACE
  output_lock.lock();
  std::cerr THREAD_ID << "Started bootstrapThread()" << std::endl;
  std::cerr.flush();
  output_lock.unlock();
#endif

  /*
   * create an "IoCtx" which is used to do IO to a pool
   */
  librados::IoCtx io_ctx;
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
    } else {
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "we just created an ioctx for our pool" << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
    }
  }
  if (ret < 0) {
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "There was a failure. Bad return from initRadosIO.  " << ret << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
  }

  int buf_len = 1;
  bufferptr p = buffer::create(buf_len);
  bufferlist bl;
  memset(p.c_str(), 0, buf_len);
  bl.push_back(p);

#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Starting to init objects for writing..."
			<< std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
  list<librados::AioCompletion *> _completions;
  for (int index = 0; index < stripe_size; index++) {
    objs_lock.lock(); // *** Get objs lock.
    obj_info info = objs[index];
    objs_lock.unlock(); // !!! Release objs lock.
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Creating object: " << info.name
			<< std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif

    librados::AioCompletion *c = rados.aio_create_completion(NULL, NULL, NULL);
    _completions.push_back(c);
    // generate object
    ret = io_ctx.aio_write(info.name, c, bl, buf_len, info.len - buf_len);
    if (ret < 0) {
      cerr << "couldn't write obj: " << info.name << " ret=" << ret << std::endl;
    }
  }
  // Cleanup bootstrap
  while (!_completions.empty()) {
    librados::AioCompletion *c = _completions.front();
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Waiting on object to finish..." << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
    c->wait_for_complete();
    ret = c->get_return_value();
    c->release();
    _completions.pop_front();
    if (ret < 0) {
      cerr << "aio_write failed" << std::endl;
    }
  }
#ifdef TRACE
  output_lock.lock();
  std::cerr THREAD_ID << "Finished bootstrapThread(), exiting." << std::endl;
  std::cerr.flush();
  output_lock.unlock();
#endif
}

/* Function used for buffer list creation thread 
 */
void bufferListCreatorThread(ErasureCodeBench ecbench) {
  bool started = false;
  uint32_t blq_last_size = 0;
  //  uint32_t blq_max_size = ecbench.max_iterations;

  if(!started) {
    started = true;
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Starting bufferListCreatorThread()" << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
  }

  while (!blq_done) {
    if (blq_last_size < (uint32_t)queue_size) {
      librados::bufferlist bl;
      if (g_is_encoding) { // encoding/writing case
	bl.append(std::string(shard_size,(char)buffers_created_count%26+97)); // start with 'a'
      }
      blq_lock.lock(); // *** blq_lock acquired ***
      blq.push(bl);
      blq_last_size = blq.size();
      buffers_created_count++;
      blq_lock.unlock(); // !!! blq_lock released !!!
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "\tCurrently there are " << blq_last_size 
			  << " buffers in the queue."
			  << std::endl
		THREAD_ID << "\tTotal buffers created: " << buffers_created_count << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
    }
    std::this_thread::sleep_for(thread_sleep_duration);
    blq_lock.lock(); // *** blq_lock acquired ***
    blq_last_size = blq.size();
    blq_lock.unlock(); // !!! blq_lock released !!!
  }
  output_lock.lock();
  std::cerr THREAD_ID << "bufferListCreator thread, all buffers created."
		      << std::endl;
  std::cerr.flush();
  output_lock.unlock();
}

/* Collect completions as they finish */
void readCompletionsCollectionThread(ErasureCodeBench ecbench) {
  librados::AioCompletion *c = NULL;
  map<int,Shard>::iterator stripe_it;
  bool started = false;
  bool stripes_read_empty = true;
  bool processing_stripe = true;
  bool completions_empty = false;
  uint32_t completions_size = 0;
  int max_stripes_read_size = (concurrentios % ecbench.k);
  int stripes_read_size = INT_MAX;
  int stripes_decode_size = INT_MAX;

  if(!started) {
    started = true;
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Starting readCompletionsCollectionThread()" << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
  }

#ifdef TRACE
    output_lock.lock();
    if (aio_done)
      std::cerr THREAD_ID  << "In readCompletionsCollectionThread() aio_done is TRUE." << std::endl;
    else
      std::cerr THREAD_ID  << "In readCompletionsCollectionThread() aio_done is FALSE." << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif

  // Run until all erasure coding operations are finished.
  while (!aio_done) { 
    /* 1. count is monotonically increasing.
     * 2. if we are only reading, stripes will get pushed onto
     *    the stripes_read queue in the orer that they are
     *    being read.
     * 3. The stripes_read queue is a FIFO queue.
     * 4. A stripe will have K shards read. Everytime we count 
     *    K completions, a stripe is done reading and for decoding.
     */
    stripes_read_empty = true; // Prime read
    std::map<int,Shard> stripe;
    while (stripes_read_empty && !aio_done) {
      processing_stripe = false;
      stripes_read_lock.lock(); // *** Get stripes read lock ***
      stripes_read_size = stripes_read.size();
      stripes_read_empty = stripes_read.empty();
    // If stripes_read is not empty, then the following is safe.
      if (!stripes_read_empty) {
	stripe = stripes_read.front();
	stripes_read.pop();
	processing_stripe = true;
      }
      stripes_read_lock.unlock(); // !!! Release stripes read lock !!!
      if (stripes_read_empty)
	std::this_thread::sleep_for(cc_thread_sleep_duration);
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Stripes Read Queue size is: "
			  << stripes_read_size << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
    }

    if (aio_done && !processing_stripe) 
      break; // Guard in the event that aio finished while waiting for a stripe
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Starting to collect completions for a read" << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif

    uint32_t count = 0; // Starting a new stripe, need K shards
    // completions cannot be empty, not possible.
    for (;count < (uint32_t)K;count++) {
      completions_empty = true; // Prime read
      completions_lock.lock(); // *** Get completions lock ***
      completions_empty = completions.empty();
      if (!completions_empty) {
	completions_size = completions.size();
	c = completions.front();
	completions.pop_front();
      }
      completions_lock.unlock(); // !!! Release completions lock !!!
      if (completions_empty) {
#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "Completions empty for a read! EXITING" << std::endl;
	std::cerr.flush();
	output_lock.unlock();
#endif
	return; // abnormal end
      }

#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "completions_size is " << completions_size
			  << " in readCompletionsThread." << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      ret = c->wait_for_complete();
      if (ret < 0) {
	std::cerr THREAD_ID << "aio_read failed" << std::endl;
	std::cerr.flush();
      }
      else {
	c->release();
      }
    } // End of shard for loop
    /*
    for (stripe_it=stripe.begin();
	 stripe_it!=stripe.end();stripe_it++) {
      Shard shard = stripe_it->second;
      shard.dereference_bufferlist();
    }
    */
    // We have finished K shards in the stripe.
    // Limit the size of the stripes decode queue
    /*
     * Comment this out to eliminate the erasure repair thread for testing
     *
     stripes_decode_size = INT_MAX;
     while ( stripes_decode_size > max_stripes_read_size) {
     stripes_decode_lock.lock(); // *** Get stripes read lock ***
     stripes_decode_size = stripes_decode.size();
     if (stripes_decode_size <= max_stripes_read_size) {
     stripes_decode.push(stripe);
     }
     stripes_decode_lock.unlock(); // !!! Release stripes read lock !!!
     if (stripes_decode_size > max_stripes_read_size) 
     std::this_thread::sleep_for(thread_sleep_duration);
     else
     break;
     }
     #ifdef TRACE
     output_lock.lock();
     std::cerr THREAD_ID << "Just pushed stripe " << stripe.begin()->second.get_stripe()
     << " to stripes_decode."
     << std::endl;
     std::cerr THREAD_ID << "shard count for stripe: " << count << std::endl;
     std::cerr.flush();
     output_lock.unlock();
     #endif
    */
  } // while waiting on aio_done
#ifdef TRACE
  output_lock.lock();
  std::cerr THREAD_ID << "Read completions thread exiting now." << std::endl;
  std::cerr.flush();
  output_lock.unlock();
#endif

  return; // Thread terminates
} // End of readCompletionsCollectionThread

  /* Collect completions as they finish */
void writeCompletionsCollectionThread() {
  librados::AioCompletion *c;
  bool started = false;
  bool processing_completion = false;
  bool completions_empty = true;
  int completions_size = 0;
  int finishing_size = 0;

  if(!started) {
    started = true;
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Starting writeCompletionsCollectionThread()" << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
  }

  // Run until all aio operations are finished.
  while (!writing_done) { 
    completions_size = INT_MIN; // Prime read
    while (completions_size < concurrentios && !writing_done) {
      processing_completion = false;
      completions_lock.lock(); // *** Get completions lock
      completions_size = completions.size();
      completions_empty = completions.empty();
      if (completions_size >= concurrentios) {
	c = completions.front();
	completions.pop_front();
	processing_completion = true;
      }
      completions_lock.unlock(); // *** Release completions lock
      if (completions_size < concurrentios) {
	std::this_thread::sleep_for(cc_thread_sleep_duration);
      }
#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "Completions size is " << completions_size
			    << std::endl;
	std::cerr.flush();
	output_lock.unlock();
#endif
    }
    if (writing_done && !processing_completion)
      break;

    // Escape when completions are empty and aio is done.
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Waiting on a thread to complete."
			<< std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif

    if (c->is_complete()) {
      ret = c->get_return_value();
      finishing_lock.lock(); // *** Get finishing lock
      finishing.push_back(c);
      finishing_size = finishing.size();
      finishing_lock.unlock(); // *** Release finishing lock
      if (ret < 0) { // yes, we leak.
	cerr << "aio_write failed" << std::endl;
	return;
      }
      /* We have written the buffer to the object store. Push to the finishing queue
       * to wait until the operation is completed, i.e. safe.
       */
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << " AIO write completed." << std::endl;
      std::cerr THREAD_ID << "finishing_size " << finishing_size
			  << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
    }
    /* This should not loop forever, completions.size() > 0 implies
     * that there will be shards in this queue.
     */
    else {
      // try again later
      // push the shard back onto the write buffers waiting queue
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Pushing objects back to try later." << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      // push the completion object back onto the completions queue
      completions_lock.lock(); // *** Get completions lock
      completions.push_back(c);
      completions_lock.unlock(); // *** Release completions lock
    }
  }

#ifdef TRACE
  output_lock.lock();
  std::cerr THREAD_ID << "Writing done. Cleaning up the completions." << std::endl;
  std::cerr.flush();
  output_lock.unlock();
#endif
  completions_empty = false;
  while (!completions_empty) {
    processing_completion = false;
    completions_lock.lock(); // *** Get completions lock
    completions_empty = completions.empty();
    if (!completions_empty) {
      c = completions.front();
      completions.pop_front();
      processing_completion = true;
      completions_size = completions.size();
    }
    completions_lock.unlock(); // *** Release completions lock

    if (processing_completion) {
      c->wait_for_complete();
      ret = c->get_return_value();
      finishing_lock.lock(); // *** Get finishing lock
      finishing.push_back(c);
      finishing_size = finishing.size();
      finishing_lock.unlock(); // *** Release finishing lock
      if (ret < 0) { // yes, we leak.
	cerr << "aio_write failed" << std::endl;
	return;
      }
      /* We have written the buffer to the object store. Push to the finishing queue
       * to wait until the operation is completed, i.e. safe.
       */
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << " AIO write completed." << std::endl;
      std::cerr THREAD_ID << "completions_size " << completions_size
			  << std::endl;
      std::cerr THREAD_ID << "finishing_size " << finishing_size
			  << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      std::this_thread::sleep_for(thread_sleep_duration);
    }
  }
  std::this_thread::sleep_for(shutdown_sleep_duration);
#ifdef TRACE
  output_lock.lock();
  std::cerr THREAD_ID << "Write completions thread exiting now." << std::endl;
  std::cerr.flush();
  output_lock.unlock();
#endif

  return; // Thread terminates
} // End of writeCompletionsThread


void writeFinishingThread() {
  librados::AioCompletion *c;
  bool started = false;
  bool processing_completion = false;
  bool finishing_empty = true;
  int finishing_size = 0;

  if(!started) {
    started = true;
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Starting writeFinishingThread()" << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
  }

  // Run until all aio operations are finished.
  while (!writing_done) { 
    finishing_size = INT_MIN; // Prime read
    while (finishing_size < concurrentios) {
      finishing_lock.lock(); // *** Get finishing lock
      finishing_size = finishing.size();
      if (finishing_size >= concurrentios) {
	c = finishing.front();
	finishing.pop_front();
	processing_completion = true;
      }
      finishing_lock.unlock(); // *** Release finishing lock
      if (finishing_size < concurrentios) {
	std::this_thread::sleep_for(cc_thread_sleep_duration);
#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "Waiting for a completion object."
			    << std::endl;
	std::cerr.flush();
	output_lock.unlock();
#endif
      }
    }
    // Escape when finishing is empty and writing is done.
    if (writing_done && !processing_completion)
      break;

#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Waiting on a thread to finish."
			<< std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif

    c->wait_for_safe();
    ret = c->get_return_value();
    c->release();
    if (ret < 0) { // yes, we leak.
      cerr << "aio_write failed to become safe" << std::endl;
      return;
    }
    /* We have written the buffer to the object store. Dereference the buffer.
     * If there was a completion, then there is a buffer in finishing_buffers_waiting.
    */
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << " AIO write finished." << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
    processing_completion = false;
  }
  while (!aio_done) {
    processing_completion = false;
    finishing_empty = false;
    while (!finishing_empty) {
      finishing_lock.lock(); // *** Get finishing lock
      finishing_empty = finishing.empty();
      if (!finishing_empty) {
	c = finishing.front();
	finishing.pop_front();
	processing_completion = true;
	finishing_size = finishing.size();
	finishing_empty = finishing.empty();
      }
      finishing_lock.unlock(); // *** Release finishing lock

      if (processing_completion) {
	c->wait_for_safe();
	ret = c->get_return_value();
	c->release();
	if (ret < 0) { // yes, we leak.
	  cerr << "aio_write failed to become safe" << std::endl;
	  return;
	}
#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << " AIO write finished." << std::endl;
	std::cerr THREAD_ID << "finishing_size " << finishing_size
			    << std::endl;
	std::cerr.flush();
	output_lock.unlock();
#endif
      }
      else {
#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "Finishing buffers are empty. Sleeping." << std::endl;
	std::cerr.flush();
	output_lock.unlock();
#endif
	std::this_thread::sleep_for(cc_thread_sleep_duration);
      }
      std::this_thread::sleep_for(thread_sleep_duration);
    }
  }
  std::this_thread::sleep_for(shutdown_sleep_duration);
#ifdef TRACE
  output_lock.lock();
  std::cerr THREAD_ID << "Write finishing thread exiting now." << std::endl;
  std::cerr.flush();
  output_lock.unlock();
#endif

  return; // Thread terminates
} // End of writeFinishingThread

/* Function used to perform erasure encoding.
 */
void erasureEncodeThread(ErasureCodeBench ecbench) {
  bool started = false;
  bool stripes_empty = true;
  int ret = 0;
  int pending_buffers_size = INT_MAX;
  Shard shard;
  map<int,Shard> stripe;
  map<int,Shard>::iterator stripe_it;
  map<int,librados::bufferlist> encoded;

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
    stripes_empty = true; // Prime read
    while (stripes_empty) {
      stripes_lock.lock(); // *** stripes_lock acquired ***
      stripes_empty = stripes.empty();
      if (!stripes_empty) {
	stripe = stripes.front();
	stripes.pop();
      }
      stripes_lock.unlock(); // !!! stripes_lock released !!!
      if (stripes_empty) {
#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "Waiting for a stripe" << std::endl;
	std::cerr.flush();
	output_lock.unlock();
#endif
	std::this_thread::sleep_for(thread_sleep_duration);
      }
    }
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Encoding a stripe" << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
    for (stripe_it=stripe.begin();
	 stripe_it!=stripe.end();stripe_it++) {
      encoded.insert(pair<int,librados::bufferlist>(stripe_it->second.get_shard(),stripe_it->second.get_bufferlist()));
    }
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Stripe " <<  stripe.begin()->second.get_stripe() << " calling encode in erasureCodeThread()" << 
      std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
    ret = ecbench.encode(&encoded);
    pending_buffers_size = INT_MAX;
    while (pending_buffers_size > ecbench.queue_size) {
      pending_buffers_lock.lock(); // *** pending_buffers_lock acquired ***
      pending_buffers_size = pending_buffers.size();
      if  (pending_buffers_size <= ecbench.queue_size) {
	for (stripe_it=stripe.begin();stripe_it!=stripe.end();stripe_it++) {
	  pending_buffers.push(stripe_it->second);
#ifdef TRACE
	  output_lock.lock();
	  std::cerr THREAD_ID << "Pushed buffer " << stripe_it->second.get_hash() <<
	    " to pending_buffer_queue."  << std::endl;
	  std::cerr.flush();
	  output_lock.unlock();
#endif
	}
      }
      pending_buffers_lock.unlock(); // !!! pending_buffers_lock released !!!
      if (pending_buffers_size > ecbench.queue_size) {
	std::this_thread::sleep_for(thread_sleep_duration);
      }
    }
    pending_buffers_lock.unlock(); // !!! pending_buffers_lock released !!!
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Encoding done, buffers inserted, in erasureCodeThread()." << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
    encoded.clear();
    if (ret < 0) {
      output_lock.lock();
      std::cerr << "Error in erasure code call to ecbench. " << ret << std::endl;
      std::cerr.flush();
      output_lock.unlock();
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
  bool stripes_decode_empty = true;
  int ret = 0;
  uint32_t stripes_decode_size = 0;
  Shard shard;
  map<int,Shard> stripe;
  map<int,Shard>::iterator stripe_it;
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
    stripes_decode_empty=true;
    while (stripes_decode_empty && !ec_done) {
      stripes_decode_lock.lock(); // *** stripes_lock acquired ***
      stripes_decode_empty = stripes_decode.empty();
      if (!stripes_decode.empty()) {
	stripe = stripes_decode.front();
	stripes_decode.pop();
	stripes_decode_size = stripes_decode.size();
      }
      stripes_decode_lock.unlock(); // !!! stripes_lock released !!!
      if (stripes_decode_empty) 
      std::this_thread::sleep_for(thread_sleep_duration);
    }

    if (ec_done)
	break;
    // We have a stripe to repair.
    for (stripe_it=stripe.begin();
	 stripe_it!=stripe.end();stripe_it++) {
      encoded.insert(pair<int,librados::bufferlist>(stripe_it->second.get_shard(),stripe_it->second.get_bufferlist()));
    }
    //    ret = ecbench.encode(&encoded);
    ret = 0;
    /* For now, we are just recreating the M parity shards, worst case. */
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
    std::cerr THREAD_ID << "Decoding done, buffers discarded, in erasureDeodeThread()."
			<< std::endl;
    std::cerr THREAD_ID << "stripes_decode_size: " << stripes_decode_size << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
    //    encoded.clear();
    /* Need to release the stripe memory. We are just measuring the time to repair
     * the stripe. Now we can release the resources*/
    //    stripe.clear();

  }
#ifdef TRACE
  output_lock.lock();
  std::cerr THREAD_ID << "erasureDecodeThread exiting now." << std::endl;
  std::cerr.flush();
  output_lock.unlock();
#endif

  return; // Thread terminates
}

/* @writing_done means that the rados has finished writing shards/objects.
 */
void radosWriteThread() {
  bool started = false;
  bool processing_buffer = false;
  bool pending_buffers_empty = true;
  librados::IoCtx io_ctx;
  int ret;
  int completions_size = INT_MAX;
  int pending_buffers_size = INT_MAX;
  uint32_t shard_counter = 0;
  uint32_t stripe = 0;
  uint32_t index = 0;
  uint32_t offset = 0;
  string name;

  if(!started) {
    started = true;
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Starting radosWriteThread()" << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif

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
  }

  // Write loop 
  while (!writing_done) {
    // wait for the request to complete, and check that it succeeded.
    Shard shard;

    pending_buffers_empty = true; // Prime read
    while (pending_buffers_empty && !writing_done) {
      processing_buffer = false;
      pending_buffers_lock.lock();  // *** pending_buffers_lock ***
      pending_buffers_empty = pending_buffers.empty();
      if (!pending_buffers_empty) {
	/* This code only executes if there are any Shards in pending_buffers.
	 * Buffers are insereted into the pending_buffers queue in order from
	 * the erasure encoded stripe. The write_buffers_waiting queue gets the
	 * shards after we write them so they can be accessed from the completions
	 * collector thread.
	 */
	shard = pending_buffers.front();
	pending_buffers.pop();
	pending_buffers_size = pending_buffers.size();
	processing_buffer = true;
      }
      pending_buffers_lock.unlock(); // !!! pending_buffers_lock released !!!
      if (pending_buffers_empty)
	std::this_thread::sleep_for(thread_sleep_duration);
    }
    if (writing_done && !processing_buffer)
      break;

    stripe = shard_counter / stripe_size;
    offset = stripe * shard_size;
    index = shard_counter % stripe_size;
    objs_lock.lock(); // *** Get objs lock.
    name = objs[index].name;
    objs_lock.unlock(); // !!! Release objs lock.
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID  << "pbit: pending_buffers size (" << pending_buffers_size
			 << ")" << std::endl;
    std::cerr THREAD_ID  << "pbit: Writing " << shard.get_object_name()
			 << " Hash: " << shard.get_hash()
			 << " Shard Object stripe position: " << shard.get_shard()
			 << " Shard Object stripe number: " << shard.get_stripe()
			 << " to storage." << std::endl;
    std::cerr THREAD_ID  << "shard: " << shard_counter << " stripe: " << stripe
			 << " offset: " << offset << " ObjectID: "
			 << name << std::endl;
    std::cerr THREAD_ID << "index: " << shard.get_hash()
			<< " In do_write. Pending buffer erased in radosWriteThread()"
			<< std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
    librados::AioCompletion *c = rados.aio_create_completion(NULL, NULL, NULL);

    librados::bufferlist _bl = shard.get_bufferlist();
    ret = io_ctx.aio_write(name,c,_bl,shard_size,offset);
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
			  << shard.get_hash() << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      ret = EXIT_FAILURE;
      failed = true; 
      // We have had a failure, so do not execute any further, 
      // fall through.
    }
    completions_lock.lock(); // *** Get completion lock
    completions.push_back(c);
    completions_size = completions.size();
    completions_lock.unlock(); // *** Release completion lock

#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "buffer: " << shard.get_hash()
			<< " pushed to write_buffers_waiting queue in radosWriteThread()"
			<< std::endl;
    std::cerr THREAD_ID << "we wrote object "
			<< shard.get_object_name() 
			<< std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
    shard_counter += 1; // The next shard we will do.

    /* throttle...
     * This block causes the write thread to wait until the number
     * of outstanding AIO completions is below the number of 
     * concurrentios that was set in the configuration. Since
     * the completions queue and the shards_in_flight queue are
     * a bijection, we are assured of dereferencing the corresponding
     * buffer once the write has completed.
     */
    uint32_t count = 0;
    while (!writing_done && completions_size > concurrentios && count++ < 2) {
      std::this_thread::sleep_for(thread_sleep_duration);
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Waiting while completions_size > concurrentios "
			  << completions_size << " < " << concurrentios
			  << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      completions_lock.lock(); // *** Get completions lock ***
      completions_size = completions.size();
      completions_lock.unlock(); // !!! completions lock released !!!
    }
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "completions_size is " << completions_size << " in radosReadThread."
			<< std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
  }   // End Write loop

  // Write thread cannot terminate until the completions
  // queue is empty, the rados instance is required to
  // keep the continuation objects in scope.
  int finishing_size = INT_MAX;
  while (!aio_done) {
    std::this_thread::sleep_for(blq_sleep_duration);
#ifdef TRACE
    completions_lock.lock(); // *** Get completions lock ***
    completions_size = completions.size();
    completions_lock.unlock(); // !!! completions lock released !!!
    finishing_lock.lock(); // *** Get completions lock ***
    finishing_size = finishing.size();
    finishing_lock.unlock(); // !!! completions lock released !!!
    output_lock.lock();
    std::cerr THREAD_ID << "completions_size is " << completions_size << " in radosReadThread."
			<< std::endl;
    std::cerr THREAD_ID << "Waiting while finishing queue is not empty "
			<< finishing_size << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
  }

  io_ctx.aio_flush();
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
  bool stripes_empty = true;
  bool stripes_read_empty = true;
  bool processing_stripe = true;

  librados::IoCtx io_ctx;
  int ret;
  int index = 0;
  int max_stripes_read_size = (concurrentios % ecbench.k);
  int stripes_read_size = INT_MAX;
  // For this thread
  uint32_t stripe = 0;
  uint32_t offset = 0;
  uint32_t completions_size = 0;
  map<int,Shard> stripeM;
  map<int,Shard>::iterator stripeM_it;
  Shard shard;

  if(!started) {
    started = true;
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Starting radosReadThread()" << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif

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
  }

#ifdef TRACE
    output_lock.lock();
    if (reading_done)
      std::cerr THREAD_ID  << "In radosReadThread() reading_done is TRUE." << std::endl;
    else
      std::cerr THREAD_ID  << "In radosReadThread() reading_done is FALSE." << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif

  // Read loop 
  while (!aio_done) {
    // wait for the request to complete, and check that it succeeded.
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID  << "In radosReadThread() outer while loop." << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
    stripes_empty = true;
    while (stripes_empty && !aio_done) {
      processing_stripe = false;
      stripes_lock.lock();  // *** stripes_lock ***
      stripes_empty = stripes.empty();
      if (!stripes_empty) {
	// Pop a stripe off of the queue
	stripeM = stripes.front();
	stripes.pop(); 
	processing_stripe = true;
	stripes_that_remain = stripes.size();
      }
      stripes_lock.unlock(); // !!! stripes_lock released !!!
      if (stripes_empty)
	std::this_thread::sleep_for(read_sleep_duration);
    }
    if (aio_done && !processing_stripe)
      break;
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID  << "Got a stripe. Stripes size: " << stripes_that_remain
			 << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif

    /* This code only executes if a stripe was found in the stripes queue. */
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Starting to read stripe " << stripeM.begin()->second.get_stripe()
			<< " in radosReadThread." << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
    // throttle... *** We need to do this differently because we must repair the stripe
    // Completions are being handled by the readCompletionsThread.
    completions_size = UINT_MAX; // Prime read.
    while (completions_size > (uint32_t)concurrentios) {
      completions_lock.lock(); // *** Get completions lock ***
      completions_size = completions.size();
      completions_lock.unlock(); // !!! completions lock released !!!
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "completions_size is " << completions_size << " in radosReadThread." << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      if (completions_size > (uint32_t)concurrentios)
	std::this_thread::sleep_for(thread_sleep_duration);
    }
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "completions_size < conncurrentios in radosReadThread." << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif

    /* We only want to readk K shards determined by the
     * input parameters. Then we wait in the readCompletionsCollectionThread for
     * the reads to complete before passing them on to the erasure code thread.
     */
    uint32_t count = 0;
    // Iterate over all K + M shards in the stripe.
    for (stripeM_it=stripeM.begin();
	 stripeM_it!=stripeM.end();stripeM_it++) {
      shard = stripeM_it->second;
      bool is_erased = false;
      obj_info info;
      count++;
      stripe = shard.get_stripe();
      offset = stripe * shard_size;
      index = shard.get_shard() % stripe_size;
      objs_lock.lock(); // *** Get objs lock.
      info = objs[index];
      objs_lock.unlock(); // !!! Release objs lock.
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID  << "RTHREAD: Reading " << shard.get_object_name()
			   << " from storage." << std::endl;
      objs_lock.lock(); // *** Get objs lock.
      std::cerr THREAD_ID  << "shard: " << index << " stripe: " << stripe
			   << " offset: " << offset << " ObjectID: "
			   << info.name << std::endl;
      objs_lock.unlock(); // !!! Release objs lock.
      std::cerr.flush();
      output_lock.unlock();
#endif
      for (int n : v_erased)
	if (index == n)
	  is_erased = true;
      // We only need to read K shards. Prefer data shards, i.e. leading elements of map.
      if (!is_erased && count <= (uint32_t)K) {
	librados::AioCompletion *c = rados.aio_create_completion();

	librados::bufferlist* _bl = shard.get_bufferlist_ptr();
	ret = io_ctx.aio_read(info.name,c,_bl,shard_size,(uint64_t)offset);
	shard.set_read(); // flag as being read
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
			      << shard.get_hash() << std::endl;
	  std::cerr.flush();
	  output_lock.unlock();
#endif
	  ret = EXIT_FAILURE;
	  failed = true; 
	  // We have had a failure, so do not execute any further, 
	  // fall through.
	}
	completions_lock.lock(); // *** Get completions lock ***
	completions.push_back(c);
	completions_lock.unlock(); // !!! completions lock released !!!
      }
    } // End of iteration over shards in the stripe
    // Keep the number of stripes less than MAX_STRIPE_QUEUE_SIZE

    stripes_read_size = INT_MAX;
    while (stripes_read_size > max_stripes_read_size) {
      stripes_read_lock.lock(); // *** Get stripes read 
      stripes_read_size = stripes_read.size();
      if (stripes_read_size <= max_stripes_read_size) {
	// Put the stripe in the stripes_read queue
	stripes_read.push(stripeM);
	stripes_read_size = stripes_read.size();
      }
      stripes_read_lock.unlock(); // *** Release stripes read lock
      if (stripes_read_size > max_stripes_read_size)
	std::this_thread::sleep_for(thread_sleep_duration);
    }
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "hash: " << shard.get_hash() << " stripe read started in radosReadThread()"
			<< std::endl;
    std::cerr THREAD_ID << "we are reading "
			<< shard.get_object_name() 
			<< std::endl;
    std::cerr THREAD_ID << "Stripes Read Queue Size: " << stripes_read_size
			<< std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
  }   // End Read loop

#ifdef TRACE
  output_lock.lock();
  std::cerr THREAD_ID << "Read thread exiting now." << std::endl;
  std::cerr.flush();
  output_lock.unlock();
#endif

  return; // Thread terminates
}


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
  int blq_last_size = 0;

  // variables used for timing
  utime_t end_time;
  utime_t begin_time;
  utime_t total_time_s;
  utime_t total_run_time_s;
  int stripe_queue_size = INT_MAX;

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
    obj_name = ecbench.obj_name;
    pool_name = ecbench.pool_name;
    K = ecbench.k;
    M = ecbench.m;
    v_erased = ecbench.erased;
    std::cout THREAD_ID << "Iterations = " << iterations << std::endl;
    std::cout THREAD_ID << "Stripe Size = " << stripe_size << std::endl;
    std::cout THREAD_ID << "Queue Size = " << queue_size << std::endl;
    std::cout THREAD_ID << "Object Size = " << shard_size << std::endl;
    std::cout THREAD_ID << "Object Name Prefix = " << obj_name << std::endl;
    std::cout THREAD_ID << "Pool Name = " << pool_name << std::endl;

    int max_stripe_queue_size = ec_threads * STRIPE_QUEUE_FACTOR;

    // store the program inputs in the global vars for access by threads
    _argc = argc;
    _argv = argv;

    // Create the data structure for the objects we will use
    for (int index = 0; index < stripe_size; index++) {
      obj_info info;
      std::stringstream object_name;
      object_name << obj_name << "." << index;
      info.name = object_name.str();
      info.len = iterations * shard_size;
      objs_lock.lock(); // *** Get objs lock.
      objs[index] = info;
      objs_lock.unlock(); // !!! Release objs lock.
    }
 
   // Start the bufferListCreatorThread
    for (int i=0;i<BLCTHREADS;i++) 
      v_blc_threads.push_back(std::thread (bufferListCreatorThread, ecbench));

    // Initialize rados
    initRadosIO();

    // Start the radosWriteThread
    if (ecbench.workload == "encode") {
      // Run the object bootstrap
      g_is_encoding = true;
      // Bootstrap the objects in the object store
      bsThread = std::thread (bootstrapThread);
      // Start the erasureEncodeThread. Only do one thread with Gibraltar
      for (int i = 0;i<ec_threads;i++) {
	v_ec_threads.push_back(std::thread (erasureEncodeThread, ecbench));
      }
      // Start the rados writer thread
      rados_threads.push_back(std::thread (radosWriteThread));
      // Start the writeCompletionsCollectionThread
      ccThread = std::thread (writeCompletionsCollectionThread);
      finishingThread = std::thread (writeFinishingThread);
    } else {
      // Start the radosReadThread
      rados_threads.push_back(std::thread (radosReadThread, ecbench));
      // Start the erasureEncodeThread. Only do one thread with Gibraltar
      /*
      for (int i = 0;i<ec_threads;i++) {
	v_ec_threads.push_back(std::thread (erasureDecodeThread, ecbench));
	} */
      // Start the readCompletionsCollectionThread
      ccThread = std::thread (readCompletionsCollectionThread, ecbench);
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
    for (int j=0;j<iterations||failed;j++) {
      int index = 0;
      stripes_lock.lock(); // *** stripes_lock acquired ***
      stripe_queue_size = stripes.size();
      stripes_lock.unlock(); // !!! stripes_lock released !!!
      while (stripe_queue_size > max_stripe_queue_size)
	{
	  std::this_thread::sleep_for(thread_sleep_duration);
	  stripes_lock.lock(); // *** stripes_lock acquired ***
	  stripe_queue_size = stripes.size();
	  stripes_lock.unlock(); // !!! stripes_lock released !!!
	}
      while (blq_last_size < stripe_size) {
#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "Waiting for bufferlist queue to get enough buffers" << std::endl
			    << "Currently there are " << blq_last_size << " buffers in the queue."
			    << std::endl;
	std::cerr.flush();
	output_lock.unlock();
#endif
	std::this_thread::sleep_for(blq_sleep_duration);
	blq_lock.lock(); // *** blq_lock acquired ***
	blq_last_size = blq.size();
	blq_lock.unlock(); // !!! blq_lock released !!!
      }
      std::map<int,Shard> stripe;
      for (int i=0;i<stripe_size;i++) {
	index = j*stripe_size+i; // index == data.get_hash()
	std::stringstream object_name;
	object_name << obj_name << "." << index;
	Shard data(j,i,stripe_size,object_name.str());
	assert(index==data.get_hash()); // disable assert checking by defining #NDEBUG
	blq_lock.lock(); // *** blq_lock acquired ***
	data.set_bufferlist(blq.front());
	stripe.insert( std::pair<int,Shard>(i,data));
	blq.pop(); // remove the bl from the queue
	blq_last_size = blq.size();
	blq_lock.unlock(); // !!! blq_lock released !!!
#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "Created a Shard for encoding with hash value " << data.get_hash() << 
	  std::endl;
	std::cerr.flush();
	output_lock.unlock();
#endif
      } 
      stripes_lock.lock(); // *** stripes_lock acquired ***
      stripes.push(stripe);
      stripe_queue_size = stripes.size();
      stripes_lock.unlock(); // !!! stripes_lock released !!!
#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "Stripe Queue Size: " << stripe_queue_size
			    << std::endl;
	std::cerr.flush();
	output_lock.unlock();
#endif
    }

    FINISH_TIMER; // Compute total time since START_TIMER
    std::cout << "All work queued." << std::endl;
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

    // All work is done, Bufferlist creator thread is no longer needed.
    blq_done = true;
    std::vector<std::thread>::iterator blcit;
    for (blcit=v_blc_threads.begin();blcit!=v_blc_threads.end();blcit++)
      blcit->join(); // This should have finished at the beginning.
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Shutdown: bufferListThreads have stopped." << std::endl;
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
	pending_buffers_lock.lock(); // *** pending_buffers_lock acquired ***
	if (pending_buffers.empty())
	  writing_done = true;
	pending_buffers_lock.unlock(); // !!! pending_buffers_lock released !!!
#ifdef TRACE
	pending_buffers_lock.lock(); // *** pending_buffers_lock acquired ***
	int pending_buffers_size = pending_buffers.size();
	pending_buffers_lock.unlock(); // !!! pending_buffers_lock released !!!

	output_lock.lock();
	std::cerr THREAD_ID << "Shutdown: pending_buffers.size() is " << pending_buffers_size <<
	  " in Shutdown writing routine." << std::endl;
	std::cerr.flush();
	output_lock.unlock();
#endif

	if (!writing_done)
	  std::this_thread::sleep_for(shutdown_sleep_duration);
      }

#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Shutdown:Done with writing." << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      int completions_size = INT_MAX;
      while (completions_size > 0) {
	completions_lock.lock(); // *** Get completions lock ***
	completions_size = completions.size();
	completions_lock.unlock(); // !!! completions lock released !!!
#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "Shutdown: completions_size is " << completions_size << std::endl;
	std::cerr.flush();
	output_lock.unlock();
#endif
	if (completions_size > 0)
	  std::this_thread::sleep_for(shutdown_sleep_duration);
      }
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Shutdown:Done with completions." << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      completions_done = true;

      int finishing_size = INT_MAX;
      while (finishing_size > 0) {
	finishing_lock.lock(); // *** Get finishing lock ***
	finishing_size = finishing.size();
	finishing_lock.unlock(); // !!! finishing lock released !!!
#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "Shutdown: finishing_size is " << finishing_size << std::endl;
	std::cerr.flush();
	output_lock.unlock();
#endif
	if (finishing_size > 0)
	  std::this_thread::sleep_for(shutdown_sleep_duration);
      }

#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Shutdown:Done with finishing." << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      aio_done = true; // this should stop the finishing collection thread
      ccThread.join(); // All objects flushed.
      finishingThread.join(); // All objects safe.

      // These threads must wait until the ccThread and the finishingThread have terminated.
      std::vector<std::thread>::iterator rit;
      for (rit=rados_threads.begin();rit!=rados_threads.end();rit++)
	rit->join(); // wait for the rados threads to finish.
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Shutdown:Done with aio." << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif

    }
    else {  // must be decoding and reading
      /* READING: TODO: rewrite this If we get here, then all of the erasure coded buffers will be in the
       * pending_buffers map. We should not set writing_done until all of the
       * buffers have been written to the object store.
       */
      while (!reading_done) {
	stripes_lock.lock();  // *** stripes_lock ***
	stripes_that_remain = stripes.size();
	stripes_lock.unlock(); // !!! stripes lock released !!!
	if (stripes_that_remain < 1) 
	  reading_done = true;
#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "Shutdown: stripes() is " << stripes_that_remain <<
	  " in reading routine." << std::endl;
	std::cerr.flush();
	output_lock.unlock();
#endif

	if (!reading_done)
	  std::this_thread::sleep_for(shutdown_sleep_duration);
      }

#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Shutdown:Done with reading." << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif

      int completions_size = INT_MAX;
      while (completions_size > 0) {
	completions_lock.lock(); // *** Get completions lock ***
	completions_size = completions.size();
	completions_lock.unlock(); // !!! completions lock released !!!
	if (completions_size > 0)
	  std::this_thread::sleep_for(shutdown_sleep_duration);
      }
      aio_done = true; // this should stop the completions collection thread
      ccThread.join(); // All objects flushed.
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Shutdown:Done with aio." << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      /*
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
      */
      // Join rados thread after everything else has stopped.
      std::vector<std::thread>::iterator rit;
      for (rit=rados_threads.begin();rit!=rados_threads.end();rit++)
	rit->join(); // wait for the rados threads to finish.
    }

    // Locking not required after this point.
    std::cerr THREAD_ID << "Shutdown: Wait for all writes to flush." << std::endl;
    std::cerr.flush();
    utime_t end_time_final = ceph_clock_now(g_ceph_context);
    long long int total_data_processed = iterations*(ecbench.k+ecbench.m)*(ecbench.shard_size/1024);
#ifdef TRACE
    std::cout << "*** Tracing is on, output is to STDERR. ***" << std::endl;
    std::cout << "Factors for computing size: iterations: " << iterations
	      << " max_iterations: " << ecbench.max_iterations << std::endl
	      << " stripe_size: " << stripe_size 
	      << " shard_size: " << shard_size 
	      << " total data processed: " << total_data_processed << " KiB"
	      << std::endl;
#endif
    std::cout << (end_time_final - begin_time_final) << "\t" << total_data_processed << " KiB\t"
	      << total_data_processed/(double)(end_time_final - begin_time_final) 
	      << " KiB/s" << std::endl;
    std::cout.flush();

    // Print out the stripe object hashes and clear the stripe_data map
    while (!blq.empty())
      blq.pop();
    for (std::map<int,Shard>::iterator shards_it = shards.begin();
	 shards_it!=shards.end();shards_it++) {
      Shard data = shards_it->second;
#ifdef TRACE
      std::cerr THREAD_ID << "Deleting Stripe object " << data.get_object_name()
			  << " with hash value "
			  << data.get_hash() << std::endl;
      std::cerr.flush();
#endif
    }
    // Done with rados connection
    rados.shutdown();
    ret = failed ? ret:EXIT_SUCCESS;

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
