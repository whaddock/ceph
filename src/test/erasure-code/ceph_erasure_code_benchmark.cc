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
#include <ctime>
#include <stdio.h>
#include <deque>
#include <queue>
#include <map>
#include <thread>
#include <mutex>
#include <cassert>
#include <chrono>

#define RADOS_THREADS 1

#ifndef EC_THREADS
#define EC_THREADS 10
#endif

#define OPS_FACTOR 2
#define BLQ_SLEEP_DURATION 100 // in millisecinds
#define THREAD_SLEEP_DURATION 25 // in millisecinds
#define THREAD_ID  << "Thread: " << std::this_thread::get_id() << " | "
#define START_TIMER begin_time = ceph_clock_now(g_ceph_context);
#define FINISH_TIMER end_time = ceph_clock_now(g_ceph_context);	\
  total_time_ms = (end_time - begin_time);
#define REPORT_TIMING std::cout << total_time_ms  << " ms\t" << end_time << std::endl;
#define REPORT_BENCHMARK std::cout << total_time_ms  << " ms\t" << ((double)stripe_size * (double)shard_size) / (1024*1024) << " MB\t" \
  << ((double)stripe_size * (double)shard_size) / (double)(1024*total_time_ms) \
  << " MB/s\t" << std::endl; \
  total_run_time_ms += total_time_ms;

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
const std::chrono::milliseconds thread_sleep_duration(THREAD_SLEEP_DURATION);
const std::chrono::milliseconds blq_sleep_duration(BLQ_SLEEP_DURATION);
librados::Rados rados;

// Create queues, maps and iterators used by the main and thread functions.
std::queue<librados::bufferlist> blq;
std::queue<map<int,Shard>> stripes;
std::queue<map<int,Shard>> stripes_read;
std::queue<map<int,Shard>> stripes_decode;
list<librados::AioCompletion *> completions;
std::queue<Shard> pending_buffers;
std::map<int,Shard> shards;
std::vector<std::thread> rados_threads;
std::vector<std::thread> v_ec_threads;
std::thread ecThread;
std::thread blThread;
std::thread ccThread;
bool is_encoding = false;
bool ec_done = false; //Becomes true at the end when we all ec operations are completed
bool reading_done = false; //Becomes true at the end when we all stripes in the stripes_decode queue are repaired.
bool writing_done = false; //Becomes true at the end when we all objects in pending_buffer are written
int pending_buffers_that_remain = 0;
int stripes_that_remain = 0;
int concurrentios = RADOS_THREADS;
int ec_threads = EC_THREADS;
int blq_last_size = 0;
int K = 0; // Number of data shards
int M = 0; // Number of erasure code shards
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
    output_lock.unlock();
#endif
    ret = EXIT_FAILURE;
  } else {
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "we just set up a rados cluster object" << std::endl;
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
      output_lock.unlock();
#endif
      ret = EXIT_FAILURE;
    } else {
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "we just parsed our config options" << std::endl;
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
      output_lock.unlock();
#endif
      ret = EXIT_FAILURE;
    } else {
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "we just connected to the rados cluster" << std::endl;
      output_lock.unlock();
#endif
    }
  }

  return;
}

/* Function used for bootstrap thread
 */
void bootstrap()
{
#ifdef TRACE
  output_lock.lock();
  std::cerr THREAD_ID << "Starting bootstrapThread()" << std::endl;
  std::cerr.flush();
  output_lock.unlock();
#endif
  int index;

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
      output_lock.unlock();
#endif
      ret = EXIT_FAILURE;
    } else {
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "we just created an ioctx for our pool" << std::endl;
      output_lock.unlock();
#endif
    }
  }
  if (ret < 0) {
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "There was a failure. Bad return from initRadosIO.  " << ret << std::endl;
    output_lock.unlock();
#endif
  }

  int buf_len = 1;
  bufferptr p = buffer::create(buf_len);
  bufferlist bl;
  memset(p.c_str(), 0, buf_len);
  bl.push_back(p);

  list<librados::AioCompletion *> _completions;
  for (index = 0; index < stripe_size; index++) {
    obj_info info;
    std::stringstream object_name;
    object_name << obj_name << "." << index;
    info.name = object_name.str();
    info.len = iterations * shard_size;
    uint32_t max_ops = stripe_size * OPS_FACTOR;

    // throttle...
    while (_completions.size() > max_ops) {
      librados::AioCompletion *c = _completions.front();
      c->wait_for_complete();
      ret = c->get_return_value();
      c->release();
      _completions.pop_front();
      if (ret < 0) {
	cerr << "aio_write failed" << std::endl;
      }
    }

    librados::AioCompletion *c = rados.aio_create_completion(NULL, NULL, NULL);
    _completions.push_back(c);
    // generate object
    ret = io_ctx.aio_write(info.name, c, bl, buf_len, info.len - buf_len);
    if (ret < 0) {
      cerr << "couldn't write obj: " << info.name << " ret=" << ret << std::endl;
    }
    objs[index] = info;
  }
  // Cleanup bootstrap
  while (!_completions.empty()) {
    librados::AioCompletion *c = _completions.front();
    c->wait_for_complete();
    ret = c->get_return_value();
    c->release();
    _completions.pop_front();
    if (ret < 0) {
      cerr << "aio_write failed" << std::endl;
    }
  }
}

/* Function used for buffer list creation thread 
 */
void bufferListCreatorThread() {
  bool started = false;
  uint32_t count = 0;

  if(!started) {
    started = true;
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Starting bufferListCreatorThread()" << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif

  }
  blq_last_size = 0; // there are not any yet
  while (!ec_done) {
    while (blq_last_size < queue_size) {
      librados::bufferlist bl;
      if (is_encoding) { // encoding/writing case
	bl.append(std::string(shard_size,(char)count++%26+97)); // start with 'a'
      }
      else {
	bl.append(std::string(shard_size,(char)20)); // Buffers for read, use space character
      }
      blq_lock.lock(); // *** blq_lock acquired ***
      blq.push(bl);
      blq_last_size = blq.size();
      blq_lock.unlock(); // !!! blq_lock released !!!
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "\tCurrently there are " << blq_last_size 
			  << " buffers in the queue."
			  << std::endl
			  << "\tTotal buffers created: " << count << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      std::this_thread::sleep_for(thread_sleep_duration);
    }
    output_lock.lock();
    std::cerr THREAD_ID << "Stopping ec thread, ec_done is true."
			<< std::endl;
    std::cerr.flush();
    output_lock.unlock();
  }
}

/* Collect completions as they finish */
void completionsCollectionThread() {
  librados::AioCompletion *c;
  uint32_t count = 0;
  std::map<int,Shard> stripe;
  bool started = false;
  bool waiting_on_stripe = false;

  if(!started) {
    started = true;
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Starting completionsCollectionThread()" << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
  }

  // Run until all erasure coding operations are finished.
  while (!ec_done) { 
    /* 1. count is monotonically increasing.
     * 2. if we are only reading, stripes will get pushed onto
     *    the stripes_read queue as they are being read.
     * 3. The stripes_read queue is a FIFO queue.
     * 4. Everytime we count K completions, a stripe is
     *    done reading and ready for decoding.
     */
    waiting_on_stripe = false;
    stripes_read_lock.lock(); // *** Get stripes read lock
    if (!stripes_read.empty()) {
      waiting_on_stripe = true;
      stripe = stripes_read.front();
      stripes_read.pop();
    }
    stripes_read_lock.unlock(); // *** Release stripes read lock

    count = 0;
    while(waiting_on_stripe && !completions.empty() 
	  && count <= (uint32_t)K) {
      completions_lock.lock(); // *** Get completions lock
      c = completions.front();
      completions.pop_front();
      completions_lock.unlock(); // *** Release completions lock

      c->wait_for_complete();
      ret = c->get_return_value();
      c->release();
      count++;
      if (ret < 0) {
	cerr << "aio_write failed" << std::endl;
	return;
      }
    }
    if (waiting_on_stripe) {
      stripes_decode_lock.lock(); // *** Get stripes decode lock
      stripes_decode.push(stripe);
      stripes_decode_lock.unlock(); // *** Release stripes decode lock
    }
  }
} // End of completionsCollectionThread

/* Function used to perform erasure encoding.
 */
void erasureEncodeThread(ErasureCodeBench ecbench) {
  bool started = false;
  int ret = 0;
  Shard shard;
  map<int,Shard> stripe;
  map<int,Shard>::iterator stripe_it;
  map<int,librados::bufferlist> encoded;
  map<int,librados::bufferlist>::iterator shard_it;

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
    bool do_stripe = false;
    stripes_lock.lock(); // *** stripes_lock acquired ***
    if (!stripes.empty()) {
      do_stripe = true;
      stripe = stripes.front();
      stripes.pop();
    }
    stripes_lock.unlock(); // !!! stripes_lock released !!!
    if (do_stripe) {
      for (stripe_it=stripe.begin();
	   stripe_it!=stripe.end();stripe_it++) {
	encoded.insert(pair<int,librados::bufferlist>(stripe_it->second.get_shard(),stripe_it->second.get_bufferlist()));
      }
      ret = ecbench.encode(&encoded);
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Stripe " <<  stripe.begin()->second.get_stripe() << " encoded in erasureCodeThread()" << 
	std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      pending_buffers_lock.lock(); // *** pending_buffers_lock acquired ***
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Got pending_buffers_lock in erasureCodeThread()." << 
	std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      for (stripe_it=stripe.begin();stripe_it!=stripe.end();stripe_it++) {
	pending_buffers.push(stripe_it->second);
#ifdef TRACE
      output_lock.lock();
	std::cerr THREAD_ID << "Pushed buffer " << stripe_it->second.get_hash() <<
	  " to pending_buffer_queue."  << std::endl;
      output_lock.unlock();
#endif
      }
      pending_buffers_lock.unlock(); // !!! pending_buffers_lock released !!!
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Released pending_buffers_lock in ecThread." << std::endl;
      std::cerr.flush();
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
    } else {
      std::this_thread::sleep_for(thread_sleep_duration);
    }
  }
}

void erasureDecodeThread(ErasureCodeBench ecbench) {
  bool started = false;
  int ret = 0;
  Shard shard;
  map<int,Shard> stripe;
  map<int,Shard>::iterator stripe_it;
  map<int,librados::bufferlist> encoded;
  map<int,librados::bufferlist>::iterator shard_it;

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
    bool do_stripe = false;
    stripes_decode_lock.lock(); // *** stripes_lock acquired ***
    if (!stripes_decode.empty()) {
      do_stripe = true;
      stripe = stripes_decode.front();
      stripes_decode.pop();
    }
    stripes_decode_lock.unlock(); // !!! stripes_lock released !!!
    if (do_stripe) {
      for (stripe_it=stripe.begin();
	   stripe_it!=stripe.end();stripe_it++) {
	encoded.insert(pair<int,librados::bufferlist>(stripe_it->second.get_shard(),stripe_it->second.get_bufferlist()));
      }
      ret = ecbench.encode(&encoded);
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Stripe " <<  stripe.begin()->second.get_stripe() 
			  << " encoded in erasureCodeThread()" << std::endl;
      std::cerr THREAD_ID << "Decoding done, buffers inserted, in erasureCodeThread()." << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      encoded.clear();
      /* Need to release the stripe memory. We are just measuring the time to repair
       * the stripe. Now we can release the resources*/
      stripe.clear();
    }
    if (ret < 0) {
      output_lock.lock();
      std::cerr << "Error in erasure code call to ecbench. " << ret << std::endl;
      std::cerr.flush();
      output_lock.unlock();
    } 
    else {
      std::this_thread::sleep_for(thread_sleep_duration);
    }
  }
}

/* @writing_done means that the rados has finished writing shards/objects.
 */
void radosWriteThread() {
  bool started = false;
  librados::IoCtx io_ctx;
  int ret;
  // For this thread
  uint32_t shard_counter = 0;
   uint32_t stripe = 0;
   uint32_t index = 0;
   uint32_t offset = 0;

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
	 output_lock.unlock();
#endif
	ret = EXIT_FAILURE;
      } 
    }
  }

  // Write loop 
  while (!writing_done) {
    // wait for the request to complete, and check that it succeeded.
    bool do_write = false;
    Shard shard;
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID  << "In radosWriteThread() outer while loop." << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif

    pending_buffers_lock.lock();  // *** pending_buffers_lock ***
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Got pending_buffers_lock in radosWriteThread()." << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif

    pending_buffers_that_remain = pending_buffers.size();
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID  << "pbit: pending_buffers size (" << pending_buffers_that_remain << 
      ")" << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif

    if (!pending_buffers.empty()) {
      /* This code only executes if there are any Shards in pending_buffers.
         Buffers are insereted into the pending_buffers queue in order from
         the erasure encoded stripe. We can derive the stripe and shard by
         keeping up with this position. We will use it to append to the objects
	 that are created at the beginning, like it is done in the rados bench
	 tool.
      */
      do_write = true; // We have a buffer to write.
      shard = pending_buffers.front();
      pending_buffers.pop();
      shard_counter += 1;
      stripe = shard_counter / stripe_size;
      offset = stripe * shard_size;
      index = shard_counter % stripe_size;
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID  << "pbit: Writing " << shard.get_object_name()
			   << " to storage." << std::endl;
      std::cerr THREAD_ID  << "shard: " << shard_counter << " stripe: " << stripe
			   << " offset: " << offset << " ObjectID: "
			   << objs[index].name << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
    } // End of loop over pending_buffer list elements.

    pending_buffers_lock.unlock(); // !!! pending_buffers_lock released !!!
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Released pending_buffers_lock in radosWriteThread." << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif

    if (do_write) {
      /* This code only executes if the buffer was found in the pending_buffer list. */
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "index: " << shard.get_hash()
			  << " pending buffer erased in radosWriteThread()"
			  << std::endl;
      output_lock.unlock();
#endif
      librados::AioCompletion *c = rados.aio_create_completion(NULL, NULL, NULL);
      completions_lock.lock(); // *** Get completion lock
      completions.push_back(c);
      completions_lock.unlock(); // *** Release completion lock

      librados::bufferlist _bl = shard.get_bufferlist();
      ret = io_ctx.aio_write(objs[index].name,c,_bl,shard_size,offset);
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Write called."
			  << std::endl;
      output_lock.unlock();
#endif
      if (ret < 0) {
#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "couldn't start write object! error at index "
			    << shard.get_hash() << std::endl;
	output_lock.unlock();
#endif
	ret = EXIT_FAILURE;
	failed = true; 
	// We have had a failure, so do not execute any further, 
	// fall through.
      }

#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "index: " << shard.get_hash() << " buffer to blq in radosWriteThread()"
			  << std::endl;
      std::cerr THREAD_ID << "we wrote our object "
			  << shard.get_object_name() 
			  << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif

    } // End of the do_write block.
    else {
      /* The pending_buffer map is empty, sleep a few milliseconds. */
      std::this_thread::sleep_for(thread_sleep_duration);
    }
  }   // End Write loop

  list<librados::AioCompletion *>::iterator iter;
  for (iter = completions.begin(); iter != completions.end(); ++iter) {
    librados::AioCompletion *c = *iter;
    c->wait_for_complete();
    ret = c->get_return_value();
    c->release();
    if (ret < 0) { // yes, we leak.
      cerr << "aio_write failed" << std::endl;
      return;
    }
  }

  // clean up the bufferlist queue
  blq_lock.lock(); // *** blq_lock acquired ***
  while (!blq.empty())
    {
      blq.front().clear();
      blq.pop();
    }
  blq_lock.unlock(); // !!! blq_lock released !!!

  rados.shutdown();
  ret = failed ? ret:EXIT_SUCCESS;
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
void radosReadThread() {
  bool started = false;
  librados::IoCtx io_ctx;
  int ret;
  // For this thread
  uint32_t shard_counter = 0;
  uint32_t stripe = 0;
  uint32_t index = 0;
  uint32_t offset = 0;
  uint32_t completions_size = 0;
  map<int,Shard> stripeM;
  map<int,Shard>::iterator stripeM_it;

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
	output_lock.unlock();
#endif
	ret = EXIT_FAILURE;
      } 
    }
  }

  // Read loop 
  while (!reading_done) {
    // wait for the request to complete, and check that it succeeded.
    bool do_read = false;
    Shard shard;
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID  << "In radosReadThread() outer while loop." << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif

    stripes_lock.lock();  // *** stripes_lock ***
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Got stripes_lock in radosReadThread()." << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif

    stripes_that_remain = stripes.size();
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID  << "rread: stripes size (" << stripes_that_remain << 
      ")" << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif

    if (!stripes.empty()) {
      do_read = true; // We have a stripe to read.
      stripeM = stripes.front();
      stripes.pop(); 
    } // End of loop over stripes.

    stripes_lock.unlock(); // !!! stripes_lock released !!!
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Released stripes_lock in radosReadThread." << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif

    if (do_read) {
      /* This code only executes if a stripe was found in the stripes queue. */
      // throttle... *** We need to do this differently because we must repair the stripe
      completions_size = UINT_MAX;
      while (completions_size > (uint32_t)concurrentios) {
	completions_lock.lock(); // Get completions lock
	completions_size = completions.size();
	completions_lock.lock(); // Get completions lock
	if (completions_size > (uint32_t)concurrentios)
	  std::this_thread::sleep_for(thread_sleep_duration);
      }

      /* Need to design this. We only want to readk K shards determined by the
       * input parameters. Then we wait in the completionsCollectionThread for
       * the reads to complete before passing them on to the erasure code thread.
       */
      uint32_t count = 0;
      for (stripeM_it=stripeM.begin();
	   stripeM_it!=stripeM.end();stripeM_it++) {
	shard = stripeM_it->second;
	bool is_erased = false;
	count++;
	shard_counter += 1;
	stripe = shard_counter / stripe_size;
	offset = stripe * shard_size;
	index = shard_counter % stripe_size;
#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID  << "RTHREAD: Reading " << shard.get_object_name()
			     << " from storage." << std::endl;
	std::cerr THREAD_ID  << "shard: " << shard_counter << " stripe: " << stripe
			     << " offset: " << offset << " ObjectID: "
			     << objs[index].name << std::endl;
	std::cerr.flush();
	output_lock.unlock();
#endif
	for (int n : v_erased)
	  if (shard.get_hash() % stripe_size == n)
	    is_erased = true;
	// We only need to read K shards. Prefer data shards, i.e. leading elements of map.
	if (!is_erased && count <= (uint32_t)K) {
	  librados::AioCompletion *c = rados.aio_create_completion(NULL, NULL, NULL);
	  completions.push_back(c);

	  librados::bufferlist* _bl = stripeM_it->second.get_bufferlist_ptr();
	  ret = io_ctx.aio_read(objs[index].name,c,_bl,shard_size,(uint64_t)offset,0);
#ifdef TRACE
	  output_lock.lock();
	  std::cerr THREAD_ID << "Read called."
			      << std::endl;
	  output_lock.unlock();
#endif
	  if (ret < 0) {
#ifdef TRACE
	    output_lock.lock();
	    std::cerr THREAD_ID << "couldn't start read object! error at index "
				<< shard.get_hash() << std::endl;
	    output_lock.unlock();
#endif
	    ret = EXIT_FAILURE;
	    failed = true; 
	    // We have had a failure, so do not execute any further, 
	    // fall through.
	  }
	}
      }
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "index: " << shard.get_hash() << " buffer read started in radosReadThread()"
			  << std::endl;
      std::cerr THREAD_ID << "we are reading "
			  << shard.get_object_name() 
			  << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif

    } // End of the do_read block.
    else {
      /* The stripes queue is empty, sleep a few milliseconds. */
      std::this_thread::sleep_for(thread_sleep_duration);
    }
  }   // End Read loop

  rados.shutdown();
  ret = failed ? ret:EXIT_SUCCESS;
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
  cout << (end_time - begin_time) << "\t" << (max_iterations * (in_size / 1024)) << endl;
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
  cout << (end_time - begin_time) << "\t" << ((in_size / 1024)) 
       << "\t" << begin_time << "\t" << end_time << endl;
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
  cout << (end_time - begin_time) << "\t" << (max_iterations * (in_size / 1024)) << endl;
  return 0;
}

int main(int argc, const char** argv) {
  ErasureCodeBench ecbench;

  // variables used for timing
  utime_t end_time;
  utime_t begin_time;
  utime_t total_time_ms;
  utime_t total_run_time_ms;

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

    // store the program inputs in the global vars for access by threads
    _argc = argc;
    _argv = argv;

    // Start the bufferListCreatorThread
    blThread = std::thread (bufferListCreatorThread);

    // Initialize rados
    initRadosIO();

    if (ecbench.plugin == "gibraltar") // Gibraltar is single threaded now
      ec_threads = 1;

    // Start the radosWriteThread
    if (ecbench.workload == "encode") {
      // Run the object bootstrap
      bootstrap();
      // Start the rados writer thread
      rados_threads.push_back(std::thread (radosWriteThread));
      // Start the erasureEncodeThread. Only do one thread with Gibraltar
      for (int i = 0;i<ec_threads;i++) {
	v_ec_threads.push_back(std::thread (erasureEncodeThread, ecbench));
      }
    } else {
      // Start the radosReadThread
      rados_threads.push_back(std::thread (radosReadThread));
      // Start the erasureEncodeThread. Only do one thread with Gibraltar
      for (int i = 0;i<ec_threads;i++) {
	v_ec_threads.push_back(std::thread (erasureDecodeThread, ecbench));
      }
      // Start the completionsCollectionThread
      ccThread = std::thread (completionsCollectionThread);
    }

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
	output_lock.unlock();
#endif
	stripes_lock.lock(); // *** stripes_lock acquired ***
	stripes.push(stripe);
	stripes_lock.unlock(); // !!! stripes_lock released !!!
      } 
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

    if (ecbench.workload == "encode") { // encoding/writing case
      is_encoding = true;
      while (!ec_done) {
	stripes_lock.lock(); // *** stripes_lock acquired ***
	if (stripes.empty())
	  ec_done = true;
	stripes_lock.unlock(); // !!! stripes_lock released !!!
	if (!ec_done)
	  std::this_thread::sleep_for(blq_sleep_duration);
      }
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Shutdown: Done with erasure codiding." << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif

      for (int i = 0;i<ec_threads;i++) 
	v_ec_threads[i].join();     // Wait for the ecThread to finish.
      blThread.join(); // This should have finished at the beginning.

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
	  " in writing routine." << std::endl;
	std::cerr.flush();
	output_lock.unlock();
#endif

	if (!writing_done)
	  std::this_thread::sleep_for(blq_sleep_duration);
      }
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Shutdown:Done with writing." << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif

      std::vector<std::thread>::iterator rit;
      for (rit=rados_threads.begin();rit!=rados_threads.end();rit++) {
	rit->join(); // Wait for writer to finish.
      }
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

    FINISH_TIMER; // Compute total time since START_TIMER
    std::cout << "Cleanup." << std::endl;
    REPORT_TIMING;
    REPORT_BENCHMARK; // Print out the elapsed time for this section
    std::cout << "Total run time " << total_run_time_ms << " ms" << std::endl;
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
