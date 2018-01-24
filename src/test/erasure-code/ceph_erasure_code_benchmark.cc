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
#include "include/rados_stripe.h"

#define DEBUG 1
#define RADOS_THREADS 0
#define BLQ_SLEEP_DURATION 25 // in millisecinds
#define THREAD_SLEEP_DURATION 10 // in millisecinds
#define THREAD_ID  << "Thread: " << std::this_thread::get_id() << " | "
#define START_TIMER begin_time = std::clock();
#define FINISH_TIMER end_time = std::clock(); \
               total_time_ms = (end_time - begin_time) / (double)(CLOCKS_PER_SEC / 1000);
#define REPORT_TIMING std::cout << total_time_ms  << " ms\t" << std::endl;
#define REPORT_BENCHMARK std::cout << total_time_ms  << " ms\t" << ((double)stripe_size * (double)object_size) / (1024*1024) << " MB\t" \
  << ((double)stripe_size * (double)object_size) / (double)(1024*total_time_ms) << " MB/s" << std::endl; \
  total_run_time_ms += total_time_ms;
#define POOL_NAME "hello_world_pool_1"

// Globals for program
int iterations = 0;
int stripe_size = 0;
int queue_size = 0;
int object_size = 0;
std::string obj_name;
std::string pool_name;
int ret = 0;
bool failed = false; // Used as a flag to indicate that a failure has occurred
int rados = 1; // Enable extended rados benchmarks. If false, only original erasure code benchmarks.
int _argc; // Global argc for threads to use
const char** _argv; // Global argv for threads to use
const std::chrono::milliseconds thread_sleep_duration(THREAD_SLEEP_DURATION);
const std::chrono::milliseconds blq_sleep_duration(BLQ_SLEEP_DURATION);


// Create queues, maps and iterators used by the main and thread functions.
std::queue<librados::bufferlist> blq;
std::map<int,map<int,bufferlist>> stripes;
std::map<int,map<int,bufferlist>>::iterator stripes_it;
std::map<int,bufferlist> pending_buffers;
std::map<int,bufferlist>::iterator pbit;
std::map<int,librados::AioCompletion*> write_completion;
std::map<int,librados::AioCompletion*>::iterator cit;
std::map<int,Stripe> stripe_data;
std::map<int,Stripe>::iterator sit;
std::vector<std::thread> rados_threads;
bool rados_done = false; //Becomes true at the end before we try to join the AIO competion thread.
int blq_last_size;
int completions_that_remain = 0;
int pending_buffers_that_remain = 0;
int num_rados_threads = 0;

// Locks for containers sharee with the handleAioCompletions thread.
std::mutex output_lock;
std::mutex blq_lock;
std::mutex stripes_lock;
std::mutex pending_buffers_lock;
std::mutex write_completion_lock;
std::mutex stripe_data_lock;

/* Function used to perform erasure coding or repair. 
 */

void erasureCodeThread(ErasureCodeBench ecbench) {
  bool started = false;
  int ret, index, stripe_index, _stripe_size;
  Stripe shard;
  map<int,bufferlist> encoded;
  map<int,bufferlist>::iterator shard_it;

  if(!started) {
    started = true;
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Starting erasureCodeThread()" << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif

  }

  while (!rados_done) {
    stripes_lock.lock();
    stripes_it = stripes.begin();
    if (stripes_it!=stripes.end()) {
      encoded = stripes_it->second;
      stripe_index = stripes_it->first; // stripe_index is the stripe number
      stripes.erase(stripes_it);
      stripes_lock.unlock();
    } else {
      stripes_lock.unlock();
      std::this_thread::sleep_for(thread_sleep_duration);
      continue;
    }
    if (ecbench.workload == "encode") {
    ret = ecbench.encode(&encoded);
    pending_buffers_lock.lock();
    for (shard_it=encoded.begin();
	 shard_it!=encoded.end(); shard_it++) {
      pending_buffers.insert(std::pair<int,
			     bufferlist>(stripe_index*ecbench.stripe_size+shard_it->first,shard_it->second));
    }
    encoded.clear();
    pending_buffers_lock.unlock();
    } else { // for reading, we have to wait until the shards are read
      for (shard_it=encoded.begin();
	   shard_it!=encoded.end();
	   shard_it++) {
	_stripe_size = ecbench.stripe_size;
	index = stripe_index * _stripe_size + shard_it->first;
	stripe_data_lock.lock();
	shard = stripe_data.find(index)->second;
	stripe_data_lock.unlock();
	while (!shard.is_read()) {
	    std::this_thread::sleep_for(thread_sleep_duration);
	    continue;
	    // waits until all of the shards in the stripe have been read
	}
      }
      ret = ecbench.decode_erasures(encoded);
      /* here, the erasures have been repaired and the stripes are ready for use.
       * For now, we're putting them back into the blq.
       */
      blq_lock.lock();
      for (shard_it=encoded.begin();shard_it!=encoded.end();shard_it++) {
	blq.push(shard_it->second);
      }
      encoded.clear();
    }
    if (ret < 0) {
      output_lock.lock();
      std::cerr << "Error in erasure code call to ecbench. " << ret << std::endl;
      std::cerr.flush();
      output_lock.unlock();
    }
  }
}

/* Function used by completion thread to handle completions. The main thread
 * pushes the AioCompletion* to the completion_q after the write operation
 * is performed. The thread concurrently gets a completion object from the
 * completion_q, waits for it to be safe, puts the bufferlist back into the
 * bufferlist queue, and releases the AioCompletion object. This effectively
 * allows reuse of the bufferlists conserving memory and time. It also lets
 * the waiting for completion be concurrent with the erasure coding and writing.
 * At the end of the program execution, the thread is joined which causes the
 * program execution to wait until the completion_q is empty, i.e, all of the
 * objects are safe on disk.
 * @rados_done means that the main thread has finished writing shards/objects.
 * @reading_done means that the rados has finished writing shards/objects.
 */
void radosReadThread() {
  bool reading_done = false; //Becomes true at the end when we all completions are safe
  bool started = false;
  int ret;
  // For this thread
  librados::IoCtx io_ctx;

  // first, we create a Rados object and initialize it
  librados::Rados rados;

  if(!started) {
    started = true;
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Starting radosReadThread()" << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
    ret = rados.init("admin"); // just use the client.admin keyring
    if (ret < 0) { // let's handle any error that might have come back
      output_lock.lock();
      std::cerr THREAD_ID << "couldn't initialize rados! error " << ret << std::endl;
      output_lock.unlock();
      ret = EXIT_FAILURE;
      goto out;
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
	output_lock.lock();
	std::cerr THREAD_ID << "failed to parse config options! error " << ret << std::endl;
	output_lock.unlock();
	ret = EXIT_FAILURE;
	goto out;
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
	      output_lock.lock();
	      std::cerr THREAD_ID << "failed to parse config file " << _argv[i+1]
				  << "! error" << ret << std::endl;
	      output_lock.unlock();
	      ret = EXIT_FAILURE;
	      goto out;
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
	output_lock.lock();
	std::cerr THREAD_ID << "couldn't connect to cluster! error " << ret << std::endl;
	output_lock.unlock();
	ret = EXIT_FAILURE;
	goto out;
      } else {
#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "we just connected to the rados cluster" << std::endl;
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
	output_lock.lock();
	std::cerr THREAD_ID << "couldn't set up ioctx! error " << ret << std::endl;
	output_lock.unlock();
	ret = EXIT_FAILURE;
	goto out;
      } else {
#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "we just created an ioctx for our pool" << std::endl;
	output_lock.unlock();
#endif
      }
    }
  }
  // Read loop 
  while (!rados_done||!reading_done) {
    // wait for the request to complete, and check that it succeeded.
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID  << "In radosReadThread() outer while loop." << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
    bufferlist _bl; // Local pointer to the selected bufferlist to read
    Stripe _data; // Local pointer to the stripe data for the buffer
    pending_buffers_lock.lock();
    pending_buffers_that_remain = pending_buffers.size();
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID  << "pending_buffers size > stripe_size " <<
      pending_buffers_that_remain << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif

    pbit = pending_buffers.begin();
    if (pbit != pending_buffers.end()) {
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID  << "Writing " << pbit->first 
			   << " to storage." << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      stripe_data_lock.lock();
      _data = stripe_data[pbit->first];
      _bl = pbit->second;
      pending_buffers.erase(pbit);
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "index: " << _data.get_hash()
			  << " pending buffer erased in radosReadThread()"
			  << std::endl;
      output_lock.unlock();
#endif
      stripe_data_lock.unlock();
      pending_buffers_lock.unlock();
    }
    else {
      pending_buffers_lock.unlock();
      continue;
    }
    try {
      ret = io_ctx.read(_data.get_object_name(),_bl,object_size,0);
    }
    catch (std::exception& e) {
      output_lock.lock();
      std::cerr THREAD_ID << "Exception while writing object. "
			  << _data.get_object_name() << std::endl;
      output_lock.unlock();
      ret = -1;
    }
    if (ret < 0) {
      output_lock.lock();
      std::cerr THREAD_ID << "couldn't start read object! error at index "
			  << _data.get_hash() << std::endl;
      output_lock.unlock();
      ret = EXIT_FAILURE;
      failed = true; 
      goto out;
      // We have had a failure, so do not execute any further, 
      // fall through.
    } else { // Flag this shard as having been read successfully
      _data.set_read();
    }

    try {
      blq_lock.lock();
      blq.push(_bl);
      blq_lock.unlock(); // in case we exit
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "index: " << _data.get_hash() << " buffer to blq in radosReadThread()"
			  << std::endl;
      output_lock.unlock();
#endif
    }
    catch (std::out_of_range& e) {
      output_lock.lock();
      std::cerr THREAD_ID << "Out of range error accessing pending_buffers "
			  << "in rados read thread " THREAD_ID
			  << std::endl;
      output_lock.unlock();
      blq_lock.unlock(); // in case we exit
      ret = -1;
    }
    pending_buffers_lock.lock();
    pending_buffers_that_remain = pending_buffers.size();
    pending_buffers_lock.unlock();
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "pbit: " << _data.get_hash()
			<< " pending buffer in radosReadThread() "
			<< "pending_buffers size after push is "
			<< pending_buffers_that_remain
			<< std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
    if (DEBUG > 0) {
      output_lock.lock();
      std::cerr THREAD_ID << "we wrote our object "
			  << _data.get_object_name() 
			  << std::endl;
      std::cerr.flush();
      output_lock.unlock();
    }
    if (pending_buffers_that_remain > num_rados_threads) {
      std::this_thread::sleep_for(thread_sleep_duration);
    }
    if (rados_done && pending_buffers_that_remain==0) reading_done = true;
  }
  // End code from thread

  // End Read loop
 out:
  rados.shutdown();
  ret = failed ? ret:EXIT_SUCCESS;

  return; // Thread terminates
}

/* Function used by completion thread to handle completions. The main thread
 * pushes the AioCompletion* to the completion_q after the write operation
 * is performed. The thread concurrently gets a completion object from the
 * completion_q, waits for it to be safe, puts the bufferlist back into the
 * bufferlist queue, and releases the AioCompletion object. This effectively
 * allows reuse of the bufferlists conserving memory and time. It also lets
 * the waiting for completion be concurrent with the erasure coding and writing.
 * At the end of the program execution, the thread is joined which causes the
 * program execution to wait until the completion_q is empty, i.e, all of the
 * objects are safe on disk.
 * @rados_done means that the main thread has finished writing shards/objects.
 * @writing_done means that the rados has finished writing shards/objects.
 */
void radosWriteThread() {
  bool writing_done = false; //Becomes true at the end when we all completions are safe
  bool started = false;
  int ret;
  // For this thread
  librados::IoCtx io_ctx;

  // first, we create a Rados object and initialize it
  librados::Rados rados;

  if(!started) {
    started = true;
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Starting radosWriteThread()" << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
    ret = rados.init("admin"); // just use the client.admin keyring
    if (ret < 0) { // let's handle any error that might have come back
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "couldn't initialize rados! error " << ret << std::endl;
      output_lock.unlock();
#endif
      ret = EXIT_FAILURE;
      goto out;
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
	goto out;
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
	      goto out;
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
	goto out;
      } else {
#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "we just connected to the rados cluster" << std::endl;
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
	output_lock.unlock();
#endif
	ret = EXIT_FAILURE;
	goto out;
      } else {
#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "we just created an ioctx for our pool" << std::endl;
	output_lock.unlock();
#endif
      }
    }
  }
  // Write loop 
  while (!rados_done||!writing_done) {
    // wait for the request to complete, and check that it succeeded.
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID  << "In radosWriteThread() outer while loop." << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
    bufferlist _bl; // Local pointer to the selected bufferlist to write
    Stripe _data; // Local pointer to the stripe data for the buffer
    pending_buffers_lock.lock();
    pending_buffers_that_remain = pending_buffers.size();
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID  << "pending_buffers size > stripe_size " <<
      pending_buffers_that_remain << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif

    pbit = pending_buffers.begin();
    if (pbit != pending_buffers.end()) {
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID  << "Writing " << pbit->first 
			   << " to storage." << std::endl;
      std::cerr.flush();
      output_lock.unlock();
#endif
      stripe_data_lock.lock();
      _data = stripe_data[pbit->first];
      _bl = pbit->second;
      pending_buffers.erase(pbit);

#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "index: " << _data.get_hash()
			  << " pending buffer erased in radosWriteThread()"
			  << std::endl;
      output_lock.unlock();
#endif

      stripe_data_lock.unlock();
      pending_buffers_lock.unlock();
    }
    else {
      pending_buffers_lock.unlock();
      continue;
    }
    try {
      ret = io_ctx.write_full(_data.get_object_name(),_bl);
    }
    catch (std::exception& e) {

#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Exception while writing object. "
			  << _data.get_object_name() << std::endl;
      output_lock.unlock();
#endif
      ret = -1;
    }
    if (ret < 0) {
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "couldn't start write object! error at index "
			  << _data.get_hash() << std::endl;
      output_lock.unlock();
#endif
      ret = EXIT_FAILURE;
      failed = true; 
      goto out;
      // We have had a failure, so do not execute any further, 
      // fall through.
    }

    try {
      blq_lock.lock();
      blq.push(_bl);
      blq_lock.unlock(); // in case we exit
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "index: " << _data.get_hash() << " buffer to blq in radosWriteThread()"
			  << std::endl;
      output_lock.unlock();
#endif
    }
    catch (std::out_of_range& e) {
#ifdef TRACE
      output_lock.lock();
      std::cerr THREAD_ID << "Out of range error accessing pending_buffers "
			  << "in rados write thread " THREAD_ID
			  << std::endl;
      output_lock.unlock();
#endif
      blq_lock.unlock(); // in case we exit
      ret = -1;
    }
    pending_buffers_lock.lock();
    pending_buffers_that_remain = pending_buffers.size();
    pending_buffers_lock.unlock();
#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "pbit: " << _data.get_hash()
			<< " pending buffer in radosWriteThread() "
			<< "pending_buffers size after push is "
			<< pending_buffers_that_remain
			<< std::endl;
    std::cerr THREAD_ID << "we wrote our object "
			<< _data.get_object_name() 
			<< std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
    if (pending_buffers_that_remain > num_rados_threads) {
      std::this_thread::sleep_for(blq_sleep_duration);
    }
    if (rados_done && pending_buffers_that_remain==0) writing_done = true;
  }
  // End code from thread

  // End Write loop
 out:
  rados.shutdown();
  ret = failed ? ret:EXIT_SUCCESS;

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
    ("rados,r", po::value<int>()->default_value(1),
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
    ("object_size,x", po::value<int>()->default_value(1024 * 1024 * 8),
     "size of the objects/shards to be encoded") 
    ("iterations,i", po::value<int>()->default_value(1),
     "number of encode/decode runs")
    ;
  std::cerr << "Added queuesize,object_size,iterations" << std::endl;
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
  rados = vm["rados"].as<int>();
  num_rados_threads = vm["threads"].as<int>();
  queue_size = vm["queuesize"].as<int>();
  object_size = vm["object_size"].as<int>(); 
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
  cout << (end_time - begin_time) << "\t" << ((in_size / 1024)) << endl;
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
  try {
    int err = ecbench.setup(argc, argv);
    if (err)
      return err;
    //    return ecbench.run();
  } catch(po::error &e) {
    cerr THREAD_ID << e.what() << endl; 
    return 1;
  }
  if (rados == 0) {
    iterations = ecbench.max_iterations;
    stripe_size = ecbench.k + ecbench.m;
    queue_size = ecbench.queue_size;
    object_size = ecbench.object_size;
    obj_name = ecbench.obj_name;
    pool_name = ecbench.pool_name;

    // variables used for timing
    std::clock_t end_time;
    std::clock_t begin_time;
    std::clock_t total_time_ms = 0;
    std::clock_t total_run_time_ms = 0;

    START_TIMER; // Code for the begin_time
    std::cout THREAD_ID << "Iterations = " << iterations << std::endl;
    std::cout THREAD_ID << "Stripe Size = " << stripe_size << std::endl;
    std::cout THREAD_ID << "Queue Size = " << queue_size << std::endl;
    std::cout THREAD_ID << "Object Size = " << object_size << std::endl;
    std::cout THREAD_ID << "Object Name Prefix = " << obj_name << std::endl;
    std::cout THREAD_ID << "Pool Name = " << pool_name << std::endl;

    // store the program inputs in the global vars for access by threads
    _argc = argc;
    _argv = argv;

    // Start the radosWriteThread
    if (ecbench.workload == "encode") {
      for (int i=0;i<num_rados_threads;i++) {
	rados_threads.push_back(std::thread (radosWriteThread));
      }
    } else {
      for (int i=0;i<num_rados_threads;i++) {
	rados_threads.push_back(std::thread (radosReadThread));
      }
    }

    // Start the erasureCodeThread
    std::thread ecThread = std::thread (erasureCodeThread, ecbench);

    // RADOS IO Test Here
    /*
     * Here we do write all of the objects and then wait for completion
     * after they have been dispatched.
     * We create a queue of bufferlists so we can reuse them.
     * We iterate over the procedure writing stripes of data.
     */

    START_TIMER; // Code for the begin_time
    blq_lock.lock(); // It's OK here, just starting up
    for (int i=0;i<queue_size;i++) {
      librados::bufferlist bl;
      bl.append(std::string(object_size,(char)i%26+97)); // start with 'a'
      blq.push(bl);
    }
    blq_last_size = blq.size();
    blq_lock.unlock();

#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Just made bufferlist queue with specified buffers." << std::endl
			<< "\tCurrently there are " << blq_last_size << " buffers in the queue."
			<< std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif
    // Get a stripe of bufferlists from the queue

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
	blq_lock.lock();
	blq_last_size = blq.size();
	blq_lock.unlock();
      }
      std::map<int,bufferlist>::iterator it;
      std::map<int,bufferlist> encoded;
      if (ecbench.workload == "encode") { // Begin Write procedure
	for (int i=0;i<stripe_size;i++) {
	  index = j*stripe_size+i; // index == data.get_hash()
	  std::stringstream object_name;
	  object_name << obj_name << "." << index;
	  Stripe data(j,i,stripe_size,object_name.str());
	  assert(index==data.get_hash()); // disable assert checking by defining #NDEBUG
	  stripe_data_lock.lock();
	  stripe_data.insert(std::pair<int,Stripe>(data.get_hash(),data));
	  stripe_data_lock.unlock();
	  blq_lock.lock();
	  encoded.insert( std::pair<int,bufferlist>((int)data.get_shard(),blq.front()));
	  blq.pop(); // remove the bl from the queue
	  blq_last_size = blq.size();
	  blq_lock.unlock();
#ifdef TRACE
	  output_lock.lock();
	  std::cerr THREAD_ID << "Created a Stripe with hash value " << data.get_hash() << std::endl;
	  output_lock.unlock();
#endif
	}

	FINISH_TIMER; // Compute total time since START_TIMER
	std::cout << "Setup for write test using AIO." << std::endl;
	REPORT_TIMING; // Print out the benchmark for this test

	// The encoded map is used by the erasure coding interface.
	START_TIMER; // Code for the begin_time of encoding

	/* Push the encoded map into a queue for the threads to 
	 * read. When the shards have been read, the map is pushed
	 * to the finished queue. For now, the threads empty the
	 * map and put the buffers back into the blq. In a real
	 * system, the data would be sent to the user.
	 */
	stripes_lock.lock();
	stripes.insert(std::pair<int,std::map<int,bufferlist>>(j,encoded));
	stripes_lock.unlock();

	output_lock.lock();
	FINISH_TIMER; // Compute total time since START_TIMER
	std::cout << "Writing aio test. Iteration " << j << " Queue size "
		  << blq_last_size << std::endl;
	REPORT_BENCHMARK; // Print out the benchmark for this test
	output_lock.unlock();
      } else { // Begin Read procedure
	for (int i=0;i<stripe_size;i++) {
	index = j*stripe_size+i; // index == data.get_hash()
	std::stringstream object_name;
	object_name << obj_name << "." << index;
	Stripe data(j,i,stripe_size,object_name.str());
	assert(index==data.get_hash()); // disable assert checking by defining #NDEBUG
	stripe_data_lock.lock();
	stripe_data.insert(std::pair<int,Stripe>(data.get_hash(),data));
	stripe_data_lock.unlock();
	blq_lock.lock();
	encoded.insert( std::pair<int,bufferlist>(data.get_hash(),blq.front()));
	blq.pop(); // remove the bl from the queue
	blq_last_size = blq.size();
	blq_lock.unlock();
	// Erase the buffers that were prescribed by the input configuration
	if (ecbench.erased.size() > 0) {
	  for (vector<int>::const_iterator i = ecbench.erased.begin();
	       i != ecbench.erased.end();i++) 
	    encoded.erase(*i);
	}
#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "Created a Stripe with hash value " << data.get_hash() << std::endl;
	output_lock.unlock();
#endif
      }

      // The encoded map is used by the erasure coding interface.

      FINISH_TIMER; // Compute total time since START_TIMER
      std::cout << "Setup for read test using AIO." << std::endl;
      REPORT_TIMING; // Print out the benchmark for this test

      START_TIMER; // Code for the begin_time
	pending_buffers_lock.lock();
	for (it=encoded.begin();
	     it!=encoded.end(); it++) {
	  pending_buffers.insert(std::pair<int,
				 bufferlist>(it->first,it->second));
	}
	pending_buffers_lock.unlock();
	/* Push the encoded map into a queue for the threads to 
	 * read. When the shards have been read, the map is pushed
	 * to the finished queue. For now, the threads empty the
	 * map and put the buffers back into the blq. In a real
	 * system, the data would be sent to the user.
	 */
	stripes_lock.lock();
	stripes.insert(std::pair<int,std::map<int,bufferlist>>(j,encoded));
	stripes_lock.unlock();
	output_lock.lock();
	FINISH_TIMER; // Compute total time since START_TIMER
	std::cout << "Writing aio test. Iteration " << j << " Queue size "
		  << blq_last_size << std::endl;
	REPORT_BENCHMARK; // Print out the benchmark for this test
	output_lock.unlock();

      }
    }


    START_TIMER; // Code for the begin_time
    rados_done = true;

    // Code from thread

    std::vector<std::thread>::iterator rit;
    for (rit=rados_threads.begin();rit!=rados_threads.end();rit++) {
      rit->join(); // Wait for writer to finish.
    }

    // Locking not required after this point.
    std::cerr << "Wait for all writes to flush." << std::endl;
    std::cerr.flush();
    utime_t end_time_final = ceph_clock_now(g_ceph_context);
    long long int total_data_processed = iterations*(ecbench.k+ecbench.m)*(ecbench.object_size/1024);
    std::cout << "Factors for computing size: iterations: " << iterations
	 << " max_iterations: " << ecbench.max_iterations << std::endl
	 << " stripe_size: " << ecbench.k+ecbench.m
	 << " object_size: " << object_size 
	 << " total data processed: " << total_data_processed << " KiB"
	 << std::endl;
    std::cout << (end_time_final - begin_time_final) << "\t" << total_data_processed << " KiB\t"
	      << total_data_processed/(double)(end_time_final - begin_time_final) 
	      << " KiB/s" << std::endl;

    // Print out the stripe object hashes and clear the stripe_data map
#ifdef TRACE
    {
      for (sit=stripe_data.begin();sit!=stripe_data.end();sit++) {
	std::cerr THREAD_ID << "Deleting Stripe object with hash value "
			    << sit->second.get_hash() << std::endl;
	std::cerr.flush();
      }
    }
#endif
    stripe_data.clear();
    std::cout.flush();

    FINISH_TIMER; // Compute total time since START_TIMER
    std::cout << "Cleanup." << std::endl;
    REPORT_TIMING; // Print out the elapsed time for this section
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
