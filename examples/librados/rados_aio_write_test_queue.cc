// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 * Copyright 2013 Inktank
 */

// install the librados-dev package to get this
#include <rados/librados.hpp>
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

#define DEBUG 1
#define START_TIMER begin_time = std::clock();
#define FINISH_TIMER end_time = std::clock(); \
               total_time_ms = (end_time - begin_time) / (double)(CLOCKS_PER_SEC / 1000);
#define REPORT_TIMING std::cerr << total_time_ms  << " ms\t" << std::endl;
#define REPORT_BENCHMARK std::cerr << total_time_ms  << " ms\t" << ((double)stripe_size * (double)object_size) / (1024*1024) << " MB\t" \
  << ((double)stripe_size * (double)object_size) / (double)(1024*total_time_ms) << " MB/s" << std::endl; \
  total_run_time_ms += total_time_ms;
#define POOL_NAME "hello_world_pool_1"

// Class to hold stripe record
class Stripe {
 public:
  uint32_t stripe;
  uint32_t shard;
  uint32_t stripe_size;
  std::string object_name;

  Stripe(uint32_t _stripe, uint32_t _shard, uint32_t _stripe_size,
	std::string _object_name) 
    : stripe (_stripe), shard (_shard), stripe_size (_stripe_size),
      object_name (_object_name) {};

  ~Stripe() {};

  uint32_t get_stripe() {
    return this->stripe;
  }
  uint32_t get_shard() {
    return this->shard;
  }
  uint32_t get_hash() {
    return Stripe::compute_hash(this->shard,this->stripe,this->stripe_size);
  }
  std::string get_object_name() {
    return object_name;
  }
  static uint32_t compute_hash(uint32_t _i,uint32_t _j, uint32_t _stripe_size) {
    return _j*_stripe_size+_i;
  }
};

// Create queues, maps and iterators used by the main and thread functions.
std::queue<librados::bufferlist> blq;
std::map<uint32_t,bufferlist> encoded, pending_buffers;
std::map<uint32_t,bufferlist>::iterator it,pbit;
std::map<uint32_t,librados::AioCompletion*> write_completion;
std::map<uint32_t,librados::AioCompletion*>::iterator cit;
std::map<uint32_t,Stripe> stripe_data;
std::map<uint32_t,Stripe>::iterator sit;
bool done = false; //Becomes true at the end before we try to join the AIO competion thread.
bool failed = false; // Used as a flag to indicate that a failure has occurred
int ret = 0;
uint32_t stripe_size = 0;

// Locks for containers sharee with the handleAioCompletions thread.
std::mutex blq_lock;
std::mutex pending_buffers_lock;
std::mutex write_completion_lock;
std::mutex stripe_data_lock;

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
 */
void handleAioCompletions() {
  bool started = false;
  if(!started) {
    started = true;
    std::cerr << "Starting handleAioCompletions()" << std::endl;
  }
  std::cerr << "About to enter while loop in handleAioCompletions(). " << std::endl
	    << "write_completion map has " << write_completion.size()
	    << " records. " << std::endl
	    << "The done boolean is " << done << std::endl;
  while (!done) {
    // wait for the request to complete, and check that it succeeded.
    std::cerr << "In handleAioCompletions() outer while loop." << std::endl;
    if(pending_buffers.size()>stripe_size) {
      write_completion_lock.lock(); // Get a lock on the write_completion map
      for (cit=write_completion.begin();
	   cit!=write_completion.end();cit++) {
	std::cerr << "Checking for " << cit->first 
		  << " safe in handleAioCompletions()" << std::endl;
	cit->second->wait_for_safe();
	std::cerr << cit->first << " is safe in handleAioCompletions()" << std::endl;
	blq_lock.lock(); // Get a lock on the bufferlist queue
	pending_buffers_lock.lock();
	try {
	  pbit = pending_buffers.find(cit->first);
	  std::cerr << pbit->first << " pending buffer in handleAioCompletions()"
		    << "pending_buffers size " << pending_buffers.size() << std::endl;
	  std::cerr.flush();
	  if (pbit!= pending_buffers.end()) {
	    blq.push(pbit->second);
	    std::cerr << cit->first << " buffer to blq in handleAioCompletions()"
		      << std::endl;
	    pending_buffers.erase(pbit);
	    std::cerr << cit->first
		      << " pending buffer erased in handleAioCompletions()"
		      << std::endl;
	  }
	}
	catch (std::out_of_range& e) {
	  std::cerr << "Out of range error accessing pending_buffers "
		    << "in AIO thread."
		    << " cit " << cit->first << "it " << it->first
		    << "sit " << sit->first << std::endl;
	  ret = -1;
	}
	pending_buffers_lock.unlock();
	blq_lock.unlock(); // Release lock on the bufferlist queue
	std::cerr << cit->first
		  << " about to release AIOCompletion in handleAioCompletions()"
		  << std::endl;
	std::cerr.flush();
	cit->second->release();
	std::cerr << cit->first
		  << " released AIOCompletion in handleAioCompletions()"
		  << std::endl;
	std::cerr.flush();
	std::cerr << pbit->first << " pending buffer in handleAioCompletions()"
		  << "pending_buffers size after push is " << pending_buffers.size()
		  << std::endl;
	std::cerr.flush();
	std::cerr << cit->first << " AIOCompletion map in handleAioCompletions()"
		  << "map size before erase " << write_completion.size() << std::endl;
	std::cerr.flush();
	/* FAILING HERE */
	write_completion.erase(cit);
	std::cerr << cit->first << " AIOCompletion map in handleAioCompletions()"
		  << "map size after erase " << write_completion.size() << std::endl;
	std::cerr.flush();
	write_completion_lock.unlock(); // Release lock on the write_completion map
	if (DEBUG > 0) {
	  std::cerr << "we wrote our object " << stripe_data.find(cit->first)->second.get_object_name() 
		    << std::endl;
	}
      }
    }
    std::chrono::milliseconds duration(250);
    std::this_thread::sleep_for(duration);
  }
}


// Rados write completion callback
void rados_write_safe_cb(rados_completion_t c, void *arg) {
  Stripe * data = (Stripe *)arg;
  std::cerr << std::clock() / (double)(CLOCKS_PER_SEC / 1000) << 
    " Rados Write Safe Callback called with " << data->get_hash() << std::endl;
  //    "," << (rados_completion_t)c->is_complete() << "," << (rados_completion_t)c->get_return_value() << std::endl;
}


void rados_write_complete_cb(rados_completion_t c, void *arg) {
  Stripe * data = (Stripe *)arg;
  std::cerr << std::clock() / (double)(CLOCKS_PER_SEC / 1000) << 
    " Rados Write Complete Callback called with " << data->get_hash() << std::endl;
  //    "," << c->is_complete() << "," << c->get_return_value() << std::endl;
}

int main(int argc, const char **argv)
{
  if(argc < 7)
  {
      std::cerr <<"Please put in correct params\n"<<
	"Iterations:\n"<<
	"Stripe Size (Number of shards):\n" <<
	"Queue Size:\n" <<
	"Object Size:\n" <<
	"Object Name:\n" <<
	"Pool Name:"<< std::endl;
      return EXIT_FAILURE;
  }
  uint32_t iterations = std::stoi(argv[1]);
  stripe_size = std::stoi(argv[2]);
  uint32_t queue_size = std::stoi(argv[3]);
  uint32_t object_size = std::stoi(argv[4]);
  std::string obj_name = argv[5];
  std::string pool_name = argv[6];
  int ret = 0;
  bool failed = false;

  // we will use all of these below
  std::string hello("hello world!");
  librados::IoCtx io_ctx;

  // variables used for timing
  std::clock_t end_time;
  std::clock_t begin_time;
  std::clock_t total_time_ms = 0;
  std::clock_t total_run_time_ms = 0;

  START_TIMER; // Code for the begin_time
  std::cerr << "Iterations = " << iterations << std::endl;
  std::cerr << "Stripe Size = " << stripe_size << std::endl;
  std::cerr << "Queue Size = " << queue_size << std::endl;
  std::cerr << "Object Size = " << object_size << std::endl;
  std::cerr << "Object Name Prefix = " << obj_name << std::endl;
  std::cerr << "Pool Name = " << pool_name << std::endl;

  // first, we create a Rados object and initialize it
  librados::Rados rados;
  {
      ret = rados.init("admin"); // just use the client.admin keyring
      if (ret < 0) { // let's handle any error that might have come back
	std::cerr << "couldn't initialize rados! error " << ret << std::endl;
	ret = EXIT_FAILURE;
	goto out;
      } else {
	std::cerr << "we just set up a rados cluster object" << std::endl;
      }
  }
  /*
   * Now we need to get the rados object its config info. It can
   * parse argv for us to find the id, monitors, etc, so let's just
   * use that.
   */
  {
    ret = rados.conf_parse_argv(argc, argv);
    if (ret < 0) {
      // This really can't happen, but we need to check to be a good citizen.
      std::cerr << "failed to parse config options! error " << ret << std::endl;
      ret = EXIT_FAILURE;
      goto out;
    } else {
      std::cerr << "we just parsed our config options" << std::endl;
      // We also want to apply the config file if the user specified
      // one, and conf_parse_argv won't do that for us.
      for (int i = 0; i < argc; ++i) {
	if ((strcmp(argv[i], "-c") == 0) || (strcmp(argv[i], "--conf") == 0)) {
	  ret = rados.conf_read_file(argv[i+1]);
	  if (ret < 0) {
	    // This could fail if the config file is malformed, but it'd be hard.
	    std::cerr << "failed to parse config file " << argv[i+1]
	              << "! error" << ret << std::endl;
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
      std::cerr << "couldn't connect to cluster! error " << ret << std::endl;
      ret = EXIT_FAILURE;
      goto out;
    } else {
      std::cerr << "we just connected to the rados cluster" << std::endl;
    }
  }

  /*
   * create an "IoCtx" which is used to do IO to a pool
   */
    {
      ret = rados.ioctx_create(pool_name.c_str(), io_ctx);
      if (ret < 0) {
	std::cerr << "couldn't set up ioctx! error " << ret << std::endl;
	ret = EXIT_FAILURE;
	goto out;
      } else {
	std::cerr << "we just created an ioctx for our pool" << std::endl;
      }
    }

  out:
    if (ret < 0) {
      return ret; // Abort the program, do not continue.
    }

    // Start the AioCompletionThread
    std::thread AioCompletionThread (handleAioCompletions);

  // RADOS AIO Write Test Here
  /*
   * now let's write the objects! Just for fun, we'll do it using
   * async IO instead of synchronous. 
   * Here we do write all of the objects and then wait for completion
   * after they have been dispatched.
   * http://ceph.com/docs/master/rados/api/librados/#asychronous-io )
   * We create a queue of bufferlists so we can reuse them.
   * We iterate over the procedure writing stripes of data.
   */
  {
    START_TIMER; // Code for the begin_time
    blq_lock.lock(); // It's OK here, just starting up
    for (int i=0;i<queue_size;i++) {
      librados::bufferlist bl;
      bl.append(std::string(object_size,(char)i%26+97)); // start with 'a'
      blq.push(bl);
    }
    blq_lock.unlock();

    // Get a stripe of bufferlists from the queue

    // the iteration loop begins here
    for (uint32_t j=0;j<iterations||failed;j++) {
      uint32_t index = 0;
      for (uint32_t i=0;i<stripe_size;i++) {
	index = j*stripe_size+i; // index == data.get_hash()
	std::stringstream object_name;
	object_name << obj_name << "." << index;
	Stripe data(j,i,stripe_size,object_name.str());
	assert(index==data.get_hash()); // disable assert checking by defining #NDEBUG
	stripe_data.insert(std::pair<uint32_t,Stripe>(data.get_hash(),data));
	blq_lock.lock();
	encoded.insert( std::pair<uint32_t,bufferlist>(data.get_hash(),blq.front()));
	blq.pop(); // remove the bl from the queue
	blq_lock.unlock();
	if(DEBUG>0) std::cerr << "Created a Stripe with hash value " << data.get_hash() << std::endl;
	write_completion_lock.lock();
	write_completion.insert( 
				std::pair<uint32_t,
				librados::AioCompletion*>
				(data.get_hash(),
				 librados::Rados::aio_create_completion((void *)&data,NULL,NULL)));
	write_completion_lock.unlock();
      }

      FINISH_TIMER; // Compute total time since START_TIMER
      std::cerr << "Setup for write test using AIO." << std::endl;
      REPORT_TIMING; // Print out the benchmark for this test

      START_TIMER; // Code for the begin_time
      write_completion_lock.lock();
	try {
	  cit = write_completion.find(j*stripe_size);
	  it = encoded.find(j*stripe_size);
	  if(cit==write_completion.end()||it==encoded.end()) {
	    //   std::throw std::out_of_range;
	  }
	}
	catch (std::out_of_range& e) {
	  std::cerr << "Out of range error accessing stripe_data. At index "
		    << index << "." << std::endl;
	  ret = -1;
	}
	for (it,cit; it!=encoded.end()||cit!=write_completion.end()||failed; 
	     it++,cit++) {
	  try {
	    ret = io_ctx.aio_write_full(
					stripe_data.find(index)->second.get_object_name(), 
					cit->second, it->second);
	  }
	  catch (std::exception& e) {
	    std::cerr << "Exception while writing object. "
		      << cit->first << std::endl;
	    ret = -1;
	  }
	  if (ret < 0) {
	    std::cerr << "couldn't start write object! error at index "
		      << index << std::endl;
	    ret = EXIT_FAILURE;
	    failed = true; 
	    // We have had a failure, so do not execute any further, 
	    //fall through.
	  }
	}
	write_completion_lock.unlock(); // if there is a failure in
	std::cerr << "The write_completion map length is " << write_completion.size()
	<< std::endl;
	// the previous block, this will still work
	pending_buffers_lock.lock();
	for (it=encoded.begin();
	     it!=encoded.end()||failed; it++,cit++) {
	  pending_buffers.insert(std::pair<uint32_t,
				 bufferlist>(it->first,it->second));
	}
	pending_buffers_lock.unlock();
	encoded.clear();
	FINISH_TIMER; // Compute total time since START_TIMER
	std::cerr << "Writing aio test. Iteration " << j << " Queue size "
		  << blq.size() << std::endl;
	REPORT_BENCHMARK; // Print out the benchmark for this test
    }
    // Print out the stripe object hashes and clear the stripe_data map
    if (DEBUG > 0) {
      for (sit=stripe_data.begin();sit!=stripe_data.end();sit++) {
	std::cerr << "Deleting Stripe object with hash value "
		  << sit->second.get_hash() << std::endl;
      }
    }
    stripe_data.clear();
  }

  START_TIMER; // Code for the begin_time
  AioCompletionThread.join(); // Wait for completions to finish.
  rados.shutdown();

  ret = failed ? ret:EXIT_SUCCESS;

  FINISH_TIMER; // Compute total time since START_TIMER
  std::cerr << "Cleanup." << std::endl;
  REPORT_TIMING; // Print out the elapsed time for this section
  std::cerr << "Total run time " << total_run_time_ms << " ms" << std::endl;
  return ret;
}
