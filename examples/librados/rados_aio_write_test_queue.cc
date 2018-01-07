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
#include <cassert>

#define DEBUG 0
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
  uint32_t stripe_size = std::stoi(argv[2]);
  uint32_t queue_size = std::stoi(argv[3]);
  uint32_t object_size = std::stoi(argv[4]);
  std::string obj_name = argv[5];
  std::string pool_name = argv[6];

  int ret = 0;

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
    // Create a queue with bufferlists
    std::queue<librados::bufferlist> blq;
    for (int i=0;i<queue_size;i++) {
      librados::bufferlist bl;
      bl.append(std::string(object_size,(char)i%26+97)); // start with 'a'
      blq.push(bl);
    }

    // Get a stripe of bufferlists from the queue
    std::map<uint32_t,bufferlist> encoded;
    std::map<uint32_t,librados::AioCompletion*> write_completion;
    std::map<uint32_t,Stripe> stripe_data;
    std::map<uint32_t,bufferlist>::iterator it;
    std::map<uint32_t,librados::AioCompletion*>::iterator cit;

    // the iteration loop begins here
    for (uint32_t j=0;j<iterations;j++) {
      uint32_t index = 0; // index = data.get_hash()
      for (uint32_t i=0;i<stripe_size;i++) {
	index = j*stripe_size+i;
	std::stringstream object_name;
	object_name << obj_name << "." << index;
	Stripe data(j,i,stripe_size,object_name.str());
	assert(index==data.get_hash()); // disable assert by defining #NDEBUG
	stripe_data.insert(std::pair<uint32_t,Stripe>(data.get_hash(),data));
	encoded.insert( std::pair<uint32_t,bufferlist>(data.get_hash(),blq.front()));
	blq.pop(); // remove the bl from the queue
	std::cerr << "Created a Stripe with hash value " << data.get_hash() << std::endl;
	write_completion.insert( 
				std::pair<uint32_t,
				librados::AioCompletion*>
				(data.get_hash(),
				 librados::Rados::
				 aio_create_completion((void *)&data,NULL,NULL)));
      }

      FINISH_TIMER; // Compute total time since START_TIMER
      std::cerr << "Setup for write test using AIO." << std::endl;
      REPORT_TIMING; // Print out the benchmark for this test
    
      START_TIMER; // Code for the begin_time
      for (it=encoded.begin(),cit=write_completion.find(j*stripe_size); it!=encoded.end()||cit!=write_completion.end(); it++,cit++) {
	try{
	  Stripe data = stripe_data.at(cit->first);
	  ret = io_ctx.aio_write_full(data.get_object_name(), cit->second, it->second);
	}
	catch (std::out_of_range& e) {
	  std::cerr << "Out of range error accessing stripe_data. " 
		    << std::endl;
	  ret = -1;
	}
	if (ret < 0) {
	  std::cerr << "couldn't start write object! error " << cit->first
	  << " " << ret << std::endl;
	  ret = EXIT_FAILURE;
	  goto out;
	}
      }
       
      for (cit=write_completion.find(j*stripe_size);cit!=write_completion.end();cit++) {
	// wait for the request to complete, and check that it succeeded.
	cit->second->wait_for_safe();
	ret = cit->second->get_return_value();
	if (ret < 0) {
	  std::cerr << "couldn't write object " << cit->first << "! error " << ret << std::endl;
	  ret = EXIT_FAILURE;
	  goto out;
	} else {
	  blq.push(encoded[cit->first]);
	  cit->second->release();
	  if (DEBUG > 0) {
	    std::cerr << "we wrote our object "
		      << cit->first
		      << ", and got back " << ret << " bytes with contents\n"
		      << "Size of buffer was " << encoded[cit->first].length()
		      << std::endl;
	  }
	}
      }
      encoded.clear();
      write_completion.clear();
      FINISH_TIMER; // Compute total time since START_TIMER
      std::cerr << "Writing aio test. Iteration " << j << " Queue size "
		<< blq.size() << std::endl;
      REPORT_BENCHMARK; // Print out the benchmark for this test
   }
    // Print out the stripe object hashes and clear the stripe_data map
    std::map<uint32_t,Stripe>::iterator sit;
    for (sit=stripe_data.begin();sit!=stripe_data.end();sit++) {
      std::cerr << "Deleting Stripe object with hash value " << sit->second.get_hash() << std::endl;
    } // End of outer j loop
    stripe_data.clear();
  }

  START_TIMER; // Code for the begin_time
  ret = EXIT_SUCCESS;
  out:

  rados.shutdown();

  FINISH_TIMER; // Compute total time since START_TIMER
  std::cerr << "Cleanup." << std::endl;
  REPORT_TIMING; // Print out the elapsed time for this section
  std::cerr << "Total run time " << total_run_time_ms << " ms" << std::endl;
  return ret;
}
