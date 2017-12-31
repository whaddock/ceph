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

#define DEBUG 0
#define START_TIMER begin_time = std::clock();
#define FINISH_TIMER end_time = std::clock(); \
               total_time_ms = (end_time - begin_time) / (double)(CLOCKS_PER_SEC / 1000);
#define REPORT_TIMING std::cout << total_time_ms  << " ms\t" << std::endl;
#define REPORT_BENCHMARK std::cout << total_time_ms  << " ms\t" << ((double)stripe_size * (double)object_size) / (1024*1024) << " MB\t" \
  << ((double)stripe_size * (double)object_size) / (double)(1024*total_time_ms) << " MB/s" << std::endl; \
  total_run_time_ms += total_time_ms;
#define POOL_NAME "hello_world_pool_1"

int main(int argc, const char **argv)
{
  if(argc < 7)
  {
      std::cout <<"Please put in correct params\n"<<
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
  std::cout << "Iterations = " << iterations << std::endl;
  std::cout << "Stripe Size = " << stripe_size << std::endl;
  std::cout << "Queue Size = " << queue_size << std::endl;
  std::cout << "Object Size = " << object_size << std::endl;
  std::cout << "Object Name Prefix = " << obj_name << std::endl;
  std::cout << "Pool Name = " << pool_name << std::endl;

  // first, we create a Rados object and initialize it
  librados::Rados rados;
  {
    ret = rados.init("admin"); // just use the client.admin keyring
    if (ret < 0) { // let's handle any error that might have come back
      std::cerr << "couldn't initialize rados! error " << ret << std::endl;
      ret = EXIT_FAILURE;
      goto out;
    } else {
      std::cout << "we just set up a rados cluster object" << std::endl;
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
      std::cout << "we just parsed our config options" << std::endl;
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
      std::cout << "we just connected to the rados cluster" << std::endl;
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
      std::cout << "we just created an ioctx for our pool" << std::endl;
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
    std::map<long,bufferlist> encoded;
    std::map<long,librados::AioCompletion*> write_completion;

    // the iteration loop begins here
    for (uint32_t j=0;j<iterations;j++) {
      for (uint32_t i=0;i<stripe_size;i++) {
	encoded.insert( std::pair<long,bufferlist>(i,blq.front()));
	blq.pop(); // remove the bl from the queue
	write_completion.insert( std::pair<long,librados::AioCompletion*>(i,librados::Rados::aio_create_completion()));
      }

      std::map<long,bufferlist>::iterator it;
      std::map<long,librados::AioCompletion*>::iterator cit;
      FINISH_TIMER; // Compute total time since START_TIMER
      std::cout << "Setup for write test using AIO." << std::endl;
      REPORT_TIMING; // Print out the benchmark for this test

      START_TIMER; // Code for the begin_time
      for (it=encoded.begin(),cit=write_completion.begin(); it!=encoded.end()||cit!=write_completion.end(); it++,cit++) {
	std::stringstream object_name;
	object_name << obj_name << "-" << j << "-" << it->first;
	ret = io_ctx.aio_write_full(object_name.str(), cit->second, it->second);
	if (ret < 0) {
	  std::cerr << "couldn't start write object! error " << object_name.str() << " " << ret << std::endl;
	  ret = EXIT_FAILURE;
	  goto out;
	}
      }
       
      for (cit=write_completion.begin();cit!=write_completion.end();cit++) {
	// wait for the request to complete, and check that it succeeded.
	cit->second->wait_for_complete();
	std::stringstream object_name;
	object_name  << obj_name << "-" << j << "-" << cit->first;
	ret = cit->second->get_return_value();
	if (ret < 0) {
	  std::cerr << "couldn't write object " << object_name.str() << "! error " << ret << std::endl;
	  ret = EXIT_FAILURE;
	  goto out;
	} else {
	  blq.push(encoded[cit->first]);
	  if (DEBUG > 0) {
	    cit->second->release();
	    std::cout << "we wrote our object " << object_name.str()
		      << ", and got back " << ret << " bytes with contents\n"
		      << "Size of buffer was " << it->second.length() << std::endl;
	  }
	}
      }
      encoded.clear();
      write_completion.clear();
      FINISH_TIMER; // Compute total time since START_TIMER
      std::cout << "Writing aio test. Iteration " << j << " Queue size " << blq.size() << std::endl;
      REPORT_BENCHMARK; // Print out the benchmark for this test
   }
  }

  START_TIMER; // Code for the begin_time
  ret = EXIT_SUCCESS;
  out:

  rados.shutdown();

  FINISH_TIMER; // Compute total time since START_TIMER
  std::cout << "Cleanup." << std::endl;
  REPORT_TIMING; // Print out the elapsed time for this section
  std::cout << "Total run time " << total_run_time_ms << " ms" << std::endl;
  return ret;
}
