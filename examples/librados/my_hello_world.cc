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

#define DEBUG 0
#define ITERATIONS 120*10
#define START_ITERATIONS 120*10*3 
#define BUFFER_SIZE 4194304*2
#define START_TIMER begin_time = std::clock();
#define FINISH_TIMER end_time = std::clock(); \
               total_time_ms = (end_time - begin_time) / (double)(CLOCKS_PER_SEC / 1000);
#define REPORT_TIMING std::cout << total_time_ms  << " ms\t" << std::endl;
#define REPORT_BENCHMARK std::cout << total_time_ms  << " ms\t" << ((double)ITERATIONS * (double)BUFFER_SIZE) / (1024*1024) << " MB\t" \
  << ((double)ITERATIONS * (double)BUFFER_SIZE) / (double)(1024*total_time_ms) << " MB/s" << std::endl; \
  total_run_time_ms += total_time_ms;

int main(int argc, const char **argv)
{
  int ret = 0;

  // we will use all of these below
  const char *pool_name = "hello_world_pool";
  std::string hello("hello world!");
  std::string object_name("hello_object");
  librados::IoCtx io_ctx;

  // variables used for timing
  std::clock_t end_time;
  std::clock_t begin_time;
  std::clock_t total_time_ms = 0;
  std::clock_t total_run_time_ms = 0;

  START_TIMER; // Code for the begin_time
  std::cout << "pool_name = " << pool_name << std::endl;

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
   * let's create our own pool instead of scribbling over real data.
   * Note that this command creates pools with default PG counts specified
   * by the monitors, which may not be appropriate for real use -- it's fine
   * for testing, though.
   */
  {
    ret = rados.pool_create(pool_name);
    if (ret < 0) {
      std::cerr << "couldn't create pool! error " << ret << std::endl;
      return EXIT_FAILURE;
    } else {
      std::cout << "we just created a new pool named " << pool_name << std::endl;
    }
  }

  /*
   * create an "IoCtx" which is used to do IO to a pool
   */
  {
    ret = rados.ioctx_create(pool_name, io_ctx);
    if (ret < 0) {
      std::cerr << "couldn't set up ioctx! error " << ret << std::endl;
      ret = EXIT_FAILURE;
      goto out;
    } else {
      std::cout << "we just created an ioctx for our pool" << std::endl;
    }
  }

  /*
   * now let's do some IO to the pool! We'll write "hello world!" to a
   * new object.
   */
  {
    /*
     * "bufferlist"s are Ceph's native transfer type, and are carefully
     * designed to be efficient about copying. You can fill them
     * up from a lot of different data types, but strings or c strings
     * are often convenient. Just make sure not to deallocate the memory
     * until the bufferlist goes out of scope and any requests using it
     * have been finished!
     */
    librados::bufferlist bl;
    bl.append(std::string(BUFFER_SIZE,'X'));

    /*
     * now that we have the data to write, let's send it to an object.
     * We'll use the synchronous interface for simplicity.
     */
    ret = io_ctx.write_full(object_name, bl);
    if (ret < 0) {
      std::cerr << "couldn't write object! error " << ret << std::endl;
      ret = EXIT_FAILURE;
      goto out;
    } else {
      std::cout << "we just wrote new object " << object_name
	        << ", with contents\n" << hello << std::endl;
    }
  }

  /*
   * now let's read that object back! Just for fun, we'll do it using
   * async IO instead of synchronous. (This would be more useful if we
   * wanted to send off multiple reads at once; see
   * http://ceph.com/docs/master/rados/api/librados/#asychronous-io )
   */
  {
    librados::bufferlist read_buf;
    long read_len = BUFFER_SIZE; // this is way more than we need
    // allocate the completion from librados
    librados::AioCompletion *read_completion = librados::Rados::aio_create_completion();
    // send off the request.
    ret = io_ctx.aio_read(object_name, read_completion, &read_buf, read_len, 0);
    if (ret < 0) {
      std::cerr << "couldn't start read object! error " << ret << std::endl;
      ret = EXIT_FAILURE;
      goto out;
    }
    // wait for the request to complete, and check that it succeeded.
    read_completion->wait_for_complete();
    ret = read_completion->get_return_value();
    if (ret < 0) {
      std::cerr << "couldn't read object! error " << ret << std::endl;
      ret = EXIT_FAILURE;
      goto out;
    } else {
      std::cout << "we read our object " << object_name
	  << ", and got back " << ret << " bytes with contents\n"
		<< "Size of buffer was " << read_buf.length() << std::endl;
    }
  }

  /*
   * We can also use xattrs that go alongside the object.
   */
  {
    librados::bufferlist version_bl;
    version_bl.append('1');
    ret = io_ctx.setxattr(object_name, "version", version_bl);
    if (ret < 0) {
      std::cerr << "failed to set xattr version entry! error "
		<< ret << std::endl;
      ret = EXIT_FAILURE;
      goto out;
    } else {
      std::cout << "we set the xattr 'version' on our object!" << std::endl;
    }
  }

  /*
   * And if we want to be really cool, we can do multiple things in a single
   * atomic operation. For instance, we can update the contents of our object
   * and set the version at the same time.
   */
  {
    librados::bufferlist bl;
    bl.append(hello);
    bl.append("v2");
    librados::ObjectWriteOperation write_op;
    write_op.write_full(bl);
    librados::bufferlist version_bl;
    version_bl.append('2');
    write_op.setxattr("version", version_bl);
    ret = io_ctx.operate(object_name, &write_op);
    if (ret < 0) {
      std::cerr << "failed to do compound write! error " << ret << std::endl;
      ret = EXIT_FAILURE;
      goto out;
    } else {
      std::cout << "we overwrote our object " << object_name
		<< " with contents\n" << bl.c_str() << std::endl;
    }
  }

  /*
   * And to be even cooler, we can make sure that the object looks the
   * way we expect before doing the write! Notice how this attempt fails
   * because the xattr differs.
   */
  {
    librados::ObjectWriteOperation failed_write_op;
    librados::bufferlist bl;
    bl.append(hello);
    bl.append("v2");
    librados::ObjectWriteOperation write_op;
    write_op.write_full(bl);
    librados::bufferlist version_bl;
    version_bl.append('2');
    librados::bufferlist old_version_bl;
    old_version_bl.append('1');
    failed_write_op.cmpxattr("version", LIBRADOS_CMPXATTR_OP_EQ, old_version_bl);
    failed_write_op.write_full(bl);
    failed_write_op.setxattr("version", version_bl);
    ret = io_ctx.operate(object_name, &failed_write_op);
    if (ret < 0) {
      std::cout << "we just failed a write because the xattr wasn't as specified"
		<< std::endl;
    } else {
      std::cerr << "we succeeded on writing despite an xattr comparison mismatch!"
		<< std::endl;
      ret = EXIT_FAILURE;
      goto out;
    }

    /*
     * Now let's do the update with the correct xattr values so it
     * actually goes through
     */
    bl.clear();
    bl.append(hello);
    bl.append("v3");
    old_version_bl.clear();
    old_version_bl.append('2');
    version_bl.clear();
    version_bl.append('3');
    librados::ObjectWriteOperation update_op;
    update_op.cmpxattr("version", LIBRADOS_CMPXATTR_OP_EQ, old_version_bl);
    update_op.write_full(bl);
    update_op.setxattr("version", version_bl);
    ret = io_ctx.operate(object_name, &update_op);
    if (ret < 0) {
      std::cerr << "failed to do a compound write update! error " << ret
		<< std::endl;
      ret = EXIT_FAILURE;
      goto out;
    } else {
      std::cout << "we overwrote our object " << object_name
	        << " following an xattr test with contents\n" << bl.c_str()
	        << std::endl;
    }
  }
  FINISH_TIMER; // Compute total time since START_TIMER
  std::cout << "Preliminary tests total time." << std::endl;
  REPORT_TIMING; // Print out the benchmark for this test

  {
    /* Here we write 4 buffers from a map<int,bufferlist>
     * like we would do in the erasure code benchmark. Then we
     * will read them back in using the async read.
     */
    START_TIMER; // Code for the begin_time
    librados::bufferlist bl;
    bl.append(std::string(BUFFER_SIZE,'X'));
    std::map<long,bufferlist> encoded;
    for (long i=START_ITERATIONS;i<ITERATIONS+START_ITERATIONS;i++) {
      encoded.insert( std::pair<long,bufferlist>(i,bl));
    }

    /*
     * now that we have the data to write, let's send it to an object.
     * We'll use the synchronous interface for simplicity.
     */
    std::map<long,bufferlist>::iterator it = encoded.begin();
    FINISH_TIMER; // Compute total time since START_TIMER
    std::cout << "Setup for first write test." << std::endl;
    REPORT_TIMING; // Print out the benchmark for this test

    START_TIMER; // Code for the begin_time
    for (it=encoded.begin(); it!=encoded.end(); it++) {
      std::stringstream object_name;
      object_name  << "Obj-" << it->first;
      ret = io_ctx.write_full(object_name.str(), it->second);
      if (ret < 0) {
	std::cerr << "couldn't write object " << object_name 
		  << " ! error " << ret << std::endl;
	ret = EXIT_FAILURE;
	goto out;
      } else {
	if (DEBUG > 0) {
	  std::cout << "we just wrote new object " << object_name.str()
		    << std::endl;
	}
      }
    }
    FINISH_TIMER; // Compute total time since START_TIMER
    std::cout << "First write test." << std::endl;
    REPORT_BENCHMARK; // Print out the benchmark for this test
  }

  {
    /* Here we write 4 buffers from a map<int,bufferlist>
     * like we would do in the erasure code benchmark. Then we
     * will read them back in using the async read.
     */
    START_TIMER; // Code for the begin_time
    librados::bufferlist bl;
    bl.append(std::string(BUFFER_SIZE,'X'));
    std::map<long,bufferlist> encoded;
    // this is the second set of objects
    for (long i=START_ITERATIONS+ITERATIONS+1;i<2*ITERATIONS+START_ITERATIONS;i++) {
      encoded.insert( std::pair<long,bufferlist>(i,bl));
    }

    /*
     * now that we have the data to write, let's send it to an object.
     * We'll use the ObjectWriteOperation.
     */
    std::map<long,bufferlist>::iterator it = encoded.begin();
    FINISH_TIMER; // Compute total time since START_TIMER
    std::cout << "Setup for second write test. Uses write operation." << std::endl;
    REPORT_TIMING; // Print out the benchmark for this test

    START_TIMER // Code for the begin_time
    for (it=encoded.begin(); it!=encoded.end(); it++) {
      std::stringstream object_name;
      object_name  << "Obj-" << it->first;
      librados::ObjectWriteOperation write_op;
      write_op.write_full(it->second);
      ret = io_ctx.operate(object_name.str(), &write_op);
      if (ret < 0) {
	std::cerr << "couldn't do a compound write object " << object_name.str()
		  << " ! error " << ret << std::endl;
	ret = EXIT_FAILURE;
	goto out;
      } else {
	if (DEBUG > 0) {
	  std::cout << "we just wrote new object " << object_name.str()
		    << std::endl;
	}
      } 
    }
    FINISH_TIMER; // Compute total time since START_TIMER
    std::cout << "Second write test." << std::endl;
    REPORT_BENCHMARK; // Print out the benchmark for this test
  }

  /*
   * now let's read that object back! Just for fun, we'll do it using
   * async IO instead of synchronous. (This would be more useful if we
   * wanted to send off multiple reads at once; see
   * http://ceph.com/docs/master/rados/api/librados/#asychronous-io )
   */
  START_TIMER; // Code for the begin_time
  {
    std::map<long,bufferlist> encoded;
    for (long i=START_ITERATIONS+ITERATIONS+1;i<2*ITERATIONS+START_ITERATIONS;i++) {
      librados::bufferlist bl;
       encoded.insert( std::pair<long,bufferlist>(i,bl));
     }

     long read_len = BUFFER_SIZE; // this is way more than we need
     // allocate the completion from librados
     std::map<long,bufferlist>::iterator it = encoded.begin();
     FINISH_TIMER; // Compute total time since START_TIMER
     std::cout << "Setup for first read test. Using AIO." << std::endl;
     REPORT_TIMING; // Print out the benchmark for this test

     START_TIMER; // Code for the begin_time
     for (it=encoded.begin(); it!=encoded.end(); it++) {
       std::stringstream object_name;
       object_name  << "Obj-" << it->first;
       librados::AioCompletion *read_completion = librados::Rados::aio_create_completion();
       // send off the request.
       ret = io_ctx.aio_read(object_name.str(), read_completion, &it->second, read_len, 0);
       if (ret < 0) {
	 std::cerr << "couldn't start read object! error " << ret << std::endl;
	 ret = EXIT_FAILURE;
	 goto out;
       }
       // wait for the request to complete, and check that it succeeded.
       read_completion->wait_for_complete();
       ret = read_completion->get_return_value();
       if (ret < 0) {
	 std::cerr << "couldn't read object " << object_name.str() << "! error " << ret << std::endl;
	 ret = EXIT_FAILURE;
	 goto out;
       } else {
	 if (DEBUG > 0) {
	   std::cout << "we read our object " << object_name.str()
		     << ", and got back " << ret << " bytes with contents\n"
		     << "Size of buffer was " << it->second.length() << std::endl;
	 }
       }
     }
     FINISH_TIMER; // Compute total time since START_TIMER
     std::cout << "Read test." << std::endl;
     REPORT_BENCHMARK; // Print out the benchmark for this test
   }

  /*
   * now let's write the objects! Just for fun, we'll do it using
   * async IO instead of synchronous. 
   * Here we do write all of the objects and then wait for completion
   * after they have been dispatched.
   * http://ceph.com/docs/master/rados/api/librados/#asychronous-io )
   */
  {
    START_TIMER; // Code for the begin_time
    std::map<long,bufferlist> encoded;
    std::map<long,librados::AioCompletion*> write_completion;
    for (long i=START_ITERATIONS+2*ITERATIONS+1;i<3*ITERATIONS+START_ITERATIONS;i++) {
      librados::bufferlist bl;
      bl.append(std::string(BUFFER_SIZE,'X'));
      encoded.insert( std::pair<long,bufferlist>(i,bl));
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
       object_name  << "Obj-" << it->first;
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
       object_name  << "Obj-" << cit->first;
       ret = cit->second->get_return_value();
       if (ret < 0) {
	 std::cerr << "couldn't write object " << object_name.str() << "! error " << ret << std::endl;
	 ret = EXIT_FAILURE;
	 goto out;
       } else {
	 if (DEBUG > 0) {
	   std::cout << "we wrote our object " << object_name.str()
		     << ", and got back " << ret << " bytes with contents\n"
		     << "Size of buffer was " << it->second.length() << std::endl;
	 }
       }
     }
     FINISH_TIMER; // Compute total time since START_TIMER
     std::cout << "Writing aio test." << std::endl;
     REPORT_BENCHMARK; // Print out the benchmark for this test
   }

  /*
   * now let's read that object back! Just for fun, we'll do it using
   * async IO instead of synchronous. 
   * Here we do write all of the objects and then wait for completion
   * after they have been dispatched.
   * http://ceph.com/docs/master/rados/api/librados/#asychronous-io )
   */
  {
    START_TIMER; // Code for the begin_time
    std::map<long,bufferlist> encoded;
    std::map<long,librados::AioCompletion*> read_completion;
    for (long i=START_ITERATIONS+2*ITERATIONS+1;i<3*ITERATIONS+START_ITERATIONS;i++) {
      librados::bufferlist bl;
      encoded.insert( std::pair<long,bufferlist>(i,bl));
      read_completion.insert( std::pair<long,librados::AioCompletion*>(i,librados::Rados::aio_create_completion()));
     }

     long read_len = BUFFER_SIZE; // this is way more than we need
     std::map<long,bufferlist>::iterator it;
     std::map<long,librados::AioCompletion*>::iterator cit;
     FINISH_TIMER; // Compute total time since START_TIMER
     std::cout << "Setup for asynchronous read test." << std::endl;
     REPORT_TIMING; // Print out the benchmark for this test

     START_TIMER; // Code for the begin_time
     for (it=encoded.begin(),cit=read_completion.begin(); it!=encoded.end()||cit!=read_completion.end(); it++,cit++) {
       std::stringstream object_name;
       object_name  << "Obj-" << it->first;
       ret = io_ctx.aio_read(object_name.str(), cit->second, &it->second, read_len, 0);
       if (ret < 0) {
	 std::cerr << "couldn't start read object! error " << object_name.str() << " " << ret << std::endl;
	 ret = EXIT_FAILURE;
	 goto out;
       }
     }
       
     for (cit=read_completion.begin();cit!=read_completion.end();cit++) {
       // wait for the request to complete, and check that it succeeded.
       cit->second->wait_for_complete();
       std::stringstream object_name;
       object_name  << "Obj-" << cit->first;
       ret = cit->second->get_return_value();
       if (ret < 0) {
	 std::cerr << "couldn't read object " << object_name.str() << "! error " << ret << std::endl;
	 ret = EXIT_FAILURE;
	 goto out;
       } else {
	 if (DEBUG > 0) {
	   std::cout << "we read our object " << object_name.str()
		     << ", and got back " << ret << " bytes with contents\n"
		     << "Size of buffer was " << it->second.length() << std::endl;
	 }
       }
     }
     FINISH_TIMER; // Compute total time since START_TIMER
     std::cout << "Reading asynchronously." << std::endl;
     REPORT_BENCHMARK; // Print out the benchmark for this test
   }
  START_TIMER; // Code for the begin_time
  ret = EXIT_SUCCESS;
  out:
  /*
   * And now we're done, so let's remove our pool and then
   * shut down the connection gracefully.
   */
  int delete_ret = rados.pool_delete(pool_name);
  if (delete_ret < 0) {
    // be careful not to
    std::cerr << "We failed to delete our test pool!" << std::endl;
    ret = EXIT_FAILURE;
  }

  rados.shutdown();

  FINISH_TIMER; // Compute total time since START_TIMER
  std::cout << "Reading asynchronously." << std::endl;
  REPORT_TIMING; // Print out the benchmark for this test
  std::cout << "Total run time " << total_run_time_ms << " ms" << std::endl;
  return ret;
}
