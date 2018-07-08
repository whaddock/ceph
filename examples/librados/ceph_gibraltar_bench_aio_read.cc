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

/* Benchmark program for Gibraltar encoding of Ceph 
 * objects.
 * Author: Walker Haddock 2017-2018
 */

// install the librados-dev package to get this
#include "common/Mutex.h"
#include <rados/librados.hpp>
#include <iostream>
#include <string>
#include <sstream>
#include <thread>
#include <chrono>
#include <queue>
#include <deque>
#include <mutex>
#include <climits>

//#define TRACE

// Global vars
std::queue<int> objects;
std::deque<std::string> object_names;
std::mutex objects_lock, object_names_lock, cout_lock, pending_ops_lock;

int completions_count = 35;
int ret = 0;
int shard_size = 16777216/2;
std::string object_name_prefix("gibraltar_object");
librados::Rados rados;
librados::IoCtx io_ctx;
const std::chrono::milliseconds read_sleep_duration(100);

struct CompletionOp {
  int id;
  std::string name;
  librados::AioCompletion *completion;
  librados::bufferlist bl;

  explicit CompletionOp(std::string _name) : id(0), name(_name), completion(NULL) {}
};

std::map<int,CompletionOp *> pending_ops;

void io_cb(librados::completion_t c, CompletionOp *op) {
  std::lock_guard<std::mutex> guard(pending_ops_lock);

  std::map<int, CompletionOp *>::iterator iter = pending_ops.find(op->id);
  if (iter != pending_ops.end())
    pending_ops.erase(iter);

  op->completion->release();
  delete op;
  std::cout << "-";
}

static void _completion_cb(librados::completion_t c, void *param)
{
  CompletionOp *op = (CompletionOp *)param;
  io_cb(c, op);
}

// guarded queue accessor functions
void print_message(std::string message) {
  std::lock_guard<std::mutex> guard(cout_lock);
  std::cout << message << std::endl;
  std::cout.flush();
}

int get_next_object() {
  int object;
  std::lock_guard<std::mutex> guard(objects_lock);
  if (!objects.empty()) {
    object = objects.front();
    objects.pop();
  }
  else 
    object = INT_MIN;
  return object;
}

bool is_objects_queue_empty() {
  std::lock_guard<std::mutex> guard(objects_lock);
  return objects.empty();
}

std::string get_next_object_name() {
  std::string name;
  std::lock_guard<std::mutex> guard(object_names_lock);
  if (!object_names.empty()) {
    name = object_names.front();
    object_names.pop_front();
  }
  else
    name = "";
  return name;
}

void push_object_names(std::string name) {
  std::lock_guard<std::mutex> guard(object_names_lock);
  object_names.push_back(name);
}

bool is_object_names_queue_empty() {
  std::lock_guard<std::mutex> guard(object_names_lock);
  return object_names.empty();
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

// Print out the list of object names
void print_object_names_queue() {
  std::lock_guard<std::mutex> guard(object_names_lock);
  std::deque<std::string>::iterator it;
  std::cout << "Object Names:" << std::endl;
  for (it = object_names.begin();
       it!=object_names.end(); it++) {
    std::cout << *it << std::endl;
  }
}

// Threads to write and read objects

void writeThread() {
  std::cout << "Starting thread " << std::this_thread::get_id() << std::endl;
  std::cout.flush();

  int object;
  while (!is_objects_queue_empty()) {
    object = get_next_object();

    // Throttle. Limit the number of operations in flight.
    while (get_pending_ops_size() > completions_count)
      std::this_thread::sleep_for(read_sleep_duration);

#ifdef TRACE
    std::cout << "Starting on object " << object << std::endl;
    std::cout << std::endl;
    std::cout.flush();
#endif

    if (object < 0)
	    break;

    std::string name = object_name_prefix + "." + std::to_string(object);
    CompletionOp *op = new CompletionOp(name);
    op->completion = rados.aio_create_completion(op, _completion_cb, NULL);
    op->id = object;
    op->bl = librados::bufferlist();

    op->bl.append(std::string(shard_size,(char)object%26+97)); // start with 'a'
    /*
     * now that we have the data to write, let's send it to an object.
     * We'll use the synchronous interface for simplicity.
     */
    ret = io_ctx.aio_write(name, op->completion, op->bl, shard_size, (uint64_t)0);
    if (ret < 0) {
      std::cerr << "couldn't write object! error " << ret << std::endl;
      std::cerr.flush();
      ret = EXIT_FAILURE;
      return;
    } else {
      push_object_names(name);
      insert_pending_op(object,op);
      std::cout << "+";

#ifdef TRACE
      std::cout << "we just wrote new object " << name  << std::endl;
      std::cout.flush();
#endif
    }
    std::cout.flush();
  }
  // Wait until all operations are completed before exiting.
  while (get_pending_ops_size() > 0)
    std::this_thread::sleep_for(read_sleep_duration);
}

void readThread() {
  {
    std::stringstream message;
    message << "Starting thread " << std::this_thread::get_id();
    print_message(message.str());
  }

  int count = 0;
  std::string name;
  while (!is_object_names_queue_empty()) {

    // Throttle. Limit the number of operations in flight.
    while (get_pending_ops_size() > completions_count)
      std::this_thread::sleep_for(read_sleep_duration);

    name = get_next_object_name();

#ifdef TRACE
    {
      std::stringstream message;
      message << "Starting on object " << name;
      print_message(message.str());
    }
#endif

    if (name.empty()) {
      std::stringstream message;
      message << "name.compare(\"\") of object " << name
	      << " was true.  Exiting.";
      print_message(message.str());
      break;
    }
    CompletionOp *op = new CompletionOp(name);
    op->completion = rados.aio_create_completion(op, _completion_cb, NULL);
    op->id = count;
    op->bl = librados::bufferlist();
    // send off the request.
    ret = io_ctx.aio_read(name, op->completion, &op->bl, shard_size, (uint64_t)0);
    if (ret < 0) {
      std::cerr << "couldn't start read object! error " << ret << std::endl;
      ret = EXIT_FAILURE;
      // goto out;
    } 
    else {
      insert_pending_op(count,op);
      count++;
      std::cout << "+";
    }
    /* On every read iteration we check the work queue size. If there are
     * objects to write, then we want to keep the specified number of aios 
     * in flight. Once the limit of completions has been reached, we begin
     * checking the front of the queue which should quickly return and wil
     */
    std::cout.flush();
  }
  // Wait until all operations are completed before exiting.
  while (get_pending_ops_size() > 0)
    std::this_thread::sleep_for(read_sleep_duration);
}


int main(int argc, const char **argv)
{
  int iterations = 3;
  int thread_count = 10;
  std::vector<std::thread> v_threads;

  // we will use all of these below
  const char *pool_name = "gibraltar_pool";
  std::string hello("hello world!");


  // first, we create a Rados object and initialize it
  {
    ret = rados.init("admin"); // just use the client.admin keyring
    if (ret < 0) { // let's handle any error that might have come back
      std::cerr << "couldn't initialize rados! error " << ret << std::endl;
      ret = EXIT_FAILURE;
      // goto out;
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
      // goto out;
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
	    // goto out;
	  }
	  break;
	}
	if ((strcmp(argv[i], "-i") == 0) || (strcmp(argv[i], "--iterations") == 0)) {
	  iterations = atoi(argv[i+1]);
	}
	if ((strcmp(argv[i], "-t") == 0) || (strcmp(argv[i], "--threads") == 0)) {
	  thread_count = atoi(argv[i+1]);
	}
	if ((strcmp(argv[i], "-z") == 0) || (strcmp(argv[i], "--completions") == 0)) {
	  completions_count = atoi(argv[i+1]);
	}
      }
    }
  }

  std::cout << "iterations == " << iterations << std::endl;
  std::cout << "thread count == " << thread_count << std::endl;
  std::cout << "completions count == " << completions_count << std::endl;

  int write_threads = thread_count;

  /*
   * next, we actually connect to the cluster
   */
  {
    ret = rados.connect();
    if (ret < 0) {
      std::cerr << "couldn't connect to cluster! error " << ret << std::endl;
      ret = EXIT_FAILURE;
      // goto out;
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
      //  goto out;
    } else {
      std::cout << "we just created an ioctx for our pool" << std::endl;
      std::cout.flush();
    }
  }

  /*
   * now let's do some IO to the pool! We'll write "hello world!" to a
   * new object.
   */
  for ( int object = 0;
	object<iterations;object++) {
    objects.push(object);
  }

  {
    /*
     * "bufferlist"s are Ceph's native transfer type, and are carefully
     * designed to be efficient about copying. You can fill them
     * up from a lot of different data types, but strings or c strings
     * are often convenient. Just make sure not to deallocate the memory
     * until the bufferlist goes out of scope and any requests using it
     * have been finished!
     */

    for (int i = 0;i<1;i++) 
      v_threads.push_back(std::thread (writeThread));

    // The threads will exit after the objects are written

    std::vector<std::thread>::iterator it;
    for (it=v_threads.begin();it!=v_threads.end();it++)
      it->join();  // Wait for the writeThread to finish.

    v_threads.clear();
  }

  /*
   * now let's read that object back! Just for fun, we'll do it using
   * async IO instead of synchronous. (This would be more useful if we
   * wanted to send off multiple reads at once; see
   * http://ceph.com/docs/master/rados/api/librados/#asychronous-io )
   */

#ifdef TRACE
  print_object_names_queue();
#endif

  /*
   * Prompt user so we wait to do the read.
   */
  {
    std::string input;
    std::cout << std::endl << "Press enter when ready to proceed with read..." << std::endl;
    std::cout << "There are " << object_names.size() << " objects." 
	      << std::endl;
    std::getline(std::cin, input);
    std::cout << "Proceeding to read object back..." << std::endl;
  }

  {
    // Only one read thread with aio.
    for (int i = 0;i<1;i++) 
      v_threads.push_back(std::thread (readThread));

    // The threads will exit after the objects are read

    std::vector<std::thread>::iterator it;
    for (it=v_threads.begin();it!=v_threads.end();it++)
      it->join();  // Wait for the readThread to finish.

    v_threads.clear();
  }

  ret = EXIT_SUCCESS;
  //  out:
  std::cerr << "Exit routine. Results: " << ret << std::endl;
  std::cerr.flush();
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

  return ret;
}
