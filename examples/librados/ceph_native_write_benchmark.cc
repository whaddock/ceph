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

/* Write benchmark using Ceph native erasure coded pool with
 * K=20, M=4, Cauchy.
 */

// install the librados-dev package to get this
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
#include <rados/librados.hpp>

#include "include/utime.h"
#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <mutex>
#include <string>
#include <map>
#include <queue>
#include <climits>

//#define TRACE
//#define VERBOSITY_1
#define THREAD_ID << ceph_clock_now(g_ceph_context)  << " Thread: " << std::this_thread::get_id() << " | "

#define RADOS_THREADS 1
#define READ_SLEEP_DURATION 2 // in milliseconds

// Globals
int in_size, rados_mode, concurrentios;
int queue_size, max_iterations;
std::string obj_name, pool_name;
boost::intrusive_ptr<CephContext> cct;
std::queue<int> stripes;
bool writing_done = false;
bool reading_done = false;
librados::Rados rados;
librados::IoCtx io_ctx;
const std::chrono::milliseconds read_sleep_duration(READ_SLEEP_DURATION);

// Locks for containers sharee with the handleAioCompletions thread.
std::mutex output_lock;
std::mutex stripes_lock;
std::mutex objs_lock;
std::mutex completions_lock;
std::mutex pending_ops_lock;
std::mutex bs_ops_lock;
std::mutex cout_lock;

struct CompletionOp {
  int id;
  std::string name;
  librados::AioCompletion *completion;
  librados::bufferlist bl;

  explicit CompletionOp(std::string _name) : id(0), name(_name), completion(NULL) {}
};
std::map<int,CompletionOp *> bs_ops, pending_ops;

// Object information. We stripe over these objects.
struct obj_info {
  string name;
  int id;
  size_t len;

  explicit obj_info(std::string _name) : name(_name), id(0), len(0) {}
};
std::queue<obj_info *> objs;

// guarded queue accessor functions
void print_message(std::string message) {
  std::lock_guard<std::mutex> guard(cout_lock);
  std::cout << message << std::endl;
  std::cout.flush();
}

int get_bs_ops_size() { 
  std::lock_guard<std::mutex> guard(bs_ops_lock);
  return bs_ops.size();
}

void insert_bs_op(int index, CompletionOp *op) {
  std::lock_guard<std::mutex> guard(bs_ops_lock);
  bs_ops.insert(std::pair<int,CompletionOp *>(index,op));
  return;
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

void insert_objs(obj_info *info) {
  std::lock_guard<std::mutex> guard(objs_lock);
  objs.push(info);
  return;
}

int get_objs_queue_size() { 
  std::lock_guard<std::mutex> guard(objs_lock);
  return objs.size();
}

obj_info* get_objs() {
  std::lock_guard<std::mutex> guard(objs_lock);
  // Caller must trap exceptions
  obj_info *stripe = objs.front();
  objs.pop();
  return stripe;
}

bool is_objs_queue_empty() {
  std::lock_guard<std::mutex> guard(objs_lock);
  return objs.empty();
}

// Bootstrap Completion object
void bs_cb(librados::completion_t c, CompletionOp *op) {
  std::lock_guard<std::mutex> guard(bs_ops_lock);

  std::map<int, CompletionOp *>::iterator iter = bs_ops.find(op->id);
  int ret = op->completion->get_return_value();
  //  std::cout << op->name << " callback return value: " << ret << std::endl;
  op->completion->release();
  obj_info *info = new obj_info(op->name);
  info->id = op->id;
  insert_objs(info);
  delete op;
  // std::cout << "Inserted " << info->name << std::endl;
  if (iter != bs_ops.end())
    bs_ops.erase(iter);
}

static void _bs_completion_cb(librados::completion_t c, void *param)
{
  CompletionOp *op = (CompletionOp *)param;
  bs_cb(c, op);
}

// Write Completion object
void io_cb(librados::completion_t c, CompletionOp *op) {
  std::lock_guard<std::mutex> guard(pending_ops_lock);

  std::map<int, CompletionOp *>::iterator iter = pending_ops.find(op->id);
  int ret = op->completion->get_return_value();
  //  std::cout << op->name << " callback return value: " << ret << std::endl;
  op->completion->release();

  delete op;
  //  std::cout << "-";
  if (iter != pending_ops.end())
    pending_ops.erase(iter);
}

static void _completion_cb(librados::completion_t c, void *param)
{
  CompletionOp *op = (CompletionOp *)param;
  io_cb(c, op);
}

void read_cb(librados::completion_t c, CompletionOp *op) {
  std::lock_guard<std::mutex> guard(pending_ops_lock);

  std::map<int, CompletionOp *>::iterator iter = pending_ops.find(op->id);
  int ret = op->completion->get_return_value();
  //  std::cout << op->name << " callback return value: " << ret << std::endl;
  op->completion->release();

  delete op;
  //  std::cout << "-";
  if (iter != pending_ops.end())
    pending_ops.erase(iter);
}

static void _read_completion_cb(librados::completion_t c, void *param)
{
  CompletionOp *op = (CompletionOp *)param;
  read_cb(c, op);
}

/* Function used for bootstrap thread
 */
void bootstrapThread()
{
  int ret = 0;
  int buf_len = 1;
  int stripe = 0;
  string name;

#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Starting bootstrapThread()" << std::endl;
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

    std::stringstream name;
    name << obj_name << stripe;
    bufferptr p = buffer::create(buf_len);
    bufferlist bl;
    memset(p.c_str(), 0, buf_len);
    bl.push_back(p);

#ifdef TRACE
    output_lock.lock();
    std::cerr THREAD_ID << "Creating object: " << name.str()
			<< std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif

    CompletionOp *op = new CompletionOp(name.str());
    op->completion = rados.aio_create_completion(op, _bs_completion_cb, NULL);
    op->id = stripe;
    op->bl = bl;

    // generate object
    ret = io_ctx.aio_write(op->name, op->completion, op->bl, buf_len, in_size - buf_len);
    if (ret < 0) {
      cerr << "couldn't write obj: " << name.str() << " ret=" << ret << std::endl;
    }
    else {
      insert_bs_op(stripe,op);
    }
  }
#ifdef TRACE
  output_lock.lock();
  std::cerr THREAD_ID << "Bootstrap thread exiting now." << std::endl;
  std::cerr.flush();
  output_lock.unlock();
#endif
}

void radosWriteThread() {
  bool started = false;
  int ret;
  obj_info *info;
  string name;

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

#ifdef TRACEX
    output_lock.lock();
    std::cerr THREAD_ID << "Write Loop entered in  radosWriteThread()" << std::endl;
    std::cerr.flush();
    output_lock.unlock();
#endif

    try {
      while (!is_objs_queue_empty()) {
	info = get_objs();

	std::stringstream name;
	CompletionOp *op = new CompletionOp(info->name);
	op->completion = rados.aio_create_completion(op, _completion_cb, NULL);
	op->id = info->id;
	op->bl = librados::bufferlist();
	op->bl.append(std::string(in_size,(char)info->id%26+97)); // start with 'a'

#ifdef VERBOSITY_1
	std::cout THREAD_ID << "Processing stripe " << info->id << std::endl;
	std::cout THREAD_ID << "op->id: " << op->id << std::endl;
	std::cout THREAD_ID << "name: " << op->name << std::endl;
	std::string sample;
	op->bl.copy((unsigned int)0, (unsigned int)10, sample);
	std::cout THREAD_ID << "bufferlist sample text: " << sample << std::endl;
	std::cout THREAD_ID << "bufferlist length: " << op->bl.length() << std::endl;
#endif

	ret = io_ctx.aio_write_full(op->name, op->completion, op->bl);

#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "Write call returned:" << ret << std::endl;
	std::cerr.flush();
	output_lock.unlock();
#endif
	if (ret < 0) {
#ifdef TRACE
	  output_lock.lock();
	  std::cerr THREAD_ID << "couldn't start write object! error at index "
			      << name << std::endl;
	  std::cerr.flush();
	  output_lock.unlock();
#endif
	  ret = EXIT_FAILURE;
	  // We have had a failure, so do not execute any further, 
	  // fall through.
	}

	insert_pending_op(info->id,op);

	/* throttle...
	 * This block causes the write thread to wait until the number
	 * of outstanding AIO completions is below the number of 
	 * concurrentios that was set in the configuration. Since
	 * the completions queue and the shards_in_flight queue are
	 * a bijection, we are assured of dereferencing the corresponding
	 * buffer once the write has completed.
	 */
#ifdef TRACE
	output_lock.lock();
	std::cerr THREAD_ID << "pending_ops_size "
			    << get_pending_ops_size() << " > " << concurrentios
			    << std::endl;
	std::cerr.flush();
	output_lock.unlock();
#endif
	
      } // End of write loop
    }
    catch (...) {
	/* anything, mostly if we try to get another item from the queue but
	 * it is already empty because we have multiple threads.
	 */ 
	continue; 
    }
  }
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

  int ret;
  int count = 0;
  int stripe = 0;
  std::string name;

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

    try {
      while (!is_stripes_queue_empty()) {
	stripe = get_stripe();
	name = obj_name + std::to_string(stripe);

	CompletionOp *op = new CompletionOp(name);
	op->completion = rados.aio_create_completion(op, _read_completion_cb, NULL);
	op->name = name.c_str();
	op->id = count;
	op->bl = librados::bufferlist();
	op->bl.append(std::string(in_size,(char)count%26+97)); // start with 'a'

	ret = io_ctx.aio_read(op->name, op->completion, &op->bl, (uint64_t)in_size, 0);
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
			      << name << std::endl;
	  std::cerr.flush();
	  output_lock.unlock();
#endif
	  ret = EXIT_FAILURE;
	  // We have had a failure, so do not execute any further, 
	  // fall through.
	}
	insert_pending_op(count,op);
	count++; // Finished with a shard. Increment count for the next one.

	/* throttle...
	 * This block causes the read thread to wait on
	 * the read ops so we don't over demand
	 * the IO system.
	 */
	while (get_pending_ops_size() > concurrentios) {
	  std::this_thread::sleep_for(read_sleep_duration);
	}
      }
    }
    catch (...) 
      { 
	/* anything, mostly if we try to get another item from the queue but
	 * it is already empty because we have multiple threads.
	 */ 
	continue; 
      }

  }  // While loop waiting for reading to be done.
#ifdef TRACE
  output_lock.lock();
  std::cerr THREAD_ID << "Read thread exiting now." << std::endl;
  std::cerr.flush();
  output_lock.unlock();
#endif

  return; // Thread terminates
}

namespace po = boost::program_options;

int setup(int argc, const char** argv) {
  std::cerr << "Entering ErasureCodeBench::setup()" << std::endl;
  std::cerr.flush();

  po::options_description desc("Allowed options");
  desc.add_options()
    ("help,h", "produce help message")
    ("verbose,v", "explain what happens")
    ("name,n", po::value<std::string>()->default_value("test"),
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
    ("queuesize,q", po::value<int>()->default_value(1024),
     "size of the buffer queue")
    ("iterations,i", po::value<int>()->default_value(1),
     "number of encode/decode runs")
    ("pool,y", po::value<std::string>()->default_value("stripe"),
     "pool name")
    ;

  po::variables_map vm;
  po::parsed_options parsed =
    po::command_line_parser(argc, argv).options(desc).allow_unregistered().run();
  po::store(
	    parsed,
	    vm);
  po::notify(vm);
  std::cerr << "Returned from parser" << std::endl;
  std::cerr.flush();

  std::vector<const char *> ceph_options, def_args;
  std::vector<std::string> ceph_option_strings = po::collect_unrecognized(
								parsed.options, po::include_positional);
  std::cerr << "Returned from po::collect_unrecognized. " << ceph_option_strings.size() << std::endl;
  std::cerr.flush();

  ceph_options.reserve(ceph_option_strings.size());
  std::cerr << "Returned from ceph_options.reserve()" << std::endl;
  std::cerr.flush();

  for (std::vector<std::string>::iterator i = ceph_option_strings.begin();
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

  in_size = vm["size"].as<int>();
  rados_mode = vm["rados_mode"].as<int>();
  concurrentios = vm["threads"].as<int>();
  queue_size = vm["queuesize"].as<int>();
  max_iterations = vm["iterations"].as<int>();
  obj_name = vm["name"].as<std::string>();
  pool_name = vm["pool"].as<std::string>();

  return 0;
}

int main(int argc, const char **argv)
{
  int ret = 0;
  utime_t begin_time_final = ceph_clock_now(g_ceph_context);
  utime_t end_time_final = ceph_clock_now(g_ceph_context);
  long long int total_data_processed = 0;

  // Get the arguments from the command line
  setup(argc, argv);

  // we will use all of these below

  // first, we create a Rados object and initialize it
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
      std::cout.flush();
    }
  }

  /*
   * now let's do some IO to the pool! We'll write "hello world!" to a
   * new object.
   */
  if (rados_mode == 0)  {
    // Bootstrap the objects in the object store
    std::thread bsThread = std::thread (bootstrapThread);

    /*
     * Do our writing here.
     */
    for ( int stripe_count = 0;
	  stripe_count<max_iterations;stripe_count++) {
      stripes.push(stripe_count);
    }
    // Start the rados writer thread
    std::thread rados_thread = (std::thread (radosWriteThread));

    while (get_bs_ops_size() > 0)
      std::this_thread::sleep_for(read_sleep_duration);
    bsThread.join();

    while (!is_objs_queue_empty())
      std::this_thread::sleep_for(read_sleep_duration);
    writing_done = true;
    rados_thread.join();

  } else if (rados_mode == 1) {
    /*
     * Read stripes back.
     */
    for ( int stripe_count = 0;
	  stripe_count<max_iterations;stripe_count++) {
      stripes.push(stripe_count);
    }
    // Start the rados writer thread
    std::thread rados_thread = (std::thread (radosReadThread));

    while (!is_stripes_queue_empty())
      std::this_thread::sleep_for(read_sleep_duration);

    reading_done = true;
    rados_thread.join();
  }

  // wait for the pending operations to finish
  while (get_pending_ops_size() > 0)
    std::this_thread::sleep_for(read_sleep_duration);

#ifdef TRACE
  std::cout << "*** Tracing is on, output is to STDERR. ***" << std::endl;
  std::cout << "Factors for computing size: max_iterations: " << max_iterations << std::endl
	    << " object size: " << in_size << std::endl
	    << " total data size: " << max_iterations*in_size
	    << " bandwidth: " << (long long int)max_iterations*in_size/(1024*1024)
	    << std::endl << std::endl;
#endif

  end_time_final = ceph_clock_now(g_ceph_context);
  total_data_processed = (long long int)max_iterations*in_size/(1024*1024);
  std::cout << "Total Time (S)\t" << "Total Data (MB)\t" << "Bandwidth (MiB/S)" << std::endl;
  std::cout << (end_time_final - begin_time_final) << "\t" << total_data_processed 
	    << "\t" << total_data_processed/(double)(end_time_final - begin_time_final) 
	    << std::endl;
  std::cout.flush();

  ret = EXIT_SUCCESS;
  out:
  std::cerr << "Exit routine. Results: " << ret << std::endl;
  std::cerr.flush();

  rados.shutdown();

  return ret;
}

/*********************************************************************
 * Example usage:
 * ./ceph_native_write_benchmark --name test2050a --size 100632960 \
 *  --rados 0   --threads 45 --ecthreads 12  --queuesize 288   \
 *  --object_size 8388608   --iterations 2   --pool stripe   \
 *  -c /etc/ceph/ceph.conf 
 */
