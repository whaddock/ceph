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

#include <rados/librados.hpp>
#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <mutex>
#include <string>
#include <map>
#include <queue>
#include <climits>

#define RADOS_THREADS 1

// Globals
int in_size, rados_mode, concurrentios;
int queue_size, max_iterations;
std::string obj_name, pool_name;
boost::intrusive_ptr<CephContext> cct;

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

// Object information. We stripe over these objects.
struct obj_info {
  std::string name;
  size_t len;
};
std::map<int, obj_info> objs;

struct Shard {
  std::string name;
};

std::queue<int> stripes;
std::queue<std::map<int,Shard>> stripes_decode_queue;
std::queue<Shard> pending_buffers_queue;

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
/*
int get_shards_map_size() {
  std::lock_guard<std::mutex> guard(shards_lock);
  return shards.size();
}

void insert_shard(int index, Shard shard) {
  std::lock_guard<std::mutex> guard(shards_lock);
  shards.insert(std::pair<int,Shard>(index,shard));
  return;
}
*/
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
/*
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

bool is_shards_map_empty() {
  std::lock_guard<std::mutex> guard(shards_lock);
  return shards.empty();
}
*/

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

void insert_stripe_decode(std::map<int,Shard> stripe) {
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
  //  insert_shard(op->id,op->shard);
  delete op;
  //  std::cout << "-";
}

static void _read_completion_cb(librados::completion_t c, void *param)
{
  CompletionOp *op = (CompletionOp *)param;
  read_cb(c, op);
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

  // Get the arguments from the command line
  setup(argc, argv);

  // we will use all of these below
  librados::IoCtx io_ctx;
  const std::chrono::milliseconds read_sleep_duration(100);

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
    ret = rados.ioctx_create(pool_name.str(), io_ctx);
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
  {
    /*
     * "bufferlist"s are Ceph's native transfer type, and are carefully
     * designed to be efficient about copying. You can fill them
     * up from a lot of different data types, but strings or c strings
     * are often convenient. Just make sure not to deallocate the memory
     * until the bufferlist goes out of scope and any requests using it
     * have been finished!
     */
    for ( int buffers_created_count = 0;
	  buffers_created_count<max_iterations;buffers_created_count++) {
      librados::bufferlist bl;
      bl.append(std::string(in_size,(char)buffers_created_count%26+97)); // start with 'a'

      /*
       * now that we have the data to write, let's send it to an object.
       * We'll use the synchronous interface for simplicity.
       */
      ret = io_ctx.write_full(obj_name + "." 
			      + std::to_string(buffers_created_count), bl);
      if (ret < 0) {
	std::cerr << "couldn't write object! error " << ret << std::endl;
	std::cerr.flush();
	ret = EXIT_FAILURE;
	goto out;
      } else {
	std::cout << "we just wrote new object " 
		  << obj_name + "." + std::to_string(buffers_created_count)
		  << ", with contents\n" << obj_name << std::endl;
	std::cout.flush();
      }
    }
  }
  /*
   * now let's read that object back! Just for fun, we'll do it using
   * async IO instead of synchronous. (This would be more useful if we
   * wanted to send off multiple reads at once; see
   * http://ceph.com/docs/master/rados/api/librados/#asychronous-io )
   */
  {
    /*
     * Prompt user so we wait to do the read.
     */

    std::string name;
    std::cout << "Press enter when ready to proceed with read..." << std::endl;
    std::getline(std::cin, name);
    std::cout << "Proceeding to read object back..." << std::endl;

    std::map<int,librados::bufferlist> read_buffers;
    std::map<int,librados::AioCompletion *> completions;
    for (int i = 0;i<max_iterations;i++) {
      librados::bufferlist read_buf;
      int read_len = in_size;
      // allocate the completion from librados
      librados::AioCompletion *read_completion = librados::Rados::aio_create_completion();
      completions.insert(std::pair<int,librados::AioCompletion *>(i,read_completion));
      // send off the request.
      std::string _name = obj_name + "." + std::to_string(i);
      ret = io_ctx.aio_read(_name.str(), read_completion, &read_buf, read_len, 0);
      if (ret < 0) {
	std::cerr << "couldn't start read object! error " << ret << std::endl;
	ret = EXIT_FAILURE;
	goto out;
      } else {
	std::cout << "we did aio read on object " << obj_name + "." + std::to_string(i)
		  << std::endl;
	std::cout.flush();
      }
      read_buffers.insert(std::pair<int,librados::bufferlist>(i,read_buf));
      std::this_thread::sleep_for(read_sleep_duration);
    }

    // wait for the request to complete, and check that it succeeded.
    librados::AioCompletion * read_completion;
    //    librados::bufferlist read_buf;
    for (int i = 0;i<max_iterations;i++) {
      auto it = completions.find(i);
      if (it != completions.end())
	read_completion = it->second;
      else
	break;
      read_completion->wait_for_complete();
      ret = read_completion->get_return_value();
      if (ret < 0) {
	std::cerr << "couldn't read object! error " << ret << std::endl;
	std::cerr.flush();
	ret = EXIT_FAILURE;
	goto out;
      } else {
	std::cout << "we read our object " << obj_name + "." + std::to_string(i)
		  << ", and got back " << ret << " bytes with contents"  << std::endl;
	std::cout.flush();
	/*
	std::string read_string;
	auto it_buf = read_buffers.find(i);
	if (it_buf != read_buffers.end()) {
	  read_buf = it_buf->second;
	  read_buf.copy(0, 20, read_string);
	  std::cout << read_string << std::endl;
	std::cout.flush();
	}
	*/
      }
      std::this_thread::sleep_for(read_sleep_duration);
    }
  }


  ret = EXIT_SUCCESS;
  out:
  std::cerr << "Exit routine. Results: " << ret << std::endl;
  std::cerr.flush();
  /*
   * And now we're done, so let's remove our pool and then
   * shut down the connection gracefully.
   */
  int delete_ret = rados.pool_delete(pool_name.str());
  if (delete_ret < 0) {
    // be careful not to
    std::cerr << "We failed to delete our test pool!" << std::endl;
    ret = EXIT_FAILURE;
  }

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
