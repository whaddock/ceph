// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/scoped_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/program_options/option.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/algorithm/string.hpp>

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
int in_size;
int rados_mode;
int concurrentios;
int queue_size;
int max_iterations;
std::string obj_name;
std::string pool_name;

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

  if (vm.count("help")) {
    std::cout << desc << std::endl;
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
  // Get the arguments from the command line
  setup(argc, argv);
  std::cout << "in_size: " << in_size << std::endl;
  std::cout << "rados_mode: " << rados_mode << std::endl;
  std::cout << "concurrentios: " << concurrentios << std::endl;
  std::cout << "queue_size: " << queue_size << std::endl;
  std::cout << "max_iterations: " << max_iterations << std::endl;
  std::cout << "obj_name: " << obj_name << std::endl;
  std::cout << "pool_name: " << pool_name << std::endl;

  return 0;
}
