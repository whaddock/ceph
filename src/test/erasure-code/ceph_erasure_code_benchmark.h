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

#ifndef CEPH_ERASURE_CODE_BENCHMARK_H
#define CEPH_ERASURE_CODE_BENCHMARK_H

#include <string>

using namespace std;

class ErasureCodeBench {
public:
  int in_size;
  int max_iterations;
  int erasures;
  int k;
  int m;
  int stripe_size;
  int queue_size;
  int object_size;
  string obj_name;
  string pool_name;
  string plugin;

  bool exhaustive_erasures;
  vector<int> erased;
  string workload;

  ErasureCodeProfile profile;
  ErasureCodeInterfaceRef erasure_code;

  bool verbose;
  boost::intrusive_ptr<CephContext> cct;
  int setup(int argc,const char** argv);
  int run();
  int decode_erasures(const map<int,bufferlist> &all_chunks,
		      const map<int,bufferlist> &chunks,
		      unsigned i,
		      unsigned want_erasures,
		      ErasureCodeInterfaceRef erasure_code);
  int decode();
  int encode();
  int encode(map<int, bufferlist> *encoded);
};

#endif
