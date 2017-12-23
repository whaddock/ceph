// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2013, 2014 Cloudwatt <libre.licensing@cloudwatt.com>
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

#ifndef CEPH_ERASURE_CODE_GIBRALTAR_H
#define CEPH_ERASURE_CODE_GIBRALTAR_H

#define MAX_CHUNKS 256

#include "erasure-code/ErasureCode.h"
#include "libgibraltar/include/gib_context.h"
#include "libgibraltar/include/gibraltar.h"

#define DEFAULT_RULESET_ROOT "default"
#define DEFAULT_RULESET_FAILURE_DOMAIN "host"

class ErasureCodeGibraltar : public ErasureCode {
public:
  int k;
  std::string DEFAULT_K;
  int m;
  std::string DEFAULT_M;
  int w;
  std::string DEFAULT_W;
  const char *technique;
  string ruleset_root;
  string ruleset_failure_domain;
  gib_context_t * gpu_context;
  bool per_chunk_alignment;
  int gib_chunk_size;
  std::string DEFAULT_GIB_CHUNK_SIZE;

  explicit ErasureCodeGibraltar(const char *_technique) :
    k(0),
    DEFAULT_K("3"),
    m(0),
    DEFAULT_M("2"),
    w(8),
    DEFAULT_W("8"),
    technique(_technique),
    ruleset_root(DEFAULT_RULESET_ROOT),
    ruleset_failure_domain(DEFAULT_RULESET_FAILURE_DOMAIN),
    gpu_context(0),
    per_chunk_alignment(false),
    gib_chunk_size(0),
    DEFAULT_GIB_CHUNK_SIZE("1048576")
  {}

  gib_context_t * get_gpu_context() const {
    return gpu_context;
  }

  virtual ~ErasureCodeGibraltar() {}
  
  virtual int create_ruleset(const string &name,
			     CrushWrapper &crush,
			     ostream *ss) const;

  virtual unsigned int get_chunk_count() const {
    return k + m;
  }

  virtual unsigned int get_data_chunk_count() const {
    return k;
  }

  virtual unsigned int get_chunk_size(unsigned int object_size) const;

  virtual int encode_chunks(const set<int> &want_to_encode,
			    map<int, bufferlist> *encoded);

  virtual int decode_chunks(const set<int> &want_to_read,
			    const map<int, bufferlist> &chunks,
			    map<int, bufferlist> *decoded);

  virtual int init(ErasureCodeProfile &profile, ostream *ss);


  virtual int gibraltar_encode(char **chunks,
                               unsigned int blocksize) = 0;
  virtual int gibraltar_decode(unsigned int *buf_ids,
			       int erasure_count,
                               char **chunks,
                               unsigned int blocksize) = 0;
  virtual unsigned get_alignment() const = 0;
  virtual void prepare() = 0;
  static bool is_prime(int value);
protected:
  virtual int parse(ErasureCodeProfile &profile, ostream *ss);
};

class ErasureCodeGibraltarReedSolomonVandermonde : public ErasureCodeGibraltar {
public:
  ErasureCodeGibraltarReedSolomonVandermonde() :
    ErasureCodeGibraltar("reed_sol_van")
  {
    DEFAULT_K = "7";
    DEFAULT_M = "3";
    DEFAULT_W = "8";
  }
  virtual ~ErasureCodeGibraltarReedSolomonVandermonde() {
    //    if (ErasureCodeGibraltar::gpu_context)
    //      gib_destroy2(ErasureCodeGibraltar::gpu_context);
  }

  virtual int gibraltar_encode(char **chunks,
                               unsigned int blocksize);

  virtual int gibraltar_decode(unsigned int *buf_ids,
                               int erasure_count,
			       char **chunks,
                               unsigned int blocksize);

  virtual unsigned get_alignment() const;

  virtual void prepare();
private:
  virtual int parse(ErasureCodeProfile &profile, ostream *ss);
};


#endif
