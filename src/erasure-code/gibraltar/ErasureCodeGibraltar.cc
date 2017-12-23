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
#include <dlfcn.h>
#include <vector>
#include <iomanip>
#include "common/debug.h"
#include "ErasureCodeGibraltar.h"
#include "crush/CrushWrapper.h"
#include "osd/osd_types.h"
extern "C" {
#include "libgibraltar/include/gibraltar.h"
}

#define LARGEST_VECTOR_WORDSIZE 64

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodeGibraltar: ";
}

int ErasureCodeGibraltar::create_ruleset(const string &name,
					CrushWrapper &crush,
					ostream *ss) const
{
  int ruleid = crush.add_simple_ruleset(name, 
					ruleset_root, 
					ruleset_failure_domain,
					"indep", 
					pg_pool_t::TYPE_ERASURE, 
					ss);
  if (ruleid < 0)
    return ruleid;
  else {
    crush.set_rule_mask_max_size(ruleid, get_chunk_count());
    return crush.get_rule_mask_ruleset(ruleid);
  }
}

int ErasureCodeGibraltar::init(ErasureCodeProfile& profile, ostream *ss)
{
  int err = 0;
  dout(10) << "technique=" << technique << dendl;
  profile["technique"] = technique;
  err |= to_string("ruleset-root", profile,
		   &ruleset_root,
		   DEFAULT_RULESET_ROOT, ss);
  err |= to_string("ruleset-failure-domain", profile,
		   &ruleset_failure_domain,
		   DEFAULT_RULESET_FAILURE_DOMAIN, ss);
  err |= parse(profile, ss);
  if (err)
    return err;
  prepare();
  ErasureCode::init(profile, ss);
  return err;
}

int ErasureCodeGibraltar::parse(ErasureCodeProfile &profile,
 			       ostream *ss)
{
  int err = ErasureCode::parse(profile, ss);
  err |= to_int("k", profile, &k, DEFAULT_K, ss);
  err |= to_int("m", profile, &m, DEFAULT_M, ss);
  err |= to_int("gib_chunk_size", profile, &gib_chunk_size, DEFAULT_GIB_CHUNK_SIZE, ss);
  if (m == 1) {
    // single parity stripe
    *ss << "m = 1 is not supported" << std::endl;
    err = -EINVAL;
  } else if (chunk_mapping.size() > 0 && (int)chunk_mapping.size() != k + m) {
    *ss << "mapping " << profile.find("mapping")->second
	<< " maps " << chunk_mapping.size() << " chunks instead of"
	<< " the expected " << k + m << " and will be ignored" << std::endl;
    chunk_mapping.clear();
    err = -EINVAL;
  }
  err |= sanity_check_k(k, ss);
  return err;
}

unsigned int ErasureCodeGibraltar::get_chunk_size(unsigned int object_size) const
{
  unsigned alignment = get_alignment();
  unsigned int my_chunk_size = ( object_size + k - 1 ) / k; 
#ifdef DEBUG
  dout(20) << "get_chunk_size: my_chunk_size " << my_chunk_size
           << " must be modulo " << alignment << dendl;
#endif
  unsigned modulo = my_chunk_size % alignment;
  if (modulo) {
#ifdef DEBUG
    dout(10) << "get_chunk_size: " << my_chunk_size
             << " padded to " << my_chunk_size + alignment - modulo << dendl;
#endif
    my_chunk_size += alignment - modulo;
  }
  return my_chunk_size;
}

int ErasureCodeGibraltar::encode_chunks(const set<int> &want_to_encode,
				       map<int, bufferlist> *encoded)
{
  unsigned int k = get_data_chunk_count();
  unsigned int m = get_chunk_count() - k;
  char *chunks[k + m];
  unsigned int blocksize = (*encoded)[0].length();
  for (unsigned int i = 0; i < k + m; i++)
    chunks[i] = (*encoded)[i].c_str();

#ifdef DEBUG
  {
    ostringstream ss;
    ss << "k = " << k << " m = " << m << std::endl;
    ss << "k = " << k << " m = " << m;
    ss << " blocksize = " << blocksize << std::endl;
    dout(20) << ss.str() << dendl;
  }
#endif

#ifdef DEBUG2
  {
    ostringstream ss;
    for (unsigned int i = 0; i < get_chunk_count(); i++) {
      ss << "Encoded chunks " << i << std::endl;
      ss << "Length (*encoded)[" << i << "]: " << (*encoded)[i].length() << std::endl;
      //      (*encoded)[i].hexdump(ss);
      ss << std::endl << flush;
    }
  }
#endif
  int re = gibraltar_encode(chunks, blocksize);

#ifdef DEBUG
  {
    ostringstream ss;
    ss << "k = " << k << " m = " << m << std::endl;
    dout(20) << ss.str() << dendl;
  }
#endif

#ifdef DEBUG2
  {
    ostringstream ss;
    for (unsigned int i = 0; i < get_chunk_count(); i++) {
      ss << "Encoded chunks " << i << std::endl;
      ss << "Length (*encoded)[" << i << "]: " << (*encoded)[i].length() << std::endl;
      //      (*encoded)[i].hexdump(ss);
      ss << std::endl << flush;
    } 
    dout(20) << ss.str() << dendl;
  }
#endif

  return re;
}

int ErasureCodeGibraltar::decode_chunks(const set<int> &want_to_read,
				       const map<int, bufferlist> &chunks,
				       map<int, bufferlist> *decoded)
{
  /* Precondition: if decode_chunks is called then we are
     guaranteed to have min(k,m) chunks. So, f_index will
     never be > k + m.
  */
  unsigned int k = get_data_chunk_count();
  int erasure_data_count = 0;
  int erasure_code_count = 0;
  char *dense_chunks[k + m];
  unsigned int code_ids[MAX_CHUNKS];
  unsigned int buf_ids[MAX_CHUNKS];
  unsigned int f_index = k;
  // Map coded chunks that are available
  for (unsigned int i = k; i < get_chunk_count(); i++) {
    while (f_index < get_chunk_count() && chunks.find(f_index) == chunks.end()) {
      f_index++;
      erasure_code_count++;
    }
    // When f_index > get_chunk_count() some coded chunks are missing.
    if (f_index < get_chunk_count()) {
      code_ids[i] = f_index;
    } else {
      code_ids[i] = buf_ids[0]; // A safe value but should never be used
    }
    f_index++;
  }

  f_index = k;
  for (unsigned int i = 0; i < k; i++) {
    if (chunks.find(i) == chunks.end()) {
      buf_ids[f_index] = i;
      buf_ids[i] = code_ids[f_index];
      f_index++;
      erasure_data_count++;
    } else {
      buf_ids[i] = i;
    }
  }
  while (f_index < get_chunk_count()) {
    buf_ids[f_index] = code_ids[f_index];
    f_index++;
  }

#ifdef DEBUG
  {
    ostringstream ss;
    ss << "decoding: ";
    ss << "erasure_data_count = " << erasure_data_count  << std::endl;
    ss << "erasrue_code_count = " << erasure_code_count  << std::endl;
    ss << std::endl;
    dout(20) << ss.str() << dendl;
  }
#endif

#ifdef DEBUG2
  {
    ostringstream ss;
    for (set<int>::iterator i = want_to_read.begin();
	 i != want_to_read.end();
	 ++i) {
      ss << *i << " ";
    } 
    ss << std::endl;

    for (unsigned int i = 0; i < get_chunk_count(); i++) {
      ss << i << "   ";
    } 
    ss << std::endl;

    for (unsigned int i = 0; i < get_chunk_count(); i++) {
      ss << buf_ids[i] << "   ";
    } 
    ss << std::endl;
    dout(20) << ss.str() << dendl;
  }
#endif

  // Move the coded chunks to the missing data chunks
  for (unsigned int i = 0; i < get_chunk_count(); i++)
    dense_chunks[i] = (*decoded)[buf_ids[i]].c_str();

#ifdef DEBUG
  {
    ostringstream ss;
    ss << "k = " << k << " m = " << get_chunk_count() - k << std::endl;
    ss << "k = " << k << " m = " << m;
    ss << " blocksize = " << (*decoded)[0].length() << std::endl;
    ss << std::endl << flush;
    dout(20) << ss.str() << dendl;
  }
#endif

#ifdef DEBUG2
  {
    ostringstream ss;
    ss << "k = " << k << " m = " << get_chunk_count() - k << std::endl;
    for (unsigned int i = 0; i < get_chunk_count(); i++) {
      ss << "Dense test before recovery " << i << " / " <<  buf_ids[i] << std::endl;
      ss << "Length (*decoded)[" << i << "]: " << (*decoded)[i].length() << std::endl;
      //      (*decoded)[i].hexdump(ss);
    } 
    ss << std::endl << flush;
    dout(20) << ss.str() << dendl;
  }
#endif

  assert(erasure_data_count + erasure_code_count > 0);

  int re = 0;
  if (erasure_data_count > 0) {
    re =  gibraltar_decode(buf_ids, erasure_data_count, dense_chunks, (*decoded)[0].length());
  }

  // Use gibraltar_encode to recover coded chunks
  if (erasure_code_count > 0) {
#ifdef DEBUG
    {
      ostringstream ss;
      ss << "encoding after decode: ";
      ss << "k = " << k << " m = " << m;
      ss << " blocksize = " << (*decoded)[0].length();
      dout(20) << ss.str() << dendl;
    }
#endif
    char *encode_chunks[k + m];
    for (unsigned int i = 0; i < get_chunk_count(); i++) {
      encode_chunks[i] = (*decoded)[i].c_str();
    }
    re = gibraltar_encode(encode_chunks, (*decoded)[0].length());
  }

#ifdef DEBUG2
  {
    ostringstream ss;
    for (unsigned int i = 0; i < get_chunk_count(); i++) {
      ss << "Dense test after recovery " << i << " / " <<  buf_ids[i] << std::endl;
      ss << "Length (*decoded)[" << i << "]: " << (*decoded)[i].length() << std::endl;
      //      (*decoded)[i].hexdump(ss);
    } 
    ss << std::endl << flush;
    dout(20) << ss.str() << dendl;
  }
#endif

  return re;
}

// -----------------------------------------------------------------------------
// ErasureCodeGibraltarReedSolomonVandermonde
// -----------------------------------------------------------------------------

unsigned ErasureCodeGibraltarReedSolomonVandermonde::get_alignment() const
{
  if (per_chunk_alignment) {
    return LARGEST_VECTOR_WORDSIZE;
  } else {
    unsigned alignment = k*sizeof(int);
    if ( (alignment%LARGEST_VECTOR_WORDSIZE) )
      alignment = k*LARGEST_VECTOR_WORDSIZE;
    return alignment;
  }
}

int ErasureCodeGibraltarReedSolomonVandermonde::gibraltar_encode(char **chunks,
                                                                unsigned int blocksize)
{
  int re = gib_generate2(chunks, blocksize, get_gpu_context());
#ifdef DEBUG
  dout(10) << "Call to gib_generate2: " << re << dendl;
#endif
  return re;
} // We're done with the encoding

int ErasureCodeGibraltarReedSolomonVandermonde::gibraltar_decode(unsigned int *buf_ids,
								 int erasure_count,
                                                                char **chunks,
                                                                unsigned int blocksize)
{
#ifdef DEBUG
  dout(10) << "Call to gib_recover2: blocksize=" << blocksize
	   << "erasure_count=" << erasure_count << dendl;
#endif
  int re = gib_recover2(chunks, blocksize, buf_ids, erasure_count,
			get_gpu_context());
  return re;
} // We're done with decoding

int ErasureCodeGibraltarReedSolomonVandermonde::parse(ErasureCodeProfile &profile,
						      ostream *ss)
{
  int err = 0;
  err = ErasureCodeGibraltar::parse(profile, ss);
  return err;
}

void ErasureCodeGibraltarReedSolomonVandermonde::prepare()
{
#ifdef DEBUG
  dout(20) << __func__ << ": Calling gib_init_cuda2(" << k << "," << m << ",&gpu_context)" << dendl;
#endif
  std::string cuda = "/lib64/libcuda.so.1";
  void *library = dlopen(cuda.c_str(), RTLD_NOW);
  if (!library){
    dout(20) << "load dlopen(" << cuda << "): " << dlerror() << dendl;
  }
  int re = 0;
  re = gib_init_cuda2(k,m,(unsigned int)ErasureCodeGibraltar::gib_chunk_size,&gpu_context);
#ifdef DEBUG
  dout(20) << "Call to gib_init_cuda2: " << re << dendl;
#endif
  // If re is not equal to 0 then there was a problem initializing the
  // CUDA instance.
  assert(re == 0);
}
