#ifndef RADOS_SHARD_H
#define RADOS_SHARD_H

#include <include/rados/librados.hpp>
#include <string>

// Class to hold shard record
class Shard {
 private:
  librados::bufferlist bl;

 public:
  bool read;
  int stripe;
  int shard;
  int stripe_size;
  std::string object_name;

  Shard(int _stripe, int _shard, int _stripe_size,
	 std::string _object_name) 
    : stripe (_stripe), shard (_shard), stripe_size (_stripe_size),
    object_name (_object_name) {
      clear_read();
    };

  Shard() {};
  ~Shard() {};

  void set_read() {
    read = true;
  }

  librados::bufferlist get_bufferlist() {
    return bl;
  }

  librados::bufferlist* get_bufferlist_ptr() {
    return &bl;
  }

  void set_bufferlist(librados::bufferlist _bl) {
    bl = _bl;
  }

  void dereference_bufferlist() {
    bl.zero();
  } // drop the reference to the bufferlist.

  void clear_read() {
    read = true;
  }

  bool is_read() {
    return read;
  }

  int get_stripe() {
    return this->stripe;
  }
  int get_shard() {
    return this->shard;
  }
  int get_hash() {
    return Shard::compute_hash(this->shard,this->stripe,this->stripe_size);
  }
  std::string get_object_name() {
    return object_name;
  }
  // This static class member returns a large integer when called
  // with a bogus reference. How do we fix this?
  static int compute_hash(int _i,int _j, int _stripe_size) {
    return _j*_stripe_size+_i;
  }
};

#endif
