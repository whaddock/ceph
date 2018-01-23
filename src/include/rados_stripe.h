#ifndef RADOS_STRIPE_H
#define RADOS_STRIPE_H

// Class to hold stripe record
class Stripe {
 public:
  bool read;
  int stripe;
  int shard;
  int stripe_size;
  std::string object_name;

  Stripe(int _stripe, int _shard, int _stripe_size,
	 std::string _object_name) 
    : stripe (_stripe), shard (_shard), stripe_size (_stripe_size),
    object_name (_object_name) {
      clear_read();
    };

  Stripe() {};
  ~Stripe() {};

  void set_read() {
    read = true;
  }

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
    return Stripe::compute_hash(this->shard,this->stripe,this->stripe_size);
  }
  std::string get_object_name() {
    return object_name;
  }
  static int compute_hash(int _i,int _j, int _stripe_size) {
    return _j*_stripe_size+_i;
  }
};

#endif
