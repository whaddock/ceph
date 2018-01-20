#ifndef RADOS_STRIPE_H
#define RADOS_STRIPE_H

// Class to hold stripe record
class Stripe {
 public:
  uint32_t stripe;
  uint32_t shard;
  uint32_t stripe_size;
  std::string object_name;

  Stripe(uint32_t _stripe, uint32_t _shard, uint32_t _stripe_size,
	std::string _object_name) 
    : stripe (_stripe), shard (_shard), stripe_size (_stripe_size),
      object_name (_object_name) {};

  Stripe() {};
  ~Stripe() {};

  uint32_t get_stripe() {
    return this->stripe;
  }
  uint32_t get_shard() {
    return this->shard;
  }
  uint32_t get_hash() {
    return Stripe::compute_hash(this->shard,this->stripe,this->stripe_size);
  }
  std::string get_object_name() {
    return object_name;
  }
  static uint32_t compute_hash(uint32_t _i,uint32_t _j, uint32_t _stripe_size) {
    return _j*_stripe_size+_i;
  }
};

#endif
