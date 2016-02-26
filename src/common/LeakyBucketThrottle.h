// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_LEAKYBUCKETTHROTTLE_H
#define CEPH_LEAKYBUCKETTHROTTLE_H

#include "include/utime.h"
#include "common/Timer.h"
#include "common/Mutex.h"

#define NANOSECONDS_PER_SECOND  1000000000.0

enum BucketType {
  THROTTLE_BPS_TOTAL,
  THROTTLE_OPS_TOTAL,
  BUCKET_TYPE_COUNT,
};

enum ThrottleMode {
  THROTTLE_MODE_NONE,
  THROTTLE_MODE_STATIC,
  THROTTLE_MODE_DYNAMIC,
  THROTTLE_MODE_MAX,
};

// For MODE_STATIC, avg is the average goal in units per second
// For MODE_DYNAMIC, min is the lower limit, avg is the current limit, max is
// the upper limit. avg falls in [min, max]. threshold is the value triggering
// the change of the average goal.
struct LeakyBucket {
  double  min;              /* min goal in units per second */
  double  avg;              /* average goal in units per second */
  double  max;              /* leaky bucket max burst in units */
  double  level;            /* bucket level in units */
  int64_t threshold;        /* threshold to adjust the average goal */

  LeakyBucket(): avg(0), max(0), level(0), threshold(0) {}
};

class LeakyBucketThrottle {
  CephContext *cct;
  map<BucketType, LeakyBucket> buckets; /* leaky buckets */
  uint64_t op_size;         /* size of an operation in bytes */
  utime_t previous_leak;    /* timestamp of the last leak done */
  Mutex lock;
  bool enable;
  ThrottleMode mode;
  bool avg_is_max;
  bool avg_reset;

  static const int THROTTLE_BPS_INCREASE_UNIT = 1048576; // 1M
  static const int THROTTLE_OPS_INCREASE_UNIT = 1;

  void throttle_do_leak();
  void throttle_leak_bucket(LeakyBucket *bkt, uint64_t delta_ns);
  double throttle_compute_wait_for();
  double throttle_compute_wait(LeakyBucket *bkt);
  bool throttle_enabling();

 public:
  LeakyBucketThrottle(CephContext *c, uint64_t op_size);
  ~LeakyBucketThrottle();

  /* configuration */
  void set_op_size(uint64_t s) { op_size = s; }
  bool enabled() const { return enable; }
  bool config_static(BucketType type, double avg);
  bool config_dynamic(BucketType type, double min, double max, int64_t threshold);
  bool config_mode(int mode);
  void get_config(map<BucketType, LeakyBucket> &_buckets) { _buckets = buckets; }
  ThrottleMode get_mode() { return mode; }

  /* usage */
  bool can_schedule();
  void account(uint64_t size);
  void adjust(BucketType type, uint64_t add_size, uint64_t substract_size);
  void increase_bucket_average();
  void reset_bucket_average();
};

#endif
