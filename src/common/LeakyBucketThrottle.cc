// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomai@xsky.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/Clock.h"
#include "LeakyBucketThrottle.h"

#define dout_subsys ceph_subsys_throttle
#undef dout_prefix
#define dout_prefix *_dout << "LeakyBucketThrottle::"

LeakyBucketThrottle::LeakyBucketThrottle(CephContext *c, uint64_t op_size)
  : cct(c), op_size(op_size), lock("LeakyBucketThrottle::lock"),
    enable(false), mode(THROTTLE_MODE_NONE),
    avg_is_max(false), avg_reset(false)
{}

LeakyBucketThrottle::~LeakyBucketThrottle() {}

/* Does any throttling must be done
 *
 * @ret: true if throttling must be done else false
 */
bool LeakyBucketThrottle::throttle_enabling()
{
  if (mode == THROTTLE_MODE_NONE) {
    return false;
  } else if (mode == THROTTLE_MODE_STATIC) {
    for (map<BucketType, LeakyBucket>::const_iterator it = buckets.begin();
         it != buckets.end(); ++it) {
      if (it->second.avg > 0) {
        return true;
      }
    }
  } else if (mode == THROTTLE_MODE_DYNAMIC) {
    // enable the throttle only when client threshold, lower limit and upper
    // limit are all set.
    if (buckets.count(THROTTLE_BPS_TOTAL) &&
        buckets[THROTTLE_BPS_TOTAL].min > 0 &&
        buckets[THROTTLE_BPS_TOTAL].max > 0 &&
        buckets[THROTTLE_BPS_TOTAL].threshold > 0)
      return true;
    if (buckets.count(THROTTLE_OPS_TOTAL) &&
        buckets[THROTTLE_OPS_TOTAL].min > 0 &&
        buckets[THROTTLE_OPS_TOTAL].max > 0 &&
	buckets[THROTTLE_OPS_TOTAL].threshold > 0)
      return true;
  }

  return false;
}

/* Configure the throttle in static mode
 *
 * @type: the throttle type we are working on
 * @avg: config the average
 * @ret: true if config success else false
 */
bool LeakyBucketThrottle::config_static(BucketType type, double avg)
{
  if (avg < 0) {
    ldout(cct, 0) << __func__ << " parameters not correct, aborting" << dendl;
    return false;
  }
  if (type >= BUCKET_TYPE_COUNT) {
    ldout(cct, 0) << __func__ << " invalid bucket type, aborting" << dendl;
    return false;
  }

  Mutex::Locker l(lock);
  if (mode != THROTTLE_MODE_STATIC) {
    ldout(cct, 0) << __func__ << " config mode first, aborting" << dendl;
    return false;
  }

  buckets[type].min = 0;
  buckets[type].avg = avg;
  buckets[type].max = 0;
  buckets[type].threshold = 0;
  enable = throttle_enabling();
  ldout(cct, 20) << __func__ << " leaky bucket throttle is "
                 << (enable ? "enabled" : "NOT enabled") << dendl;

  avg_is_max = false;
  avg_reset = false;
  return true;
}

/* Configure the throttle in dynamic mode
 *
 * @type: the throttle type we are working on
 * @min: config the min
 * @max: config the max
 * @threshold: the threshold to adjust the average goal
 * @ret: true if config success else false
 */
bool LeakyBucketThrottle::config_dynamic(BucketType type, double min,
                                         double max, int64_t threshold)
{
  if (min < 0 || max < 0 || min > max || threshold < 0) {
    ldout(cct, 0) << __func__ << " parameters not correct, aborting" << dendl;
    return false;
  }
  if (type >= BUCKET_TYPE_COUNT) {
    ldout(cct, 0) << __func__ << " invalid bucket type, aborting" << dendl;
    return false;
  }

  Mutex::Locker l(lock);
  if (mode != THROTTLE_MODE_DYNAMIC) {
    ldout(cct, 0) << __func__ << " config mode first, aborting" << dendl;
    return false;
  }

  buckets[type].min = min;
  buckets[type].avg = min; // avg starting from min
  buckets[type].max = max;
  buckets[type].threshold = threshold;
  enable = throttle_enabling();
  ldout(cct, 20) << __func__ << " leaky bucket throttle is "
                 << (enable ? "enabled" : "NOT enabled") << dendl;

  avg_is_max = false;
  avg_reset = false;
  return true;
}

/* Set the throttling mode
 *
 * @_mode: the throttle mode to set. 0 - none, 1 - static, 2 - dynamic
 */
bool LeakyBucketThrottle::config_mode(int _mode)
{
  Mutex::Locker l(lock);

  if (_mode == 0) {
    mode = THROTTLE_MODE_NONE;
  } else if (_mode == 1) {
    mode = THROTTLE_MODE_STATIC;
  } else if (_mode == 2) {
    mode = THROTTLE_MODE_DYNAMIC;
  } else {
    ldout(cct, 0) << __func__ << " invalid mode " << _mode << ", aborting" << dendl;
    return false;
  }
  enable = throttle_enabling();
  ldout(cct, 20) << __func__ << "Leaky bucket throttle is "
                 << (enable ? "enabled" : "NOT enabled") << dendl;
  return true;
}

/*
 * NOTE: Check if we are able to schedule an operation
 */
bool LeakyBucketThrottle::can_schedule()
{
  Mutex::Locker l(lock);

  /* leak proportionally to the time elapsed */
  throttle_do_leak();

  /* compute the wait time if any */
  double wait = throttle_compute_wait_for();
  if (wait > 0) {
    return false;
  }

  return true;
}

/*
 * NOTE: do the accounting for this operation
 *
 * @size:     the size of the operation
 */
void LeakyBucketThrottle::account(uint64_t size)
{
  Mutex::Locker l(lock);
  double units = 1.0;

  /* if op_size is defined and smaller than size we compute unit count */
  if (op_size && size > op_size)
    units = (double) size / op_size;

  if (buckets.count(THROTTLE_BPS_TOTAL))
    buckets[THROTTLE_BPS_TOTAL].level += size;
  if (buckets.count(THROTTLE_OPS_TOTAL))
    buckets[THROTTLE_OPS_TOTAL].level += units;
}

/* This function make a bucket leak
 *
 * @bkt:   the bucket to make leak
 * @delta_ns: the time delta
 */
void LeakyBucketThrottle::throttle_leak_bucket(LeakyBucket *bkt, uint64_t delta_ns)
{
  /* compute how much to leak */
  double leak = (bkt->avg * (double) delta_ns) / NANOSECONDS_PER_SECOND;
  /* make the bucket leak */
  bkt->level = MAX(bkt->level - leak, 0);
}

/* Calculate the time delta since last leak and make proportionals leaks
 *
 * @now:      the current timestamp in ns
 */
void LeakyBucketThrottle::throttle_do_leak()
{
  utime_t delta, now = ceph_clock_now(cct);
  /* compute the time elapsed since the last leak */
  if (now > previous_leak)
    delta = now - previous_leak;

  previous_leak = now;

  if (delta.is_zero()) {
    return;
  }

  /* make each bucket leak */
  for (map<BucketType, LeakyBucket>::iterator it = buckets.begin();
       it != buckets.end(); ++it)
    throttle_leak_bucket(&it->second, delta.to_nsec());
}

/* This function compute the wait time in ns that a leaky bucket should trigger
 *
 * @bkt: the leaky bucket we operate on
 * @ret: the resulting wait time in seconds or 0 if the operation can go through
 */
double LeakyBucketThrottle::throttle_compute_wait(LeakyBucket *bkt)
{
  if (!bkt->avg)
    return 0;

  /* the number of extra units blocking the io */
  double extra = 0;
  if (mode == THROTTLE_MODE_STATIC && allow_burst)
    extra = bkt->level - bkt->max;
  else
    extra = bkt->level - bkt->avg;

  if (extra <= 0)
    return 0;

  return extra / bkt->avg;
}

/* This function compute the time that must be waited
 *
 * @ret: time to wait(seconds)
 */
double LeakyBucketThrottle::throttle_compute_wait_for()
{
  BucketType to_check[2] = {THROTTLE_BPS_TOTAL, THROTTLE_OPS_TOTAL};
  double wait = 0, max_wait = 0;

  for (int i = 0; i < 2; i++) {
    BucketType index = to_check[i];
    if (buckets.count(index))
      wait = throttle_compute_wait(&buckets[index]);

    if (wait > max_wait)
      max_wait = wait;
  }

  return max_wait;
}

/* adjust the level in the bucket
 *
 * @add_size:           the size to add
 * @substract_size:     the size to substract 
 */
void LeakyBucketThrottle::adjust(BucketType type, uint64_t add_size,
                                 uint64_t substract_size)
{
  assert(type == THROTTLE_BPS_TOTAL || type == THROTTLE_OPS_TOTAL);
  Mutex::Locker l(lock);
  if (buckets.count(type)) {
    buckets[type].level += add_size;
    if (buckets[type].level >= substract_size)
      buckets[type].level -= substract_size;
    else
      buckets[type].level = 0;
  }
}

/*
 * increase an unit on the bucket average for the dynamic mode
 */
void LeakyBucketThrottle::increase_bucket_average()
{
  Mutex::Locker l(lock);
  if (avg_is_max)
    return;

  avg_reset = false;
  uint32_t num_max = 0;
  for (map<BucketType, LeakyBucket>::iterator it = buckets.begin();
       it != buckets.end(); ++it) {
    if (it->first == THROTTLE_BPS_TOTAL) {
      double new_avg = it->second.avg + THROTTLE_BPS_INCREASE_UNIT;
      if (new_avg < it->second.max) {
        it->second.avg = new_avg;
      } else {
        it->second.avg = it->second.max;
	num_max++;
      }
      ldout(cct, 10) << __func__ << " increase throttle bw to "
                     << it->second.avg << dendl;
    } else if (it->first == THROTTLE_OPS_TOTAL) {
      double new_avg = it->second.avg + THROTTLE_OPS_INCREASE_UNIT;
      if (new_avg <= it->second.max) {
        it->second.avg = new_avg;
      } else {
        it->second.avg = it->second.max;
	num_max++;
      }
      ldout(cct, 10) << __func__ << " increase throttle iops to "
                     << it->second.avg << dendl;
    }
  }
  if (num_max == buckets.size())
    avg_is_max = true;

  return;
}

/*
 * reset the bucket average for the dynamic mode
 */
void LeakyBucketThrottle::reset_bucket_average()
{
  Mutex::Locker l(lock);
  if (avg_reset)
    return;

  avg_is_max = false;
  for (map<BucketType, LeakyBucket>::iterator it = buckets.begin();
       it != buckets.end(); ++it) {
    it->second.avg = it->second.min;
  }
  avg_reset = true;
  ldout(cct, 10) << __func__ << " reset throttle bw/iops to min" << dendl;
  return;
}
