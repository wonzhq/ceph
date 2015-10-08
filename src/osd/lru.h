// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */



#ifndef CEPH_LRU_H
#define CEPH_LRU_H

#include <stdint.h>

#include "common/config.h"

struct CacheObjLoc {
  uint32_t slab_index;
  uint32_t obj_index;

  CacheObjLoc() : slab_index(0), obj_index(0) {}
  void dump(Formatter *f) const;
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  static void generate_test_instances(std::list<CacheObjLoc*>& o);
};
WRITE_CLASS_ENCODER(CacheObjLoc)

struct CacheObj {
  uint32_t hash1;
  uint32_t hash2;
  uint32_t state;
  CacheObjLoc loc_prev, loc_next;

  CacheObj() : hash1(0), hash2(0) {}
  void dump(Formatter *f) const;
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  static void generate_test_instances(std::list<CacheObj*>& o);
};
WRITE_CLASS_ENCODER(CacheObj)

/*
class LRUObject {
 private:
  LRUObject *lru_next, *lru_prev;
  bool lru_pinned;
  class LRU *lru;
  class LRUList *lru_list;

 public:
  LRUObject() {
    lru_next = lru_prev = NULL;
    lru_list = 0;
    lru_pinned = false;
    lru = 0;
  }

  // pin/unpin item in cache
  void lru_pin(); 
  void lru_unpin();
  bool lru_is_expireable() { return !lru_pinned; }

  friend class LRU;
  friend class LRUList;
};
*/

class CacheLRUList {
 private:
  CacheObjLoc head, tail;
  uint32_t len;
  ObjCache cache;

 public:
  CacheLRUList() : len(0) { }
  
  uint32_t  get_length() { return len; }

  CacheObjLoc get_head() {
    return head;
  }
  CacheObjLoc get_tail() {
    return tail;
  }

  void clear() {
    while (len > 0) {
      remove(get_head());
    }
  }

  void insert_head(CacheObj *o, CacheObjLoc loc) {
    o->loc_next = head;
    o->loc_prev = INVALID;
    if (head != INVALID) {
      CacheObj *obj = cache.get_slab_object(head);
      obj->loc_prev = loc;
    } else {
      tail = loc;
    }
    head = loc;
    len++;
  }

  void insert_tail(CacheObj *o, CacheObjLoc loc) {
    o->loc_next = INVALID;
    o->loc_prev = tail;
    if (tail != INVALID) {
      CacheObj *obj = cache.get_slab_object(tail);
      obj->loc_next = loc;
    } else {
      head = loc;
    }
    tail = loc;
    len++;
  }

  void remove(CacheObj *o) {
    CacheObj *obj;
    if (o->loc_next != INVALID) {
      obj = get_slab_object(o->loc_next);
      obj->loc_prev = o->loc_prev;
    } else {
      tail = o->loc_prev;
    }
    if (o->loc_prev != INVALID) {
      obj = get_slab_object(o->loc_prev);
      obj->loc_next = o->loc_next;
    } else {
      head = o->lru_next;
    }
    o->loc_next = o->loc_prev = INVALID;
    assert(len>0);
    len--;
  }
  
};

const uint32_t SLAB_SIZE = 1024 * 1024 * 10 / sizeof(CacheObj);
class CacheSlab {
 private:
  uint32_t slab_index;
  uint32_t free;
  uint32_t search_index;
  uint32_t bm[SLAB_SIZE/32];
  CacheObj objects[SLAB_SIZE];

 public:
  CacheSlab() : slab_index(0), free(SLAB_SIZE), search_index(0) {
    for (uint32_t i = 0; i < SLAB_SIZE / 4; i++) {
      bm[i] = 0;
    }
  }

  CacheSlab(uint32_t index) : slab_index(index), free(SLAB_SIZE), search_index(0) {
    for (uint32_t i = 0; i < SLAB_SIZE / 4; i++) {
      bm[i] = 0;
    }
  }

  uint32_t get_free() {
    return free;
  }

  CacheObj* get_obj(uint32_t index) {
    if (index >= SLAB_SIZE)
      return NULL;
    // return NULL if not allocated
    if (bm[index/32] & (1 << index % 32) == 0)
      return NULL;
    return &objects[index];
  }

  void free_obj(uint32_t index) {
    if (index >= SLAB_SIZE)
      return;
    bm[index/32] &= ~(1 << index % 32);
  }

  CacheObj* find_next_free_obj() {
    if (free == 0)
      return NULL;
    for (uint32_t i = 0; i < SLAB_SIZE/4; i++) {
      if (bm[i] == 0xffffffff)
	continue;
      for (uint32_t j = 0; j < 32; j++) {
	if (bm[i] & (1 << j) == 0) {
	  bm[i] |= (1 << j);
	  free--;
	  return &objects[32*i+j];
	}
      }
    }
  }
}

class ObjCache {
 private:
  vector<pair<CacheSlab*, bool> > slabs;
  // uint32_t slab_index;
  CacheLRUList lru;

 public:
  ObjCache() {}

  CacheObj* get_slab_object(CacheObjLoc loc) {
    if (!slabs[loc.slab_index].second)
      return NULL;
    assert(slabs[loc.slab_index].first);
    return slabs[loc.slab_index].first->get_object(loc.obj_index);
  }

  bool remove_slab_object(CacheObjLoc loc) {
    if (!slabs[loc.slab_index].second)
      return false;
    assert(slabs[loc.slab_index].first);
    return slabs[loc.slab_index].first->free_object(loc.obj_index);
  }

  CacheObj* alloc_object(hobject_t oid) {
    CacheObj *obj = NULL;

    for (uint32_t i = 0; i < slabs.size(); i++) {
      if (!slabs[i].second)
	continue;
      if (slabs[i].first->get_free() == 0)
	continue;
      obj = slabs[i].first->find_next_free_obj();
      assert(obj);
      return obj;
    }

    // need to allocate a new slab
    CacheSlab *slab = new CacheSlab(slabs.size());
    slabs.push_back(make_pair(slab, true));
    obj = slab->find_next_free_obj();
    assert(obj);
    return obj;
  }

  CacheObj* find_object(hobject_t obj) {
    uint32_t h1 = obj.get_hash();
    uint32_t h2 = obj.get_hash2();

    CacheObjLoc loc = lru.get_head();
    if (loc == INVALID)
      return NULL;
    CacheObj *obj = get_slab_object(loc);
    while (obj) {
      if (obj->hash1 == h1 && obj->hash2 == h2)
	return obj;
      loc = obj.loc_next;
      if (loc == INVALID)
	return NULL;
      obj = get_slab_object(loc);
    }
    return NULL;
  }

  void touch_object(hobject_t oid) {
    CacheObj* object = find_object(oid);
    if (object) {
      lru.lru_remove(object);
      lru.lru_insert_top(object);
      return;
    }

    object = alloc_object(oid);
    assert(object);
    lru.lru_insert_top(object);
  }

  bool get_next_evict_object(hobject_t &oid) {
    CacheObjLoc loc = lru.get_tail();
    if (loc == INVALID)
      return false;
    CacheObj *obj = get_slab_object(loc);
    while (obj) {
      if (obj->state & DIRTY) {
	loc = obj->loc_prev;
        if (loc == INVALID)
          return false;
        obj = get_slab_object(loc);
      } else {
	// list the object from the backend
      }
    }
    return false;
  }

  bool get_next_flush_object(hobject_t &oid) {
  }
}

/*
class LRU {
 protected:
  LRUList lru_top, lru_bot, lru_pintail;
  uint32_t lru_num, lru_num_pinned;
  uint32_t lru_max;   // max items
  double lru_midpoint;

  friend class LRUObject;
  //friend class MDCache; // hack
  
 public:
  LRU(int max = 0) {
    lru_num = 0;
    lru_num_pinned = 0;
    lru_midpoint = .6;
    lru_max = max;
  }

  uint32_t lru_get_size() { return lru_num; }
  uint32_t lru_get_top() { return lru_top.get_length(); }
  uint32_t lru_get_bot() { return lru_bot.get_length(); }
  uint32_t lru_get_pintail() { return lru_pintail.get_length(); }
  uint32_t lru_get_max() { return lru_max; }
  uint32_t lru_get_num_pinned() { return lru_num_pinned; }

  void lru_set_max(uint32_t m) { lru_max = m; }
  void lru_set_midpoint(float f) { lru_midpoint = f; }
  
  void lru_clear() {
    lru_top.clear();
    lru_bot.clear();
    lru_pintail.clear();
    lru_num = 0;
  }

  // insert at top of lru
  void lru_insert_top(LRUObject *o) {
    //assert(!o->lru_in_lru);
    //o->lru_in_lru = true;
    assert(!o->lru);
    o->lru = this;
    lru_top.insert_head( o );
    lru_num++;
    if (o->lru_pinned) lru_num_pinned++;
    lru_adjust();
  }

  // insert at mid point in lru
  void lru_insert_mid(LRUObject *o) {
    //assert(!o->lru_in_lru);
    //o->lru_in_lru = true;
    assert(!o->lru);
    o->lru = this;
    lru_bot.insert_head(o);
    lru_num++;
    if (o->lru_pinned) lru_num_pinned++;
  }

  // insert at bottom of lru
  void lru_insert_bot(LRUObject *o) {
    assert(!o->lru);
    o->lru = this;
    lru_bot.insert_tail(o);
    lru_num++;
    if (o->lru_pinned) lru_num_pinned++;
  }

  // insert at bottom of lru
  void lru_insert_pintail(LRUObject *o) {
    assert(!o->lru);
    o->lru = this;
    
    assert(o->lru_pinned);

    lru_pintail.insert_head(o);
    lru_num++;
    lru_num_pinned += o->lru_pinned;
  }

  


  // adjust top/bot balance, as necessary
  void lru_adjust() {
    if (!lru_max) return;

    unsigned toplen = lru_top.get_length();
    unsigned topwant = (unsigned)(lru_midpoint * ((double)lru_max - lru_num_pinned));
    while (toplen > 0 && 
           toplen > topwant) {
      // remove from tail of top, stick at head of bot
      // FIXME: this could be way more efficient by moving a whole chain of items.

      LRUObject *o = lru_top.get_tail();
      lru_top.remove(o);
      lru_bot.insert_head(o);
      toplen--;
    }
  }


  // remove an item
  LRUObject *lru_remove(LRUObject *o) {
    // not in list
    //assert(o->lru_in_lru);
    //if (!o->lru_in_lru) return o;  // might have expired and been removed that way.
    if (!o->lru) return o;

    assert((o->lru_list == &lru_pintail) ||
           (o->lru_list == &lru_top) ||
           (o->lru_list == &lru_bot));
    o->lru_list->remove(o);

    lru_num--;
    if (o->lru_pinned) lru_num_pinned--;
    o->lru = 0;
    return o;
  }

  // touch item -- move to head of lru
  bool lru_touch(LRUObject *o) {
    lru_remove(o);
    lru_insert_top(o);
    return true;
  }

  // touch item -- move to midpoint (unless already higher)
  bool lru_midtouch(LRUObject *o) {
    if (o->lru_list == &lru_top) return false;
    
    lru_remove(o);
    lru_insert_mid(o);
    return true;
  }

  // touch item -- move to bottom
  bool lru_bottouch(LRUObject *o) {
    lru_remove(o);
    lru_insert_bot(o);
    return true;
  }

  void lru_touch_entire_pintail() {
    // promote entire pintail to the top lru
    while (lru_pintail.get_length() > 0) {
      LRUObject *o = lru_pintail.get_head();
      lru_pintail.remove(o);
      lru_top.insert_tail(o);
    }
  }


  // expire -- expire a single item
  LRUObject *lru_get_next_expire() {
    LRUObject *p;
    
    // look through tail of bot
    while (lru_bot.get_length()) {
      p = lru_bot.get_tail();
      if (!p->lru_pinned) return p;

      // move to pintail
      lru_bot.remove(p);
      lru_pintail.insert_head(p);
    }

    // ok, try head then
    while (lru_top.get_length()) {
      p = lru_top.get_tail();
      if (!p->lru_pinned) return p;

      // move to pintail
      lru_top.remove(p);
      lru_pintail.insert_head(p);
    }
    
    // no luck!
    return NULL;
  }
  
  LRUObject *lru_expire() {
    LRUObject *p = lru_get_next_expire();
    if (p) 
      return lru_remove(p);
    return NULL;
  }


  void lru_status() {
    //generic_dout(10) << "lru: " << lru_num << " items, " << lru_top.get_length() << " top, " << lru_bot.get_length() << " bot, " << lru_pintail.get_length() << " pintail" << dendl;
  }

};


inline void LRUObject::lru_pin() {
  if (lru && !lru_pinned) {
    lru->lru_num_pinned++;
    lru->lru_adjust();
  }
  lru_pinned = true;
}

inline void LRUObject::lru_unpin() {
  if (lru && lru_pinned) {
    lru->lru_num_pinned--;

    // move from pintail -> bot
    if (lru_list == &lru->lru_pintail) {
      lru->lru_pintail.remove(this);
      lru->lru_bot.insert_tail(this);
    }
    lru->lru_adjust();
  }
  lru_pinned = false;
}
*/

#endif
