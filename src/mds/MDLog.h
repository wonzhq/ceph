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


#ifndef CEPH_MDLOG_H
#define CEPH_MDLOG_H

enum {
  l_mdl_first = 5000,
  l_mdl_evadd,
  l_mdl_evex,
  l_mdl_evtrm,
  l_mdl_ev,
  l_mdl_evexg,
  l_mdl_evexd,
  l_mdl_segadd,
  l_mdl_segex,
  l_mdl_segtrm,
  l_mdl_seg,
  l_mdl_segexg,
  l_mdl_segexd,
  l_mdl_expos,
  l_mdl_wrpos,
  l_mdl_rdpos,
  l_mdl_jlat,
  l_mdl_last,
};

#include "include/types.h"
#include "include/Context.h"

#include "common/Thread.h"

#include "LogSegment.h"

#include <list>

class Journaler;
class JournalPointer;
class LogEvent;
class MDS;
class LogSegment;
class ESubtreeMap;

class PerfCounters;

#include <map>
using std::map;

#include "common/Finisher.h"


class MDLog {
public:
  MDS *mds;

protected:

  Mutex lock;
  int num_events; // in events
  int unflushed;
  bool capped;
  inodeno_t ino;
  Journaler *journaler;
  PerfCounters *logger;


  LogSegment *peek_current_segment() {
    return segments.empty() ? NULL : segments.rbegin()->second;
  }

  // -- replay --
  class ReplayThread : public Thread {
    MDLog *log;
  public:
    ReplayThread(MDLog *l) : log(l) {}
    void* entry() {
      log->_replay_thread();
      return 0;
    }
  } replay_thread;
  bool already_replayed;

  friend class ReplayThread;
  friend class C_MDL_Replay;

  list<Context*> waitfor_replay;

  void _replay();         // old way
  void _replay_thread();  // new way

  // Journal recovery/rewrite logic
  class RecoveryThread : public Thread {
    MDLog *log;
    Context *completion;
  public:
    void set_completion(Context *c) {completion = c;}
    RecoveryThread(MDLog *l) : log(l), completion(NULL) {}
    void* entry() {
      log->_recovery_thread(completion);
      return 0;
    }
  } recovery_thread;
  void _recovery_thread(Context *completion);
  void _reformat_journal(JournalPointer const &jp, Journaler *old_journal, Context *completion);

  // -- segments --
  // XXX so segments are a problem for making MDLog's lock self contained, because
  // callers use get_current_segment while prepare log events before submitting them.
  map<uint64_t,LogSegment*> segments;
  set<LogSegment*> expiring_segments;
  set<LogSegment*> expired_segments;
  int expiring_events;
  int expired_events;

  // -- subtreemaps --
  friend class ESubtreeMap;
  friend class C_MDS_WroteImportMap;
  friend class MDCache;
  friend class C_MaybeExpiredSegment;

  struct C_MDL_WriteError : public Context {
    MDLog *mdlog;
    C_MDL_WriteError(MDLog *m) : mdlog(m) {}
    void finish(int r) {
      mdlog->handle_journaler_write_error(r);
    }
  };
  void handle_journaler_write_error(int r);

  LogEvent *cur_event;

  void try_expire(LogSegment *ls, int op_prio);
  void _maybe_expired(LogSegment *ls, int op_prio);
  void _expired(LogSegment *ls);
  void _trim_expired_segments();

  void write_head(Context *onfinish);
public:
  MDLog(MDS *m) : mds(m),
		  lock("MDLog"),
		  num_events(0), 
		  unflushed(0),
		  capped(false),
		  journaler(0),
		  logger(0),
		  replay_thread(this),
		  already_replayed(false),
		  recovery_thread(this),
		  expiring_events(0), expired_events(0),
		  cur_event(NULL) { }		  
  ~MDLog();

  void create_logger();
  // Capping is done during shutdown: the capped flag indicates that
  // it is safe to expire the last log segment.
  bool is_capped() const { return capped; }
  void cap();

  void start_entry(LogEvent *e);
  void cancel_entry(LogEvent *e);
  bool entry_is_open() const { return cur_event != NULL; }

  // Asynchronous I/O operations
  // ===========================
  void submit_entry(LogEvent *e, Context *c = 0);
  void start_submit_entry(LogEvent *e, Context *c = 0) {
    start_entry(e);
    submit_entry(e, c);
  }
  void create(Context *onfinish);  // fresh, empty log! 
  void open(Context *onopen);      // append() or replay() to follow!
  void replay(Context *onfinish);
  void wait_for_safe( Context *c );

  // -- segments --
  void start_new_segment();
  void prepare_new_segment();
  void journal_segment_subtree_map();

  LogSegment *get_current_segment() { 
    assert(!segments.empty());
    return segments.rbegin()->second;
  }

  LogSegment *get_segment(uint64_t off);

  size_t get_num_events() const { return num_events; }
  size_t get_num_segments() const { return segments.size(); }  

  uint64_t get_read_pos() const;
  uint64_t get_write_pos() const;
  uint64_t get_safe_pos() const;
  Journaler *get_journaler() { return journaler; }
  bool empty() const { return segments.empty(); }


  void flush();
  /*
  bool is_flushed() {
    return unflushed == 0;
  }
  */
  void trim(int max=-1);
  void append();

  void standby_trim_segments();

  
  uint64_t get_last_segment_offset() {
    assert(!segments.empty());
    return segments.rbegin()->first;
  }
  LogSegment *get_oldest_segment() {
    return segments.begin()->second;
  }
  void remove_oldest_segment() {
    map<uint64_t, LogSegment*>::iterator p = segments.begin();
    delete p->second;
    segments.erase(p);
  }
};

#endif
