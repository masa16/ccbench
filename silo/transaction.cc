#include <stdio.h>
#include <algorithm>
#include <string>

#include "include/atomic_tool.hh"
#include "include/log.hh"
#include "include/transaction.hh"
#if DURABLE_EPOCH
#include "include/util.hh"
#endif

extern void displayDB();

#ifdef PWAL
extern uint *GlobalLSN;
std::atomic<uint> MutexLSN;
static constexpr uint LockBit = 0x01;                                                  static constexpr uint UnlockBit = 0x00;
#endif

TxnExecutor::TxnExecutor(int thid, Result *sres) : thid_(thid), sres_(sres) {
  read_set_.reserve(FLAGS_max_ope);
  write_set_.reserve(FLAGS_max_ope);
  pro_set_.reserve(FLAGS_max_ope);
  // log_set_.reserve(LOGSET_SIZE);

  // latest_log_header_.init();

  max_rset_.obj_ = 0;
  max_wset_.obj_ = 0;

  genStringRepeatedNumber(write_val_, VAL_SIZE, thid);
}

void TxnExecutor::abort() {
  read_set_.clear();
  write_set_.clear();

#if BACK_OFF
#if ADD_ANALYSIS
  std::uint64_t start(rdtscp());
#endif

  Backoff::backoff(FLAGS_clocks_per_us);

#if ADD_ANALYSIS
  sres_->local_backoff_latency_ += rdtscp() - start;
#endif
#endif
}

void TxnExecutor::begin() {
  status_ = TransactionStatus::kInFlight;
  max_wset_.obj_ = 0;
  max_rset_.obj_ = 0;
#if DURABLE_EPOCH
  nid_ = NotificationId(nid_counter_++, thid_, rdtscp());
#endif
}

void TxnExecutor::displayWriteSet() {
  printf("display_write_set()\n");
  printf("--------------------\n");
  for (auto &ws : write_set_) {
    printf("key\t:\t%lu\n", ws.key_);
  }
}

void TxnExecutor::insert([[maybe_unused]] std::uint64_t key, [[maybe_unused]] std::string_view val) { // NOLINT

}

void TxnExecutor::lockWriteSet() {
  Tidword expected, desired;

[[maybe_unused]] retry
  :
  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {
    expected.obj_ = loadAcquire((*itr).rcdptr_->tidword_.obj_);
    for (;;) {
      if (expected.lock) {
#if NO_WAIT_LOCKING_IN_VALIDATION
        this->status_ = TransactionStatus::kAborted;
        if (itr != write_set_.begin()) unlockWriteSet(itr);
        return;
#elif NO_WAIT_OF_TICTOC
        if (itr != write_set_.begin()) unlockWriteSet(itr);
        goto retry;
#endif
      } else {
        desired = expected;
        desired.lock = 1;
        if (compareExchange((*itr).rcdptr_->tidword_.obj_, expected.obj_,
                            desired.obj_))
          break;
      }
    }

    max_wset_ = std::max(max_wset_, expected);
  }
}

void TxnExecutor::read(std::uint64_t key) {
#if ADD_ANALYSIS
  std::uint64_t start = rdtscp();
#endif

  // these variable cause error (-fpermissive)
  // "crosses initialization of ..."
  // So it locate before first goto instruction.
  Tidword expected, check;

  /**
   * read-own-writes or re-read from local read set.
   */
  if (searchReadSet(key) || searchWriteSet(key)) goto FINISH_READ;

  /**
   * Search tuple from data structure.
   */
  Tuple *tuple;
#if MASSTREE_USE
  tuple = MT.get_value(key);
#if ADD_ANALYSIS
  ++sres_->local_tree_traversal_;
#endif
#else
  tuple = get_tuple(Table, key);
#endif

  //(a) reads the TID word, spinning until the lock is clear

  expected.obj_ = loadAcquire(tuple->tidword_.obj_);
  // check if it is locked.
  // spinning until the lock is clear

  for (;;) {
    while (expected.lock) {
      expected.obj_ = loadAcquire(tuple->tidword_.obj_);
    }

    //(b) checks whether the record is the latest version
    // omit. because this is implemented by single version

    //(c) reads the data
    memcpy(return_val_, tuple->val_, VAL_SIZE);

    //(d) performs a memory fence
    // don't need.
    // order of load don't exchange.

    //(e) checks the TID word again
    check.obj_ = loadAcquire(tuple->tidword_.obj_);
    if (expected == check) break;
    expected = check;
#if ADD_ANALYSIS
    ++sres_->local_extra_reads_;
#endif
  }

  read_set_.emplace_back(key, tuple, return_val_, expected);
  // emplace is often better performance than push_back.

#if SLEEP_READ_PHASE
  sleepTics(SLEEP_READ_PHASE);
#endif

FINISH_READ:

#if ADD_ANALYSIS
  sres_->local_read_latency_ += rdtscp() - start;
#endif
  return;
}

void tx_delete([[maybe_unused]]std::uint64_t key) {

}

ReadElement<Tuple> *TxnExecutor::searchReadSet(std::uint64_t key) {
  for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr) {
    if ((*itr).key_ == key) return &(*itr);
  }

  return nullptr;
}

WriteElement<Tuple> *TxnExecutor::searchWriteSet(std::uint64_t key) {
  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {
    if ((*itr).key_ == key) return &(*itr);
  }

  return nullptr;
}

void TxnExecutor::unlockWriteSet() {
  Tidword expected, desired;

  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {
    expected.obj_ = loadAcquire((*itr).rcdptr_->tidword_.obj_);
    desired = expected;
    desired.lock = 0;
    storeRelease((*itr).rcdptr_->tidword_.obj_, desired.obj_);
  }
}

void TxnExecutor::unlockWriteSet(
        std::vector<WriteElement<Tuple>>::iterator end) {
  Tidword expected, desired;

  for (auto itr = write_set_.begin(); itr != end; ++itr) {
    expected.obj_ = loadAcquire((*itr).rcdptr_->tidword_.obj_);
    desired = expected;
    desired.lock = 0;
    storeRelease((*itr).rcdptr_->tidword_.obj_, desired.obj_);
  }
}

bool TxnExecutor::validationPhase() {
#if ADD_ANALYSIS
  std::uint64_t start = rdtscp();
#endif

  /* Phase 1
   * lock write_set_ sorted.*/
  sort(write_set_.begin(), write_set_.end());
  lockWriteSet();
#if NO_WAIT_LOCKING_IN_VALIDATION
  if (this->status_ == TransactionStatus::kAborted) return false;
#endif

  asm volatile("":: : "memory");
  atomicStoreThLocalEpoch(thid_, atomicLoadGE());
  asm volatile("":: : "memory");

  /* Phase 2 abort if any condition of below is satisfied.
   * 1. tid of read_set_ changed from it that was got in Read Phase.
   * 2. not latest version
   * 3. the tuple is locked and it isn't included by its write set.*/

  Tidword check;
  for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr) {
    // 1
    check.obj_ = loadAcquire((*itr).rcdptr_->tidword_.obj_);
    if ((*itr).get_tidword().epoch != check.epoch ||
        (*itr).get_tidword().tid != check.tid) {
#if ADD_ANALYSIS
      sres_->local_vali_latency_ += rdtscp() - start;
#endif
      this->status_ = TransactionStatus::kAborted;
      unlockWriteSet();
      return false;
    }
    // 2
    // if (!check.latest) return false;

    // 3
    if (check.lock && !searchWriteSet((*itr).key_)) {
#if ADD_ANALYSIS
      sres_->local_vali_latency_ += rdtscp() - start;
#endif
      this->status_ = TransactionStatus::kAborted;
      unlockWriteSet();
      return false;
    }
    max_rset_ = std::max(max_rset_, check);
  }

  // goto Phase 3
#if ADD_ANALYSIS
  sres_->local_vali_latency_ += rdtscp() - start;
#endif
  this->status_ = TransactionStatus::kCommitted;
  return true;
}

#if DURABLE_EPOCH
void TxnExecutor::wal(std::uint64_t ctid) {
  Tidword old_tid;
  Tidword new_tid;
  old_tid.obj_ = loadAcquire(CTIDW[thid_].obj_);
  new_tid.obj_ = ctid;
  bool new_epoch_begins = (old_tid.epoch != new_tid.epoch);
  log_buffer_pool_.push(ctid, nid_, write_set_, write_val_, new_epoch_begins);
  if (new_epoch_begins) {
    // store CTIDW
    __atomic_store_n(&(CTIDW[thid_].obj_), ctid, __ATOMIC_RELEASE);
  }
}
#elif PMEMTEST
void TxnExecutor::wal(std::uint64_t ctid) {
  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {
    LogRecord log(ctid, (*itr).key_, write_val_);
    logfile_.write((void*)&log, sizeof(LogRecord));
  }
  //logfile_.fsync();
}
#else
void TxnExecutor::wal(std::uint64_t ctid) {
  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {
    LogRecord log(ctid, (*itr).key_, write_val_);
    log_set_.emplace_back(log);
    latest_log_header_.chkSum_ += log.computeChkSum();
    ++latest_log_header_.logRecNum_;
  }

  if (log_set_.size() > LOGSET_SIZE / 2) {
    // prepare write header
    latest_log_header_.convertChkSumIntoComplementOnTwo();

    // write header
    logfile_.write((void *) &latest_log_header_, sizeof(LogHeader));

    // write log record
    // for (auto itr = log_set_.begin(); itr != log_set_.end(); ++itr)
    //  logfile_.write((void *)&(*itr), sizeof(LogRecord));
    logfile_.write((void *) &(log_set_[0]),
                   sizeof(LogRecord) * latest_log_header_.logRecNum_);

    // logfile_.fdatasync();

    // clear for next transactions.
    latest_log_header_.init();
    log_set_.clear();
  }
}
#endif

#ifdef PWAL
bool my_lock_core() {
	auto lock = MutexLSN.load();
	if (lock == LockBit) {
		usleep(1); //backoff
		return false;
	}
	return MutexLSN.compare_exchange_strong(lock, LockBit);
}

void my_lock(void) {
	while (true) {
		bool ok = my_lock_core();
		if (ok == true) break;
	}
}

void my_unlock() {
	MutexLSN.store(UnlockBit);
}

uint getLogSeqNum()
{
	static uint lsn = 0;
	uint old_lsn;
	my_lock();
	old_lsn = lsn;
	lsn++;
	my_unlock();

	return old_lsn;
}

uint TxnExecutor::pwal(std::uint64_t ctid) {
  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {
    LogRecord log(ctid, (*itr).key_, write_val_);
    log_set_.emplace_back(log);
    latest_log_header_.chkSum_ += log.computeChkSum();
    ++latest_log_header_.logRecNum_;
  }
	uint lsn = latest_log_header_.lsn_ += getLogSeqNum();

	// prepare write header
	latest_log_header_.convertChkSumIntoComplementOnTwo();

	// write header
	logfile_.write((void *) &latest_log_header_, sizeof(LogHeader));

	// write log record
	// for (auto itr = log_set_.begin(); itr != log_set_.end(); ++itr)
	//  logfile_.write((void *)&(*itr), sizeof(LogRecord));
	logfile_.write((void *) &(log_set_[0]),
								 sizeof(LogRecord) * latest_log_header_.logRecNum_);

	logfile_.fdatasync();

	// clear for next transactions.
	latest_log_header_.init();
	log_set_.clear();

	return lsn;
}
#endif // PWAL

void TxnExecutor::write(std::uint64_t key, std::string_view val) {
#if ADD_ANALYSIS
  std::uint64_t start = rdtscp();
#endif

  if (searchWriteSet(key)) goto FINISH_WRITE;

  /**
   * Search tuple from data structure.
   */
  Tuple *tuple;
  ReadElement<Tuple> *re;
  re = searchReadSet(key);
  if (re) {
    tuple = re->rcdptr_;
  } else {
#if MASSTREE_USE
    tuple = MT.get_value(key);
#if ADD_ANALYSIS
    ++sres_->local_tree_traversal_;
#endif
#else
    tuple = get_tuple(Table, key);
#endif
  }

  write_set_.emplace_back(key, tuple, val);

FINISH_WRITE:

#if ADD_ANALYSIS
  sres_->local_write_latency_ += rdtscp() - start;
#endif
  return;
}

void TxnExecutor::writePhase() {
  // It calculates the smallest number that is
  //(a) larger than the TID of any record read or written by the transaction,
  //(b) larger than the worker's most recently chosen TID,
  // and (C) in the current global epoch.

  Tidword tid_a, tid_b, tid_c;

  // calculates (a)
  // about read_set_
  tid_a = std::max(max_wset_, max_rset_);
  tid_a.tid++;

  // calculates (b)
  // larger than the worker's most recently chosen TID,
  tid_b = mrctid_;
  tid_b.tid++;

  // calculates (c)
  tid_c.epoch = ThLocalEpoch[thid_].obj_;

  // compare a, b, c
  Tidword maxtid = std::max({tid_a, tid_b, tid_c});
  maxtid.lock = 0;
  maxtid.latest = 1;
  mrctid_ = maxtid;

#if WAL
  wal(maxtid.obj_);
#endif

#if PWAL
	GlobalLSN[thid] = pwal(maxtid.obj_);
#endif

  // write(record, commit-tid)
  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {
    // update and unlock
    if ((*itr).get_val_length() == 0) {
      // fast approach for benchmark
      memcpy((*itr).rcdptr_->val_, write_val_, VAL_SIZE);
    } else {
      memcpy((*itr).rcdptr_->val_, (*itr).get_val_ptr(), (*itr).get_val_length());
    }
    storeRelease((*itr).rcdptr_->tidword_.obj_, maxtid.obj_);
  }

#if DURABLE_EPOCH
  wal(maxtid.obj_);
#endif

  read_set_.clear();
  write_set_.clear();
}

#if DURABLE_EPOCH
bool TxnExecutor::pauseCondition() {
  auto dlepoch = loadAcquire(ThLocalDurableEpoch[logger_thid_].obj_);
  return loadAcquire(ThLocalEpoch[thid_].obj_) > dlepoch + FLAGS_epoch_diff;
}

void TxnExecutor::epochWork(uint64_t &epoch_timer_start,
                            uint64_t &epoch_timer_stop) {
  usleep(1);
  if (thid_ == 0) {
    leaderWork(epoch_timer_start, epoch_timer_stop);
  }
  Tidword old_tid;
  old_tid.obj_ = loadAcquire(CTIDW[thid_].obj_);
  // load Global Epoch
  atomicStoreThLocalEpoch(thid_, atomicLoadGE());
  uint64_t new_epoch = loadAcquire(ThLocalEpoch[thid_].obj_);
  if (old_tid.epoch != new_epoch) {
    Tidword tid;
    tid.epoch = new_epoch;
    tid.lock = 0;
    tid.latest = 1;
    // store CTIDW
    __atomic_store_n(&(CTIDW[thid_].obj_), tid.obj_, __ATOMIC_RELEASE);
  }
}

void TxnExecutor::durableEpochWork(uint64_t &epoch_timer_start,
                                   uint64_t &epoch_timer_stop, const bool &quit) {
  std::uint64_t t = rdtscp();
  // pause this worker until Durable epoch catches up
  if (FLAGS_epoch_diff > 0) {
    if (pauseCondition()) {
      log_buffer_pool_.publish();
      do {
        epochWork(epoch_timer_start, epoch_timer_stop);
        if (loadAcquire(quit)) return;
      } while (pauseCondition());
    }
  }
  while (!log_buffer_pool_.is_ready()) {
    epochWork(epoch_timer_start, epoch_timer_stop);
    if (loadAcquire(quit)) return;
  }
  if (log_buffer_pool_.current_buffer_==NULL) std::abort();
  sres_->local_wait_depoch_latency_ += rdtscp() - t;
}
#endif
