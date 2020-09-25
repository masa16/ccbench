#include "include/logger.hh"

extern void genLogFile(std::string &logpath, const int thid);

void Logger::gen_logfile() {
  std::string logpath;
  genLogFile(logpath, thid_);
  logfile_.open(logpath, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  logfile_.ftruncate(10 ^ 9);
}

void Logger::add_txn_executor(TxnExecutor *trans) {
  thid_set_.push_back(trans->thid_);
  trans->log_buffer_.queue_ = &queue_;
}

void Logger::loop(){
  while(queue_.wait_deq()) {
    // calculate min(ctid_w)
    auto itr = thid_set_.begin();
    auto min_ctid = CTIDW[*itr].obj_;
    for (++itr; itr != thid_set_.end(); ++itr) {
      auto ctid = CTIDW[*itr].obj_;
      if (ctid < min_ctid) {
        min_ctid = ctid;
      }
    }
    // write log
    while(!queue_.empty()) {
      auto log_buffer = queue_.deq();
      log_buffer->write(logfile_);
    }
    logfile_.fsync();
    // publish Durable Epoch of this thread
    Tidword tid;
    tid.obj_ = min_ctid;
    auto dl = tid.epoch - 1;
    asm volatile("":: : "memory");
    __atomic_store_n(&(ThLocalDurableEpoch[thid_].obj_), dl, __ATOMIC_RELEASE);
    asm volatile("":: : "memory");
  }
  // calculate min(d_l)
  auto min_dl = ThLocalDurableEpoch[0].obj_;
  for (unsigned int i=1; i < FLAGS_logger_num; ++i) {
    auto dl = ThLocalDurableEpoch[i].obj_;
    if (dl < min_dl) {
      min_dl = dl;
    }
  }
  // store Durable Epoch
  auto dl = DurableEpoch.obj_;
  if (dl != min_dl) {
    asm volatile("":: : "memory");
    __atomic_store_n(&(DurableEpoch.obj_), min_dl, __ATOMIC_RELEASE);
    asm volatile("":: : "memory");
  }
}


void Logger::terminate(){
  queue_.terminate();
}
