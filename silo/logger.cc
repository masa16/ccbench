#include "include/logger.hh"

extern void genLogFile(std::string &logpath, const int thid);

void Logger::add_txn_executor(TxnExecutor *trans) {
  LogBuffer *lb = new LogBuffer;
  lb->queue_ = &queue_;
  trans->log_buffer_ = lb;
  std::lock_guard<std::mutex> lock(mutex_);
  log_buffer_map_[trans->thid_] = lb;
  thid_set_.push_back(trans->thid_);
}

void Logger::logging(bool quit) {
  // calculate min(ctid_w)
  uint64_t min_ctid = ~(uint64_t)0;
  for (auto itr : thid_set_) {
    auto ctid = __atomic_load_n(&(CTIDW[itr].obj_), __ATOMIC_ACQUIRE);
    if (ctid > 0 && ctid < min_ctid) {
      min_ctid = ctid;
    }
  }
  // check
  Tidword tid;
  tid.obj_ = min_ctid;
  if (tid.epoch == 0 || min_ctid == ~(uint64_t)0) return;

  // write log
  while(!queue_.empty()) {
    auto log_buffer = queue_.deq();
    counter_ += log_buffer->nid_set_->size();
    log_buffer->write(logfile_, nid_buffer_);
  }
  logfile_.fsync();
  //usleep(1000000); // increase latency

  // publish Durable Epoch of this thread
  auto new_dl = tid.epoch - 1;
  auto old_dl = ThLocalDurableEpoch[thid_].obj_;
  if (new_dl != old_dl || quit) {
    asm volatile("":: : "memory");
    __atomic_store_n(&(ThLocalDurableEpoch[thid_].obj_), new_dl, __ATOMIC_RELEASE);
    asm volatile("":: : "memory");
    notifier_->push(nid_buffer_, quit);
  }
}

void Logger::worker(){
  std::string logpath;
  genLogFile(logpath, thid_);
  logfile_.open(logpath, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  logfile_.ftruncate(10 ^ 9);

  while(queue_.wait_deq()) logging(false);
  logging(true);
}

void Logger::run() {
  thread_ = std::thread([this]{worker();});
}

void Logger::terminate() {
  queue_.terminate();
}

void Logger::join() {
  thread_.join();
#if 1
  for(auto itr : log_buffer_map_)
    cout<<itr.first
        <<" : nid_set_.size="<<itr.second->nid_set_->size()
        <<", local_nid_set_.size="<<itr.second->local_nid_set_->size()
        <<", nid_buffer_.size="<<nid_buffer_.size()
        <<endl;
  cout<<"counter="<<counter_<<endl;
#endif
}
