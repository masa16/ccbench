#include "include/logger.hh"

extern void genLogFile(std::string &logpath, const int thid);

void Logger::add_txn_executor(TxnExecutor *trans) {
  thid_set_.push_back(trans->thid_);
  LogBuffer *lb = new LogBuffer;
  lb->queue_ = &queue_;
  trans->log_buffer_ = lb;
  log_buffer_map_[trans->thid_] = lb;
}

void Logger::logging() {
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
    //counter_ += log_buffer->nid_set_->size();
    log_buffer->write(logfile_, nid_buffer_);
  }
  logfile_.fsync();

  // publish Durable Epoch of this thread
  auto new_dl = tid.epoch - 1;
  auto old_dl = ThLocalDurableEpoch[thid_].obj_;
  if (new_dl != old_dl || quit_) {
    asm volatile("":: : "memory");
    __atomic_store_n(&(ThLocalDurableEpoch[thid_].obj_), new_dl, __ATOMIC_RELEASE);
    asm volatile("":: : "memory");
    notifier_->push(nid_buffer_);
    nid_buffer_.clear();
  }
}

void Logger::worker(){
  std::string logpath;
  genLogFile(logpath, thid_);
  logfile_.open(logpath, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  logfile_.ftruncate(10 ^ 9);

  while(queue_.wait_deq()) logging();
}

void Logger::run() {
  thread_ = std::thread([this]{worker();});
}

void Logger::terminate() {
  quit_ = true;
  for(auto itr : log_buffer_map_) {
    auto log_buffer = itr.second;
    log_buffer->publish(true); // switch buffer
  }
  queue_.terminate();
}

void Logger::join() {
  thread_.join();
  logging(); // final process
  logfile_.fsync();
#if 0
  for(auto itr : log_buffer_map_)
    cout<<itr.first
        <<" : nid_set_.size="<<itr.second->nid_set_->size()
        <<", local_nid_set_.size="<<itr.second->local_nid_set_->size()
        <<endl;
  cout<<"counter="<<counter_<<endl;
#endif
}
