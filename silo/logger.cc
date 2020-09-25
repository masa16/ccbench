#include "include/logger.hh"

extern void genLogFile(std::string &logpath, const int thid);

void Logger::gen_logfile(int thid) {
  std::string logpath;
  genLogFile(logpath, thid);
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
    // publish durable_epoch_
    Tidword tid;
    tid.obj_ = min_ctid;
    durable_epoch_ = tid.epoch - 1;
  }
}


void Logger::terminate(){
  queue_.terminate();
}
