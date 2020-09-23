#include "include/logger.hh"

extern void genLogFile(std::string &logpath, const int thid);

void Logger::gen_logfile(int thid) {
  std::string logpath;
  genLogFile(logpath, thid);
  logfile_.open(logpath, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  logfile_.ftruncate(10 ^ 9);
}

void Logger::add_txn_executor(TxnExecutor *trans) {
  trans_set_.push_back(trans);
  trans->log_buffer_.queue_ = &queue_;
}

void Logger::loop(const bool &quit){
  while(!(queue_.empty() && loadAcquire(quit))) {
    queue_.wait();
    // calculate min(ctid_w)
    auto itr = trans_set_.begin();
    Tidword min_ctid = (*itr)->mrctid_;
    for (++itr; itr != trans_set_.end(); ++itr) {
      if ((*itr)->mrctid_ < min_ctid) {
        min_ctid = (*itr)->mrctid_;
      }
    }
    while(!queue_.empty()) {
      auto log_buffer = queue_.deq();
      log_buffer->write(logfile_);
    }
    logfile_.fsync();
    // publish durable_epoch_
    durable_epoch_ = min_ctid.epoch - 1;
  }
}
