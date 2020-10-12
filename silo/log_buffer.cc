#include "include/log_buffer.hh"
#include "include/log_queue.hh"
#include "include/notifier.hh"

// no lock required since each thread has unique LogBuffer

void LogBuffer::push(std::uint64_t tid, NotificationId nid,
                     std::vector<WriteElement<Tuple>> &write_set,
                     char *val, bool new_epoch_begins) {
  // check buffer capa
  if (log_set_.size() + write_set.size() > LOG_BUFFER_SIZE) {
    pool_.publish();
  }
  // buffering
  for (auto itr = write_set.begin(); itr != write_set.end(); ++itr) {
    log_set_.emplace_back(tid, (*itr).key_, val);
  }
  nid_set_.emplace_back(nid);
  // buffer full or new epoch begins
  if (log_set_.size() == LOG_BUFFER_SIZE ||
      nid_set_.size() == NID_BUFFER_SIZE || new_epoch_begins) {
    pool_.publish();
  }
}

void LogBufferPool::publish() {
    // enqueue
  queue_->enq(current_buffer_);
  // take buffer from pool
  std::unique_lock<std::mutex> lock(mutex_);
  cv_deq_.wait(lock, [this]{return quit_ || !pool_.empty();});
  if (!pool_.empty()) {
    current_buffer_ = pool_.back();
    pool_.pop_back();
  }
}

void LogBufferPool::return_buffer(LogBuffer *lb) {
  std::lock_guard<std::mutex> lock(mutex_);
  pool_.push_back(lb);
  cv_deq_.notify_one();
}

void LogBufferPool::terminate() {
  quit_ = true;
  if (!current_buffer_->empty()) {
    publish();
  }
}

size_t LogBuffer::write(File &logfile, std::vector<NotificationId> &nid_buffer) {
  // prepare header
  LogHeader log_header;
  for (auto log : log_set_)
    log_header.chkSum_ += log.computeChkSum();
  log_header.logRecNum_ = log_set_.size();
  log_header.convertChkSumIntoComplementOnTwo();
  // write to file
  logfile.write((void*)&log_header, sizeof(LogHeader));
  logfile.write((void*)&(log_set_[0]), sizeof(LogRecord)*log_header.logRecNum_);
  // clear for next transactions.
  log_set_.clear();
  // copy NotificationID
  for (auto nid : nid_set_) nid_buffer.emplace_back(nid);
  size_t nid_set_size = nid_set_.size();
  nid_set_.clear();
  pool_.return_buffer(this);
  return nid_set_size;
}

bool LogBuffer::empty() {
  return nid_set_.empty();
}
