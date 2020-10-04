#include "include/log_buffer.hh"
#include "include/log_queue.hh"
#include "include/notifier.hh"

// no lock required since each thread has unique LogBuffer

void LogBuffer::push(uint64_t tid, unsigned int key, char *val) {
  log_set_[id_writing_].emplace_back(tid, key, val);
}

void LogBuffer::push(NotificationId &nid) {
  nid_set_[id_writing_].emplace_back(nid);
}

bool LogBuffer::publish(bool new_epoch_begins) {
  new_epoch_begins_ = new_epoch_begins_ || new_epoch_begins;
  if (nid_set_[id_public_].empty() && log_set_[id_public_].empty() &&
      (log_set_[id_writing_].size() >= LOG_BUFFER_SIZE || new_epoch_begins_)) {
    // swap buffer
    std::swap(id_writing_, id_public_);
    // enqueue
    queue_->enq(this);
    new_epoch_begins_ = false;
    return true;
  }
  return false;
}

void LogBuffer::terminate() {
  while (!log_set_[id_writing_].empty() || !nid_set_[id_writing_].empty()) {
    publish(true);
  }
}

void LogBuffer::write(File &logfile, std::vector<NotificationId> &nid_buffer) {
  // prepare header
  log_header_.init();
  for (auto log : log_set_[id_public_])
    log_header_.chkSum_ += log.computeChkSum();
  log_header_.logRecNum_ = log_set_[id_public_].size();
  log_header_.convertChkSumIntoComplementOnTwo();
  // write to file
  logfile.write((void*)&log_header_, sizeof(LogHeader));
  logfile.write((void*)&(log_set_[id_public_][0]), sizeof(LogRecord)*log_header_.logRecNum_);
  // clear for next transactions.
  log_set_[id_public_].clear();
  // copy NotificationID
  for (auto nid : nid_set_[id_public_]) nid_buffer.emplace_back(nid);
  nid_set_[id_public_].clear();
}

size_t LogBuffer::nid_set_size() {
  return nid_set_[id_public_].size();
}
