#include "include/log_buffer.hh"
#include "include/log_queue.hh"
#include "include/notifier.hh"

// no lock required since each thread has unique LogBuffer

void LogBuffer::push(LogRecord &log) {
  local_log_set_->emplace_back(log);
  local_log_header_->chkSum_ += log.computeChkSum();
  ++local_log_header_->logRecNum_;
}

void LogBuffer::push(NotificationId &nid) {
  local_nid_set_->emplace_back(nid);
}

bool LogBuffer::publish(bool new_epoch_begins) {
  new_epoch_begins_ = new_epoch_begins_ || new_epoch_begins;
  if (nid_set_->empty() && log_set_->empty() &&
      (local_log_set_->size() >= LOG_SIZE || new_epoch_begins_)) {
    // prepare write header
    local_log_header_->convertChkSumIntoComplementOnTwo();
    // swap
    std::swap(local_log_header_, log_header_);
    std::swap(local_log_set_, log_set_);
    std::swap(local_nid_set_, nid_set_);
    // enqueue
    queue_->enq(this);
    new_epoch_begins_ = false;
    return true;
  }
  return false;
}

void LogBuffer::write(File &logfile, std::vector<NotificationId> &nid_buffer) {
  logfile.write((void*)log_header_, sizeof(LogHeader));
  logfile.write((void*)&((*log_set_)[0]), sizeof(LogRecord)*log_header_->logRecNum_);
  // clear for next transactions.
  log_set_->clear();
  log_header_->init();
  // copy NotificationID
  for (auto nid : *nid_set_) nid_buffer.emplace_back(nid);
  nid_set_->clear();
}
