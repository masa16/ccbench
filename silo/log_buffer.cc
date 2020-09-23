#include "include/log_buffer.hh"
#include "include/log_queue.hh"

void LogBuffer::add(LogRecord &log) {
  local_log_set_->emplace_back(log);
  local_log_header_->chkSum_ += log.computeChkSum();
  ++local_log_header_->logRecNum_;
}

void LogBuffer::publish() {
  if (log_set_->empty()  && local_log_set_->size() > LOGSET_SIZE / 2) {
    // prepare write header
    local_log_header_->convertChkSumIntoComplementOnTwo();
    // swap
    std::swap(local_log_header_, log_header_);
    std::swap(local_log_set_, log_set_);
    // enqueue
    queue_->enq(this);
  }
}

void LogBuffer::write(File &logfile) {
  logfile.write((void*) log_header_, sizeof(LogHeader));
  logfile.write((void*) &((*log_set_)[0]), sizeof(LogRecord)*log_header_->logRecNum_);
  // clear for next transactions.
  log_header_->init();
  log_set_->clear();
}
