#if DURABLE_EPOCH
#include "include/log_buffer.hh"
#include "include/log_queue.hh"
#include "include/notifier.hh"

// no lock required since each thread has unique LogBuffer

void LogBufferPool::push(std::uint64_t tid, NotificationId &nid,
                         std::vector<WriteElement<Tuple>> &write_set,
                         char *val, bool new_epoch_begins) {
  nid.tid_ = tid;
  nid.t_mid_ = rdtscp();
  assert(current_buffer_ != NULL);
  // check buffer capa
  if (current_buffer_->log_set_size_ + write_set.size() > LOG_BUFFER_SIZE
      || new_epoch_begins) {
    publish();
  }
  while(!is_ready()) {std::this_thread::sleep_for(std::chrono::microseconds(10));}
  if (quit_) return;
  current_buffer_->push(tid, nid, write_set, val);
  // buffer full
  if (current_buffer_->log_set_size_ + FLAGS_max_ope > LOG_BUFFER_SIZE) {
    publish();
  }
  auto t = rdtscp();
  txn_latency_ += t - nid.tx_start_;
  bkpr_latency_ += t - nid.t_mid_;
}

void LogBuffer::push(std::uint64_t tid, NotificationId &nid,
                     std::vector<WriteElement<Tuple>> &write_set,
                     char *val) {
  assert(write_set.size() > 0);
  // buffering
  for (auto &itr : write_set) {
    log_set_[log_set_size_++] = LogRecord(tid, itr.key_, val);
  }
  nid_set_.emplace_back(nid);
  // epoch
  Tidword tidw;
  tidw.obj_ = tid;
  std::uint64_t epoch = tidw.epoch;
  if (epoch < min_epoch_) min_epoch_ = epoch;
  if (epoch > max_epoch_) max_epoch_ = epoch;
  assert(min_epoch_ == max_epoch_);
}

bool LogBufferPool::is_ready() {
  if (current_buffer_ != NULL || quit_)
    return true;
  bool r = false;
  my_lock();
  //std::lock_guard<std::mutex> lock(mutex_);
  if (!pool_.empty()) {
    current_buffer_ = pool_.back();
    pool_.pop_back();
    r = true;
  }
  my_unlock();
  return r;
}

static std::mutex smutex;
static bool first = true;

void LogBufferPool::publish() {
  uint64_t t = rdtscp();
  while(!is_ready()) {usleep(5);}
  assert(current_buffer_ != NULL);
  // enqueue
  if (!current_buffer_->empty()) {
    LogBuffer *p = current_buffer_;
    current_buffer_ = NULL;
    queue_->enq(p);
  }
  publish_latency_ += rdtscp() - t;
  publish_counts_++;
}

void LogBufferPool::return_buffer(LogBuffer *lb) {
  //std::lock_guard<std::mutex> lock(mutex_);
  my_lock();
  pool_.emplace_back(lb);
  my_unlock();
  cv_deq_.notify_one();
}

void LogBufferPool::terminate(Result &myres) {
  quit_ = true;
  if (current_buffer_ != NULL)
    if (!current_buffer_->empty()) {
      publish();
    }
  myres.local_txn_latency_ = txn_latency_;
  myres.local_bkpr_latency_ = bkpr_latency_;
  myres.local_publish_latency_ = publish_latency_;
  myres.local_publish_counts_ = publish_counts_;
}

void LogBuffer::write(File &logfile, size_t &byte_count) {
  if (log_set_size_==0) return;
  // prepare header
  alignas(512) LogHeader log_header;
  for (size_t i=0; i<log_set_size_; i++)
    log_header.chkSum_ += log_set_[i].computeChkSum();
  log_header.logRecNum_ = log_set_size_;
  log_header.convertChkSumIntoComplementOnTwo();
  // write to file
  size_t header_size = sizeof(LogHeader);
  size_t record_size = sizeof(LogRecord) * log_header.logRecNum_;
  byte_count += header_size + record_size;
  logfile.write((void*)&log_header, header_size);
  logfile.write((void*)log_set_, record_size);
  // clear for next transactions.
  log_set_size_ = 0;
}

void LogBuffer::pass_nid(NidBuffer &nid_buffer,
                         NidStats &stats, std::uint64_t deq_time) {
  std::size_t n = nid_set_.size();
  if (n==0) return;
  std::uint64_t t = rdtscp();
  for (auto &nid : nid_set_) {
    stats.txn_latency_ += nid.t_mid_ - nid.tx_start_;
    stats.log_queue_latency_ += deq_time - nid.t_mid_;
    nid.t_mid_ = t;
  }
  // copy NotificationID
  nid_buffer.store(nid_set_, min_epoch_);
  stats.write_latency_ += (t - deq_time) * n;
  stats.count_ += n;
  // clear nid_set_
  nid_set_.clear();
  // init epoch
  min_epoch_ = ~(uint64_t)0;
  max_epoch_ = 0;
}

void LogBuffer::return_buffer() {
  pool_.return_buffer(this);
}

bool LogBuffer::empty() {
  return nid_set_.empty();
}
#endif
