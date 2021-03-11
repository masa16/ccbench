#if DURABLE_EPOCH
#include "include/log_buffer.hh"
#include "include/log_queue.hh"
#include "include/notifier.hh"
#include "include/logger.hh"

// no lock required since each thread has unique LogBuffer

void LogBuffer::push(std::uint64_t tid, NotificationId &nid,
                     std::vector<WriteElement<Tuple>> &write_set,
                     char *val, bool new_epoch_begins) {
  nid.tid_ = tid;
  nid.t_mid_ = rdtscp();
  // check buffer capa
  if (log_set_size_ + write_set.size() > LOG_BUFFER_SIZE) {
    pool_.publish();
  }
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
  // buffer full or new epoch begins
  if (log_set_size_ == LOG_BUFFER_SIZE ||
      nid_set_.size() == NID_BUFFER_SIZE || new_epoch_begins) {
    pool_.publish();
  }
  auto t = rdtscp();
  pool_.txn_latency_ += t - nid.tx_start_;
  pool_.bkpr_latency_ += t - nid.t_mid_;
}

static std::mutex smutex;
static bool first = true;

void LogBufferPool::publish() {
  uint64_t t = rdtscp();
  // enqueue
  queue_->enq(current_buffer_);
  // pool empty message
  if (pool_.empty() && first) {
    std::lock_guard<std::mutex> lock(smutex);
    if (first) {
      std::cerr << "Back pressure." << std::endl;
      first = false;
    }
  }
  // take buffer from pool
  {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_deq_.wait(lock, [this]{return quit_ || !pool_.empty();});
    current_buffer_ = pool_.back();
    pool_.pop_back();
  }
  publish_latency_ += rdtscp() - t;
  publish_counts_++;
}

void LogBufferPool::return_buffer(LogBuffer *lb) {
  if (quit_) return;
  std::lock_guard<std::mutex> lock(mutex_);
  pool_.emplace_back(lb);
  cv_deq_.notify_one();
}

void LogBufferPool::terminate(Result &myres) {
  quit_ = true;
  if (!current_buffer_->empty()) {
    publish();
  }
  myres.local_txn_latency_ = txn_latency_;
  myres.local_bkpr_latency_ = bkpr_latency_;
  myres.local_publish_latency_ = publish_latency_;
  myres.local_publish_counts_ = publish_counts_;
}

void LogBuffer::write(File &logfile, size_t &byte_count) {
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
                         LoggerResult &stats, std::uint64_t deq_time) {
  std::uint64_t t = rdtscp();
  for (auto &nid : nid_set_) {
    stats.txn_latency_ += nid.t_mid_ - nid.tx_start_;
    stats.log_queue_latency_ += deq_time - nid.t_mid_;
    nid.t_mid_ = t;
  }
  // copy NotificationID
  std::size_t n = nid_set_.size();
  nid_buffer.store(nid_set_, min_epoch_);
  stats.write_latency_ += (t - deq_time) * n;
  stats.nid_count_ += n;
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
