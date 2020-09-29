#include "include/notifier.hh"

void Notifier::worker() {
  while(!(quit_ && buffer_.empty())) {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_deq_.wait(lock, [this]{return quit_ || !buffer_.empty();});
    // calculate min(d_l)
    uint64_t min_dl = __atomic_load_n(&(ThLocalDurableEpoch[0].obj_), __ATOMIC_ACQUIRE);
    for (unsigned int i=1; i < FLAGS_logger_num; ++i) {
      uint64_t dl = __atomic_load_n(&(ThLocalDurableEpoch[i].obj_), __ATOMIC_ACQUIRE);
      if (dl < min_dl) {
        min_dl = dl;
      }
    }
    // store Durable Epoch
    uint64_t d = __atomic_load_n(&(DurableEpoch.obj_), __ATOMIC_ACQUIRE);
    if (d < min_dl || quit_) {
      asm volatile("":: : "memory");
      __atomic_store_n(&(DurableEpoch.obj_), min_dl, __ATOMIC_RELEASE);
      asm volatile("":: : "memory");
      for (auto &nid : buffer_) {
        // notify client here
        latency_ += rdtscp() - nid.clock_;
      }
      count_ += buffer_.size();
      buffer_.clear();
      cv_enq_.notify_one();
    }
  }
}

void Notifier::run() {
  thread_ = std::thread([this]{worker();});
}

void Notifier::push(std::vector<NotificationId> &nid_buffer) {
  std::unique_lock<std::mutex> lock(mutex_);
  push_size_ = nid_buffer.size();
  cv_enq_.wait(lock, [this]{return buffer_.size() + push_size_ <= capa_;});
  push_size_ = 0;
  for (auto &nid : nid_buffer) {
    buffer_.emplace_back(nid);
  }
  cv_deq_.notify_one();
}

void Notifier::terminate() {
  std::lock_guard<std::mutex> lock(mutex_);
  quit_ = true;
  cv_deq_.notify_all();
}

void Notifier::join() {
  thread_.join();
}

void Notifier::display() {
  double t = (double)latency_ / FLAGS_clocks_per_us / count_ / 1000;
  std::cout << std::fixed << std::setprecision(4)
            << "durable_latency[ms]:\t" << t << endl;
  std::cout << "durable_count:\t\t" << count_ << endl;
  uint64_t d = __atomic_load_n(&(DurableEpoch.obj_), __ATOMIC_ACQUIRE);
  std::cout << "durable_epoch:\t\t" << d << endl;
  //std::cout << "buffer_.size():\t\t" << buffer_.size() << endl;
}
