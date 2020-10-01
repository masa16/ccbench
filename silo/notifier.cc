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
#if NOTIFIER_THREAD
  thread_ = std::thread([this]{worker();});
#endif
}

#if NOTIFIER_THREAD
void Notifier::push(std::vector<NotificationId> &nid_buffer, bool quit) {
  std::unique_lock<std::mutex> lock(mutex_);
  push_size_ = nid_buffer.size();
  cv_enq_.wait(lock, [this]{return buffer_.size() + push_size_ <= capa_;});
  push_size_ = 0;
  for (auto &nid : nid_buffer) {
    buffer_.emplace_back(nid);
  }
  cv_deq_.notify_one();
  nid_buffer.clear();
  //quit_ = quit_ || quit;
}
#else
void Notifier::push(std::vector<NotificationId> &nid_buffer, bool quit) {
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
  if (d < min_dl || quit) {
    asm volatile("":: : "memory");
    __atomic_store_n(&(DurableEpoch.obj_), min_dl, __ATOMIC_RELEASE);
    asm volatile("":: : "memory");
    uint64_t latency = 0;
    for (auto &nid : nid_buffer) {
      // notify client here
      latency += rdtscp() - nid.clock_;
    }
    //asm volatile("":: : "memory");
    __atomic_fetch_add(&latency_, latency, __ATOMIC_ACQ_REL);
    __atomic_fetch_add(&count_, nid_buffer.size(), __ATOMIC_ACQ_REL);
    //asm volatile("":: : "memory");
    nid_buffer.clear();
  }
}
#endif

void Notifier::terminate() {
#if NOTIFIER_THREAD
  std::lock_guard<std::mutex> lock(mutex_);
  quit_ = true;
  cv_deq_.notify_all();
#else
  quit_ = true;
#endif
}

void Notifier::join() {
#if NOTIFIER_THREAD
  thread_.join();
#endif
}

void Notifier::display() {
  double t = (double)latency_ / FLAGS_clocks_per_us / count_ / 1000;
  std::cout << std::fixed << std::setprecision(4)
            << "durable_latency[ms]:\t" << t << endl;
  std::cout << "durable_count:\t\t" << count_ << endl;
  uint64_t d = __atomic_load_n(&(DurableEpoch.obj_), __ATOMIC_ACQUIRE);
  std::cout << "durable_epoch:\t\t" << d << endl;
  std::cout << "buffer_.size():\t\t" << buffer_.size() << endl;
}
