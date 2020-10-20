#if DURABLE_EPOCH
#include <iostream>
#include <fstream>
#include "include/notifier.hh"
#include "include/logger.hh"

void Notifier::add_logger(Logger *logger) {
  std::unique_lock<std::mutex> lock(mutex_);
  logger_set_.emplace(logger);
}

void Notifier::make_durable(std::vector<NotificationId> &nid_buffer, bool quit) {
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
    uint64_t t = rdtscp();
    uint64_t min_latency = ~(uint64_t)0;
    uint64_t max_latency = 0;
    for (auto &nid : nid_buffer) {
      // notify client here
      auto dt = t - nid.tx_start_;
      latency += dt;
      if (dt < min_latency) min_latency = dt;
      if (dt > max_latency) max_latency = dt;
    }
    auto n = nid_buffer.size();
#if NOTIFIER_THREAD
    latency_ += latency;
    count_ += n;
    cv_enq_.notify_one();
#else
    asm volatile("":: : "memory");
    __atomic_fetch_add(&latency_, latency, __ATOMIC_ACQ_REL);
    __atomic_fetch_add(&count_, n, __ATOMIC_ACQ_REL);
    asm volatile("":: : "memory");
#endif
    latency_log_.emplace_back(std::array<std::uint64_t,5>{t-start_clock_,n,latency/n,min_latency,max_latency});
    nid_buffer.clear();
  }
}

#if NOTIFIER_THREAD
void Notifier::worker() {
  while(!(quit_ && buffer_.empty())) {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_deq_.wait(lock, [this]{return quit_ || !buffer_.empty();});
    make_durable(buffer_, quit_);
  }
}
#endif

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
}
#else
void Notifier::push(std::vector<NotificationId> &nid_buffer, bool quit) {
  make_durable(nid_buffer, quit);
}
#endif

void Notifier::join() {
#if NOTIFIER_THREAD
  thread_.join();
  std::lock_guard<std::mutex> lock(mutex_);
  joined_ = true;
  cv_finish_.notify_all();
#endif
}

void Notifier::logger_end(Logger *logger) {
  std::unique_lock<std::mutex> lock(mutex_);
  max_buffers_ += logger->max_buffers_;
  nid_count_ += logger->nid_count_;
  byte_count_ += logger->byte_count_;
  write_latency_ += logger->write_latency_;
  wait_latency_ += logger->wait_latency_;
  throughput_ += (double)logger->byte_count_/logger->write_latency_;
  if (write_start_ > logger->write_start_) write_start_ = logger->write_start_;
  if (write_end_ < logger->write_end_) write_end_ = logger->write_end_;
  logger_set_.erase(logger);
#if NOTIFIER_THREAD
  if (logger_set_.empty()) {
    quit_ = true;
    cv_deq_.notify_all();
  }
  cv_finish_.wait(lock, [this]{return joined_;});
#endif
}

void Notifier::display() {
  double cps = FLAGS_clocks_per_us*1e6;
  size_t n = FLAGS_logger_num;
  std::cout<<"mean_max_buffers:\t" << max_buffers_/n << endl;
  std::cout<<"nid_count:\t" << nid_count_<< endl;
  std::cout<<"wait_time[s]:\t" << wait_latency_/cps << endl;
  std::cout<<"write_time[s]:\t" << write_latency_/cps << endl;
  std::cout<<"byte_count[B]:\t" << byte_count_<< endl;
  std::cout<<"throughput(thread_sum)[B/s]:\t" << throughput_*cps << endl;
  std::cout<<"throughput(byte_sum)[B/s]:\t" << cps*byte_count_/write_latency_*n << endl;
  std::cout<<"throughput(elap)[B/s]:\t" << cps*byte_count_/(write_end_-write_start_) << endl;
  double t = (double)latency_ / FLAGS_clocks_per_us / count_ / 1000;
  std::cout << std::fixed << std::setprecision(4)
            << "durable_latency[ms]:\t" << t << endl;
  std::cout << "durable_count:\t" << count_ << endl;
  uint64_t d = __atomic_load_n(&(DurableEpoch.obj_), __ATOMIC_ACQUIRE);
  std::cout << "durable_epoch:\t" << d << endl;
  //std::cout << "buffer_.size():\t\t" << buffer_.size() << endl;
  std::ofstream o("latency.dat");
  o << "time,n,mean_latency,min_latency,max_latency" << std::endl;
  for (auto x : latency_log_) {
    o << x[0]/cps << "," << x[1] << "," << x[2]/cps << "," << x[3]/cps<< "," << x[4]/cps <<std::endl;
  }
  o.close();
}
#endif
