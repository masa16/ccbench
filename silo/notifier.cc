#if DURABLE_EPOCH
#include <iostream>
#include <fstream>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include "include/notifier.hh"
#include "include/logger.hh"

void PepochFile::open() {
  fd_ = ::open(file_name_.c_str(), O_CREAT|O_TRUNC|O_RDWR, 0644);
  if (fd_ == -1) {
    std::cerr << "open failed: " << file_name_ << std::endl;
    ERR;
  }
  std::uint64_t zero=0;
  auto sz = ::write(fd_, &zero, sizeof(std::uint64_t));
  if (sz == -1) {
    std::cerr << "write failed";
    ERR;
  }
  addr_ = (std::uint64_t*)::mmap(NULL, sizeof(std::uint64_t), PROT_WRITE, MAP_SHARED, fd_, 0);
  if (addr_ == MAP_FAILED) {
    std::cerr << "mmap failed";
    ERR;
  }
}

void PepochFile::write(std::uint64_t epoch) {
  *addr_ = epoch;
  ::msync(addr_, sizeof(std::uint64_t), MS_SYNC);
}

void PepochFile::close() {
  ::munmap(addr_, sizeof(std::uint64_t));
  ::close(fd_);
  fd_ = -1;
}

void Notifier::add_logger(Logger *logger) {
  std::unique_lock<std::mutex> lock(mutex_);
  logger_set_.emplace(logger);
}

void Notifier::make_durable(bool quit) {
  __atomic_fetch_add(&try_count_, 1, __ATOMIC_ACQ_REL);
  // calculate min(d_l)
  uint64_t min_dl = __atomic_load_n(&(ThLocalDurableEpoch[0].obj_), __ATOMIC_ACQUIRE);
  for (unsigned int i=1; i < FLAGS_logger_num; ++i) {
    uint64_t dl = __atomic_load_n(&(ThLocalDurableEpoch[i].obj_), __ATOMIC_ACQUIRE);
    if (dl < min_dl) {
      min_dl = dl;
    }
  }
  uint64_t d = __atomic_load_n(&(DurableEpoch.obj_), __ATOMIC_ACQUIRE);
  /*
  std::vector<uint64_t> v;
  v.emplace_back(d);
  for (int i=0; i<FLAGS_logger_num; ++i) {
    uint64_t x = __atomic_load_n(&(ThLocalDurableEpoch[i].obj_), __ATOMIC_ACQUIRE);
    v.emplace_back(x);
  }
  epoch_log_.emplace_back(v);
  */
  if (d >= min_dl && !quit) {
#if NOTIFIER_THREAD
    cv_enq_.notify_one();
#endif
    return;
  }
  // store Durable Epoch
  pepoch_file_.write(min_dl);
  asm volatile("":: : "memory");
  __atomic_store_n(&(DurableEpoch.obj_), min_dl, __ATOMIC_RELEASE);
  asm volatile("":: : "memory");
  // Durable Latency
  uint64_t epoch = (quit) ? (~(uint64_t)0) : min_dl;
  NotifyStats stats;
  // notify client
  buffer_.notify(epoch, stats);
#if NOTIFIER_THREAD
  cv_enq_.notify_one();
#endif
  if (stats.count_ > 0) {
    notify_stats_.add(stats);
    uint64_t t = rdtscp();
    latency_log_.emplace_back(
      std::array<std::uint64_t,6>{
        min_dl, t-start_clock_, stats.count_, stats.latency_/stats.count_,
          stats.min_latency_, stats.max_latency_,
      });
  }
}

#if NOTIFIER_THREAD
void Notifier::worker() {
  while(!(quit_ && buffer_.empty())) {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_deq_.wait(lock, [this]{return quit_ || !buffer_.empty();});
    make_durable(quit_);
  }
}
#endif

void set_cpu(std::thread &th, int cpu);

void Notifier::run() {
#if NOTIFIER_THREAD
  thread_ = std::thread([this]{worker();});
  if (FLAGS_notifier_cpu >= 0) {
    set_cpu(thread_,FLAGS_notifier_cpu);
  }
#endif
}

void NidBuffer::notify(std::uint64_t min_dl, NotifyStats &stats) {
  if (front_ == NULL) return;
  stats.notify_count_ ++;
  NidBufferItem *orig_front = front_;
  while (front_->epoch_ <= min_dl) {
    //printf("front_->epoch_=%lu min_dl=%lu\n",front_->epoch_,min_dl);
    std::uint64_t ltc = 0;
    std::uint64_t ntf_ltc = 0;
    std::uint64_t min_ltc = ~(uint64_t)0;
    std::uint64_t max_ltc = 0;
    std::uint64_t t = rdtscp();
    for (auto &nid : front_->buffer_) {
      // notify client here
      std::uint64_t dt = t - nid.tx_start_;
      ltc += dt;
      if (dt < min_ltc) min_ltc = dt;
      if (dt > max_ltc) max_ltc = dt;
      ntf_ltc += t - nid.t_mid_;
    }
    stats.latency_ += ltc;
    stats.notify_latency_ += ntf_ltc;
    if (min_ltc < stats.min_latency_) stats.min_latency_ = min_ltc;
    if (max_ltc > stats.max_latency_) stats.max_latency_ = max_ltc;
    if (min_dl != ~(uint64_t)0)
      stats.epoch_diff_ += min_dl - front_->epoch_;
    stats.epoch_count_ ++;
    std::size_t n = front_->buffer_.size();
    stats.count_ += n;
    // clear buffer
    size_ -= n;
    front_->buffer_.clear();
    if (front_->next_ == NULL) break;
    //if (end_->epoch_ > max_epoch_+4) {
    if (false) {
      // release buffer
      //printf("delete front_=%lx front_->next_=%lx front_->epoch_=%lu end_->epoch_=%lu max_epoch_=%lu min_dl=%lu\n",(uint64_t)front_,(uint64_t)front_->next_,front_->epoch_,end_->epoch_,max_epoch_,min_dl);
      NidBufferItem *old_front = front_;
      front_ = front_->next_;
      delete old_front;
    } else {
      // recycle buffer
      front_->epoch_ = end_->epoch_ + 1;
      end_->next_ = front_;
      end_ = front_;
      front_ = front_->next_;
      end_->next_ = NULL;
    }
    if (front_ == orig_front) break;
  }
}

void NidBuffer::store(std::vector<NotificationId> &nid_buffer) {
  for (auto &nid : nid_buffer) {
    auto epoch = nid.epoch();
    NidBufferItem *itr = front_;
    while (itr->epoch_ < epoch) {
      if (itr->next_ == NULL) {
        // create buffer
        itr->next_ = end_ = new NidBufferItem(epoch);
        if (max_epoch_ < epoch) max_epoch_ = epoch;
        //printf("create end_=%lx end_->next_=%lx end_->epoch_=%lu epoch=%lu max_epoch_=%lu\n",(uint64_t)end_,(uint64_t)end_->next_, end_->epoch_, epoch, max_epoch_);
      }
      itr = itr->next_;
    }
    assert(itr->epoch == epoch);
    itr->buffer_.emplace_back(nid);
  }
  size_ += nid_buffer.size();
  nid_buffer.clear();
}

#if NOTIFIER_THREAD
void Notifier::push(std::vector<NotificationId> &nid_buffer, bool quit) {
  std::unique_lock<std::mutex> lock(mutex_);
  push_size_ = nid_buffer.size();
  cv_enq_.wait(lock, [this]{return buffer_.size() + push_size_ <= capa_;});
  push_size_ = 0;
  buffer_.store(nid_buffer);
  cv_deq_.notify_one();
}
#else
void Notifier::push(std::vector<NotificationId> &nid_buffer, bool quit) {
  std::unique_lock<std::mutex> lock(mutex_);
  //if (buffer_.size() + nid_buffer.size() > capa_) return;
  buffer_.store(nid_buffer);
  make_durable(quit);
}
#endif

void Notifier::temp_durable() {
  std::unique_lock<std::mutex> lock(mutex_);
  make_durable(false);
}

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
  nid_stats_.add(logger->nid_stats_);
  byte_count_ += logger->byte_count_;
  write_count_ += logger->write_count_;
  buffer_count_ += logger->buffer_count_;
  write_latency_ += logger->write_latency_;
  wait_latency_ += logger->wait_latency_;
  depoch_diff_.unify(logger->depoch_diff_);
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
  std::cout<<"wait_time[s]:\t" << wait_latency_/cps << endl;
  std::cout<<"write_time[s]:\t" << write_latency_/cps << endl;
  std::cout<<"write_count:\t" << write_count_<< endl;
  std::cout<<"byte_count[B]:\t" << byte_count_<< endl;
  std::cout<<"buffer_count:\t" << buffer_count_<< endl;
  depoch_diff_.display("depoch_diff");
  std::cout<<"throughput(thread_sum)[B/s]:\t" << throughput_*cps << endl;
  std::cout<<"throughput(byte_sum)[B/s]:\t" << cps*byte_count_/write_latency_*n << endl;
  std::cout<<"throughput(elap)[B/s]:\t" << cps*byte_count_/(write_end_-write_start_) << endl;
  nid_stats_.display();
  notify_stats_.display();
  std::cout << "try_count:\t" << try_count_ << endl;
  uint64_t d = __atomic_load_n(&(DurableEpoch.obj_), __ATOMIC_ACQUIRE);
  std::cout << "durable_epoch:\t" << d << endl;
  std::ofstream o("latency.dat");
  o << "epoch,time,count,mean_latency,min_latency,max_latency" << std::endl;
  for (auto x : latency_log_) {
    o << x[0] << "," << x[1]/cps << "," << x[2] << "," << x[3]/cps << "," << x[4]/cps<< "," << x[5]/cps <<std::endl;
  }
  o.close();
  /*
  std::ofstream q("epoch.dat");
  for (auto v : epoch_log_) {
    auto itr = v.begin();
    q << *itr;
    while (++itr != v.end()) {
      q << "," << *itr;
    }
    q << std::endl;
  }
  q.close();
  */
}
#endif
