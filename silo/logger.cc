#if DURABLE_EPOCH
//#define Linux 1
#include <iostream>
#include <fstream>
#include "include/logger.hh"
#include <sys/stat.h>
#include <sys/types.h>

extern void genLogFile(std::string &logpath, const int thid);

void LoggerAffinity::init(std::string s) {
  size_t start=0, pos, len;
  worker_num_ = 0;
  logger_num_ = 0;
  while (start < s.size()) {
    pos = s.find("+",start);
    if (pos == std::string::npos)
      len = s.size() - start;
    else
      len = pos - start;
    if (len>0) {
      nodes_.emplace_back(s.substr(start,len));
      worker_num_ += nodes_.back().worker_cpu_.size();
      logger_num_ += 1;
    }
    if (pos == std::string::npos)
      break;
    start = pos + 1;
  }
}

void LoggerAffinity::init(unsigned worker_num, unsigned logger_num) {
  unsigned num_cpus = std::thread::hardware_concurrency();
  if (logger_num > num_cpus || worker_num > num_cpus) {
    std::cerr << "too many threads" << std::endl;
  }
  worker_num_ = worker_num;
  logger_num_ = logger_num;
  for (unsigned i=0; i<logger_num; i++) {
    nodes_.emplace_back();
  }
  unsigned thread_num = logger_num + worker_num;
  if (thread_num > num_cpus) {
    for (unsigned i=0; i<worker_num; i++) {
      nodes_[i*logger_num/worker_num].worker_cpu_.emplace_back(i);
    }
    for (unsigned i=0; i<logger_num; i++) {
      nodes_[i].logger_cpu_ = nodes_[i].worker_cpu_.back();
    }
  } else {
    for (unsigned i=0; i<thread_num; i++) {
      nodes_[i*logger_num/thread_num].worker_cpu_.emplace_back(i);
    }
    for (unsigned i=0; i<logger_num; i++) {
      nodes_[i].logger_cpu_ = nodes_[i].worker_cpu_.back();
      nodes_[i].worker_cpu_.pop_back();
    }
  }
}

void Logger::add_txn_executor(TxnExecutor &trans) {
  trans.logger_thid_ = thid_;
  LogBufferPool &pool = std::ref(trans.log_buffer_pool_);
  pool.queue_ = &queue_;
  std::lock_guard<std::mutex> lock(mutex_);
  thid_vec_.emplace_back(trans.thid_);
  thid_set_.emplace(trans.thid_);
}

void Logger::logging(bool quit) {
  if (queue_.empty()) {
    if (quit) {
      NotifyStats stats;
      nid_buffer_.notify(~(uint64_t)0, stats);
      std::uint64_t d = __atomic_load_n(&(DurableEpoch.obj_), __ATOMIC_ACQUIRE);
      res_.add(stats, thid_, d);
    }
    return;
  }
  //if (max_buffers_ < q_size) max_buffers_ = q_size;
  //if (q_size > thid_vec_.size()) q_size = thid_vec_.size();
  // calculate min(ctid_w)
  uint64_t min_ctid = ~(uint64_t)0;
  for (auto itr : thid_vec_) {
    auto ctid = __atomic_load_n(&(CTIDW[itr].obj_), __ATOMIC_ACQUIRE);
    if (ctid > 0 && ctid < min_ctid) {
      min_ctid = ctid;
    }
  }
  // check
  Tidword tid;
  tid.obj_ = min_ctid;
  if (tid.epoch == 0 || min_ctid == ~(uint64_t)0) return;

  // min_epoch
  std::uint64_t min_epoch = queue_.min_epoch();
  if (min_epoch > tid.epoch) min_epoch = tid.epoch;
  // compare with durable epoch
  std::uint64_t d = __atomic_load_n(&(DurableEpoch.obj_), __ATOMIC_ACQUIRE);
  res_.depoch_diff_.sample(min_epoch - d);

  // write log
  std::uint64_t max_epoch = 0;
  std::uint64_t t = rdtscp();
  if (res_.write_start_==0) res_.write_start_ = t;
  std::vector<LogBuffer*> log_buffer_vec = queue_.deq();
  size_t buffer_num = log_buffer_vec.size();
  for (LogBuffer *log_buffer : log_buffer_vec) {
    std::uint64_t deq_time = rdtscp();
    if (log_buffer->max_epoch_ > max_epoch)
      max_epoch = log_buffer->max_epoch_;
    log_buffer->write(logfile_, res_.byte_count_);
    log_buffer->pass_nid(nid_buffer_, res_, deq_time);
    log_buffer->return_buffer();
  }
#ifdef Linux
  logfile_.fdatasync();
#else
  logfile_.fsync();
#endif
  //usleep(1000000); // increase latency
  res_.write_end_ = rdtscp();
  res_.write_time_ += res_.write_end_ - t;
  res_.write_count_++;
  res_.buffer_count_ += buffer_num;

  // publish Durable Epoch of this thread
  auto new_dl = min_epoch - 1;
  auto old_dl = __atomic_load_n(&(ThLocalDurableEpoch[thid_].obj_), __ATOMIC_ACQUIRE);
  if (old_dl < new_dl) {
    asm volatile("":: : "memory");
    __atomic_store_n(&(ThLocalDurableEpoch[thid_].obj_), new_dl, __ATOMIC_RELEASE);
    asm volatile("":: : "memory");
    // // rotate logfile
    // if (max_epoch >= rotate_epoch_)
    //   rotate_logfile(max_epoch);
  }

  // notify persistency to client
  auto min_dl = check_durable();
  if (nid_buffer_.min_epoch() <= min_dl) {
    //if (thid_ == 0)
    //  printf("old_dl=%lu new_dl=%lu min_dl=%lu size()=%lu min_epoch()=%lu\n",
    //         old_dl,new_dl,min_dl,nid_buffer_.size(),nid_buffer_.min_epoch());
    NotifyStats stats;
    nid_buffer_.notify(min_dl, stats);
    res_.add(stats, thid_, min_dl);
  }
}


uint64_t Logger::check_durable() {
  static std::mutex smutex;
  static PepochFile pepoch_file;
  res_.try_count_ += 1;
  std::unique_lock<std::mutex> lock(smutex);
  uint64_t d = __atomic_load_n(&(DurableEpoch.obj_), __ATOMIC_ACQUIRE);
  //if (thid_ != 0) {
  //  return d;
  //}
  // calculate min(d_l)
  uint64_t min_dl = __atomic_load_n(&(ThLocalDurableEpoch[0].obj_), __ATOMIC_ACQUIRE);
  for (unsigned int i=1; i < FLAGS_logger_num; ++i) {
    uint64_t dl = __atomic_load_n(&(ThLocalDurableEpoch[i].obj_), __ATOMIC_ACQUIRE);
    if (dl < min_dl) {
      min_dl = dl;
    }
  }
  if (d < min_dl) {
    // store Durable Epoch
    pepoch_file.open();
    pepoch_file.write(min_dl);
    pepoch_file.close();
    asm volatile("":: : "memory");
    __atomic_store_n(&(DurableEpoch.obj_), min_dl, __ATOMIC_RELEASE);
    asm volatile("":: : "memory");
  }
  return min_dl;
}

void Logger::wait_deq() {
  while (!queue_.wait_deq()) {
    // calculate min(ctid_w)
    uint64_t min_ctid = ~(uint64_t)0;
    for (auto itr : thid_vec_) {
      auto ctid = __atomic_load_n(&(CTIDW[itr].obj_), __ATOMIC_ACQUIRE);
      if (ctid > 0 && ctid < min_ctid) {
        min_ctid = ctid;
      }
    }
    // min_epoch
    Tidword tid;
    tid.obj_ = min_ctid;
    if (tid.epoch == 0 || min_ctid == ~(uint64_t)0) continue;
    auto new_dl = tid.epoch - 1;
    auto old_dl = __atomic_load_n(&(ThLocalDurableEpoch[thid_].obj_), __ATOMIC_ACQUIRE);
    if (new_dl > old_dl) {
      asm volatile("":: : "memory");
      __atomic_store_n(&(ThLocalDurableEpoch[thid_].obj_), new_dl, __ATOMIC_RELEASE);
      asm volatile("":: : "memory");
    }
  }
}

void Logger::worker() {
  logdir_ = "log" + std::to_string(thid_);
  struct stat statbuf;
  if (::stat(logdir_.c_str(), &statbuf)) {
    if (::mkdir(logdir_.c_str(), 0755)) ERR;
  } else {
    if ((statbuf.st_mode & S_IFMT) != S_IFDIR) ERR;
  }
  logpath_ = logdir_ + "/data.log";
  logfile_.open(logpath_, O_CREAT | O_WRONLY, 0644);
  //logfile_.open(logpath_, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  //logfile_.ftruncate(10 ^ 9);

  for (;;) {
    std::uint64_t t = rdtscp();
    wait_deq();
    if (queue_.quit()) break;
    res_.wait_time_ += rdtscp() - t;
    logging(false);
  }
  logging(true);
  // ending
  logger_end();
  show_result();
}

// log rotation is described in SiloR paper
void Logger::rotate_logfile(uint64_t epoch) {
  // close
  logfile_.close();
  // rename
  std::string path = logdir_ + "/old_data." + std::to_string(epoch);
  if (::rename(logpath_.c_str(), path.c_str())) ERR;
  rotate_epoch_ = (epoch+100)/100*100;
  // open
  logfile_.open(logpath_, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  logfile_.ftruncate(10 ^ 9);
}

void Logger::worker_end(int thid) {
  std::unique_lock<std::mutex> lock(mutex_);
  thid_set_.erase(thid);
  if (thid_set_.empty()) {
    queue_.terminate();
  }
  cv_finish_.wait(lock, [this]{return joined_;});
}

void Logger::logger_end() {
  std::lock_guard<std::mutex> lock(mutex_);
  joined_ = true;
  cv_finish_.notify_all();
}

void Logger::show_result() {
  double cps = FLAGS_clocks_per_us*1e6;
  static std::mutex mtx;
  std::lock_guard<std::mutex> lock(mtx);
  cout << "Logger#"<<thid_
       <<" byte_count[B]=" << res_.byte_count_
       <<" write_latency[s]=" << res_.write_time_/cps
       <<" throughput[B/s]=" << res_.byte_count_/(res_.write_time_/cps)
       <<" wait_latency[s]=" << res_.wait_time_/cps
       << endl;
#if 0
  for(auto itr : log_buffer_map_)
    cout<<itr.first
        <<" : nid_set_.size="<<itr.second->nid_set_->size()
        <<", local_nid_set_.size="<<itr.second->local_nid_set_->size()
        <<", nid_buffer_.size="<<nid_buffer_.size()
        <<endl;
  cout<<"counter="<<counter_<<endl;
#endif
}


void LoggerResult::add(NotifyStats &other, int thid, uint64_t min_dl) {
  if (other.count_ == 0) return;
  latency_        += other.latency_;
  notify_latency_ += other.notify_latency_;
  count_          += other.count_;
  notify_count_   += other.notify_count_;
  epoch_count_    += other.epoch_count_;
  epoch_diff_     += other.epoch_diff_;
  uint64_t t = rdtscp();
  latency_log_.emplace_back(
    std::array<std::uint64_t,7>{
      min_dl, t-StartClock, (uint64_t)thid, other.count_, other.latency_/other.count_,
        other.min_latency_, other.max_latency_,
    });
}

void LoggerResult::add(LoggerResult &other) {
  // Logger
  byte_count_   += other.byte_count_;
  write_count_  += other.write_count_;
  buffer_count_ += other.buffer_count_;
  write_time_   += other.write_time_;
  wait_time_    += other.wait_time_;
  throughput_   += (double)other.byte_count_/other.write_time_;
  depoch_diff_.unify(other.depoch_diff_);
  if (write_start_ > other.write_start_) write_start_ = other.write_start_;
  if (write_end_ < other.write_end_) write_end_ = other.write_end_;
  try_count_ += other.try_count_;
  // NidStats
  nid_count_         += other.nid_count_;
  txn_latency_       += other.txn_latency_;
  log_queue_latency_ += other.log_queue_latency_;
  write_latency_     += other.write_latency_;
  // NotifyStats
  latency_        += other.latency_;
  notify_latency_ += other.notify_latency_;
  count_          += other.count_;
  notify_count_   += other.notify_count_;
  epoch_count_    += other.epoch_count_;
  epoch_diff_     += other.epoch_diff_;
  std::copy(other.latency_log_.begin(), other.latency_log_.end(),
            std::back_inserter(latency_log_));
  std::sort(latency_log_.begin(), latency_log_.end(),
            [](auto a, auto b) {
              return a[1] < b[1];
            });
}

void LoggerResult::display() {
  double cpms = FLAGS_clocks_per_us*1e3;
  double cps = FLAGS_clocks_per_us*1e6;
  size_t n = FLAGS_logger_num;
  // Logger
  std::cout << "wait_time[s]:\t" << wait_time_/cps << endl;
  std::cout << "write_time[s]:\t" << write_time_/cps << endl;
  std::cout << "write_count:\t" << write_count_<< endl;
  std::cout << "byte_count[B]:\t" << byte_count_<< endl;
  std::cout << "buffer_count:\t" << buffer_count_<< endl;
  depoch_diff_.display("depoch_diff");
  std::cout << "throughput(thread_sum)[B/s]:\t" << throughput_*cps << endl;
  std::cout << "throughput(byte_sum)[B/s]:\t" << cps*byte_count_/write_time_*n << endl;
  std::cout << "throughput(elap)[B/s]:\t" << cps*byte_count_/(write_end_-write_start_) << endl;
  // NidStats
  std::cout << "nid_count:\t" << nid_count_ << endl;
  std::cout << std::fixed << std::setprecision(4)
            << "txn_latency[ms]:\t" << txn_latency_/cpms/nid_count_ << endl;
  std::cout << std::fixed << std::setprecision(4)
            << "log_queue_latency[ms]:\t" << log_queue_latency_/cpms/nid_count_ << endl;
  std::cout << std::fixed << std::setprecision(4)
            << "write_latency[ms]:\t" << write_latency_/cpms/nid_count_ << endl;
  // NotifyStats
  std::cout << std::fixed << std::setprecision(4)
            << "notify_latency[ms]:\t" << notify_latency_/cpms/count_ << endl;
  std::cout << std::fixed << std::setprecision(4)
            << "durable_latency[ms]:\t" << latency_/cpms/count_ << endl;
  std::cout << "notify_count:\t" << notify_count_ << endl;
  std::cout << "durable_count:\t" << count_ << endl;
  std::cout << "mean_durable_count:\t" << (double)count_/notify_count_ << endl;
  std::cout << std::fixed << std::setprecision(2)
            << "mean_epoch_diff:\t" << (double)epoch_diff_/epoch_count_ << endl;

  std::cout << "try_count:\t" << try_count_ << endl;
  uint64_t d = __atomic_load_n(&(DurableEpoch.obj_), __ATOMIC_ACQUIRE);
  std::cout << "durable_epoch:\t" << d << endl;
  std::ofstream o("latency.dat");
  o << "epoch,time,thid,count,mean_latency,min_latency,max_latency" << std::endl;
  for (auto x : latency_log_) {
    o << x[0] << "," << x[1]/cps << "," << x[2] << "," << x[3] << "," << x[4]/cps << "," << x[5]/cps<< "," << x[6]/cps <<std::endl;
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
