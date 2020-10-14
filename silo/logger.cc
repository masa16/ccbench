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
  LogBufferPool &pool = std::ref(trans.log_buffer_pool_);
  pool.queue_ = &queue_;
  std::lock_guard<std::mutex> lock(mutex_);
  log_buffer_pool_map_[trans.thid_] = &pool;
  thid_vec_.emplace_back(trans.thid_);
  thid_set_.emplace(trans.thid_);
}

void Logger::logging(bool quit) {
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

  // write log
  while(!queue_.empty()) {
    auto sz = queue_.size();
    if (max_buffers_ < sz) max_buffers_ = sz;
    auto log_buffer = queue_.deq();
    counter_ += log_buffer->write(logfile_, nid_buffer_);
  }
  logfile_.fsync();
  //usleep(1000000); // increase latency

  // publish Durable Epoch of this thread
  auto new_dl = tid.epoch - 1;
  auto old_dl = ThLocalDurableEpoch[thid_].obj_;
  if (new_dl != old_dl || quit) {
    asm volatile("":: : "memory");
    __atomic_store_n(&(ThLocalDurableEpoch[thid_].obj_), new_dl, __ATOMIC_RELEASE);
    asm volatile("":: : "memory");
    // rotate logfile
    if (new_dl >= rotate_epoch_ + 100)
      rotate_logfile(new_dl);
    notifier_.push(nid_buffer_, quit);
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
  logfile_.open(logpath_, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  logfile_.ftruncate(10 ^ 9);

  while(queue_.wait_deq()) logging(false);
  logging(true);
  show_result();
}

// log rotation is described in SiloR paper
void Logger::rotate_logfile(uint64_t epoch) {
  // close
  logfile_.close();
  // rename
  std::string path = logdir_ + "/old_data." + std::to_string(epoch);
  if (::rename(logpath_.c_str(), path.c_str())) ERR;
  rotate_epoch_ = epoch;
  // open
  logfile_.open(logpath_, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  logfile_.ftruncate(10 ^ 9);
}

void Logger::finish_txn(int thid) {
  std::unique_lock<std::mutex> lock(mutex_);
  thid_set_.erase(thid);
  if (thid_set_.empty()) {
    queue_.terminate();
  }
  cv_finish_.wait(lock, [this]{return joined_;});
}

void Logger::thread_end() {
  std::lock_guard<std::mutex> lock(mutex_);
  joined_ = true;
  cv_finish_.notify_all();
}

void Logger::show_result() {
  cout << "Logger#"<<thid_<<":\tmax_buffers:\t" << max_buffers_ << endl;
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
