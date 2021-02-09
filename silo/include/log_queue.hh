#pragma once
#include <mutex>
#include <condition_variable>
#include <map>
#include <atomic>

class LogBuffer;

class LogQueue {
private:
  std::atomic<unsigned int> my_mutex_;
  std::mutex mutex_;
  std::condition_variable cv_deq_;
  std::map<uint64_t,std::vector<LogBuffer*>> queue_;
  std::size_t capacity_ = 1000;
  std::atomic<bool> quit_;
  std::chrono::microseconds timeout;

private:
  void my_lock() {
    for (;;) {
      unsigned int lock = 0;
      if (my_mutex_.compare_exchange_strong(lock, 1)) return;
      usleep(1);
    }
  }
  void my_unlock() {
    my_mutex_.store(0);
  }

public:
  LogQueue() {
    my_mutex_.store(0);
    quit_.store(false);
    timeout = std::chrono::microseconds((int)(FLAGS_epoch_time*1000));
  }

  void enq(LogBuffer* x) {
    my_lock();
    std::vector<LogBuffer*> &v = queue_[x->min_epoch_];
    v.emplace_back(x);
    cv_deq_.notify_one();
    my_unlock();
  }

  bool wait_deq() {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_deq_.wait_for(lock, timeout,
                            [this]{return quit_.load() || !queue_.empty();});
  }

  bool quit() {
    return quit_.load() && queue_.empty();
  }

  std::vector<LogBuffer*> deq() {
    my_lock();
#if DEQUEUE_MIN_EPOCH
    auto itr = queue_.cbegin();
    auto ret = itr->second;
    queue_.erase(itr);
#else
    std::vector<LogBuffer*> ret;
    ret.reserve(queue_.size());
    while (!queue_.empty()) {
      auto itr = queue_.cbegin();
      auto &v = itr->second;
      std::copy(v.begin(),v.end(),std::back_inserter(ret));
      queue_.erase(itr);
    }
#endif
    my_unlock();
    return ret;
  }

  bool empty() {
    return queue_.empty();
  }

  size_t size() {
    return queue_.size();
  }

  uint64_t min_epoch() {
    return queue_.cbegin()->first;
  }

  void terminate() {
    quit_.store(true);
  }
};
