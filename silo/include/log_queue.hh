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
  std::condition_variable cv_enq_;
  std::condition_variable cv_deq_;
  std::multimap<uint64_t,LogBuffer*> queue_;
  std::size_t capacity_ = 1000;
  bool quit_ = false;

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
  LogQueue() {my_mutex_.store(0);}

  void enq(LogBuffer* x) {
    my_lock();
    queue_.emplace(x->min_epoch_, x);
    cv_deq_.notify_one();
    my_unlock();
  }

  bool wait_deq() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_deq_.wait(lock, [this]{return quit_ || !queue_.empty();});
    return !(quit_ && queue_.empty());
  }

  LogBuffer *deq() {
    my_lock();
    auto itr = queue_.cbegin();
    auto ret = itr->second;
    queue_.erase(itr);
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
    std::lock_guard<std::mutex> lock(mutex_);
    quit_ = true;
    cv_deq_.notify_all();
  }
};
