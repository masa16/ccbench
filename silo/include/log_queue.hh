#pragma once
#include <mutex>
#include <condition_variable>
#include <queue>

class LogBuffer;

class LogQueue {
private:
  std::mutex mutex_;
  std::condition_variable cv_enq_;
  std::condition_variable cv_deq_;
  std::queue<LogBuffer*> queue_;
  std::size_t capacity_ = 1000;
  bool quit_ = false;

public:

  void enq(LogBuffer* x) {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_enq_.wait(lock, [this]{return queue_.size() < capacity_;});
    queue_.push(x);
    cv_deq_.notify_one();
  }

  bool wait_deq() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_deq_.wait(lock, [this]{return quit_ || !queue_.empty();});
    return !(quit_ && queue_.empty());
  }

  LogBuffer *deq() {
    std::unique_lock<std::mutex> lock(mutex_);
    auto ret = queue_.front();
    queue_.pop();
    cv_enq_.notify_one();
    return ret;
  }

  bool empty() {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.empty();
  }

  void terminate() {
    std::lock_guard<std::mutex> lock(mutex_);
    quit_ = true;
    cv_deq_.notify_all();
  }
};
