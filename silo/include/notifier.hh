#pragma once
#include <vector>
#include <unordered_set>
#include <thread>
#include <mutex>
#include <condition_variable>
#include "../include/common.hh"

class NotificationId {
public:
  uint64_t id_;
  uint64_t thread_id_;
  uint64_t clock_;

  NotificationId(uint64_t id, uint64_t thread_id, uint64_t clock) :
    id_(id), thread_id_(thread_id), clock_(clock) {}

  NotificationId() {NotificationId(0,0,0);}
};

class Notifier {
private:
  std::thread thread_;
  std::mutex mutex_;
  std::condition_variable cv_enq_;
  std::condition_variable cv_deq_;
  std::condition_variable cv_finish_;
  std::vector<NotificationId> buffer_;
  std::size_t capa_ = 100000000;
  std::size_t count_ = 0;
  std::size_t latency_ = 0;
  std::size_t push_size_ = 0;
  bool quit_ = false;
  bool joined_ = false;
  std::unordered_set<int> thid_set_;

public:
  Notifier() {
    buffer_.reserve(65536);
  }
  void worker();
  void run(int thread_num);
  void push(std::vector<NotificationId> &nid_buffer, bool quit);
  void join();
  void display();
  void finish(int thid);
  void wait_for_join();
};
