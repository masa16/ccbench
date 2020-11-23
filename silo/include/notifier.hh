#pragma once
#include <utility>
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
  uint64_t tx_start_;
  uint64_t tx_end_ = 0;
  uint64_t tid_;

  NotificationId(uint64_t id, uint64_t thread_id, uint64_t tx_start) :
    id_(id), thread_id_(thread_id), tx_start_(tx_start) {}

  NotificationId() {NotificationId(0,0,0);}
};

class PepochFile {
private:
  std::string file_name_ = "pepoch";
  std::uint64_t *addr_;
  int fd_ = -1;
public:
  void open();
  void write(std::uint64_t epoch);
  void close();
  ~PepochFile() { if (fd_ != -1) close(); }
};

class Logger;

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
  std::size_t max_buffers_ = 0;
  std::size_t nid_count_ = 0;
  std::size_t byte_count_ = 0;
  std::size_t write_count_ = 0;
  std::size_t buffer_count_ = 0;
  std::uint64_t wait_latency_ = 0;
  std::uint64_t write_latency_ = 0;
  std::uint64_t write_start_ = ~(uint64_t)0;
  std::uint64_t write_end_ = 0;
  double throughput_ = 0;
  std::uint64_t start_clock_;
  std::vector<std::array<std::uint64_t,6>> latency_log_;
  bool quit_ = false;
  bool joined_ = false;
  std::unordered_set<Logger*> logger_set_;

public:
  PepochFile pepoch_file_;

  Notifier() {
    buffer_.reserve(65536);
    latency_log_.reserve(65536);
    start_clock_ = rdtscp();
    pepoch_file_.open();
  }
  void add_logger(Logger *logger);
  void make_durable(std::vector<NotificationId> &nid_buffer, bool quit);
  void worker();
  void run();
  void push(std::vector<NotificationId> &nid_buffer, bool quit);
  void join();
  void logger_end(Logger *logger);
  void display();
};
