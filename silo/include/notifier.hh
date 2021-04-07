#pragma once
#include <utility>
#include <vector>
#include <unordered_set>
#include <thread>
#include <mutex>
#include <condition_variable>
#include "../include/common.hh"
#include "stats.hh"

class NotificationId {
public:
  uint32_t id_;
  uint32_t thread_id_;
  uint64_t tx_start_;
  uint64_t t_mid_;
  uint64_t tid_;

  NotificationId(uint32_t id, uint32_t thread_id, uint64_t tx_start) :
    id_(id), thread_id_(thread_id), tx_start_(tx_start) {}

  NotificationId() {NotificationId(0,0,0);}

  std::uint64_t epoch() {
    Tidword tid;
    tid.obj_ = tid_;
    return tid.epoch;
  }
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

class NidBufferItem {
 public:
  std::uint64_t epoch_;
  std::vector<NotificationId> buffer_;
  NidBufferItem *next_ = NULL;
  NidBufferItem(std::uint64_t epoch) : epoch_(epoch) {
    buffer_.reserve(2097152);
  }
};


class NidBuffer {
private:
  std::size_t size_ = 0;
  std::uint64_t max_epoch_ = 0;
  std::mutex mutex_;
public:
  NidBufferItem *front_;
  NidBufferItem *end_;
  NidBuffer() {
    front_ = end_ = new NidBufferItem(0);
  }
  void store(std::vector<NotificationId> &nid_buffer, std::uint64_t epoch);
  void notify(std::uint64_t min_dl, NotifyStats &stats);
  std::size_t size() {return size_;}
  bool empty() {return size_ == 0;}
  std::uint64_t min_epoch() {
    return front_->epoch_;
  }
};


class Notifier {
private:
  std::thread thread_;
  std::mutex mutex_;
  std::condition_variable cv_enq_;
  std::condition_variable cv_deq_;
  std::condition_variable cv_finish_;
  std::size_t capa_ = 100000000;
  std::size_t push_size_ = 0;
  std::size_t max_buffers_ = 0;
  std::size_t byte_count_ = 0;
  std::size_t write_count_ = 0;
  std::size_t buffer_count_ = 0;
  std::uint64_t wait_latency_ = 0;
  std::uint64_t write_latency_ = 0;
  std::uint64_t write_start_ = ~(uint64_t)0;
  std::uint64_t write_end_ = 0;
  std::size_t try_count_ = 0;
  double throughput_ = 0;
  std::uint64_t start_clock_;
  std::vector<std::array<std::uint64_t,6>> latency_log_;
  std::vector<std::vector<std::uint64_t>> epoch_log_;
  bool quit_ = false;
  bool joined_ = false;
  std::unordered_set<Logger*> logger_set_;
  Stats depoch_diff_;
  NidStats nid_stats_;
  NotifyStats notify_stats_;

public:
  PepochFile pepoch_file_;
  NidBuffer buffer_;

  Notifier() {
    latency_log_.reserve(65536);
    start_clock_ = rdtscp();
    pepoch_file_.open();
  }
  void add_logger(Logger *logger);
  uint64_t check_durable();
  void make_durable(NidBuffer &buffer, bool quit);
  void temp_durable();
  void worker();
  void push(NidBuffer &nid_buffer, bool quit);
  void logger_end(Logger *logger);
  void display();
};
