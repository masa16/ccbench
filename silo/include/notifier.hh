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


class LoggerResult;

class NidBuffer {
private:
  std::size_t size_ = 0;
  std::uint64_t max_epoch_ = 0;
public:
  NidBufferItem *front_;
  NidBufferItem *end_;
  NidBuffer() {
    front_ = end_ = new NidBufferItem(0);
  }
  void store(std::vector<NotificationId> &nid_buffer, std::uint64_t epoch);
  void notify(std::uint64_t min_dl, LoggerResult &stats);
  std::size_t size() {return size_;}
  bool empty() {return size_ == 0;}
};
