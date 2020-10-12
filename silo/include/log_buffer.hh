#pragma once
#include <array>
#include <queue>
#include <condition_variable>
#include "../../include/fileio.hh"
#include "tuple.hh"
#include "silo_op_element.hh"
#include "log.hh"

#define LOG_BUFFER_SIZE (512000/sizeof(LogRecord))
#define NID_BUFFER_SIZE (51200/sizeof(LogRecord))
#define BUFFER_NUM (268435456/LOG_BUFFER_SIZE/4)

class LogQueue;
class NotificationId;
class Notifier;
class LogBufferPool;

class LogBuffer {
private:
  std::vector<LogRecord> log_set_;
  std::vector<NotificationId> nid_set_;
  LogBufferPool &pool_;
public:
  void push(std::uint64_t tid, NotificationId nid,
            std::vector<WriteElement<Tuple>> &write_set,
            char *val, bool new_epoch_begins);
  size_t write(File &logfile, std::vector<NotificationId> &nid_buffer);
  bool empty();

  LogBuffer(LogBufferPool &pool) : pool_(pool) {
    log_set_.reserve(LOG_BUFFER_SIZE);
    nid_set_.reserve(NID_BUFFER_SIZE);
  }
};

class LogBufferPool {
public:
  LogQueue *queue_;
  std::mutex mutex_;
  std::condition_variable cv_deq_;
  std::vector<LogBuffer> buffer_;
  std::vector<LogBuffer*> pool_;
  LogBuffer *current_buffer_;
  bool quit_ = false;

  void publish();
  void return_buffer(LogBuffer *lb);
  void terminate();

  LogBufferPool() {
    printf("BUFFER_NUM=%lu\n",BUFFER_NUM);
    for (int i=0; i<BUFFER_NUM; i++) {
      buffer_.emplace_back(*this);
      pool_.push_back(&buffer_[i]);
    }
    current_buffer_ = pool_.back();
    pool_.pop_back();
  }
};
