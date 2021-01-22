#pragma once
#include <array>
#include <queue>
#include <condition_variable>
#include <numa.h>
#include "../../include/fileio.hh"
#include "tuple.hh"
#include "silo_op_element.hh"
#include "log.hh"
#include "common.hh"

#define LOG_BUFFER_SIZE (FLAGS_buffer_size*1024/sizeof(LogRecord))
#define LOG_ALLOC_SIZE (LOG_BUFFER_SIZE+512/sizeof(LogRecord)+1)
#define NID_BUFFER_SIZE (LOG_BUFFER_SIZE/4)

class LogQueue;
class NotificationId;
class Notifier;
class NidStats;
class LogBufferPool;

class LogBuffer {
private:
  LogRecord *log_set_;
  LogRecord *log_set_ptr_;
  size_t log_set_size_ = 0;
  std::vector<NotificationId> nid_set_;
  LogBufferPool &pool_;
public:
  uint64_t min_epoch_ = ~(uint64_t)0;
  uint64_t max_epoch_ = 0;
  void push(std::uint64_t tid, NotificationId &nid,
            std::vector<WriteElement<Tuple>> &write_set,
            char *val, bool new_epoch_begins);
  void write(File &logfile, size_t &byte_count);
  void pass_nid(std::vector<NotificationId> &nid_buffer,
                NidStats &nid_stats, std::uint64_t deq_time);
  void return_buffer();
  bool empty();

  LogBuffer(LogBufferPool &pool) : pool_(pool) {
    nid_set_.reserve(NID_BUFFER_SIZE);
    void *ptr = log_set_ptr_ = new LogRecord[LOG_ALLOC_SIZE];
    std::size_t space = LOG_ALLOC_SIZE*sizeof(LogRecord);
    std::align(512, LOG_BUFFER_SIZE*sizeof(LogRecord), ptr, space);
    if (space < LOG_BUFFER_SIZE*sizeof(LogRecord)) ERR;
    log_set_ = (LogRecord*)ptr;
  }
  ~LogBuffer() {
    delete log_set_ptr_;
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
  std::uint64_t txn_latency_ = 0;
  std::uint64_t bkpr_latency_ = 0;
  std::uint64_t publish_latency_ = 0;
  std::uint64_t publish_counts_ = 0;

  LogBufferPool() {
    struct bitmask *mask = numa_get_interleave_mask();
    numa_set_localalloc();
    buffer_.reserve(FLAGS_buffer_num);
    for (int i=0; i<FLAGS_buffer_num; i++) {
      buffer_.emplace_back(*this);
    }
    pool_.reserve(FLAGS_buffer_num);
    current_buffer_ = &buffer_[0];
    for (int i=1; i<FLAGS_buffer_num; i++) {
      pool_.push_back(&buffer_[i]);
    }
    numa_set_interleave_mask(mask);
  }
  void publish();
  void return_buffer(LogBuffer *lb);
  void terminate(Result &myres);
};
