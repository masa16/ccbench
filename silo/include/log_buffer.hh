#pragma once
#include <array>
#include <queue>
#include <condition_variable>
#include "../../include/fileio.hh"
#include "tuple.hh"
#include "silo_op_element.hh"
#include "log.hh"

#define LOG_BUFFER_SIZE 10000
#define NID_BUFFER_SIZE 1000

class LogQueue;
class NotificationId;
class Notifier;

class LogBuffer {
private:
  LogHeader log_header_;
  std::array<std::vector<LogRecord>,2> log_set_;
  std::array<std::vector<NotificationId>,2> nid_set_;
public:
  bool new_epoch_begins_ = false;
  LogQueue *queue_;
  int id_writing_ = 0;
  int id_public_ = 1;

  void push(uint64_t tid, unsigned int key, char *val);
  void push(NotificationId &nid);
  bool publish(bool new_epoch_begins);
  void write(File &logfile, std::vector<NotificationId> &nid_buffer);
  void terminate();
  size_t nid_set_size();

  LogBuffer() {
    log_set_[0].reserve(LOG_BUFFER_SIZE);
    log_set_[1].reserve(LOG_BUFFER_SIZE);
    log_header_.init();
    nid_set_[0].reserve(NID_BUFFER_SIZE);
    nid_set_[1].reserve(NID_BUFFER_SIZE);
  }
};
