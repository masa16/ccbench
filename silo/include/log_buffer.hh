#pragma once
#include <queue>
#include <condition_variable>
#include "../../include/fileio.hh"
#include "log.hh"

#define LOG_BUFFER_SIZE 10000
#define NID_BUFFER_SIZE 1000

class LogQueue;
class NotificationId;
class Notifier;

class LogBuffer {
private:
  std::vector<LogRecord> log_set_1;
  std::vector<LogRecord> log_set_2;
  std::vector<LogRecord> *local_log_set_ = &log_set_2;
  LogHeader log_header_1;
  LogHeader log_header_2;
  LogHeader *local_log_header_ = &log_header_2;
  std::vector<NotificationId> nid_set_1;
  std::vector<NotificationId> nid_set_2;
  std::vector<NotificationId> *local_nid_set_ = &nid_set_2;
public:
  std::vector<LogRecord> *log_set_ = &log_set_1;
  LogHeader *log_header_ = &log_header_1;
  std::vector<NotificationId> *nid_set_ = &nid_set_1;
  bool new_epoch_begins_ = false;
  LogQueue *queue_;

  void push(LogRecord &log);
  void push(NotificationId &nid);
  bool publish(bool new_epoch_begins);
  void write(File &logfile, std::vector<NotificationId> &nid_buffer);

  LogBuffer() {
    log_set_1.reserve(LOG_BUFFER_SIZE);
    log_set_2.reserve(LOG_BUFFER_SIZE);
    log_header_1.init();
    log_header_2.init();
    nid_set_1.reserve(NID_BUFFER_SIZE);
    nid_set_2.reserve(NID_BUFFER_SIZE);
  }
};
