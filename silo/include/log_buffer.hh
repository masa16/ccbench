#pragma once
#include <queue>
#include <condition_variable>
#include "../../include/fileio.hh"
#include "log.hh"

#define LOG_SIZE 10000

class LogQueue;
class NotificationId;
class Notifier;

class LogBuffer {
public:
  std::vector<LogRecord> log_set_1;
  std::vector<LogRecord> log_set_2;
  std::vector<LogRecord> *log_set_ = &log_set_1;
  std::vector<LogRecord> *local_log_set_ = &log_set_2;
  LogHeader log_header_1;
  LogHeader log_header_2;
  LogHeader *log_header_ = &log_header_1;
  LogHeader *local_log_header_ = &log_header_2;
  std::vector<NotificationId> nid_set_1;
  std::vector<NotificationId> nid_set_2;
  std::vector<NotificationId> *nid_set_ = &nid_set_1;
  std::vector<NotificationId> *local_nid_set_ = &nid_set_2;
  bool new_epoch_begins_ = false;
  LogQueue *queue_;

  void push(LogRecord &log);
  void push(NotificationId &nid);
  bool publish(bool new_epoch_begins);
  void write(File &logfile, std::vector<NotificationId> &nid_buffer);
  //void recycle();
};
