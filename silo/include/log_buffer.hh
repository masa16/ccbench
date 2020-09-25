#pragma once
#include <queue>
#include <condition_variable>
#include "../../include/fileio.hh"
#include "log.hh"

#define LOGSET_SIZE 1000

class LogQueue;

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
  bool new_epoch_begins_ = false;
  LogQueue *queue_;

  void add(LogRecord &log);
  bool publish(bool new_epoch_begins);
  void write(File &logfile);
  //void recycle();
};
