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
  std::vector<LogRecord> *_log_set_ = &log_set_2;
  LogHeader log_header_1;
  LogHeader log_header_2;
  LogHeader *log_header_ = &log_header_1;
  LogHeader *_log_header_ = &log_header_2;
  LogQueue  *queue_;

  void add(LogRecord &log);
  void publish();
  void write(File &logfile);
  //void recycle();
};
