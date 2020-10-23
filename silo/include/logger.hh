#include <mutex>
#include <condition_variable>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include "../../include/fileio.hh"
#include "log.hh"
#include "log_buffer.hh"
#include "log_queue.hh"
#include "transaction.hh"
#include "notifier.hh"

class LoggerNode {
public:
  int logger_cpu_;
  std::vector<int> worker_cpu_;

  LoggerNode() {}
  LoggerNode(std::string s) {
    unsigned x = 0;
    size_t i = 0;
    for (; i<s.size(); ++i) {
      auto c = s[i];
      if (c>='0' && c<='9') {
        x = x*10 + (c-'0');
      }
      else if (c==':') {
        logger_cpu_ = x;
        ++i;
        break;
      }
      else {
        std::cerr << "parse error" << std::endl;
        std::abort();
      }
    }
    x = 0;
    for (; i<s.size(); ++i) {
      auto c = s[i];
      if (c>='0' && c<='9') {
        x = x*10 + (c-'0');
        if (i == s.size()-1) {
          worker_cpu_.push_back(x);
        }
      }
      else if (c==',') {
        worker_cpu_.push_back(x);
        x = 0;
      }
      else {
        std::cerr << "parse error" << std::endl;
        std::abort();
      }
    }
  }
};


class LoggerAffinity {
public:
  std::vector<LoggerNode> nodes_;
  unsigned worker_num_=0;
  unsigned logger_num_=0;
  void init(std::string s);
  void init(unsigned worker_num, unsigned logger_num);
};


class Logger {
private:
  std::mutex mutex_;
  std::thread thread_;
  std::condition_variable cv_enq_;
  std::condition_variable cv_deq_;
  std::condition_variable cv_finish_;
  bool joined_ = false;
  std::size_t capacity_ = 1000;
  std::vector<NotificationId> nid_buffer_;
  unsigned int counter_=0;

  void logging(bool quit);
  void rotate_logfile(uint64_t epoch);
  void show_result();

public:
  int thid_;
  std::vector<int> thid_vec_;
  std::unordered_set<int> thid_set_;
  LogQueue queue_;
  File logfile_;
  std::string logdir_;
  std::string logpath_;
  std::uint64_t rotate_epoch_ = 100;
  Notifier &notifier_;
  std::unordered_map<int, LogBufferPool*> log_buffer_pool_map_;
  std::size_t max_buffers_ = 0;
  std::size_t nid_count_ = 0;
  std::size_t byte_count_ = 0;
  std::size_t write_count_ = 0;
  std::size_t buffer_count_ = 0;
  std::uint64_t wait_latency_ = 0;
  std::uint64_t write_latency_ = 0;
  std::uint64_t write_start_ = 0;
  std::uint64_t write_end_ = 0;

  Logger(int i, Notifier &n) : thid_(i), notifier_(n) {}

  void add_txn_executor(TxnExecutor &trans);
  void worker();
  void worker_end(int thid);
  void logger_end();
};
