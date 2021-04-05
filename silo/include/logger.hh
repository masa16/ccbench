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


class NotifyStats {
public:
  std::uint64_t latency_ = 0;
  std::uint64_t notify_latency_ = 0;
  std::uint64_t min_latency_ = ~(uint64_t)0;
  std::uint64_t max_latency_ = 0;
  std::size_t count_ = 0;
  std::size_t notify_count_ = 0;
  std::size_t epoch_count_ = 0;
  std::size_t epoch_diff_ = 0;
};


class LoggerResult {
public:
  // Logger
  std::size_t byte_count_ = 0;
  std::size_t write_count_ = 0;
  std::size_t buffer_count_ = 0;
  std::uint64_t wait_time_ = 0;
  std::uint64_t write_time_ = 0;
  std::uint64_t write_start_ = ~(uint64_t)0;
  std::uint64_t write_end_ = 0;
  Stats depoch_diff_;
  // Notifier
  std::size_t try_count_ = 0;
  double throughput_ = 0;
  // NidStats
  std::size_t nid_count_ = 0;
  std::uint64_t txn_latency_ = 0;
  std::uint64_t log_queue_latency_ = 0;
  std::uint64_t write_latency_ = 0;
  // NotifyStats
  std::uint64_t latency_ = 0;
  std::uint64_t notify_latency_ = 0;
  std::uint64_t min_latency_ = ~(uint64_t)0;
  std::uint64_t max_latency_ = 0;
  std::size_t count_ = 0;
  std::size_t notify_count_ = 0;
  std::size_t epoch_count_ = 0;
  std::size_t epoch_diff_ = 0;
  std::vector<std::array<std::uint64_t,7>> latency_log_;

  LoggerResult() {
    latency_log_.reserve(65536);
  }
  void add(LoggerResult &other);
  void add(NotifyStats &other, int thid, uint64_t min_dl);
  void display();
};


class Logger {
private:
  std::mutex mutex_;
  std::thread thread_;
  std::condition_variable cv_finish_;
  bool joined_ = false;
  std::size_t capacity_ = 1000;
  NidBuffer nid_buffer_;
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
  LoggerResult &res_;

  Logger(int thid, LoggerResult &res_) : thid_(thid), res_(res_) {
    //if (thid_ == 0) pepoch_file_.open();
  }
  ~Logger(){
    //if (thid_ == 0) pepoch_file_.close();
  }

  void add_txn_executor(TxnExecutor &trans);
  void worker();
  void wait_deq();
  void worker_end(int thid);
  void logger_end();
  std::uint64_t check_durable();
};
