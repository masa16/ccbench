#include <mutex>
#include <condition_variable>
#include <queue>
#include "../../include/fileio.hh"
#include "log.hh"
#include "log_buffer.hh"
#include "log_queue.hh"
#include "transaction.hh"

class Logger {
private:
  std::mutex mutex_;
  std::condition_variable cv_enq_;
  std::condition_variable cv_deq_;
  std::size_t capacity_ = 1000;
public:
  int thid_;
  std::vector<int> thid_set_;
  LogQueue queue_;
  File logfile_;

  Logger(int i) : thid_(i) {}

  void gen_logfile();
  void add_txn_executor(TxnExecutor *trans);
  void loop();
  void terminate();
};
