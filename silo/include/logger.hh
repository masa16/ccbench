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
  uint64_t durable_epoch_ = 0;
  std::vector<TxnExecutor*> trans_set_;
  LogQueue queue_;
  File logfile_;

  void gen_logfile(int thid);
  void add_txn_executor(TxnExecutor *trans);
  void loop(const bool &quit);
};
