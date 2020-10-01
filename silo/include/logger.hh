#include <mutex>
#include <condition_variable>
#include <queue>
#include <unordered_map>
#include "../../include/fileio.hh"
#include "log.hh"
#include "log_buffer.hh"
#include "log_queue.hh"
#include "transaction.hh"
#include "notifier.hh"

class Logger {
private:
  std::mutex mutex_;
  std::thread thread_;
  std::condition_variable cv_enq_;
  std::condition_variable cv_deq_;
  std::size_t capacity_ = 1000;
  std::vector<NotificationId> nid_buffer_;
  Notifier *notifier_;
  unsigned int counter_=0;
  void worker();

public:
  int thid_;
  std::vector<int> thid_set_;
  LogQueue queue_;
  File logfile_;
  std::unordered_map<int, LogBuffer*> log_buffer_map_;

  Logger(int i, Notifier *n) : thid_(i), notifier_(n) {}
  ~Logger() {for(auto itr : log_buffer_map_) delete itr.second;}

  void add_txn_executor(TxnExecutor *trans);
  void run();
  void logging(bool quit);
  void terminate();
  void join();
};
