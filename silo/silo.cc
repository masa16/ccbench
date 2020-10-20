#include <ctype.h>
#include <pthread.h>
#include <sched.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <cctype>

#include "boost/filesystem.hpp"

#define GLOBAL_VALUE_DEFINE

#include "include/atomic_tool.hh"
#include "include/common.hh"
#include "include/result.hh"
#include "include/transaction.hh"
#include "include/util.hh"
#include "include/logger.hh"
#if DURABLE_EPOCH
#include "include/notifier.hh"
#endif

#include "../include/atomic_wrapper.hh"
#include "../include/backoff.hh"
#include "../include/cpu.hh"
#include "../include/debug.hh"
#include "../include/fileio.hh"
#include "../include/masstree_wrapper.hh"
#include "../include/random.hh"
#include "../include/result.hh"
#include "../include/tsc.hh"
#include "../include/util.hh"
#include "../include/zipf.hh"

using namespace std;
uint *GlobalLSN;

#if DURABLE_EPOCH
void worker(size_t thid, char &ready, const bool &start, const bool &quit, Logger **logp)
{
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
#if 0
  if (!FLAGS_affinity.empty()) {
    std::cout << "Worker #" << thid << ": on CPU " << sched_getcpu() << "\n";
  }
#endif
#else
void worker(size_t thid, char &ready, const bool &start, const bool &quit)
{
#endif
  Result &myres = std::ref(SiloResult[thid]);
  Xoroshiro128Plus rnd;
  rnd.init();
  TxnExecutor trans(thid, (Result *) &myres);
  FastZipf zipf(&rnd, FLAGS_zipf_skew, FLAGS_tuple_num);
  uint64_t epoch_timer_start, epoch_timer_stop;
#if BACK_OFF
  Backoff backoff(FLAGS_clocks_per_us);
#endif

#if WAL
  /*
  const boost::filesystem::path log_dir_path("/tmp/ccbench");
  if (boost::filesystem::exists(log_dir_path)) {
  } else {
    boost::system::error_code error;
    const bool result = boost::filesystem::create_directory(log_dir_path,
  error); if (!result || error) { ERR;
    }
  }
  std::string logpath("/tmp/ccbench");
  */
  std::string logpath;
  genLogFile(logpath, thid);
  trans.logfile_.open(logpath, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  trans.logfile_.ftruncate(10 ^ 9);
#endif

#ifdef Linux
  setThreadAffinity(thid);
  // printf("Thread #%d: on CPU %d\n", res.thid_, sched_getcpu());
  // printf("sysconf(_SC_NPROCESSORS_CONF) %d\n",
  // sysconf(_SC_NPROCESSORS_CONF));
#endif

#if MASSTREE_USE
  MasstreeWrapper<Tuple>::thread_init(int(thid));
#endif

#if DURABLE_EPOCH
  Logger* logger = *logp;
  logger->add_txn_executor(trans);
#endif

  storeRelease(ready, 1);
  while (!loadAcquire(start)) _mm_pause();
  if (thid == 0) epoch_timer_start = rdtscp();
  while (!loadAcquire(quit)) {
#if PARTITION_TABLE
    makeProcedure(trans.pro_set_, rnd, zipf, FLAGS_tuple_num, FLAGS_max_ope,
                  FLAGS_thread_num, FLAGS_rratio, FLAGS_rmw, FLAGS_ycsb, true,
                  thid, myres);
#else
    makeProcedure(trans.pro_set_, rnd, zipf, FLAGS_tuple_num, FLAGS_max_ope,
                  FLAGS_thread_num, FLAGS_rratio, FLAGS_rmw, FLAGS_ycsb, false,
                  thid, myres);
#endif

#if PROCEDURE_SORT
    sort(trans.pro_set_.begin(), trans.pro_set_.end());
#endif

RETRY:
    if (thid == 0) {
      leaderWork(epoch_timer_start, epoch_timer_stop);
#if BACK_OFF
      leaderBackoffWork(backoff, SiloResult);
#endif
      // printf("Thread #%d: on CPU %d\n", thid, sched_getcpu());
    }

    if (loadAcquire(quit)) break;

    trans.begin();
    for (auto itr = trans.pro_set_.begin(); itr != trans.pro_set_.end();
         ++itr) {
      if ((*itr).ope_ == Ope::READ) {
        trans.read((*itr).key_);
      } else if ((*itr).ope_ == Ope::WRITE) {
        trans.write((*itr).key_);
      } else if ((*itr).ope_ == Ope::READ_MODIFY_WRITE) {
        trans.read((*itr).key_);
        trans.write((*itr).key_);
      } else {
        ERR;
      }
    }

    if (trans.validationPhase()) {
      trans.writePhase();
      /**
       * local_commit_counts is used at ../include/backoff.hh to calcurate about
       * backoff.
       */
      storeRelease(myres.local_commit_counts_,
                   loadAcquire(myres.local_commit_counts_) + 1);
    } else {
      trans.abort();
      ++myres.local_abort_counts_;
      goto RETRY;
    }
  }

#if DURABLE_EPOCH
  trans.log_buffer_pool_.terminate(myres); // swith buffer
  logger->worker_end(thid);
#endif
  return;
}

#if DURABLE_EPOCH
void logger_th(int thid, Notifier &notifier, Logger** logp){
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
#if 0
  if (!FLAGS_affinity.empty()) {
    std::cout << "Logger #" << thid << ": on CPU " << sched_getcpu() << "\n";
  }
#endif
  alignas(CACHE_LINE_SIZE) Logger logger(thid, notifier);
  notifier.add_logger(&logger);
  *logp = &logger;
  logger.worker();
}

void set_cpu(std::thread &th, int cpu) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu, &cpuset);
  int rc = pthread_setaffinity_np(th.native_handle(), sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
  }
}
#endif

int main(int argc, char *argv[]) try {
  gflags::SetUsageMessage("Silo benchmark.");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
#if DURABLE_EPOCH
  LoggerAffinity affin;
  if (FLAGS_affinity.empty()) {
    affin.init(FLAGS_thread_num,FLAGS_logger_num);
  } else {
    affin.init(FLAGS_affinity);
    FLAGS_thread_num = affin.worker_num_;
    FLAGS_logger_num = affin.logger_num_;
  }
#endif
  chkArg();
  makeDB();

  alignas(CACHE_LINE_SIZE) bool start = false;
  alignas(CACHE_LINE_SIZE) bool quit = false;
  initResult();
  std::vector<char> readys(FLAGS_thread_num);
  std::vector<std::thread> thv;
#if PWAL
	GlobalLSN = (uint *)calloc(FLAGS_thread_num, sizeof(uint));
	if (!GlobalLSN) ERR;
#endif
#if DURABLE_EPOCH
  Logger *logs[FLAGS_logger_num];
  Notifier notifier;
  std::vector<std::thread> lthv;

  int i=0, j=0;
  for (auto itr = affin.nodes_.begin(); itr != affin.nodes_.end(); ++itr,++j) {
    int lcpu = itr->logger_cpu_;
    lthv.emplace_back(logger_th, j, std::ref(notifier), &(logs[j]));
    if (!FLAGS_affinity.empty()) {
      set_cpu(lthv.back(), lcpu);
    }
    for (auto wcpu = itr->worker_cpu_.begin(); wcpu != itr->worker_cpu_.end(); ++wcpu,++i) {
      thv.emplace_back(worker, i, std::ref(readys[i]),
                       std::ref(start), std::ref(quit), &(logs[j]));
      if (!FLAGS_affinity.empty()) {
        set_cpu(thv.back(), *wcpu);
      }
    }
  }
  notifier.run();
#else
  for (size_t i = 0; i < FLAGS_thread_num; ++i)
    thv.emplace_back(worker, i, std::ref(readys[i]), std::ref(start),
                     std::ref(quit));
#endif
  waitForReady(readys);
  storeRelease(start, true);
  for (size_t i = 0; i < FLAGS_extime; ++i) {
    sleepMs(1000);
  }
  storeRelease(quit, true);
#if DURABLE_EPOCH
  notifier.join();
  for (auto &th : lthv) th.join();
#endif
  for (auto &th : thv) th.join();

  for (unsigned int i = 0; i < FLAGS_thread_num; ++i) {
    SiloResult[0].addLocalAllResult(SiloResult[i]);
  }
  ShowOptParameters();
#if DURABLE_EPOCH
  notifier.display();
#endif
  SiloResult[0].displayAllResult(FLAGS_clocks_per_us, FLAGS_extime,
                                 FLAGS_thread_num);

  return 0;
} catch (bad_alloc) {
  ERR;
}
