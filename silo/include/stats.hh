#pragma once
#include <utility>
#include <string.h>
#include "../include/common.hh"

class Stats {
public:
  double sum_ = 0;
  double sum2_ = 0;
  double min_ = 1.0/0.0;
  double max_ = -1.0/0.0;
  size_t n_ = 0;
  void sample(double x) {
    n_ ++;
    sum_ += x;
    sum2_ += x*x;
    if (x<min_) min_ = x;
    if (x>max_) max_ = x;
  }
  void unify(Stats other) {
    n_ += other.n_;
    sum_ += other.sum_;
    sum2_ += other.sum2_;
    if (other.min_ < min_) min_ = other.min_;
    if (other.max_ > max_) max_ = other.max_;
  }
  void display(std::string s) {
    std::cout << "mean_" << s << ":\t" << sum_/n_ << endl;
    std::cout << "sdev_" << s << ":\t" << sqrt(sum2_/(n_-1) - sum_*sum_/n_/(n_-1)) << endl;
    std::cout << "min_" << s << ":\t" << min_ << endl;
    std::cout << "max_" << s << ":\t" << max_ << endl;
  }
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

  void add(NotifyStats &other) {
#if NOTIFIER_THREAD
    latency_        += other.latency_;
    notify_latency_ += other.notify_latency_;
    nid_count_      += other.nid_count_;
    notify_count_   += other.notify_count_;
    epoch_count_    += other.epoch_count_;
    epoch_diff_     += other.epoch_diff_;
#else
    __atomic_fetch_add(&latency_ ,other.latency_, __ATOMIC_ACQ_REL);
    __atomic_fetch_add(&notify_latency_ ,other.notify_latency_, __ATOMIC_ACQ_REL);
    __atomic_fetch_add(&count_ ,other.count_, __ATOMIC_ACQ_REL);
    __atomic_fetch_add(&notify_count_ ,other.notify_count_, __ATOMIC_ACQ_REL);
    __atomic_fetch_add(&epoch_count_ ,other.epoch_count_, __ATOMIC_ACQ_REL);
    __atomic_fetch_add(&epoch_diff_ ,other.epoch_diff_, __ATOMIC_ACQ_REL);
#endif
  }

  void display() {
    double cpms = FLAGS_clocks_per_us*1e3;
    std::cout << std::fixed << std::setprecision(4)
              << "notify_latency[ms]:\t" << notify_latency_/cpms/count_ << endl;
    std::cout << std::fixed << std::setprecision(4)
              << "durable_latency[ms]:\t" << latency_/cpms/count_ << endl;
    std::cout << "notify_count:\t" << notify_count_ << endl;
    std::cout << "durable_count:\t" << count_ << endl;
    std::cout << "mean_durable_count:\t" << count_ / notify_count_ << endl;
    std::cout << std::fixed << std::setprecision(2)
              << "mean_epoch_diff:\t" << (double)epoch_diff_/epoch_count_ << endl;
  }
};


class NidStats {
public:
  std::size_t count_ = 0;
  std::uint64_t txn_latency_ = 0;
  std::uint64_t log_queue_latency_ = 0;
  std::uint64_t write_latency_ = 0;

  void add(NidStats &other) {
    count_ += other.count_;
    txn_latency_ += other.txn_latency_;
    log_queue_latency_ += other.log_queue_latency_;
    write_latency_ += other.write_latency_;
  }

  void display() {
    double cpms = FLAGS_clocks_per_us*1e3;
    std::cout << std::fixed << std::setprecision(4)
              << "txn_latency[ms]:\t" << txn_latency_/cpms/count_ << endl;
    std::cout << std::fixed << std::setprecision(4)
              << "log_queue_latency[ms]:\t" << log_queue_latency_/cpms/count_ << endl;
    std::cout << std::fixed << std::setprecision(4)
              << "write_latency[ms]:\t" << write_latency_/cpms/count_ << endl;
  }
};
