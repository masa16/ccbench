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
