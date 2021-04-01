#if DURABLE_EPOCH
#include <iostream>
#include <fstream>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include "include/notifier.hh"
#include "include/logger.hh"

void PepochFile::open() {
  fd_ = ::open(file_name_.c_str(), O_CREAT|O_TRUNC|O_RDWR, 0644);
  if (fd_ == -1) {
    std::cerr << "open failed: " << file_name_ << std::endl;
    ERR;
  }
  std::uint64_t zero=0;
  auto sz = ::write(fd_, &zero, sizeof(std::uint64_t));
  if (sz == -1) {
    std::cerr << "write failed";
    ERR;
  }
  addr_ = (std::uint64_t*)::mmap(NULL, sizeof(std::uint64_t), PROT_WRITE, MAP_SHARED, fd_, 0);
  if (addr_ == MAP_FAILED) {
    std::cerr << "mmap failed";
    ERR;
  }
}

void PepochFile::write(std::uint64_t epoch) {
  *addr_ = epoch;
  ::msync(addr_, sizeof(std::uint64_t), MS_SYNC);
}

void PepochFile::close() {
  ::munmap(addr_, sizeof(std::uint64_t));
  ::close(fd_);
  fd_ = -1;
}

void NidBuffer::notify(std::uint64_t min_dl, LoggerResult &stats) {
  if (front_ == NULL) return;
  stats.notify_count_ ++;
  NidBufferItem *orig_front = front_;
  while (front_->epoch_ <= min_dl) {
    //printf("front_->epoch_=%lu min_dl=%lu\n",front_->epoch_,min_dl);
    std::uint64_t ltc = 0;
    std::uint64_t ntf_ltc = 0;
    std::uint64_t min_ltc = ~(uint64_t)0;
    std::uint64_t max_ltc = 0;
    std::uint64_t t = rdtscp();
    for (auto &nid : front_->buffer_) {
      // notify client here
      std::uint64_t dt = t - nid.tx_start_;
      ltc += dt;
      if (dt < min_ltc) min_ltc = dt;
      if (dt > max_ltc) max_ltc = dt;
      ntf_ltc += t - nid.t_mid_;
    }
    stats.latency_ += ltc;
    stats.notify_latency_ += ntf_ltc;
    if (min_ltc < stats.min_latency_) stats.min_latency_ = min_ltc;
    if (max_ltc > stats.max_latency_) stats.max_latency_ = max_ltc;
    if (min_dl != ~(uint64_t)0)
      stats.epoch_diff_ += min_dl - front_->epoch_;
    stats.epoch_count_ ++;
    std::size_t n = front_->buffer_.size();
    stats.count_ += n;
    // clear buffer
    size_ -= n;
    front_->buffer_.clear();
    if (front_->next_ == NULL) break;
    //if (end_->epoch_ > max_epoch_+4) {
    if (false) {
      // release buffer
      //printf("delete front_=%lx front_->next_=%lx front_->epoch_=%lu end_->epoch_=%lu max_epoch_=%lu min_dl=%lu\n",(uint64_t)front_,(uint64_t)front_->next_,front_->epoch_,end_->epoch_,max_epoch_,min_dl);
      NidBufferItem *old_front = front_;
      front_ = front_->next_;
      delete old_front;
    } else {
      // recycle buffer
      front_->epoch_ = end_->epoch_ + 1;
      end_->next_ = front_;
      end_ = front_;
      front_ = front_->next_;
      end_->next_ = NULL;
    }
    if (front_ == orig_front) break;
  }
}

void NidBuffer::store(std::vector<NotificationId> &nid_buffer, std::uint64_t epoch) {
  NidBufferItem *itr = front_;
  while (itr->epoch_ < epoch) {
    if (itr->next_ == NULL) {
      // create buffer
      itr->next_ = end_ = new NidBufferItem(epoch);
      if (max_epoch_ < epoch) max_epoch_ = epoch;
      //printf("create end_=%lx end_->next_=%lx end_->epoch_=%lu epoch=%lu max_epoch_=%lu\n",(uint64_t)end_,(uint64_t)end_->next_, end_->epoch_, epoch, max_epoch_);
    }
    itr = itr->next_;
  }
  assert(itr->epoch == epoch);
  std::copy(nid_buffer.begin(), nid_buffer.end(), std::back_inserter(itr->buffer_));
  size_ += nid_buffer.size();
}

#endif
