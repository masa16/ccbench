#pragma once

#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <mutex>
#include <stdexcept>
#include <string>

#include "debug.hh"
#include "util.hh"

#ifdef Linux
#include <linux/fs.h>
#endif  // Linux

#if PMEMCPY
#include <libpmem.h>
#endif

class File {
private:
#if PMEMCPY
  char *pmemaddr_ = 0;
  char *pmem_sync_;
  char *pmem_offset_;
  size_t sync_size_;
  size_t write_size_;
  size_t mapped_len_;
  int is_pmem_;
  std::string file_path_;
#endif
  int fd_;
  bool autoClose_;

  void throwOpenError(const std::string &filePath) const {
    const int err = errno;
    std::string s("open failed: ");
    s += filePath;
    throw LibcError(err, s);
  }

public:
  File() : fd_(-1), autoClose_(false) {}

#if NOLOG
  bool open(const std::string &filePath, int flags) {return true;}
  bool open(const std::string &filePath, int flags, mode_t mode) {return true;}
  File(const std::string &filePath, int flags) : File() {}
  File(const std::string &filePath, int flags, mode_t mode) : File() {}
  explicit File(int fd, bool autoClose = false)
    : fd_(fd), autoClose_(autoClose) {}
  ~File() noexcept try {close();} catch (...) {
  }
  void close() {}
  void write(const void *data, size_t size) {}
  void read(void *data, size_t size) {}
  size_t readsome(void *data, size_t size) {return size;}
  int fd() const { return fd_; }
  void close() {}
  void fdatasync() {}
  void fsync() {}
  void ftruncate(off_t length) {}

#else

#if PMEMCPY
#define BUF_LEN 2000000000
  bool open(const std::string &filePath, int flags) {
    /* create a pmem file and memory map it */
    pmemaddr_ = (char*)pmem_map_file(filePath.c_str(), BUF_LEN,
                                    PMEM_FILE_CREATE,
                                    0644, &mapped_len_, &is_pmem_);
    pmem_sync_ = pmem_offset_ = pmemaddr_;
    sync_size_ = write_size_ = 0;
    file_path_ = filePath;
    autoClose_ = true;
    return pmemaddr_ != 0;
  }

  bool open(const std::string &filePath, int flags, mode_t mode) {
    /* create a pmem file and memory map it */
    pmemaddr_ = (char*)pmem_map_file(filePath.c_str(), BUF_LEN,
                                    PMEM_FILE_CREATE,
                                    mode, &mapped_len_, &is_pmem_);
    pmem_sync_ = pmem_offset_ = pmemaddr_;
    sync_size_ = write_size_ = 0;
    file_path_ = filePath;
    autoClose_ = true;
    return pmemaddr_ != 0;
  }
#else
  bool open(const std::string &filePath, int flags) {
    fd_ = ::open(filePath.c_str(), flags);
    autoClose_ = true;
    return fd_ >= 0;
  }

  bool open(const std::string &filePath, int flags, mode_t mode) {
    fd_ = ::open(filePath.c_str(), flags, mode);
    autoClose_ = true;
    return fd_ >= 0;
  }
#endif

  File(const std::string &filePath, int flags) : File() {
    if (!this->open(filePath, flags)) throwOpenError(filePath);
  }

  File(const std::string &filePath, int flags, mode_t mode) : File() {
    if (!this->open(filePath, flags, mode)) throwOpenError(filePath);
  }

  explicit File(int fd, bool autoClose = false)
          : fd_(fd), autoClose_(autoClose) {}

  ~File() noexcept try { close(); } catch (...) {
  }

#if PMEMCPY

  void close() {
    if (!autoClose_ || pmemaddr_ == 0) return;
    pmem_unmap(pmemaddr_, mapped_len_);
    pmemaddr_ = 0;
    if (::truncate(file_path_.c_str(), write_size_) != 0) ERR;
  }

  void write(const void *data, size_t size) {
    if (write_size_+size > BUF_LEN) {
      perror("out of buffer");
      ERR;
    }
    if (is_pmem_) {
      pmem_memcpy(pmem_offset_, data, size, 0);
    } else {
      memcpy(pmem_offset_, data, size);
    }
    pmem_offset_ += size;
    sync_size_ += size;
    write_size_ += size;
  }

  void fsync() {
    if (pmemaddr_ != 0) {
      if (is_pmem_) {
        pmem_persist(pmem_sync_, sync_size_);
      } else {
        pmem_msync(pmem_sync_, sync_size_);
      }
    }
    pmem_sync_ += sync_size_;
    sync_size_ = 0;
  }

  void ftruncate(off_t length) {
  }

  void read(void *data, size_t size) {
  }

  size_t readsome(void *data, size_t size) {
  }

#else
  int fd() const {
    if (fd_ < 0) ERR;
    return fd_;
  }

  void close() {
    if (!autoClose_ || fd_ < 0) return;
    if (::close(fd_) < 0) {
      throw LibcError(errno, "close failed: ");
    }
    fd_ = -1;
  }

  size_t readsome(void *data, size_t size) {
    ssize_t r = ::read(fd(), data, size);
    if (r < 0) throw LibcError(errno, "read failed: ");
    return r;
  }

  void read(void *data, size_t size) {
    char *buf = reinterpret_cast<char *>(data);
    size_t s = 0;
    while (s < size) {
      size_t r = readsome(&buf[s], size - s);
      if (r == 0) ERR;
      s += r;
    }
  }

  void write(const void *data, size_t size) {
    const char *buf = reinterpret_cast<const char *>(data);
    size_t s = 0;
    while (s < size) {
      ssize_t r = ::write(fd(), &buf[s], size - s);
      if (r < 0) throw LibcError(errno, "write failed: ");
      if (r == 0) ERR;
      s += r;
    }
  }

#ifdef Linux
  void fdatasync() {
    if (::fdatasync(fd()) < 0) {
      throw LibcError(errno, "fdsync failed: ");
    }
  }
#endif  // Linux

  void fsync() {
    if (::fsync(fd()) < 0) {
      throw LibcError(errno, "fsync failed: ");
    }
  }

  void ftruncate(off_t length) {
    if (::ftruncate(fd(), length) < 0) {
      throw LibcError(errno, "ftruncate failed: ");
    }
  }
#endif //PMEMCPY
#endif //DUMMYIO
};

// create a file if it does not exist.
inline void createEmptyFile(const std::string &path, mode_t mode = 0644) {
  struct stat st;
  if (::stat(path.c_str(), &st) == 0) return;
  File writer(path, O_CREAT | O_TRUNC | O_RDWR, mode);
  writer.close();
}

#if WALPMEM
#include <numa.h>

inline void genLogFileName(std::string &logpath, const int thid) {
  unsigned node_num = numa_max_node() + 1;
  unsigned node = thid % node_num;
  if (numa_run_on_node(node) != 0) ERR;
  logpath.clear();
  logpath += "log" + std::to_string(node) + "/thid" + std::to_string(thid);
}

#else
/**
 * Read all contents from a file.
 * Do not specify streams.
 * Do not use this for large files.
 *
 * String : it must have size(), resize(), and operator[].
 * such as std::string and std::vector<char>.
 */
template<typename String>
inline void readAllFromFile(File &file, String &buf) {
  constexpr const size_t usize = 4096;  // unit size.
  size_t rsize = buf.size();            // read data will be appended to buf.

  for (;;) {
    if (buf.size() < rsize + usize) buf.resize(rsize + usize);
    const size_t r = file.readsome(&buf[rsize], usize);
    if (r == 0) break;
    rsize += r;
  }
  buf.resize(rsize);
}

template<typename String>
inline void readAllFromFile(const std::string &path, String &buf) {
  File file(path, O_RDONLY);
  readAllFromFile(file, buf);
  file.close();
}

inline void genLogFileName(std::string &logpath, const int thid) {
  const int PATHNAME_SIZE = 512;
  char pathname[PATHNAME_SIZE];

  memset(pathname, '\0', PATHNAME_SIZE);

  if (getcwd(pathname, PATHNAME_SIZE) == NULL) ERR;

  logpath = pathname;
  logpath += "/log/log" + std::to_string(thid);
}
#endif
