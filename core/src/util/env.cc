// 在 .cc 文件中
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include "util/env.h"


namespace titankv {

class PosixWritableFile : public WritableFile {
 private:
  std::string filename_;
  int fd_;

 public:
  explicit PosixWritableFile(const std::string& filename, int fd)
      : filename_(filename), fd_(fd) {}

  ~PosixWritableFile() override {
    if (fd_ >= 0) {
      Close();
    }
  }

  Status Append(const Slice& data) override {
    ssize_t ret = write(fd_, data.data(), data.size());
    if (ret != static_cast<ssize_t>(data.size())) {
      return Status::IOError("Failed to write to file", strerror(errno));
    }
    return Status::OK();
  }

  Status Close() override {
    if (close(fd_) != 0) {
      return Status::IOError("Failed to close file", strerror(errno));
    }
    fd_ = -1;
    return Status::OK();
  }

  Status Sync() override {
    // fdatasync() 比 fsync() 更高效，因为它不刷元数据
    if (fdatasync(fd_) != 0) {
      return Status::IOError("Failed to sync file", strerror(errno));
    }
    return Status::OK();
  }

  Status Flush() override {
        return Status::OK();
  }
  
};

Status NewWritableFile(const std::string& filename, std::unique_ptr<WritableFile>* result) {
    int fd = open(filename.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
    if (fd < 0) {
        return Status::IOError("Failed to open file for writing", strerror(errno));
    }
    result->reset(new PosixWritableFile(filename, fd));
    return Status::OK();
}

class PosixSequentialFile : public SequentialFile {
private:
    std::string filename_;
    int fd_;

public:
    PosixSequentialFile(const std::string& fname, int fd) : filename_(fname), fd_(fd) {}
    ~PosixSequentialFile() override { close(fd_); }

    Status Read(size_t n, Slice* result, char* scratch) override {
        ssize_t r = read(fd_, scratch, n);
        if (r < 0) {
            return Status::IOError("Read failed", strerror(errno));
        }
        *result = Slice(scratch, r);
        return Status::OK();
    }

    Status Skip(uint64_t n) override {
        if (lseek(fd_, n, SEEK_CUR) == static_cast<off_t>(-1)) {
            return Status::IOError("Skip failed", strerror(errno));
        }
        return Status::OK();
    }
};

Status NewSequentialFile(const std::string& fname, std::unique_ptr<SequentialFile>* result) {
    int fd = open(fname.c_str(), O_RDONLY);
    if (fd < 0) {
        return Status::IOError("Failed to open file for reading", strerror(errno));
    }
    result->reset(new PosixSequentialFile(fname, fd));
    return Status::OK();
}

class PosixRandomAccessFile : public RandomAccessFile {
private:
    std::string filename_;
    int fd_;

public:
    PosixRandomAccessFile(const std::string& fname, int fd) : filename_(fname), fd_(fd) {}
    ~PosixRandomAccessFile() override { close(fd_); }

    Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override {
        // pread 是原子操作，不改变文件指针，适合多线程并发读
        ssize_t r = pread(fd_, scratch, n, static_cast<off_t>(offset));
        if (r < 0) {
            return Status::IOError("pread failed", strerror(errno));
        }
        *result = Slice(scratch, r);
        return Status::OK();
    }
};

Status NewRandomAccessFile(const std::string& fname, std::unique_ptr<RandomAccessFile>* result) {
    int fd = open(fname.c_str(), O_RDONLY);
    if (fd < 0) {
        return Status::IOError("Failed to open file for random reading", strerror(errno));
    }
    result->reset(new PosixRandomAccessFile(fname, fd));
    return Status::OK();
}

}