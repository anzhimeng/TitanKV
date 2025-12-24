#pragma once

#include <memory>
#include "util/env.h"
#include "blob/blob_format.h"
#include "titankv/status.h"
#include "titankv/slice.h"

namespace titankv {

class BlobWriter {
 public:
  explicit BlobWriter(std::unique_ptr<WritableFile> file);

  Status AddRecord(const Slice& key, const Slice& value);

  uint64_t FileSize() const { return file_size_; }

 private:
  std::unique_ptr<WritableFile> file_;
  uint64_t file_size_;
};

}