/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <hdfs/hdfs.h>
#include "velox/common/file/File.h"

namespace facebook::velox {

struct HdfsFile {
  hdfsFS client_;
  hdfsFile handle_;

  HdfsFile() : client_(nullptr), handle_(nullptr) {}
  ~HdfsFile() {
    if (handle_ && hdfsCloseFile(client_, handle_) == -1) {
      LOG(ERROR) << "Unable to close file, errno: " << errno;
    }
  }

  void open(hdfsFS client, const std::string& path) {
    client_ = client;
    handle_ = hdfsOpenFile(client, path.data(), O_RDONLY, 0, 0, 0);
    VELOX_CHECK_NOT_NULL(
        handle_,
        "Unable to open file {}. got error: {}",
        path,
        hdfsGetLastError());
  }

  void seek(uint64_t offset) const {
    VELOX_CHECK_EQ(
        hdfsSeek(client_, handle_, offset),
        0,
        "Cannot seek through HDFS file, error is : {}",
        std::string(hdfsGetLastError()));
  }

  int32_t read(char* pos, uint64_t length) const {
    auto bytesRead = hdfsRead(client_, handle_, pos, length);
    VELOX_CHECK(bytesRead >= 0, "Read failure in HDFSReadFile::preadInternal.");
    return bytesRead;
  }
};

/**
 * Implementation of hdfs read file.
 */
class HdfsReadFile final : public ReadFile {
 public:
  explicit HdfsReadFile(hdfsFS hdfs, std::string_view path);
  ~HdfsReadFile() override;

  std::string_view pread(uint64_t offset, uint64_t length, void* buf)
      const final;

  std::string pread(uint64_t offset, uint64_t length) const final;

  uint64_t size() const final;

  uint64_t memoryUsage() const final;

  bool shouldCoalesce() const final;

  std::string getName() const final {
    return filePath_;
  }

  uint64_t getNaturalReadSize() const final {
    return 72 << 20;
  }

 private:
  void preadInternal(uint64_t offset, uint64_t length, char* pos) const;
  void checkFileReadParameters(uint64_t offset, uint64_t length) const;

  hdfsFS hdfsClient_;
  hdfsFileInfo* fileInfo_;
  std::string filePath_;
  folly::ThreadLocal<HdfsFile> file_;
};

} // namespace facebook::velox
