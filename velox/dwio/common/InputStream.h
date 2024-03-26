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

#pragma once

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "velox/common/file/File.h"
#include "velox/common/file/Region.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/dwio/common/MetricsLog.h"

namespace facebook::velox::dwio::common {
using namespace facebook::velox::io;

/**
 * An abstract interface for providing readers a stream of bytes.
 */
class InputStream {
 public:
  explicit InputStream(
      const std::string& path,
      const MetricsLogPtr& metricsLog = MetricsLog::voidLog(),
      IoStatistics* stats = nullptr)
      : path_{path}, metricsLog_{metricsLog}, stats_(stats) {}

  virtual ~InputStream() = default;

  /**
   * Get the stats object
   */
  IoStatistics* getStats() const {
    return stats_;
  }

  /**
   * Get the total length of the file in bytes.
   */
  virtual uint64_t getLength() const = 0;

  /**
   * Get the natural size for reads.
   * @return the number of bytes that should be read at once
   */
  virtual uint64_t getNaturalReadSize() const = 0;

  /**
   * Read length bytes from the file starting at offset into
   * the buffer starting at buf.
   * @param buf the starting position of a buffer.
   * @param length the number of bytes to read.
   * @param offset the position in the stream to read from.
   */
  virtual void read(void* FOLLY_NONNULL, uint64_t, uint64_t, LogType) = 0;

  /**
   * Read starting at offset into buffers, filling the buffers left to right. A
   * buffer with data() == nullptr indicates a gap in the read, so that its
   * size() worth bytes are skipped. There must be enough data in 'this' to
   * cover the sum of the sizes of 'buffers'.
   *
   * @buffers - The buffers to read into.
   * @param offset the position in the stream to read from.
   */
  virtual void read(
      const std::vector<folly::Range<char*>>& buffers,
      uint64_t offset,
      LogType logType) {
    uint64_t bufferOffset = 0;
    for (auto& range : buffers) {
      if (range.data()) {
        read(range.data(), range.size(), offset + bufferOffset, logType);
      }
      bufferOffset += range.size();
    }
  }

  /// Like read() with the same arguments but returns the result or
  /// exception via SemiFuture. Use only if hasReadAsync() is true.
  virtual folly::SemiFuture<uint64_t> readAsync(
      const std::vector<folly::Range<char*>>& buffers,
      uint64_t offset,
      LogType logType);

  /// Returns true if readAsync has a native implementation that is
  /// asynchronous.
  virtual bool hasReadAsync() const {
    return false;
  }

  /**
   * Take advantage of vectorized read API provided by some file system.
   * Allow file system to do optimzied reading plan to disk to minimize
   * total bytes transferred through network. Stores the result in an IOBuf
   * range named at `iobufs`, which must have the same size as `regions`.
   */
  virtual void vread(
      folly::Range<const velox::common::Region*> regions,
      folly::Range<folly::IOBuf*> iobufs,
      const LogType purpose) = 0;

  const std::string& getName() const;

  virtual void logRead(uint64_t offset, uint64_t length, LogType purpose);

 protected:
  std::string path_;
  MetricsLogPtr metricsLog_;
  IoStatistics* stats_;
};

// An input stream that reads from an already opened ReadFile.
class ReadFileInputStream final : public InputStream {
 public:
  // Take shared ownership of |readFile|.
  explicit ReadFileInputStream(
      std::shared_ptr<velox::ReadFile>,
      const MetricsLogPtr& metricsLog = MetricsLog::voidLog(),
      IoStatistics* stats = nullptr);

  ~ReadFileInputStream() override = default;

  uint64_t getLength() const final override {
    return readFile_->size();
  }

  uint64_t getNaturalReadSize() const final override {
    return readFile_->getNaturalReadSize();
  }

  void read(void* FOLLY_NONNULL, uint64_t, uint64_t, LogType) override;

  void read(
      const std::vector<folly::Range<char*>>& buffers,
      uint64_t offset,
      LogType logType) override;

  folly::SemiFuture<uint64_t> readAsync(
      const std::vector<folly::Range<char*>>& buffers,
      uint64_t offset,
      LogType logType) override;

  bool hasReadAsync() const override;

  void vread(
      folly::Range<const velox::common::Region*> regions,
      folly::Range<folly::IOBuf*> iobufs,
      const LogType purpose) override;

  const std::shared_ptr<velox::ReadFile>& getReadFile() const {
    return readFile_;
  }

 private:
  std::shared_ptr<velox::ReadFile> readFile_;
};

} // namespace facebook::velox::dwio::common
