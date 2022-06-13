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
#include "velox/dwio/common/IoStatistics.h"
#include "velox/dwio/common/MetricsLog.h"

namespace facebook::velox::dwio::common {

constexpr uint64_t DEFAULT_AUTO_PRELOAD_SIZE =
    (static_cast<const uint64_t>((1ul << 20) * 72));

// define a disk region to read
struct Region {
  uint64_t offset;
  uint64_t length;

  Region(uint64_t offset = 0, uint64_t length = 0)
      : offset{offset}, length{length} {}

  bool operator<(const Region& other) const;
};

/**
 * An abstract interface for providing readers a stream of bytes.
 */
class InputStream {
 public:
  explicit InputStream(
      const std::string& path,
      const MetricsLogPtr& metricsLog = MetricsLog::voidLog(),
      IoStatistics* FOLLY_NULLABLE stats = nullptr)
      : path_{path}, metricsLog_{metricsLog}, stats_(stats) {}

  virtual ~InputStream() = default;

  /**
   * Get the stats object
   */
  IoStatistics* FOLLY_NULLABLE getStats() const {
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
   * total bytes transferred through network
   */
  virtual void vread(
      const std::vector<void*>& buffers,
      const std::vector<Region>& regions,
      const LogType purpose);

  // case insensitive find
  static uint32_t ifind(const std::string& src, const std::string& target);

  const std::string& getName() const;

  virtual void logRead(uint64_t offset, uint64_t length, LogType purpose);

  using Factory = std::function<std::unique_ptr<InputStream>(
      const std::string&,
      const MetricsLogPtr&,
      IoStatistics* FOLLY_NULLABLE stats)>;

  static std::unique_ptr<InputStream> create(
      const std::string&,
      const MetricsLogPtr& = MetricsLog::voidLog(),
      IoStatistics* FOLLY_NULLABLE stats = nullptr);

  static bool registerFactory(Factory factory);

 protected:
  std::string path_;
  MetricsLogPtr metricsLog_;
  IoStatistics* FOLLY_NULLABLE stats_;
};

class FileInputStream : public InputStream {
 private:
  int file;
  uint64_t totalLength;

 public:
  explicit FileInputStream(
      const std::string& filename,
      const MetricsLogPtr& metricsLog = MetricsLog::voidLog(),
      IoStatistics* FOLLY_NULLABLE stats = nullptr);

  ~FileInputStream() override;

  uint64_t getLength() const override;

  uint64_t getNaturalReadSize() const override;

  void read(void* FOLLY_NONNULL, uint64_t, uint64_t, LogType) override;

  static void registerFactory();
};

// An input stream that reads from an already opened ReadFile.
class ReadFileInputStream final : public InputStream {
 public:
  // Does not take ownership of |readFile|.
  explicit ReadFileInputStream(
      velox::ReadFile* FOLLY_NONNULL readFile,
      const MetricsLogPtr& metricsLog = MetricsLog::voidLog(),
      IoStatistics* FOLLY_NULLABLE stats = nullptr);

  virtual ~ReadFileInputStream() {}

  uint64_t getLength() const final {
    return readFile_->size();
  }

  uint64_t getNaturalReadSize() const final {
    // TODO: configure this accurately if it actually has impact.
    return 10ULL << 20;
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

 private:
  velox::ReadFile* FOLLY_NONNULL readFile_;
};

class ReferenceableInputStream : public InputStream {
 private:
  uint64_t autoPreloadLength_;
  bool prefetching_;

 public:
  explicit ReferenceableInputStream(
      const std::string& /* UNUSED*/,
      const MetricsLogPtr& metricsLog = MetricsLog::voidLog(),
      IoStatistics* FOLLY_NULLABLE stats = nullptr)
      : InputStream("ReferenceablelnputStream", metricsLog, stats),
        autoPreloadLength_(0),
        prefetching_(false) {}
  virtual ~ReferenceableInputStream() = default;
  virtual uint64_t getPreloadLength() const;
  virtual void setPreloadLength(uint64_t length);
  virtual void preload(uint64_t, uint64_t, LogType) {}
  virtual bool getPrefetching();
  virtual void setPrefetching(bool pf);
  virtual const void* FOLLY_NULLABLE readReference(
      void* FOLLY_NONNULL buf,
      uint64_t length,
      uint64_t offset,
      LogType) = 0;
  virtual const void* FOLLY_NULLABLE
  readReferenceOnly(uint64_t length, uint64_t offset, LogType) = 0;
};
} // namespace facebook::velox::dwio::common

#define VELOX_STATIC_REGISTER_INPUT_STREAM(function)                           \
  namespace {                                                                  \
  static bool FB_ANONYMOUS_VARIABLE(g_InputStreamFunction) =                   \
      facebook::velox::dwio::common::InputStream::registerFactory((function)); \
  }

#define VELOX_REGISTER_INPUT_STREAM_METHOD_DEFINITION(class, function)       \
  void class ::registerFactory() {                                           \
    facebook::velox::dwio::common::InputStream::registerFactory((function)); \
  }
