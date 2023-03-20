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

#include "velox/dwio/common/InputStream.h"

#include <fcntl.h>
#include <folly/container/F14Map.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <functional>
#include <istream>
#include <stdexcept>
#include <string_view>
#include <type_traits>

#include "velox/common/time/Timer.h"
#include "velox/dwio/common/exception/Exception.h"

namespace facebook::velox::dwio::common {

folly::SemiFuture<uint64_t> InputStream::readAsync(
    const std::vector<folly::Range<char*>>& buffers,
    uint64_t offset,
    LogType logType) {
  try {
    read(buffers, offset, logType);
    uint64_t size = 0;
    for (auto& range : buffers) {
      size += range.size();
    }
    return folly::SemiFuture<uint64_t>(size);
  } catch (const std::exception& e) {
    return folly::makeSemiFuture<uint64_t>(e);
  }
}

ReadFileInputStream::ReadFileInputStream(
    std::shared_ptr<velox::ReadFile> readFile,
    const MetricsLogPtr& metricsLog,
    IoStatistics* stats)
    : InputStream(readFile->getName(), metricsLog, stats),
      readFile_(std::move(readFile)) {}

void ReadFileInputStream::read(
    void* buf,
    uint64_t length,
    uint64_t offset,
    MetricsLog::MetricsType purpose) {
  if (!buf) {
    throw std::invalid_argument("Buffer is null");
  }
  logRead(offset, length, purpose);
  auto readStartMicros = getCurrentTimeMicro();
  std::string_view data_read = readFile_->pread(offset, length, buf);
  if (stats_) {
    stats_->incRawBytesRead(length);
    stats_->incTotalScanTime((getCurrentTimeMicro() - readStartMicros) * 1000);
  }

  DWIO_ENSURE_EQ(
      data_read.size(),
      length,
      "Should read exactly as requested. File name: ",
      getName(),
      ", offset: ",
      offset,
      ", length: ",
      length,
      ", read: ",
      data_read.size());
}

void ReadFileInputStream::read(
    const std::vector<folly::Range<char*>>& buffers,
    uint64_t offset,
    LogType logType) {
  int64_t bufferSize = 0;
  for (auto& buffer : buffers) {
    bufferSize += buffer.size();
  }
  logRead(offset, bufferSize, logType);
  auto size = readFile_->preadv(offset, buffers);
  DWIO_ENSURE_EQ(
      size,
      bufferSize,
      "Should read exactly as requested. File name: ",
      getName(),
      ", offset: ",
      offset,
      ", length: ",
      bufferSize,
      ", read: ",
      size);
}

folly::SemiFuture<uint64_t> ReadFileInputStream::readAsync(
    const std::vector<folly::Range<char*>>& buffers,
    uint64_t offset,
    LogType logType) {
  int64_t bufferSize = 0;
  for (auto& buffer : buffers) {
    bufferSize += buffer.size();
  }
  logRead(offset, bufferSize, logType);
  return readFile_->preadvAsync(offset, buffers);
}

bool ReadFileInputStream::hasReadAsync() const {
  return readFile_->hasPreadvAsync();
}

bool Region::operator<(const Region& other) const {
  return offset < other.offset ||
      (offset == other.offset && length < other.length);
}

void InputStream::vread(
    const std::vector<void*>& buffers,
    const std::vector<Region>& regions,
    const LogType purpose) {
  const auto size = buffers.size();
  // the default implementation of this is to do the read sequentially
  DWIO_ENSURE_GT(size, 0, "invalid vread parameters");
  DWIO_ENSURE_EQ(regions.size(), size, "mismatched region->buffer");

  // convert buffer to IOBufs and convert regions to VReadIntervals
  LOG(INFO) << "[VREAD] fall back vread to sequential reads.";
  for (size_t i = 0; i < size; ++i) {
    // fill each buffer
    const auto& r = regions[i];
    read(buffers[i], r.length, r.offset, purpose);
  }
}

const std::string& InputStream::getName() const {
  return path_;
}

void InputStream::logRead(uint64_t offset, uint64_t length, LogType purpose) {
  metricsLog_->logRead(
      0, "readFully", getLength(), 0, 0, offset, length, purpose, 1, 0);
}

uint64_t ReferenceableInputStream::getPreloadLength() const {
  return autoPreloadLength_;
}

void ReferenceableInputStream::setPreloadLength(uint64_t length) {
  autoPreloadLength_ = length;
}

bool ReferenceableInputStream::getPrefetching() {
  return prefetching_;
}

void ReferenceableInputStream::setPrefetching(bool pf) {
  prefetching_ = pf;
}

} // namespace facebook::velox::dwio::common
