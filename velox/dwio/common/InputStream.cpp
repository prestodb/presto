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

FileInputStream::FileInputStream(
    const std::string& path,
    const MetricsLogPtr& metricsLog,
    IoStatistics* stats)
    : InputStream(path, metricsLog, stats) {
  file = open(path.c_str(), O_RDONLY);
  if (file == -1) {
    throw std::runtime_error(
        "Can't open \"" + path + "\". Error: " + std::to_string(errno));
  }
  struct stat fileStat;
  if (fstat(file, &fileStat) == -1) {
    throw std::runtime_error(
        "Can't stat \"" + path + "\". Error: " + std::to_string(errno));
  }
  totalLength = static_cast<uint64_t>(fileStat.st_size);
}

void FileInputStream::read(
    void* buf,
    uint64_t length,
    uint64_t offset,
    MetricsLog::MetricsType purpose) {
  if (!buf) {
    throw std::invalid_argument("Buffer is null");
  }

  // log the metric
  logRead(offset, length, purpose);

  auto dest = static_cast<char*>(buf);
  uint64_t totalBytesRead = 0;
  while (totalBytesRead < length) {
    ssize_t bytesRead = pread(
        file,
        dest,
        length - totalBytesRead,
        static_cast<off_t>(offset + totalBytesRead));

    DWIO_ENSURE_GE(
        bytesRead,
        0,
        "pread failure . File name: ",
        getName(),
        ", offset: ",
        offset,
        ", length: ",
        length,
        ", read: ",
        totalBytesRead,
        ", errno: ",
        errno);

    DWIO_ENSURE_NE(
        bytesRead,
        0,
        "Unexepected EOF. File name: ",
        getName(),
        ", offset: ",
        offset,
        ", length: ",
        length,
        ", read: ",
        totalBytesRead);

    totalBytesRead += bytesRead;
    dest += bytesRead;
  }

  DWIO_ENSURE_EQ(
      totalBytesRead,
      length,
      "Should read exactly as requested. File name: ",
      getName(),
      ", offset: ",
      offset,
      ", length: ",
      length,
      ", read: ",
      totalBytesRead);

  if (stats_) {
    stats_->incRawBytesRead(length);
  }
}

static constexpr char kReadFilePath[] = "ReadFileFakePath";

ReadFileInputStream::ReadFileInputStream(
    velox::ReadFile* readFile,
    const MetricsLogPtr& metricsLog,
    IoStatistics* stats)
    : InputStream(kReadFilePath, metricsLog, stats), readFile_(readFile) {}

void ReadFileInputStream::read(
    void* buf,
    uint64_t length,
    uint64_t offset,
    MetricsLog::MetricsType purpose) {
  if (!buf) {
    throw std::invalid_argument("Buffer is null");
  }
  logRead(offset, length, purpose);
  std::string_view data_read = readFile_->pread(offset, length, buf);
  if (stats_) {
    stats_->incRawBytesRead(length);
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

FileInputStream::~FileInputStream() {
  close(file);
}

uint64_t FileInputStream::getLength() const {
  return totalLength;
}

uint64_t FileInputStream::getNaturalReadSize() const {
  return DEFAULT_AUTO_PRELOAD_SIZE;
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

static std::vector<InputStream::Factory>& factories() {
  static std::vector<InputStream::Factory> factories;
  return factories;
}

bool InputStream::registerFactory(InputStream::Factory factory) {
  factories().push_back(factory);
  return true;
}

std::unique_ptr<InputStream> InputStream::create(
    const std::string& path,
    const MetricsLogPtr& metricsLog,
    IoStatistics* stats) {
  DWIO_ENSURE_NOT_NULL(metricsLog.get());
  for (auto& factory : factories()) {
    auto result = factory(path, metricsLog, stats);
    if (result) {
      return result;
    }
  }
  return std::make_unique<FileInputStream>(path, metricsLog, stats);
}

static std::unique_ptr<InputStream> fileInputStreamFactory(
    const std::string& filename,
    const MetricsLogPtr& metricsLog,
    IoStatistics* stats = nullptr) {
  if (strncmp(filename.c_str(), "file:", 5) == 0) {
    return std::make_unique<FileInputStream>(
        filename.substr(5), metricsLog, stats);
  }
  return nullptr;
}

VELOX_REGISTER_INPUT_STREAM_METHOD_DEFINITION(
    FileInputStream,
    fileInputStreamFactory)

} // namespace facebook::velox::dwio::common
