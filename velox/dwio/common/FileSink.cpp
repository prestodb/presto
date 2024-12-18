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

#include "velox/dwio/common/FileSink.h"

#include "velox/common/base/Fs.h"
#include "velox/common/file/FileSystems.h"
#include "velox/dwio/common/exception/Exception.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

namespace facebook::velox::dwio::common {
namespace {
constexpr std::string_view kFileScheme("file:");
constexpr std::string_view kFileSep("/");

std::vector<FileSink::Factory>& factories() {
  static std::vector<FileSink::Factory> factories;
  return factories;
}

std::unique_ptr<FileSink> localFileSink(
    const std::string& filePath,
    const FileSink::Options& options) {
  if (filePath.find(kFileScheme) == 0) {
    return std::make_unique<LocalFileSink>(filePath.substr(5), options);
  }
  if (filePath.find(kFileSep) == 0) {
    return std::make_unique<LocalFileSink>(filePath, options);
  }
  return nullptr;
}
} // namespace

void FileSink::write(DataBuffer<char> buffer) {
  std::vector<DataBuffer<char>> vec;
  vec.push_back(std::move(buffer));
  writeWithLogging(vec);
}

void FileSink::writeWithLogging(std::vector<DataBuffer<char>>& buffers) {
  uint64_t timeUs{0};
  {
    MicrosecondTimer timer(&timeUs);
    write(buffers);
  }
  metricLogger_->logWrite(
      size_, timeUs / Timestamp::kMicrosecondsInMillisecond);
}

void FileSink::writeImpl(
    std::vector<DataBuffer<char>>& buffers,
    const std::function<uint64_t(const DataBuffer<char>&)>& callback) {
  DWIO_ENSURE(!isClosed(), "Cannot write to closed sink.");
  const uint64_t oldSize = size_;
  uint64_t writeTimeUs{0};
  {
    MicrosecondTimer timer(&writeTimeUs);
    for (auto& buf : buffers) {
      // NOTE: we need to update 'size_' after each 'callback' invocation as
      // some file sink implementation like MemorySink depends on the updated
      // 'size_' for new write.
      size_ += callback(buf);
    }
  }
  if (stats_ != nullptr) {
    stats_->incRawBytesWritten(size_ - oldSize);
    stats_->incWriteIOTimeUs(writeTimeUs);
  }
  // Writing buffer is treated as transferring ownership. So clearing the
  // buffers after all buffers are written.
  buffers.clear();
}

// static
bool FileSink::registerFactory(const FileSink::Factory& factory) {
  factories().push_back(factory);
  return true;
}

// static.
std::unique_ptr<FileSink> FileSink::create(
    const std::string& filePath,
    const Options& options) {
  VELOX_CHECK_NOT_NULL(options.metricLogger);
  for (auto& factory : factories()) {
    auto result = factory(filePath, options);
    if (result) {
      return result;
    }
  }
  VELOX_FAIL("FileSink is not registered for {}", filePath);
}

WriteFileSink::WriteFileSink(
    std::unique_ptr<WriteFile> writeFile,
    std::string name,
    MetricsLogPtr metricLogger,
    IoStatistics* stats)
    : FileSink(
          std::move(name),
          {.metricLogger = std::move(metricLogger), .stats = stats}),
      writeFile_{std::move(writeFile)} {
  VELOX_CHECK_NOT_NULL(writeFile_);
}

void WriteFileSink::write(std::vector<DataBuffer<char>>& buffers) {
  writeImpl(buffers, [&](auto& buffer) {
    const uint64_t size = buffer.size();
    writeFile_->append({buffer.data(), size});
    return size;
  });
}

void WriteFileSink::doClose() {
  VLOG(1) << "closing file: " << name()
          << ",  total size: " << succinctBytes(size_);
  if (writeFile_ != nullptr) {
    writeFile_->close();
  }
}

LocalFileSink::LocalFileSink(const std::string& name, const Options& options)
    : FileSink{name, options}, writeFile_() {
  const auto dir = fs::path(name_).parent_path();
  if (!fs::exists(dir)) {
    VELOX_CHECK(velox::common::generateFileDirectory(dir.c_str()));
  }
  auto fs = filesystems::getFileSystem(name_, nullptr);
  writeFile_ = fs->openFileForWrite(name_);
}

LocalFileSink::LocalFileSink(
    const std::string& name,
    const Options& options,
    bool initializeWriter)
    : FileSink{name, options}, writeFile_() {
  const auto dir = fs::path(name_).parent_path();
  if (!fs::exists(dir)) {
    VELOX_CHECK(velox::common::generateFileDirectory(dir.c_str()));
  }
}

void LocalFileSink::doClose() {
  VLOG(1) << "closing file: " << name()
          << ",  total size: " << succinctBytes(size_);
  if (writeFile_ != nullptr) {
    writeFile_->close();
  }
}

void LocalFileSink::write(std::vector<DataBuffer<char>>& buffers) {
  writeImpl(buffers, [&](auto& buffer) {
    const uint64_t size = buffer.size();
    writeFile_->append({buffer.data(), size});
    return size;
  });
}

MemorySink::MemorySink(size_t capacity, const Options& options)
    : FileSink{"MemorySink", options}, data_{*options.pool, capacity} {}

void MemorySink::write(std::vector<DataBuffer<char>>& buffers) {
  writeImpl(buffers, [&](auto& buffer) {
    const auto size = buffer.size();
    data_.extendAppend(size_, buffer.data(), size);
    return size;
  });
}

VELOX_REGISTER_DATA_SINK_METHOD_DEFINITION(LocalFileSink, localFileSink);

void registerFileSinks() {
  dwio::common::LocalFileSink::registerFactory();
}

} // namespace facebook::velox::dwio::common
