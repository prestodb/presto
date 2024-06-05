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
#include "velox/common/caching/SsdCache.h"
#include <folly/Executor.h>
#include <folly/portability/SysUio.h>
#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/FileIds.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/common/time/Timer.h"

#include <filesystem>
#include <numeric>

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::cache {

SsdCache::SsdCache(const Config& config)
    : filePrefix_(config.filePrefix),
      numShards_(config.numShards),
      groupStats_(std::make_unique<FileGroupStats>()),
      executor_(config.executor) {
  // Make sure the given path of Ssd files has the prefix for local file system.
  // Local file system would be derived based on the prefix.
  VELOX_CHECK(
      filePrefix_.find('/') == 0,
      "Ssd path '{}' does not start with '/' that points to local file system.",
      filePrefix_);
  VELOX_CHECK_NOT_NULL(executor_);

  auto checksumReadVerificationEnabled = config.checksumReadVerificationEnabled;
  if (config.checksumReadVerificationEnabled && !config.checksumEnabled) {
    VELOX_SSD_CACHE_LOG(WARNING)
        << "Checksum read has been disabled as checksum is not enabled.";
    checksumReadVerificationEnabled = false;
  }
  filesystems::getFileSystem(filePrefix_, nullptr)
      ->mkdir(std::filesystem::path(filePrefix_).parent_path().string());

  files_.reserve(numShards_);
  // Cache size must be a multiple of this so that each shard has the same max
  // size.
  const uint64_t sizeQuantum = numShards_ * SsdFile::kRegionSize;
  const int32_t fileMaxRegions =
      bits::roundUp(config.maxBytes, sizeQuantum) / sizeQuantum;
  for (auto i = 0; i < numShards_; ++i) {
    const auto fileConfig = SsdFile::Config(
        fmt::format("{}{}", filePrefix_, i),
        i,
        fileMaxRegions,
        config.checkpointIntervalBytes / config.numShards,
        config.disableFileCow,
        config.checksumEnabled,
        checksumReadVerificationEnabled,
        executor_);
    files_.push_back(std::make_unique<SsdFile>(fileConfig));
  }
}

SsdFile& SsdCache::file(uint64_t fileId) {
  const auto index = fileId % numShards_;
  return *files_[index];
}

bool SsdCache::startWrite() {
  std::lock_guard<std::mutex> l(mutex_);
  checkNotShutdownLocked();
  if (writesInProgress_ == 0) {
    // No write was pending, so now all shards are counted as writing.
    writesInProgress_ += numShards_;
    return true;
  }
  VELOX_CHECK_GE(writesInProgress_, 0);
  return false;
}

void SsdCache::write(std::vector<CachePin> pins) {
  VELOX_CHECK_EQ(numShards_, writesInProgress_);

  TestValue::adjust("facebook::velox::cache::SsdCache::write", this);

  const auto startTimeUs = getCurrentTimeMicro();

  uint64_t bytes = 0;
  std::vector<std::vector<CachePin>> shards(numShards_);
  for (auto& pin : pins) {
    bytes += pin.checkedEntry()->size();
    const auto& target = file(pin.checkedEntry()->key().fileNum.id());
    shards[target.shardId()].push_back(std::move(pin));
  }

  int32_t numNoStore = 0;
  for (auto i = 0; i < numShards_; ++i) {
    if (shards[i].empty()) {
      ++numNoStore;
      continue;
    }

    struct PinHolder {
      std::vector<CachePin> pins;

      explicit PinHolder(std::vector<CachePin>&& _pins)
          : pins(std::move(_pins)) {}
    };

    // We move the mutable vector of pins to the executor. These must be wrapped
    // in a shared struct to be passed via lambda capture.
    auto pinHolder = std::make_shared<PinHolder>(std::move(shards[i]));
    executor_->add([this, i, pinHolder, bytes, startTimeUs]() {
      try {
        files_[i]->write(pinHolder->pins);
      } catch (const std::exception& e) {
        // Catch so as not to miss updating 'writesInProgress_'. Could
        // theoretically happen for std::bad_alloc or such.
        VELOX_SSD_CACHE_LOG(WARNING)
            << "Ignoring error in SsdFile::write: " << e.what();
      }
      pinHolder->pins.clear();
      if (--writesInProgress_ == 0) {
        // Typically occurs every few GB. Allows detecting unusually slow rates
        // from failing devices.
        VELOX_SSD_CACHE_LOG(INFO) << fmt::format(
            "Wrote {}, {} bytes/s",
            succinctBytes(bytes),
            static_cast<float>(bytes) / (getCurrentTimeMicro() - startTimeUs));
      }
    });
  }
  writesInProgress_.fetch_sub(numNoStore);
}

bool SsdCache::removeFileEntries(
    const folly::F14FastSet<uint64_t>& filesToRemove,
    folly::F14FastSet<uint64_t>& filesRetained) {
  if (!startWrite()) {
    return false;
  }

  bool success = true;
  for (auto i = 0; i < numShards_; i++) {
    try {
      success &= files_[i]->removeFileEntries(filesToRemove, filesRetained);
    } catch (const std::exception& e) {
      VELOX_SSD_CACHE_LOG(ERROR)
          << "Error removing file entries from SSD shard "
          << files_[i]->shardId() << ": " << e.what();
      success = false;
    }
    --writesInProgress_;
  }
  return success;
}

SsdCacheStats SsdCache::stats() const {
  SsdCacheStats stats;
  for (auto& file : files_) {
    file->updateStats(stats);
  }
  return stats;
}

std::string SsdCache::toString() const {
  const auto data = stats();
  const uint64_t capacity = maxBytes();
  std::stringstream out;
  out << "Ssd cache IO: Write " << succinctBytes(data.bytesWritten) << " read "
      << succinctBytes(data.bytesRead) << " Size " << succinctBytes(capacity)
      << " Occupied " << succinctBytes(data.bytesCached);
  out << " " << (data.entriesCached >> 10) << "K entries.";
  out << "\nGroupStats: " << groupStats_->toString(capacity);
  return out.str();
}

void SsdCache::shutdown() {
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (shutdown_) {
      VELOX_SSD_CACHE_LOG(INFO) << "SSD cache has already been shutdown";
    }
    shutdown_ = true;
  }

  VELOX_SSD_CACHE_LOG(INFO) << "SSD cache is shutting down";
  while (writesInProgress_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100)); // NOLINT
  }
  for (auto& file : files_) {
    file->checkpoint(true);
  }
  VELOX_SSD_CACHE_LOG(INFO) << "SSD cache has been shutdown";
}

void SsdCache::testingClear() {
  for (auto& file : files_) {
    file->testingClear();
  }
}

void SsdCache::testingDeleteFiles() {
  for (auto& file : files_) {
    file->testingDeleteFile();
  }
}

void SsdCache::testingDeleteCheckpoints() {
  for (auto& file : files_) {
    file->deleteCheckpoint();
  }
}

uint64_t SsdCache::testingTotalLogEvictionFilesSize() {
  uint64_t size = 0;
  for (auto& file : files_) {
    std::filesystem::path p{file->getEvictLogFilePath()};
    size += std::filesystem::file_size(p);
  }
  return size;
}

void SsdCache::testingWaitForWriteToFinish() {
  while (writesInProgress_ != 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100)); // NOLINT
  }
}

} // namespace facebook::velox::cache
