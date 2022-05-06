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
#include <folly/Executor.h>
#include <folly/portability/SysUio.h>
#include <numeric>
#include "velox/common/caching/FileIds.h"

#include "velox/common/caching/SsdCache.h"
#include "velox/common/time/Timer.h"

namespace facebook::velox::cache {

SsdCache::SsdCache(
    std::string_view filePrefix,
    uint64_t maxBytes,
    int32_t numShards,
    folly::Executor* executor,
    int64_t checkpointIntervalBytes)
    : filePrefix_(filePrefix),
      numShards_(numShards),
      groupStats_(std::make_unique<FileGroupStats>()),
      executor_(executor) {
  files_.reserve(numShards_);
  // Cache size must be a multiple of this so that each shard has the same max
  // size.
  uint64_t sizeQuantum = numShards_ * SsdFile::kRegionSize;
  int32_t fileMaxRegions = bits::roundUp(maxBytes, sizeQuantum) / sizeQuantum;
  for (auto i = 0; i < numShards_; ++i) {
    files_.push_back(std::make_unique<SsdFile>(
        fmt::format("{}{}", filePrefix_, i),
        i,
        fileMaxRegions,
        checkpointIntervalBytes / numShards));
  }
}

SsdFile& SsdCache::file(uint64_t fileId) {
  auto index = fileId % numShards_;
  return *files_[index];
}

bool SsdCache::startWrite() {
  if (isShutdown_) {
    return false;
  }
  if (0 == writesInProgress_.fetch_add(numShards_)) {
    // No write was pending, so now all shards are counted as writing.
    return true;
  }
  // There were writes in progress, so compensate for the increment.
  writesInProgress_.fetch_sub(numShards_);
  return false;
}

void SsdCache::write(std::vector<CachePin> pins) {
  VELOX_CHECK_LE(numShards_, writesInProgress_);
  uint64_t bytes = 0;
  auto start = getCurrentTimeMicro();
  std::vector<std::vector<CachePin>> shards(numShards_);
  for (auto& pin : pins) {
    bytes += pin.checkedEntry()->size();
    auto& target = file(pin.checkedEntry()->key().fileNum.id());
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

    // We move the mutable vector of pins to the executor. These must
    // be wrapped in a shared struct to be passed via lambda capture.
    auto pinHolder = std::make_shared<PinHolder>(std::move(shards[i]));
    executor_->add([this, i, pinHolder, bytes, start]() {
      try {
        files_[i]->write(pinHolder->pins);
      } catch (const std::exception& e) {
        // Catch so as not to miss updating 'writesInProgress_'. Could
        // theoretically happen for std::bad_alloc or such.
        LOG(INFO) << "Ignoring error in SsdFile::write: " << e.what();
      }
      if (--writesInProgress_ == 0) {
        // Typically occurs every few GB. Allows detecting unusually slow rates
        // from failing devices.
        LOG(INFO) << fmt::format(
            "SSDCA: Wrote {}MB, {} MB/s",
            bytes >> 20,
            static_cast<float>(bytes) / (getCurrentTimeMicro() - start));
      }
    });
  }
  writesInProgress_.fetch_sub(numNoStore);
}

SsdCacheStats SsdCache::stats() const {
  SsdCacheStats stats;
  for (auto& file : files_) {
    file->updateStats(stats);
  }
  return stats;
}

void SsdCache::clear() {
  for (auto& file : files_) {
    file->clear();
  }
}

std::string SsdCache::toString() const {
  auto data = stats();
  uint64_t capacity = maxBytes();
  std::stringstream out;
  out << "Ssd cache IO: Write " << (data.bytesWritten >> 20) << "MB read "
      << (data.bytesRead >> 20) << "MB Size " << (capacity >> 30)
      << "GB Occupied " << (data.bytesCached >> 30) << "GB";
  out << (data.entriesCached >> 10) << "K entries.";
  out << "\nGroupStats: " << groupStats_->toString(capacity);
  return out.str();
}

void SsdCache::deleteFiles() {
  for (auto& file : files_) {
    file->deleteFile();
  }
}

void SsdCache::shutdown() {
  isShutdown_ = true;
  while (writesInProgress_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100)); // NOLINT
  }
  for (auto& file : files_) {
    file->checkpoint(true);
  }
}

} // namespace facebook::velox::cache
