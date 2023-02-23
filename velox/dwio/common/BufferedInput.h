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

#include "velox/dwio/common/DataBuffer.h"
#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/common/StreamIdentifier.h"

namespace facebook::velox::dwio::common {

class BufferedInput {
 public:
  constexpr static uint64_t kMaxMergeDistance = 1024 * 1024 * 1.25;

  BufferedInput(
      std::shared_ptr<ReadFile> readFile,
      memory::MemoryPool& pool,
      const MetricsLogPtr& metricsLog = MetricsLog::voidLog(),
      IoStatistics* FOLLY_NULLABLE stats = nullptr)
      : input_{std::make_shared<ReadFileInputStream>(
            std::move(readFile),
            metricsLog,
            stats)},
        pool_{pool} {}

  BufferedInput(
      std::shared_ptr<ReadFileInputStream> input,
      memory::MemoryPool& pool)
      : input_(std::move(input)), pool_(pool) {}

  BufferedInput(BufferedInput&&) = default;
  virtual ~BufferedInput() = default;

  BufferedInput(const BufferedInput&) = delete;
  BufferedInput& operator=(const BufferedInput&) = delete;
  BufferedInput& operator=(BufferedInput&&) = delete;

  virtual const std::string& getName() const {
    return input_->getName();
  }

  // The previous API was taking a vector of regions
  // Now we allow callers to enqueue region any time/place
  // and we do final load into buffer in 2 steps (enqueue....load)
  // 'si' allows tracking which streams actually get read. This may control
  // read-ahead and caching for BufferedInput implementations supporting these.
  virtual std::unique_ptr<SeekableInputStream> enqueue(
      Region region,
      const StreamIdentifier* FOLLY_NULLABLE si = nullptr);

  // load all regions to be read in an optimized way (IO efficiency)
  virtual void load(const LogType);

  virtual bool isBuffered(uint64_t offset, uint64_t length) const {
    return !!readBuffer(offset, length);
  }

  virtual std::unique_ptr<SeekableInputStream>
  read(uint64_t offset, uint64_t length, LogType logType) const {
    std::unique_ptr<SeekableInputStream> ret = readBuffer(offset, length);
    if (!ret) {
      VLOG(1) << "Unplanned read. Offset: " << offset << ", Length: " << length;
      // We cannot do enqueue/load here because load() clears previously loaded
      // data. TODO: figure out how we can use the data cache for
      // this access.
      ret = std::make_unique<SeekableFileInputStream>(
          input_, offset, length, pool_, logType, input_->getNaturalReadSize());
    }
    return ret;
  }

  // True if there is free memory for prefetching the stripe. This is
  // called to check if a stripe that is not next for read should be
  // prefetched. 'numPages' is added to the already enqueued pages, so
  // that this can be called also before enqueueing regions.
  virtual bool shouldPreload(int32_t /*numPages*/ = 0) {
    return false;
  }

  // True if caller should enqueue and load regions for stripe
  // metadata after reading a file footer. The stripe footers are
  // likely to be hit and should be read ahead of demand if
  // BufferedInput has background load.
  virtual bool shouldPrefetchStripes() const {
    return false;
  }

  virtual void setNumStripes(int32_t /*numStripes*/) {}

  // Create a new (clean) instance of BufferedInput sharing the same underlying
  // file and memory pool.  The enqueued regions are NOT copied.
  virtual std::unique_ptr<BufferedInput> clone() const {
    return std::make_unique<BufferedInput>(input_, pool_);
  }

  const std::shared_ptr<ReadFile>& getReadFile() const {
    return input_->getReadFile();
  }

  // Internal API, do not use outside Velox.
  const std::shared_ptr<ReadFileInputStream>& getInputStream() const {
    return input_;
  }

  virtual folly::Executor* FOLLY_NULLABLE executor() const {
    return nullptr;
  }

 protected:
  std::shared_ptr<ReadFileInputStream> input_;
  memory::MemoryPool& pool_;

 private:
  std::vector<uint64_t> offsets_;
  std::vector<DataBuffer<char>> buffers_;
  std::vector<Region> regions_;

  std::unique_ptr<SeekableInputStream> readBuffer(
      uint64_t offset,
      uint64_t length) const;
  std::tuple<const char*, uint64_t> readInternal(
      uint64_t offset,
      uint64_t length) const;

  void readRegion(
      const Region& region,
      const LogType logType,
      std::function<void(void* FOLLY_NONNULL, uint64_t, uint64_t, LogType)>
          action) {
    offsets_.push_back(region.offset);
    DataBuffer<char> buffer(pool_, region.length);

    // action is required
    DWIO_ENSURE_NOT_NULL(action);
    action(buffer.data(), region.length, region.offset, logType);

    buffers_.push_back(std::move(buffer));
  }

  // we either load data parallelly or sequentially according to flag
  void loadWithAction(
      const LogType logType,
      std::function<void(void* FOLLY_NONNULL, uint64_t, uint64_t, LogType)>
          action);

  // tries and merges WS read regions into one
  bool tryMerge(Region& first, const Region& second);
};

} // namespace facebook::velox::dwio::common
