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

#include "velox/common/memory/AllocationPool.h"
#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/common/StreamIdentifier.h"

// Use WS VRead API to load
DECLARE_bool(wsVRLoad);

namespace facebook::velox::dwio::common {

class BufferedInput {
 public:
  constexpr static uint64_t kMaxMergeDistance = 1024 * 1024 * 1.25;

  BufferedInput(
      std::shared_ptr<ReadFile> readFile,
      memory::MemoryPool& pool,
      const MetricsLogPtr& metricsLog = MetricsLog::voidLog(),
      IoStatistics* stats = nullptr,
      uint64_t maxMergeDistance = kMaxMergeDistance,
      std::optional<bool> wsVRLoad = std::nullopt)
      : BufferedInput(
            std::make_shared<ReadFileInputStream>(
                std::move(readFile),
                metricsLog,
                stats),
            pool,
            maxMergeDistance,
            wsVRLoad) {}

  BufferedInput(
      std::shared_ptr<ReadFileInputStream> input,
      memory::MemoryPool& pool,
      uint64_t maxMergeDistance = kMaxMergeDistance,
      std::optional<bool> wsVRLoad = std::nullopt)
      : input_{std::move(input)},
        pool_{pool},
        maxMergeDistance_{maxMergeDistance},
        wsVRLoad_{wsVRLoad},
        allocPool_{std::make_unique<memory::AllocationPool>(&pool)} {}

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
  // read-ahead and caching for BufferedInput implementations supporting
  // these.
  virtual std::unique_ptr<SeekableInputStream> enqueue(
      velox::common::Region region,
      const StreamIdentifier* si = nullptr);

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
      // We cannot do enqueue/load here because load() clears previously
      // loaded data. TODO: figure out how we can use the data cache for
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

  // Create a new (clean) instance of BufferedInput sharing the same
  // underlying file and memory pool.  The enqueued regions are NOT copied.
  virtual std::unique_ptr<BufferedInput> clone() const {
    return std::make_unique<BufferedInput>(input_, pool_);
  }

  std::unique_ptr<SeekableInputStream> loadCompleteFile() {
    auto stream = enqueue({0, input_->getLength()});
    load(dwio::common::LogType::FILE);
    return stream;
  }

  const std::shared_ptr<ReadFile>& getReadFile() const {
    return input_->getReadFile();
  }

  // Internal API, do not use outside Velox.
  const std::shared_ptr<ReadFileInputStream>& getInputStream() const {
    return input_;
  }

  virtual folly::Executor* executor() const {
    return nullptr;
  }

  virtual int64_t prefetchSize() const {
    return 0;
  }

 protected:
  std::shared_ptr<ReadFileInputStream> input_;
  memory::MemoryPool& pool_;

 private:
  uint64_t maxMergeDistance_;
  std::optional<bool> wsVRLoad_;
  std::unique_ptr<memory::AllocationPool> allocPool_;

  // Regions enqueued for reading
  std::vector<velox::common::Region> regions_;

  // Offsets in the file to which the corresponding Region belongs
  std::vector<uint64_t> offsets_;

  // Buffers allocated for reading each Region.
  std::vector<folly::Range<char*>> buffers_;

  // Maps the position in which the Region was originally enqueued to the
  // position that it went to after sorting and merging. Thus this maps from the
  // enqueued position to its corresponding buffer offset.
  std::vector<size_t> enqueuedToBufferOffset_;

  std::unique_ptr<SeekableInputStream> readBuffer(
      uint64_t offset,
      uint64_t length) const;
  std::tuple<const char*, uint64_t> readInternal(
      uint64_t offset,
      uint64_t length,
      std::optional<size_t> i = std::nullopt) const;
  void readToBuffer(
      uint64_t offset,
      folly::Range<char*> allocated,
      const LogType logType);

  folly::Range<char*> allocate(const velox::common::Region& region) {
    // Save the file offset and the buffer to which we'll read it
    offsets_.push_back(region.offset);
    buffers_.emplace_back(
        allocPool_->allocateFixed(region.length), region.length);
    return folly::Range<char*>(buffers_.back().data(), region.length);
  }

  bool useVRead() const;
  void sortRegions();
  void mergeRegions();

  // tries and merges WS read regions into one
  bool tryMerge(
      velox::common::Region& first,
      const velox::common::Region& second);
};

} // namespace facebook::velox::dwio::common
