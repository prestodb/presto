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

#include <folly/Executor.h>

#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/FileGroupStats.h"
#include "velox/common/caching/ScanTracker.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/common/io/Options.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/CacheInputStream.h"
#include "velox/dwio/common/InputStream.h"

namespace facebook::velox::dwio::common {

struct LoadRequest {
  LoadRequest() = default;
  LoadRequest(velox::common::Region& _region, cache::TrackingId _trackingId)
      : region(_region), trackingId(_trackingId) {}

  velox::common::Region region;
  cache::TrackingId trackingId;
  bool processed{false};

  const SeekableInputStream* stream;

  /// Buffers to be handed to 'stream' after load.
  memory::Allocation data;
  std::string tinyData;
  /// Number of bytes in 'data/tinyData'.
  int32_t loadSize{0};
};

/// Represents planned loads that should be performed as a single IO.
class DirectCoalescedLoad : public cache::CoalescedLoad {
 public:
  DirectCoalescedLoad(
      std::shared_ptr<ReadFileInputStream> input,
      std::shared_ptr<IoStatistics> ioStats,
      uint64_t groupId,
      const std::vector<LoadRequest*>& requests,
      memory::MemoryPool& pool,
      int32_t loadQuantum)
      : CoalescedLoad({}, {}),
        ioStats_(ioStats),
        groupId_(groupId),
        input_(std::move(input)),
        loadQuantum_(loadQuantum),
        pool_(pool) {
    requests_.reserve(requests.size());
    for (auto i = 0; i < requests.size(); ++i) {
      requests_.push_back(std::move(*requests[i]));
    }
  };

  /// Loads the regions. Returns {} since no cache entries are made. The loaded
  /// data is retrieved with getData().
  std::vector<cache::CachePin> loadData(bool prefetch) override;

  /// Returns the buffer for 'region' in either 'data' or 'tinyData'. 'region'
  /// must match a region given to DirectBufferedInput::enqueue().
  int32_t
  getData(int64_t offset, memory::Allocation& data, std::string& tinyData);

  const std::vector<LoadRequest>& requests() {
    return requests_;
  }

  int64_t size() const override {
    int64_t size = 0;
    for (auto& request : requests_) {
      size += request.region.length;
    }
    return size;
  }

 private:
  const std::shared_ptr<IoStatistics> ioStats_;
  const uint64_t groupId_;
  const std::shared_ptr<ReadFileInputStream> input_;
  const int32_t loadQuantum_;
  memory::MemoryPool& pool_;
  std::vector<LoadRequest> requests_;
};

class DirectBufferedInput : public BufferedInput {
 public:
  static constexpr int32_t kTinySize = 2'000;

  DirectBufferedInput(
      std::shared_ptr<ReadFile> readFile,
      const MetricsLogPtr& metricsLog,
      uint64_t fileNum,
      std::shared_ptr<cache::ScanTracker> tracker,
      uint64_t groupId,
      std::shared_ptr<IoStatistics> ioStats,
      folly::Executor* executor,
      const io::ReaderOptions& readerOptions)
      : BufferedInput(
            std::move(readFile),
            readerOptions.memoryPool(),
            metricsLog),
        fileNum_(fileNum),
        tracker_(std::move(tracker)),
        groupId_(groupId),
        ioStats_(std::move(ioStats)),
        executor_(executor),
        fileSize_(input_->getLength()),
        options_(readerOptions) {}

  ~DirectBufferedInput() override {
    for (auto& load : coalescedLoads_) {
      load->cancel();
    }
  }

  std::unique_ptr<SeekableInputStream> enqueue(
      velox::common::Region region,
      const StreamIdentifier* sid) override;

  bool supportSyncLoad() const override {
    return false;
  }

  void load(const LogType /*unused*/) override;

  bool isBuffered(uint64_t offset, uint64_t length) const override;

  bool shouldPreload(int32_t numPages = 0) override;

  bool shouldPrefetchStripes() const override {
    return false;
  }

  void setNumStripes(int32_t numStripes) override {
    auto* stats = tracker_->fileGroupStats();
    if (stats) {
      stats->recordFile(fileNum_, groupId_, numStripes);
    }
  }

  virtual std::unique_ptr<BufferedInput> clone() const override {
    std::unique_ptr<DirectBufferedInput> input(new DirectBufferedInput(
        input_, fileNum_, tracker_, groupId_, ioStats_, executor_, options_));
    return input;
  }

  memory::MemoryPool* pool() {
    return pool_;
  }

  /// Returns the CoalescedLoad that contains the correlated loads for
  /// 'stream' or nullptr if none. Returns nullptr on all but first
  /// call for 'stream' since the load is to be triggered by the first
  /// access.
  std::shared_ptr<DirectCoalescedLoad> coalescedLoad(
      const SeekableInputStream* stream);

  std::unique_ptr<SeekableInputStream>
  read(uint64_t offset, uint64_t length, LogType logType) const override;

  folly::Executor* executor() const override {
    return executor_;
  }

  uint64_t nextFetchSize() const override {
    VELOX_NYI();
  }

 private:
  /// Constructor used by clone().
  DirectBufferedInput(
      std::shared_ptr<ReadFileInputStream> input,
      uint64_t fileNum,
      std::shared_ptr<cache::ScanTracker> tracker,
      uint64_t groupId,
      std::shared_ptr<IoStatistics> ioStats,
      folly::Executor* executor,
      const io::ReaderOptions& readerOptions)
      : BufferedInput(std::move(input), readerOptions.memoryPool()),
        fileNum_(fileNum),
        tracker_(std::move(tracker)),
        groupId_(groupId),
        ioStats_(std::move(ioStats)),
        executor_(executor),
        fileSize_(input_->getLength()),
        options_(readerOptions) {}

  // Sorts requests and makes CoalescedLoads for nearby requests. If 'prefetch'
  // is true, starts background loading.
  void makeLoads(std::vector<LoadRequest*> requests, bool prefetch);

  // Makes a CoalescedLoad for 'requests' to be read together, coalescing IO if
  // appropriate. If 'prefetch' is set, schedules the CoalescedLoad on
  // 'executor_'. Links the CoalescedLoad  to all DirectInputStreams that it
  // covers.
  void readRegion(std::vector<LoadRequest*> requests, bool prefetch);

  const uint64_t fileNum_;
  const std::shared_ptr<cache::ScanTracker> tracker_;
  const uint64_t groupId_;
  const std::shared_ptr<IoStatistics> ioStats_;
  folly::Executor* const executor_;
  const uint64_t fileSize_;

  // Regions that are candidates for loading.
  std::vector<LoadRequest> requests_;

  // Coalesced loads spanning multiple streams in one IO.
  folly::Synchronized<folly::F14FastMap<
      const SeekableInputStream*,
      std::shared_ptr<DirectCoalescedLoad>>>
      streamToCoalescedLoad_;

  // Distinct coalesced loads in 'coalescedLoads_'.
  std::vector<std::shared_ptr<cache::CoalescedLoad>> coalescedLoads_;

  io::ReaderOptions options_;
};

} // namespace facebook::velox::dwio::common
