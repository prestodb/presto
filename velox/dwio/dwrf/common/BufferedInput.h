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
#include "velox/dwio/common/Options.h"
#include "velox/dwio/dwrf/common/Common.h"
#include "velox/dwio/dwrf/common/InputStream.h"

namespace facebook::velox::dwrf {

class BufferedInput {
 public:
  constexpr static uint64_t kMaxMergeDistance = 1024 * 1024 * 1.25;
  BufferedInput(dwio::common::InputStream& input, memory::MemoryPool& pool)
      : input_{input}, pool_{pool} {}

  BufferedInput(BufferedInput&&) = default;
  virtual ~BufferedInput() = default;

  BufferedInput(const BufferedInput&) = delete;
  BufferedInput& operator=(const BufferedInput&) = delete;
  BufferedInput& operator=(BufferedInput&&) = delete;

  virtual const std::string& getName() const {
    return input_.getName();
  }

  // The previous API was taking a vector of regions
  // Now we allow callers to enqueue region any time/place
  // and we do final load into buffer in 2 steps (enqueue....load)
  // 'si' allows tracking which streams actually get read. This may control
  // read-ahead and caching for BufferedInput implementations supporting these.
  virtual std::unique_ptr<SeekableInputStream> enqueue(
      dwio::common::Region region,
      const dwio::common::StreamIdentifier* FOLLY_NULLABLE si = nullptr);

  // load all regions to be read in an optimized way (IO efficiency)
  virtual void load(const dwio::common::LogType);

  virtual bool isBuffered(uint64_t offset, uint64_t length) const {
    return !!readBuffer(offset, length);
  }

  virtual std::unique_ptr<SeekableInputStream>
  read(uint64_t offset, uint64_t length, dwio::common::LogType logType) const {
    std::unique_ptr<SeekableInputStream> ret = readBuffer(offset, length);
    if (!ret) {
      VLOG(1) << "Unplanned read. Offset: " << offset << ", Length: " << length;
      // We cannot do enqueue/load here because load() clears previously laoded
      // data. TODO: figure out how we can use the data cache for
      // this access.
      ret = std::make_unique<SeekableFileInputStream>(
          input_, offset, length, pool_, logType);
    }
    return ret;
  }

  // True if there is free memory for prefetching the stripe. This is
  // called to check if a stripe that is not next for read should be
  // prefetched.
  virtual bool shouldPreload() {
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

 protected:
  dwio::common::InputStream& input_;

 private:
  memory::MemoryPool& pool_;
  std::vector<uint64_t> offsets_;
  std::vector<dwio::common::DataBuffer<char>> buffers_;
  std::vector<dwio::common::Region> regions_;

  std::unique_ptr<SeekableInputStream> readBuffer(
      uint64_t offset,
      uint64_t length) const;
  std::tuple<const char*, uint64_t> readInternal(
      uint64_t offset,
      uint64_t length) const;

  void readRegion(
      const dwio::common::Region& region,
      const dwio::common::LogType logType,
      std::function<
          void(void* FOLLY_NONNULL, uint64_t, uint64_t, dwio::common::LogType)>
          action) {
    offsets_.push_back(region.offset);
    dwio::common::DataBuffer<char> buffer(pool_, region.length);

    // action is required
    DWIO_ENSURE_NOT_NULL(action);
    action(buffer.data(), region.length, region.offset, logType);

    buffers_.push_back(std::move(buffer));
  }

  // we either load data parallelly or sequentially according to flag
  void loadWithAction(
      const dwio::common::LogType logType,
      std::function<
          void(void* FOLLY_NONNULL, uint64_t, uint64_t, dwio::common::LogType)>
          action);

  // tries and merges WS read regions into one
  bool tryMerge(
      dwio::common::Region& first,
      const dwio::common::Region& second);
};

class BufferedInputFactory {
 public:
  virtual ~BufferedInputFactory() = default;

  virtual std::unique_ptr<BufferedInput> create(
      dwio::common::InputStream& input,
      velox::memory::MemoryPool& pool,
      uint64_t /*fileNum*/) const {
    return std::make_unique<BufferedInput>(input, pool);
  }

  // Returns an executor for parallelizing IO. The executor is
  // expected to have an indefinite lifetime and the actions queued on
  // the executor must be self-contained and capture and own such
  // context as they need and not rely on a BufferedInput being live.
  virtual folly::Executor* FOLLY_NULLABLE executor() const {
    return nullptr;
  }

  // Returns a static factory for producing basic BufferedInput instances.
  static std::shared_ptr<BufferedInputFactory> baseFactoryShared();

  static BufferedInputFactory* FOLLY_NONNULL baseFactory() {
    return baseFactoryShared().get();
  }
};

} // namespace facebook::velox::dwrf
