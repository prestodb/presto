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

#include "velox/common/caching/FileIds.h"
#include "velox/common/caching/ScanTracker.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/dwio/common/InputStream.h"
#include "velox/dwio/common/IoStatistics.h"
#include "velox/dwio/common/SeekableInputStream.h"

namespace facebook::velox::dwio::common {

class CachedBufferedInput;

class CacheInputStream : public SeekableInputStream {
 public:
  CacheInputStream(
      CachedBufferedInput* cache,
      IoStatistics* ioStats,
      const Region& region,
      InputStream& input,
      uint64_t fileNum,
      std::shared_ptr<cache::ScanTracker> tracker,
      cache::TrackingId trackingId,
      uint64_t groupId,
      int32_t loadQuantum);

  bool Next(const void** data, int* size) override;
  void BackUp(int count) override;
  bool Skip(int count) override;
  google::protobuf::int64 ByteCount() const override;
  void seekToPosition(PositionProvider& position) override;
  std::string getName() const override;
  size_t positionSize() override;

  /// Returns a copy of 'this', ranging over the same bytes. The clone
  /// is initially positioned at the position of 'this' and can be
  /// moved independently within 'region_'.  This is used for first
  /// caching a range of bytes from a file and then giving out
  /// delimited subranges of this to callers. skip() and
  /// setRemainingBytes() set the bounds of the window exposed by the
  /// clone. In specific, reading protocol buffers requires a stream
  /// that begins and ends at the exact start and end of the
  /// serialization. Reading these from cache requires an exactly
  /// delimited stream.
  std::unique_ptr<CacheInputStream> clone() {
    auto copy = std::make_unique<CacheInputStream>(
        bufferedInput_,
        ioStats_,
        region_,
        input_,
        fileNum_,
        tracker_,
        trackingId_,
        groupId_,
        loadQuantum_);
    copy->position_ = position_;
    return copy;
  }

  /// Sets the stream to range over a window that starts at the current position
  /// and is 'remainingBytes' bytes in size. 'remainingBytes' must be <=
  /// 'region_.length - position_'. The stream cannot be used for reading
  /// outside of the window. Use together wiht clone() and skip().
  void setRemainingBytes(uint64_t remainingBytes);

 private:
  // Ensures that the current position is covered by 'pin_'.
  void loadPosition();

  // Synchronously sets 'pin_' to cover 'region'.
  void loadSync(Region region);

  // Returns true if there is an SSD cache and 'entry' is present there and
  // successfully loaded.
  bool loadFromSsd(Region region, cache::AsyncDataCacheEntry& entry);

  CachedBufferedInput* const bufferedInput_;
  cache::AsyncDataCache* const cache_;
  IoStatistics* ioStats_;
  InputStream& input_;
  // The region of 'input' 'this' ranges over.
  const Region region_;
  const uint64_t fileNum_;
  std::shared_ptr<cache::ScanTracker> tracker_;
  const cache::TrackingId trackingId_;
  const uint64_t groupId_;

  // Maximum number of bytes read from 'input' at a time. This gives the maximum
  // pin_.entry()->size().
  const int32_t loadQuantum_;

  // Handle of cache entry.
  cache::CachePin pin_;

  // Offset of current run from start of 'entry_->data()'
  uint64_t offsetOfRun_;

  // Pointer  to start of  current run in 'entry->data()' or
  // 'entry->tinyData()'.
  uint8_t* run_{nullptr};
  // Position of stream relative to 'run_'.
  int offsetInRun_{0};
  // Index of run in 'entry_->data()'
  int runIndex_ = -1;
  // Number of valid bytes above 'run_'.
  uint32_t runSize_ = 0;
  // Position relative to 'region_.offset'.
  uint64_t position_ = 0;

  // A restricted view over 'region'. offset is relative to 'region_'. A cloned
  // CacheInputStream can cover a subrange of the range of the original.
  std::optional<Region> window_;
};

} // namespace facebook::velox::dwio::common
