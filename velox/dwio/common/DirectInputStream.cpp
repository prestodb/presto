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

#include <folly/executors/QueuedImmediateExecutor.h>

#include "velox/common/process/TraceContext.h"
#include "velox/common/time/Timer.h"
#include "velox/dwio/common/DirectBufferedInput.h"
#include "velox/dwio/common/DirectInputStream.h"

using ::facebook::velox::common::Region;

namespace facebook::velox::dwio::common {

using velox::cache::ScanTracker;
using velox::cache::TrackingId;
using velox::memory::MemoryAllocator;

DirectInputStream::DirectInputStream(
    DirectBufferedInput* bufferedInput,
    IoStatistics* ioStats,
    const Region& region,
    std::shared_ptr<ReadFileInputStream> input,
    uint64_t fileNum,
    std::shared_ptr<ScanTracker> tracker,
    TrackingId trackingId,
    uint64_t groupId,
    int32_t loadQuantum)
    : bufferedInput_(bufferedInput),
      ioStats_(ioStats),
      input_(std::move(input)),
      region_(region),
      fileNum_(fileNum),
      tracker_(std::move(tracker)),
      trackingId_(trackingId),
      groupId_(groupId),
      loadQuantum_(loadQuantum) {}

bool DirectInputStream::Next(const void** buffer, int32_t* size) {
  if (offsetInRegion_ >= region_.length) {
    *size = 0;
    return false;
  }
  loadPosition();

  *buffer = reinterpret_cast<const void**>(run_ + offsetInRun_);
  *size = runSize_ - offsetInRun_;
  if (offsetInRegion_ + *size > region_.length) {
    *size = region_.length - offsetInRegion_;
  }
  offsetInRun_ += *size;
  offsetInRegion_ += *size;

  if (tracker_) {
    tracker_->recordRead(trackingId_, *size, fileNum_, groupId_);
  }
  return true;
}

void DirectInputStream::BackUp(int32_t count) {
  VELOX_CHECK_GE(count, 0, "can't backup negative distances");

  const uint64_t unsignedCount = static_cast<uint64_t>(count);
  VELOX_CHECK_LE(unsignedCount, offsetInRun_, "Can't backup that much!");
  offsetInRegion_ -= unsignedCount;
}

bool DirectInputStream::SkipInt64(int64_t count) {
  if (count < 0) {
    return false;
  }
  const uint64_t unsignedCount = static_cast<uint64_t>(count);
  if (unsignedCount + offsetInRegion_ <= region_.length) {
    offsetInRegion_ += unsignedCount;
    return true;
  }
  offsetInRegion_ = region_.length;
  return false;
}

google::protobuf::int64 DirectInputStream::ByteCount() const {
  return static_cast<google::protobuf::int64>(offsetInRegion_);
}

void DirectInputStream::seekToPosition(PositionProvider& seekPosition) {
  offsetInRegion_ = seekPosition.next();
  VELOX_CHECK_LE(offsetInRegion_, region_.length);
}

std::string DirectInputStream::getName() const {
  return fmt::format(
      "DirectInputStream {} of {}", offsetInRegion_, region_.length);
}

size_t DirectInputStream::positionSize() {
  // not compressed, so only need 1 position (uncompressed position)
  return 1;
}

namespace {
std::vector<folly::Range<char*>>
makeRanges(size_t size, memory::Allocation& data, std::string& tinyData) {
  std::vector<folly::Range<char*>> buffers;
  if (data.numPages() > 0) {
    buffers.reserve(data.numRuns());
    uint64_t offsetInRuns = 0;
    for (int i = 0; i < data.numRuns(); ++i) {
      auto run = data.runAt(i);
      uint64_t bytes = memory::AllocationTraits::pageBytes(run.numPages());
      uint64_t readSize = std::min(bytes, size - offsetInRuns);
      buffers.push_back(folly::Range<char*>(run.data<char>(), readSize));
      offsetInRuns += readSize;
    }
  } else {
    buffers.push_back(folly::Range<char*>(tinyData.data(), size));
  }
  return buffers;
}
} // namespace

void DirectInputStream::loadSync() {
  if (region_.length < DirectBufferedInput::kTinySize &&
      data_.numPages() == 0) {
    tinyData_.resize(region_.length);
  } else {
    const auto numPages =
        memory::AllocationTraits::numPages(loadedRegion_.length);
    if (numPages > data_.numPages()) {
      bufferedInput_->pool()->allocateNonContiguous(numPages, data_);
    }
  }

  process::TraceContext trace("DirectInputStream::loadSync");

  ioStats_->incRawBytesRead(loadedRegion_.length);
  auto ranges = makeRanges(loadedRegion_.length, data_, tinyData_);
  uint64_t usecs = 0;
  {
    MicrosecondTimer timer(&usecs);
    input_->read(ranges, loadedRegion_.offset, LogType::FILE);
  }
  ioStats_->read().increment(loadedRegion_.length);
  ioStats_->queryThreadIoLatency().increment(usecs);
  ioStats_->incTotalScanTime(usecs * 1'000);
}

void DirectInputStream::loadPosition() {
  VELOX_CHECK_LT(offsetInRegion_, region_.length);
  if (!loaded_) {
    loaded_ = true;
    auto load = bufferedInput_->coalescedLoad(this);
    if (load != nullptr) {
      folly::SemiFuture<bool> waitFuture(false);
      uint64_t loadUs = 0;
      {
        MicrosecondTimer timer(&loadUs);
        if (!load->loadOrFuture(&waitFuture)) {
          waitFuture.wait();
        }
        loadedRegion_.offset = region_.offset;
        loadedRegion_.length = load->getData(region_.offset, data_, tinyData_);
      }
      ioStats_->queryThreadIoLatency().increment(loadUs);
    } else {
      // Standalone stream, not part of coalesced load.
      loadedRegion_.offset = 0;
      loadedRegion_.length = 0;
    }
  }

  // Check if position outside of loaded bounds.
  if (loadedRegion_.length == 0 ||
      region_.offset + offsetInRegion_ < loadedRegion_.offset ||
      region_.offset + offsetInRegion_ >=
          loadedRegion_.offset + loadedRegion_.length) {
    loadedRegion_.offset = region_.offset + offsetInRegion_;
    loadedRegion_.length = (offsetInRegion_ + loadQuantum_ <= region_.length)
        ? loadQuantum_
        : (region_.length - offsetInRegion_);

    // Since the loadSync method updates the metric, but it is conditionally
    // executed, we also need to update the metric in the loadData method.
    loadSync();
  }

  const auto offsetInData =
      offsetInRegion_ - (loadedRegion_.offset - region_.offset);
  if (data_.numPages() == 0) {
    run_ = reinterpret_cast<uint8_t*>(tinyData_.data());
    runSize_ = tinyData_.size();
    offsetInRun_ = offsetInData;
    offsetOfRun_ = 0;
  } else {
    data_.findRun(offsetInData, &runIndex_, &offsetInRun_);
    offsetOfRun_ = offsetInData - offsetInRun_;
    auto run = data_.runAt(runIndex_);
    run_ = run.data();
    runSize_ = memory::AllocationTraits::pageBytes(run.numPages());
    if (offsetOfRun_ + runSize_ > loadedRegion_.length) {
      runSize_ = loadedRegion_.length - offsetOfRun_;
    }
  }
  VELOX_CHECK_LT(offsetInRun_, runSize_);
}

} // namespace facebook::velox::dwio::common
