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

#include <fmt/format.h>

#include "velox/dwio/common/BufferedInput.h"

DEFINE_bool(wsVRLoad, false, "Use WS VRead API to load");

namespace facebook::velox::dwio::common {

void BufferedInput::load(const LogType logType) {
  // no regions to load
  if (regions_.size() == 0) {
    return;
  }

  offsets_.clear();
  buffers_.clear();
  allocPool_->clear();

  sortRegions();
  mergeRegions();

  // After sorting and merging we have the accurate sizes
  offsets_.reserve(regions_.size());
  buffers_.reserve(regions_.size());

  if (useVRead()) {
    std::vector<void*> buffers;
    buffers.reserve(regions_.size());
    loadWithAction(
        logType,
        [&buffers](
            void* buf, uint64_t /* length */, uint64_t /* offset */, LogType) {
          buffers.push_back(buf);
        });

    // Now we have all buffers and regions, load it in parallel
    input_->vread(buffers, regions_, logType);
  } else {
    loadWithAction(
        logType,
        [this](void* buf, uint64_t length, uint64_t offset, LogType type) {
          input_->read(buf, length, offset, type);
        });
  }

  // clear the loaded regions
  regions_.clear();
}

std::unique_ptr<SeekableInputStream> BufferedInput::enqueue(
    Region region,
    const dwio::common::StreamIdentifier* /*si*/) {
  if (region.length == 0) {
    return std::make_unique<SeekableArrayInputStream>(
        static_cast<const char*>(nullptr), 0);
  }

  // if the region is already in buffer - such as metadata
  auto ret = readBuffer(region.offset, region.length);
  if (ret) {
    return ret;
  }

  // push to region pool and give the caller the callback
  regions_.push_back(region);
  return std::make_unique<SeekableArrayInputStream>(
      // Save "i", the position in which this region was enqueued. This will
      // help faster lookup using enqueuedToBufferOffset_ later.
      [region, this, i = regions_.size() - 1]() {
        return readInternal(region.offset, region.length, i);
      });
}

bool BufferedInput::useVRead() const {
  // Use value explicitly set by the user if any, otherwise use the GFLAG
  // We want to update this on every use for now because during the onboarding
  // to wsVRLoad=true we may change the value of this GFLAG programatically from
  // a config update so we can rollback fast from config without the need of a
  // deployment
  return wsVRLoad_.value_or(FLAGS_wsVRLoad);
}

// Sort regions and enqueuedToOffset in the same way
void BufferedInput::sortRegions() {
  auto& r = regions_;
  auto& e = enqueuedToBufferOffset_;

  e.resize(r.size());
  std::iota(e.begin(), e.end(), 0);

  if (std::is_sorted(r.cbegin(), r.cend())) {
    return;
  }

  // Sort indices from low to high regions
  // "e" will contain the positions to which each region should be sorted to
  std::sort(
      e.begin(), e.end(), [&](size_t a, size_t b) { return r[a] < r[b]; });

  // Now actually sort. This way we sorted and saved the mapping of the sort
  std::vector<Region> regions;
  regions.reserve(r.size());
  for (auto i : e) {
    regions.push_back(r[i]);
  }
  std::swap(r, regions);
}

void BufferedInput::mergeRegions() {
  auto& r = regions_;
  auto& e = enqueuedToBufferOffset_;
  size_t ia = 0;
  // We want to map here where each region ended in the final merged regions
  // vector.
  // For example, if this is the regions vector: {{6, 3}, {24, 3}, {3, 3}, {0,
  // 3}, {29, 3}} After sorting, "e" would look like this: [3,2,0,1,4]. Because
  // region in position number 3 ended up in position 0 and so on.
  // For a maxMergeDistance of 1, "te" will look like: [0,1,0,0,2], because
  // original regions 3, 2 and 0 were merged into a larger region, now in
  // position 0. The original region 1, became region 1, and original region 4
  // became region 2
  std::vector<size_t> te(e.size());

  DWIO_ENSURE(!r.empty(), "Assumes that there's at least one region");
  DWIO_ENSURE_GT(r[ia].length, 0, "invalid region");

  te[e[0]] = 0;
  for (size_t ib = 1; ib < r.size(); ++ib) {
    DWIO_ENSURE_GT(r[ib].length, 0, "invalid region");
    if (!tryMerge(r[ia], r[ib])) {
      r[++ia] = r[ib];
    }
    te[e[ib]] = ia;
  }
  // After merging, remove what's left.
  r.resize(ia + 1);
  std::swap(e, te);
}

void BufferedInput::loadWithAction(
    const LogType logType,
    std::function<void(void*, uint64_t, uint64_t, LogType)> action) {
  for (const auto& region : regions_) {
    readRegion(region, logType, action);
  }
}

bool BufferedInput::tryMerge(Region& first, const Region& second) {
  DWIO_ENSURE_GE(second.offset, first.offset, "regions should be sorted.");
  const int64_t gap = second.offset - first.offset - first.length;

  // Duplicate regions (extension==0) is the only case allowed to merge for
  // useVRead()
  const int64_t extension = gap + second.length;
  if (useVRead()) {
    return extension == 0;
  }

  // compare with 0 since it's comparison in different types
  if (gap < 0 || gap <= maxMergeDistance_) {
    // the second region is inside first one if extension is negative
    if (extension > 0) {
      first.length += extension;
      if ((input_->getStats() != nullptr) && gap > 0) {
        input_->getStats()->incRawOverreadBytes(gap);
      }
    }

    return true;
  }

  return false;
}

std::unique_ptr<SeekableInputStream> BufferedInput::readBuffer(
    uint64_t offset,
    uint64_t length) const {
  const auto result = readInternal(offset, length);

  auto size = std::get<1>(result);
  if (size == MAX_UINT64) {
    return {};
  }

  return std::make_unique<SeekableArrayInputStream>(std::get<0>(result), size);
}

std::tuple<const char*, uint64_t> BufferedInput::readInternal(
    uint64_t offset,
    uint64_t length,
    std::optional<size_t> i) const {
  // return dummy one for zero length stream
  if (length == 0) {
    return std::make_tuple(nullptr, 0);
  }

  std::optional<size_t> index;
  if (i.has_value()) {
    auto vi = i.value();
    // There's a possibility that our user enqueued, then tried to read before
    // calling load(). In that case, enqueuedToBufferOffset_ will be empty or
    // have the values from a previous load. So I want to make sure that he ends
    // up in a valid offset, and that this offset is <= offset. Otherwise we
    // just go for the binary search.
    if (vi < enqueuedToBufferOffset_.size() &&
        enqueuedToBufferOffset_[vi] < offsets_.size() &&
        offsets_[enqueuedToBufferOffset_[vi]] <= offset) {
      index = enqueuedToBufferOffset_[i.value()];
    }
  }
  if (!index.has_value()) {
    // Binary search to get the first fileOffset for which: offset < fileOffset
    auto it = std::upper_bound(offsets_.cbegin(), offsets_.cend(), offset);
    // If the first element was already greater than the target offset we don't
    // have it
    if (it != offsets_.cbegin()) {
      index = std::distance(offsets_.cbegin(), it) - 1;
    }
  }

  if (index.has_value()) {
    const uint64_t bufferOffset = offsets_[index.value()];
    const auto& buffer = buffers_[index.value()];
    if (bufferOffset + buffer.size() >= offset + length) {
      DWIO_ENSURE_LE(bufferOffset, offset, "Invalid offset for readInternal");
      DWIO_ENSURE_LE(
          (offset - bufferOffset) + length,
          buffer.size(),
          "Invalid readOffset for read Internal ",
          fmt::format(
              "{} {} {} {}", offset, bufferOffset, length, buffer.size()));

      return std::make_tuple(buffer.data() + (offset - bufferOffset), length);
    }
  }

  return std::make_tuple(nullptr, MAX_UINT64);
}

} // namespace facebook::velox::dwio::common
