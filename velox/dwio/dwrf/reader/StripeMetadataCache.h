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

#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/dwrf/common/Common.h"

namespace facebook::velox::dwrf {

constexpr uint64_t INVALID_INDEX = std::numeric_limits<uint64_t>::max();

class StripeMetadataCache {
 public:
  StripeMetadataCache(
      StripeCacheMode mode,
      const Footer& footer,
      std::shared_ptr<dwio::common::DataBuffer<char>> buffer)
      : StripeMetadataCache{mode, std::move(buffer), getOffsets(footer)} {}

  StripeMetadataCache(
      StripeCacheMode mode,
      std::shared_ptr<dwio::common::DataBuffer<char>> buffer,
      std::vector<uint32_t>&& offsets)
      : mode_{mode}, buffer_{std::move(buffer)}, offsets_{std::move(offsets)} {}

  ~StripeMetadataCache() = default;

  StripeMetadataCache(const StripeMetadataCache&) = delete;
  StripeMetadataCache(StripeMetadataCache&&) = delete;
  StripeMetadataCache& operator=(const StripeMetadataCache&) = delete;
  StripeMetadataCache& operator=(StripeMetadataCache&&) = delete;

  bool has(StripeCacheMode mode, uint64_t stripeIndex) const {
    return getIndex(mode, stripeIndex) != INVALID_INDEX;
  }

  std::unique_ptr<dwio::common::SeekableArrayInputStream> get(
      StripeCacheMode mode,
      uint64_t stripeIndex) const {
    auto index = getIndex(mode, stripeIndex);
    if (index != INVALID_INDEX) {
      auto offset = offsets_[index];
      return std::make_unique<dwio::common::SeekableArrayInputStream>(
          buffer_->data() + offset, offsets_[index + 1] - offset);
    }
    return {};
  }

 private:
  StripeCacheMode mode_;
  std::shared_ptr<dwio::common::DataBuffer<char>> buffer_;

  std::vector<uint32_t> offsets_;

  uint64_t getIndex(StripeCacheMode mode, uint64_t stripeIndex) const {
    if (mode_ & mode) {
      uint64_t index =
          (mode_ == mode ? stripeIndex
                         : stripeIndex * 2 + mode - StripeCacheMode::INDEX);
      // offsets has N + 1 items, so length[N] = offset[N+1]- offset[N]
      if (index < offsets_.size() - 1) {
        return index;
      }
    }
    return INVALID_INDEX;
  }

  std::vector<uint32_t> getOffsets(const Footer& footer) {
    std::vector<uint32_t> offsets;
    offsets.reserve(footer.stripeCacheOffsetsSize());
    const auto& from = footer.stripeCacheOffsets();
    offsets.assign(from.begin(), from.end());
    return offsets;
  }
};

} // namespace facebook::velox::dwrf
