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

#include "velox/dwio/dwrf/writer/StatisticsBuilder.h"

namespace facebook::velox::dwrf {

class PhysicalSizeAggregator {
 public:
  explicit PhysicalSizeAggregator(PhysicalSizeAggregator* parent = nullptr)
      : parent_{parent} {}

  virtual ~PhysicalSizeAggregator() = default;

  virtual void recordSize(const DwrfStreamIdentifier& id, uint64_t streamSize) {
    result_ += streamSize;
    if (parent_) {
      parent_->recordSize(id, streamSize);
    }
  }

  uint64_t getResult() {
    return result_;
  }

 private:
  uint64_t result_{0};
  PhysicalSizeAggregator* parent_;
};

class MapPhysicalSizeAggregator : public PhysicalSizeAggregator {
 public:
  explicit MapPhysicalSizeAggregator(PhysicalSizeAggregator* parent = nullptr)
      : PhysicalSizeAggregator{parent} {}

  virtual ~MapPhysicalSizeAggregator() = default;

  void recordSize(const DwrfStreamIdentifier& id, uint64_t streamSize)
      override {
    PhysicalSizeAggregator::recordSize(id, streamSize);
  }

  void prepare(
      folly::F14FastMap<uint32_t, const proto::KeyInfo&> sequenceToKey,
      MapStatisticsBuilder* mapStatsBuilder) {
    sequenceToKey_ = std::move(sequenceToKey);
    mapStatsBuilder_ = mapStatsBuilder;
  }

 private:
  folly::F14FastMap<uint32_t, const proto::KeyInfo&> sequenceToKey_;
  MapStatisticsBuilder* mapStatsBuilder_{nullptr};
};

} // namespace facebook::velox::dwrf
