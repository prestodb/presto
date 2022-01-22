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

#include "velox/common/caching/ScanTracker.h"

namespace facebook::velox::cache {

// Dummy implementation of SsdCache admission stats.
class FileGroupStats {
 public:
  // Returns true if groupId, trackingId qualify the data to be cached to SSD.
  bool shouldSaveToSsd(uint64_t /*groupId*/, TrackingId /*trackingId*/) const {
    return true;
  }

  // Updates the SSD selection criteria. 'ssdsize' is the capacity,
  // 'decayPct' gives by how much old accesses are discounted.
  void updateSsdFilter(uint64_t /*ssdSize*/, int32_t /*decayPct*/ = 0) {}

  // Recalculates the best groups and makes a human readable
  // summary. 'cacheBytes' is used to compute what fraction of the tracked
  // working set can be cached in 'cacheBytes'.
  std::string toString(uint64_t /*cacheBytes*/) {
    return "<dummy FileGroupStats>";
  }
};

} // namespace facebook::velox::cache
