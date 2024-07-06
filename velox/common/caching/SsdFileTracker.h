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

#include <cstdint>
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/Portability.h"

namespace facebook::velox::cache {

/// Tracks reads on an SsdFile. Reads are counted for fixed size regions and
/// periodically decayed. Not thread safe, synchronization is the caller's
/// responsibility.
class SsdFileTracker {
 public:
  void resize(int32_t numRegions) {
    resizeTsanAtomic(regionScores_, numRegions);
  }

  void regionRead(int32_t region, int32_t bytes) {
    regionScores_[region] += bytes;
  }

  void regionCleared(int32_t region) {
    regionScores_[region] = 0;
  }

  // Marks that a region has been filled and transits from writable to
  // evictable. Set its score to be at least the best score + a small margin so
  // that it gets time to live. Otherwise, it has had the least time to get hits
  // and would be the first evicted.
  void regionFilled(int32_t region);

  // Increments event count and periodically decays
  // scores. 'totalEntries' is the count of distinct entries in the
  // tracked file.
  void fileTouched(int32_t totalEntries);

  /// Returns up to 'numCandidates' least used regions. 'numRegions' is the
  /// count of existing regions. This can be less than the size of the tracker
  /// if the file cannot grow to full size. Regions with a non-zero count in
  /// 'regionPins' are not considered.
  std::vector<int32_t> findEvictionCandidates(
      int32_t numCandidates,
      int32_t numRegions,
      const std::vector<int32_t>& regionPins);

  // Expose the region access data. Used in checkpointing cache state.
  std::vector<tsan_atomic<double>>& regionScores() {
    return regionScores_;
  }

  void setRegionScores(const std::vector<double>& scores) {
    VELOX_CHECK_EQ(scores.size(), regionScores_.size());
    for (auto i = 0; i < scores.size(); ++i) {
      regionScores_[i] = scores[i];
    }
  }

  /// Exports a copy of the scores. Tsan will report an error if a
  /// pointer to atomics is passed to write(). Therefore copy the
  /// atomics into non-atomics before writing.
  std::vector<double> copyScores() {
    std::vector<double> scores(regionScores_.size());
    for (auto i = 0; i < scores.size(); ++i) {
      scores[i] = tsanAtomicValue(regionScores_[i]);
    }
    return scores;
  }

  /// Resets scores of all regions.
  ///
  /// NOTE: this is only used by test and Prestissimo worker operation.
  void clear() {
    std::fill(regionScores_.begin(), regionScores_.end(), 0);
  }

 private:
  static constexpr int32_t kDecayInterval = 1000;

  std::vector<tsan_atomic<double>> regionScores_;

  // Count of lookups. The scores are decayed every time the count goes
  // over kDecayInterval or half count of cache entries, whichever comes first.
  uint64_t numTouches_{0};
};

} // namespace facebook::velox::cache
