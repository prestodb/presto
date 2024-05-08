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

#include "velox/common/caching/SsdFileTracker.h"
#include <algorithm>

namespace facebook::velox::cache {

void SsdFileTracker::fileTouched(int32_t totalEntries) {
  ++numTouches_;
  if (numTouches_ > kDecayInterval && numTouches_ > totalEntries / 2) {
    numTouches_ = 0;
    for (auto& score : regionScores_) {
      score = (score * 15) / 16;
    }
  }
}

void SsdFileTracker::regionFilled(int32_t region) {
  const double best =
      *std::max_element(regionScores_.begin(), regionScores_.end());
  regionScores_[region] = std::max<double>(regionScores_[region], best * 1.1);
}

std::vector<int32_t> SsdFileTracker::findEvictionCandidates(
    int32_t numCandidates,
    int32_t numRegions,
    const std::vector<int32_t>& regionPins) {
  // Calculates average score of regions with no pins. Returns up to
  // 'numCandidates' unpinned regions with score <= average, lowest scoring
  // region first.
  double scoreSum = 0;
  int32_t numUnpinned = 0;
  for (int i = 0; i < numRegions; ++i) {
    if (regionPins[i] > 0) {
      continue;
    }
    ++numUnpinned;
    scoreSum += regionScores_[i];
  }
  if (numUnpinned == 0) {
    return {};
  }
  const auto avg = scoreSum / numUnpinned;
  std::vector<int32_t> candidates;
  for (auto i = 0; i < regionScores_.size(); ++i) {
    if ((regionPins[i] == 0) && (regionScores_[i] <= avg)) {
      candidates.push_back(i);
    }
  }
  // Sort by score to evict less read regions first.
  std::sort(
      candidates.begin(), candidates.end(), [&](int32_t left, int32_t right) {
        return regionScores_[left] < regionScores_[right];
      });
  candidates.resize(std::min<int32_t>(candidates.size(), numCandidates));
  return candidates;
}

} // namespace facebook::velox::cache
