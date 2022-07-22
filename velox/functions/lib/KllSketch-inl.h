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

#include <cmath>
#include <queue>
#include <type_traits>
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::functions::kll {

namespace detail {

constexpr uint8_t kMaxLevel = 60;

uint32_t computeTotalCapacity(uint32_t k, uint8_t numLevels);

uint32_t levelCapacity(uint32_t k, uint8_t numLevels, uint8_t height);

// Collect elements in odd or even positions to first half of buf.
template <typename T, typename RandomBit>
void randomlyHalveDown(
    T* buf,
    uint32_t start,
    uint32_t length,
    RandomBit& randomBit) {
  VELOX_DCHECK_EQ(length & 1, 0);
  const uint32_t halfLength = length / 2;
  const uint32_t offset = randomBit();
  uint32_t j = start + offset;
  for (uint32_t i = start; i < start + halfLength; i++) {
    buf[i] = buf[j];
    j += 2;
  }
}

// Collect elements in odd or even positions to second half of buf.
template <typename T, typename RandomBit>
void randomlyHalveUp(
    T* buf,
    uint32_t start,
    uint32_t length,
    RandomBit& randomBit) {
  VELOX_DCHECK_EQ(length & 1, 0);
  const uint32_t halfLength = length / 2;
  const uint32_t offset = randomBit();
  uint32_t j = (start + length) - 1 - offset;
  for (uint32_t i = (start + length) - 1; i >= (start + halfLength); i--) {
    buf[i] = buf[j];
    j -= 2;
  }
}

// Merge 2 sorted ranges:
//   buf[startA] to buf[startA + lenA]
//   buf[startB] to buf[startB + lenB]
//
// The target range starting buf[startC] could overlap with range B,
// so we cannot use std::merge here.
template <typename T, typename C>
void mergeOverlap(
    T* buf,
    uint32_t startA,
    uint32_t lenA,
    uint32_t startB,
    uint32_t lenB,
    uint32_t startC,
    C compare) {
  const uint32_t limA = startA + lenA;
  const uint32_t limB = startB + lenB;
  VELOX_DCHECK_LE(limA, startC);
  VELOX_DCHECK_LE(startC + lenA, startB);
  uint32_t a = startA;
  uint32_t b = startB;
  uint32_t c = startC;
  while (a < limA && b < limB) {
    if (compare(buf[a], buf[b])) {
      buf[c++] = buf[a++];
    } else {
      buf[c++] = buf[b++];
    }
  }
  while (a < limA) {
    buf[c++] = buf[a++];
  }
  while (b < limB) {
    buf[c++] = buf[b++];
  }
}

// Return floor(log2(p/q)).
uint8_t floorLog2(uint64_t p, uint64_t q);

struct CompressResult {
  uint8_t finalNumLevels;
  uint32_t finalCapacity;
  uint32_t finalNumItems;
};

/*
 * Here is what we do for each level:
 * If it does not need to be compacted, then simply copy it over.
 *
 * Otherwise, it does need to be compacted, so...
 *   Copy zero or one guy over.
 *   If the level above is empty, halve up.
 *   Else the level above is nonempty, so...
 *        halve down, then merge up.
 *   Adjust the boundaries of the level above.
 *
 * It can be proved that generalCompress returns a sketch that satisfies the
 * space constraints no matter how much data is passed in.
 * All levels except for level zero must be sorted before calling this, and will
 * still be sorted afterwards.
 * Level zero is not required to be sorted before, and may not be sorted
 * afterwards.
 */
template <typename T, typename C, typename RandomBit>
CompressResult generalCompress(
    uint32_t k,
    uint8_t numLevelsIn,
    T* items,
    uint32_t* inLevels,
    uint32_t* outLevels,
    bool isLevelZeroSorted,
    RandomBit& randomBit) {
  VELOX_DCHECK_GT(numLevelsIn, 0);
  uint8_t currentNumLevels = numLevelsIn;
  // `currentItemCount` decreases with each compaction.
  uint32_t currentItemCount = inLevels[numLevelsIn] - inLevels[0];
  // Increases if we add levels.
  uint32_t targetItemCount = computeTotalCapacity(k, currentNumLevels);
  outLevels[0] = 0;
  for (uint8_t level = 0; level < currentNumLevels; ++level) {
    // If we are at the current top level, add an empty level above it
    // for convenience, but do not increment currentNumLevels until later.
    if (level == (currentNumLevels - 1)) {
      inLevels[level + 2] = inLevels[level + 1];
    }
    const auto rawBeg = inLevels[level];
    const auto rawLim = inLevels[level + 1];
    const auto rawPop = rawLim - rawBeg;
    if ((currentItemCount < targetItemCount) ||
        (rawPop < levelCapacity(k, currentNumLevels, level))) {
      // Move level over as is.
      // Make sure we are not moving data upwards.
      VELOX_DCHECK_GE(rawBeg, outLevels[level]);
      std::move(&items[rawBeg], &items[rawLim], &items[outLevels[level]]);
      outLevels[level + 1] = outLevels[level] + rawPop;
    } else {
      // The sketch is too full AND this level is too full, so we compact it.
      // Note: this can add a level and thus change the sketches capacities.
      const auto popAbove = inLevels[level + 2] - rawLim;
      const bool oddPop = rawPop & 1;
      const auto adjBeg = rawBeg + oddPop;
      const auto adjPop = rawPop - oddPop;
      const auto halfAdjPop = adjPop / 2;

      if (oddPop) { // Move one guy over.
        items[outLevels[level]] = std::move(items[rawBeg]);
        outLevels[level + 1] = outLevels[level] + 1;
      } else { // Even number of items in this level.
        outLevels[level + 1] = outLevels[level];
      }

      // Level zero might not be sorted, so we must sort it if we wish
      // to compact it.
      if ((level == 0) && !isLevelZeroSorted) {
        std::sort(&items[adjBeg], &items[adjBeg + adjPop], C());
      }

      if (popAbove == 0) { // Level above is empty, so halve up.
        randomlyHalveUp(items, adjBeg, adjPop, randomBit);
      } else { // Level above is nonempty, so halve down, then merge up.
        randomlyHalveDown(items, adjBeg, adjPop, randomBit);
        mergeOverlap(
            items,
            adjBeg,
            halfAdjPop,
            rawLim,
            popAbove,
            adjBeg + halfAdjPop,
            C());
      }

      // Track the fact that we just eliminated some data.
      currentItemCount -= halfAdjPop;

      // Adjust the boundaries of the level above.
      inLevels[level + 1] = inLevels[level + 1] - halfAdjPop;

      // Increment num levels if we just compacted the old top level
      // this creates some more capacity (the size of the new bottom
      // level).
      if (level == (currentNumLevels - 1)) {
        ++currentNumLevels;
        targetItemCount += levelCapacity(k, currentNumLevels, 0);
      }
    }
  }
  VELOX_DCHECK_EQ(outLevels[currentNumLevels] - outLevels[0], currentItemCount);
  return {currentNumLevels, targetItemCount, currentItemCount};
}

uint64_t sumSampleWeights(uint8_t numLevels, const uint32_t* levels);

template <typename T>
void write(T value, char* out, size_t& offset) {
  *reinterpret_cast<T*>(out + offset) = value;
  offset += sizeof(T);
}

template <typename T, typename A>
void writeVector(const std::vector<T, A>& data, char* out, size_t& offset) {
  write(data.size(), out, offset);
  auto bytes = sizeof(T) * data.size();
  memcpy(out + offset, data.data(), bytes);
  offset += bytes;
}

template <typename T>
void read(const char* data, size_t& offset, T& out) {
  out = *reinterpret_cast<const T*>(data + offset);
  offset += sizeof(T);
}

template <typename T>
void readRange(const char* data, size_t& offset, folly::Range<const T*>& out) {
  size_t size;
  read(data, offset, size);
  auto bytes = sizeof(T) * size;
  out.reset(reinterpret_cast<const T*>(data + offset), size);
  offset += bytes;
}

} // namespace detail

template <typename T, typename A, typename C>
KllSketch<T, A, C>::KllSketch(uint32_t k, const A& allocator, uint32_t seed)
    : k_(k),
      allocator_(allocator),
      randomBit_(seed),
      n_(0),
      items_(allocator),
      levels_(2, AllocU32(allocator)),
      isLevelZeroSorted_(false) {}

template <typename T, typename A, typename C>
KllSketch<T, A, C>::KllSketch(const A& allocator, uint32_t seed)
    : allocator_(allocator),
      randomBit_(seed),
      items_(allocator),
      levels_(AllocU32(allocator)) {}

template <typename T, typename A, typename C>
void KllSketch<T, A, C>::setK(uint32_t k) {
  if (k_ == k) {
    return;
  }
  VELOX_CHECK_EQ(n_, 0);
  k_ = k;
  levels_.resize(2);
}

template <typename T, typename A, typename C>
void KllSketch<T, A, C>::insert(T value) {
  if (n_ == 0) {
    minValue_ = maxValue_ = value;
  } else {
    minValue_ = std::min(minValue_, value, C());
    maxValue_ = std::max(maxValue_, value, C());
  }
  doInsert(value);
}

template <typename T, typename A, typename C>
void KllSketch<T, A, C>::doInsert(T value) {
  VELOX_DCHECK_GT(k_, 0);
  VELOX_DCHECK_GE(levels_.size(), 2);
  if (items_.size() < k_ && numLevels() == 1) {
    // Do not allocate all k elements in the beginning because in some group-by
    // aggregation most of the group size is small and won't use all k spaces.
    items_.push_back(value);
    ++levels_[1];
  } else {
    items_[insertPosition()] = value;
  }
  ++n_;
  isLevelZeroSorted_ = false;
}

template <typename T, typename A, typename C>
uint32_t KllSketch<T, A, C>::insertPosition() {
  if (levels_[0] == 0) {
    const uint8_t level = findLevelToCompact();

    if (level == numLevels() - 1) {
      if (int delta =
              detail::computeTotalCapacity(k_, numLevels()) - items_.size();
          delta > 0) {
        // Level zero must be under-populated (otherwise `findLevelToCompact()`
        // would return 0), just grow it.
        shiftItems(delta);
        return --levels_[0];
      }
      // It is important to add the new top level right here. Be aware
      // that this operation grows the buffer and shifts the data and
      // also the boundaries of the data and grows the levels array.
      addEmptyTopLevelToCompletelyFullSketch();
    }

    const uint32_t rawBeg = levels_[level];
    const uint32_t rawLim = levels_[level + 1];
    // +2 is OK because we already added a new top level if necessary.
    const uint32_t popAbove = levels_[level + 2] - rawLim;
    const uint32_t rawPop = rawLim - rawBeg;
    const bool oddPop = rawPop & 1;
    const uint32_t adjBeg = rawBeg + oddPop;
    const uint32_t adjPop = rawPop - oddPop;
    const uint32_t halfAdjPop = adjPop / 2;

    // Level zero might not be sorted, so we must sort it if we wish
    // to compact it.
    if (level == 0 && !isLevelZeroSorted_) {
      std::sort(items_.data() + adjBeg, items_.data() + adjBeg + adjPop, C());
    }
    if (popAbove == 0) {
      detail::randomlyHalveUp(items_.data(), adjBeg, adjPop, randomBit_);
    } else {
      detail::randomlyHalveDown(items_.data(), adjBeg, adjPop, randomBit_);
      detail::mergeOverlap(
          items_.data(),
          adjBeg,
          halfAdjPop,
          rawLim,
          popAbove,
          adjBeg + halfAdjPop,
          C());
    }
    levels_[level + 1] -= halfAdjPop; // Adjust boundaries of the level above.
    if (oddPop) {
      // The current level now contains one item.
      levels_[level] = levels_[level + 1] - 1;
      if (levels_[level] != rawBeg) {
        // Namely this leftover guy.
        items_[levels_[level]] = std::move(items_[rawBeg]);
      }
    } else {
      levels_[level] = levels_[level + 1]; // The current level is now empty.
    }

    // Verify that we freed up halfAdjPop array slots just below the
    // current level.
    VELOX_DCHECK_EQ(levels_[level], rawBeg + halfAdjPop);

    // Finally, we need to shift up the data in the levels below
    // so that the freed-up space can be used by level zero.
    if (level > 0) {
      const uint32_t amount = rawBeg - levels_[0];
      std::move_backward(
          items_.data() + levels_[0],
          items_.data() + levels_[0] + amount,
          items_.data() + levels_[0] + halfAdjPop + amount);
      for (uint8_t lvl = 0; lvl < level; lvl++) {
        levels_[lvl] += halfAdjPop;
      }
    }
  }
  return --levels_[0];
}

template <typename T, typename A, typename C>
int KllSketch<T, A, C>::findLevelToCompact() const {
  for (int level = 0;; ++level) {
    VELOX_DCHECK_LT(level + 1, levels_.size());
    const uint32_t pop = levels_[level + 1] - levels_[level];
    const uint32_t cap = detail::levelCapacity(k_, numLevels(), level);
    if (pop >= cap || level + 1 == numLevels()) {
      return level;
    }
  }
}

template <typename T, typename A, typename C>
void KllSketch<T, A, C>::addEmptyTopLevelToCompletelyFullSketch() {
  const uint32_t curTotalCap = levels_.back();

  // Make sure that we are following a certain growth scheme.
  VELOX_DCHECK_EQ(levels_[0], 0);
  VELOX_DCHECK_EQ(items_.size(), curTotalCap);

  const uint32_t deltaCap = detail::levelCapacity(k_, numLevels() + 1, 0);
  shiftItems(deltaCap);
  VELOX_DCHECK_EQ(levels_.back(), curTotalCap + deltaCap);
  levels_.push_back(levels_.back());
}

template <typename T, typename A, typename C>
void KllSketch<T, A, C>::shiftItems(uint32_t delta) {
  auto oldTotal = items_.size();
  items_.resize(items_.size() + delta);
  std::move_backward(items_.begin(), items_.begin() + oldTotal, items_.end());
  for (auto& lvl : levels_) {
    lvl += delta;
  }
}

template <typename T, typename A, typename C>
void KllSketch<T, A, C>::compact() {
  finish();
  uint32_t k = 0;
  std::vector<T> tmp;
  auto beg = levels_[0];
  auto end = levels_[1];
  levels_[0] = 0;
  for (int i = 0; i < numLevels(); ++i) {
    if (!tmp.empty()) {
      // Merge the items from lower levels.
      VELOX_DCHECK_LE(k + tmp.size(), beg);
      auto beg2 = beg - tmp.size();
      std::copy(tmp.begin(), tmp.end(), items_.begin() + beg2);
      std::inplace_merge(
          items_.data() + beg2, items_.data() + beg, items_.data() + end, C());
      beg = beg2;
      tmp.clear();
    }
    for (auto j = beg; j < end;) {
      if (j + 1 < end && items_[j] == items_[j + 1]) {
        // Move to upper level.
        tmp.push_back(items_[j]);
        j += 2;
      } else {
        items_[k++] = items_[j++];
      }
    }
    if (i + 1 == numLevels()) {
      if (!tmp.empty()) {
        levels_.push_back(levels_.back());
      }
    }
    beg = end;
    if (i + 1 < numLevels()) {
      end = levels_[i + 2];
    }
    levels_[i + 1] = k;
  }
  VELOX_DCHECK_LE(k, items_.size());
  items_.resize(k);
  VELOX_DCHECK_EQ(items_.size(), levels_.back());
}

template <typename T, typename A, typename C>
void KllSketch<T, A, C>::finish() {
  if (!isLevelZeroSorted_) {
    std::sort(items_.data() + levels_[0], items_.data() + levels_[1], C());
    isLevelZeroSorted_ = true;
  }
}

template <typename T, typename A, typename C>
std::vector<std::pair<T, uint64_t>> KllSketch<T, A, C>::getFrequencies() const {
  VELOX_USER_CHECK(
      isLevelZeroSorted_, "finish() must be called before estimate quantiles");
  std::vector<std::pair<T, uint64_t>> entries;
  entries.reserve(levels_.back());
  for (int level = 0; level < numLevels(); ++level) {
    auto oldLen = entries.size();
    for (int i = levels_[level]; i < levels_[level + 1]; ++i) {
      entries.emplace_back(items_[i], 1 << level);
    }
    if (oldLen > 0) {
      std::inplace_merge(
          entries.begin(),
          entries.begin() + oldLen,
          entries.end(),
          [](auto& x, auto& y) { return C()(x.first, y.first); });
    }
  }
  int k = 0;
  for (int i = 0; i < entries.size();) {
    entries[k] = entries[i];
    int j = i + 1;
    while (j < entries.size() && entries[j].first == entries[i].first) {
      entries[k].second += entries[j++].second;
    }
    ++k;
    i = j;
  }
  entries.resize(k);
  return entries;
}

template <typename T, typename A, typename C>
T KllSketch<T, A, C>::estimateQuantile(double fraction) const {
  T ans;
  estimateQuantiles(folly::Range(&fraction, 1), &ans);
  return ans;
}

template <typename T, typename A, typename C>
template <typename Iter>
std::vector<T, A> KllSketch<T, A, C>::estimateQuantiles(
    const folly::Range<Iter>& fractions) const {
  std::vector<T, A> ans(fractions.size(), T{}, allocator_);
  estimateQuantiles(fractions, ans.data());
  return ans;
}

template <typename T, typename A, typename C>
template <typename Iter>
void KllSketch<T, A, C>::estimateQuantiles(
    const folly::Range<Iter>& fractions,
    T* out) const {
  VELOX_USER_CHECK_GT(n_, 0, "estimateQuantiles called on empty sketch");
  auto entries = getFrequencies();
  uint64_t totalWeight = 0;
  for (auto& [_, w] : entries) {
    totalWeight += w;
    w = totalWeight;
  }
  int i = 0;
  for (auto& q : fractions) {
    VELOX_CHECK_GE(q, 0.0);
    VELOX_CHECK_LE(q, 1.0);
    if (fractions[i] == 0.0) {
      out[i++] = minValue_;
      continue;
    }
    if (fractions[i] == 1.0) {
      out[i++] = maxValue_;
      continue;
    }
    uint64_t maxWeight = q * totalWeight;
    auto it = std::upper_bound(
        entries.begin(),
        entries.end(),
        std::make_pair(T{}, maxWeight),
        [](auto& x, auto& y) { return x.second < y.second; });
    if (it == entries.end()) {
      out[i++] = entries.back().first;
    } else {
      out[i++] = it->first;
    }
  }
}

template <typename T, typename A, typename C>
void KllSketch<T, A, C>::mergeViews(const folly::Range<const View*>& others) {
  auto newN = n_;
  for (auto& other : others) {
    if (other.n == 0) {
      continue;
    }
    if (newN == 0) {
      minValue_ = other.minValue;
      maxValue_ = other.maxValue;
    } else {
      minValue_ = std::min(minValue_, other.minValue, C());
      maxValue_ = std::max(maxValue_, other.maxValue, C());
    }
    newN += other.n;
  }
  if (newN == n_) {
    return;
  }
  // Merge bottom level.
  for (auto& other : others) {
    if (other.n == 0) {
      continue;
    }
    for (uint32_t j = other.levels[0]; j < other.levels[1]; ++j) {
      doInsert(other.items[j]);
    }
  }
  // Merge higher levels.
  auto tmpNumItems = getNumRetained();
  auto provisionalNumLevels = numLevels();
  for (auto& other : others) {
    if (other.numLevels() >= 2) {
      tmpNumItems += other.levels.back() - other.levels[1];
      provisionalNumLevels = std::max(provisionalNumLevels, other.numLevels());
    }
  }
  if (tmpNumItems > getNumRetained()) {
    std::vector<T, A> workbuf(tmpNumItems, allocator_);
    const uint8_t ub = 1 + detail::floorLog2(newN, 1);
    const size_t workLevelsSize = ub + 2;
    std::vector<uint32_t, AllocU32> worklevels(
        workLevelsSize, 0, AllocU32(allocator_));
    std::vector<uint32_t, AllocU32> outlevels(
        workLevelsSize, 0, AllocU32(allocator_));
    // Populate work arrays.
    worklevels[0] = 0;
    std::move(
        items_.data() + levels_[0], items_.data() + levels_[1], workbuf.data());
    worklevels[1] = safeLevelSize(0);
    // Merge each level, each level in all sketches are already sorted.
    for (uint8_t lvl = 1; lvl < provisionalNumLevels; ++lvl) {
      using Entry = std::pair<const T*, const T*>;
      using AllocEntry =
          typename std::allocator_traits<A>::template rebind_alloc<Entry>;
      auto gt = [](const Entry& x, const Entry& y) {
        return C()(*y.first, *x.first);
      };
      std::priority_queue<Entry, std::vector<Entry, AllocEntry>, decltype(gt)>
          pq(gt, AllocEntry(allocator_));
      if (auto sz = safeLevelSize(lvl); sz > 0) {
        pq.emplace(
            items_.data() + levels_[lvl], items_.data() + levels_[lvl] + sz);
      }
      for (auto& other : others) {
        if (auto sz = other.safeLevelSize(lvl); sz > 0) {
          pq.emplace(
              &other.items[other.levels[lvl]],
              &other.items[other.levels[lvl]] + sz);
        }
      }
      int outIndex = worklevels[lvl];
      while (!pq.empty()) {
        auto [s, t] = pq.top();
        pq.pop();
        workbuf[outIndex++] = *s++;
        if (s < t) {
          pq.emplace(s, t);
        }
      }
      worklevels[lvl + 1] = outIndex;
    }
    auto result = detail::generalCompress<T, C>(
        k_,
        provisionalNumLevels,
        workbuf.data(),
        worklevels.data(),
        outlevels.data(),
        isLevelZeroSorted_,
        randomBit_);
    VELOX_DCHECK_LE(result.finalNumLevels, ub);
    // Now we need to transfer the results back into "this" sketch.
    items_.resize(result.finalCapacity);
    const auto freeSpaceAtBottom = result.finalCapacity - result.finalNumItems;
    std::move(
        workbuf.data() + outlevels[0],
        workbuf.data() + outlevels[0] + result.finalNumItems,
        items_.data() + freeSpaceAtBottom);
    levels_.resize(result.finalNumLevels + 1);
    const auto offset = freeSpaceAtBottom - outlevels[0];
    for (unsigned lvl = 0; lvl < levels_.size(); ++lvl) {
      levels_[lvl] = outlevels[lvl] + offset;
    }
  }
  n_ = newN;
  VELOX_DCHECK_EQ(detail::sumSampleWeights(numLevels(), levels_.data()), n_);
}

template <typename T, typename A, typename C>
void KllSketch<T, A, C>::merge(const KllSketch<T, A, C>& other) {
  View view = other.toView();
  mergeViews(folly::Range(&view, 1));
}

template <typename T, typename A, typename C>
template <typename Iter>
void KllSketch<T, A, C>::merge(const folly::Range<Iter>& others) {
  std::vector<View> views;
  views.reserve(others.size());
  for (auto& other : others) {
    views.push_back(other.toView());
  }
  mergeViews(views);
}

template <typename T, typename A, typename C>
void KllSketch<T, A, C>::mergeDeserialized(const char* data) {
  View view;
  view.deserialize(data);
  return mergeViews(folly::Range(&view, 1));
}

template <typename T, typename A, typename C>
template <typename Iter>
void KllSketch<T, A, C>::mergeDeserialized(const folly::Range<Iter>& others) {
  std::vector<View> views;
  views.reserve(others.size());
  for (auto& other : others) {
    views.emplace_back();
    views.back().deserialize(other);
  }
  mergeViews(views);
}

template <typename T, typename A, typename C>
typename KllSketch<T, A, C>::View KllSketch<T, A, C>::toView() const {
  return {
      .k = k_,
      .n = n_,
      .minValue = minValue_,
      .maxValue = maxValue_,
      .items = {items_},
      .levels = {levels_},
  };
}

template <typename T, typename A, typename C>
KllSketch<T, A, C> KllSketch<T, A, C>::fromView(
    const typename KllSketch<T, A, C>::View& view,
    const A& allocator,
    uint32_t seed) {
  KllSketch<T, A, C> ans(allocator, seed);
  ans.k_ = view.k;
  ans.n_ = view.n;
  ans.minValue_ = view.minValue;
  ans.maxValue_ = view.maxValue;
  ans.items_.assign(view.items.begin(), view.items.end());
  ans.levels_.assign(view.levels.begin(), view.levels.end());
  ans.isLevelZeroSorted_ = std::is_sorted(
      &ans.items_[ans.levels_[0]], &ans.items_[ans.levels_[1]], C());
  return ans;
}

template <typename T, typename A, typename C>
size_t KllSketch<T, A, C>::serializedByteSize() const {
  size_t ans = sizeof k_ + sizeof n_ + sizeof minValue_ + sizeof maxValue_;
  ans += sizeof(size_t) + sizeof(T) * items_.size();
  ans += sizeof(size_t) + sizeof(uint32_t) * levels_.size();
  return ans;
}

template <typename T, typename A, typename C>
void KllSketch<T, A, C>::serialize(char* out) const {
  size_t i = 0;
  detail::write(k_, out, i);
  detail::write(n_, out, i);
  detail::write(minValue_, out, i);
  detail::write(maxValue_, out, i);
  detail::writeVector(items_, out, i);
  detail::writeVector(levels_, out, i);
  VELOX_DCHECK_EQ(i, serializedByteSize());
}

template <typename T, typename A, typename C>
void KllSketch<T, A, C>::View::deserialize(const char* data) {
  size_t i = 0;
  detail::read(data, i, k);
  detail::read(data, i, n);
  detail::read(data, i, minValue);
  detail::read(data, i, maxValue);
  detail::readRange(data, i, items);
  detail::readRange(data, i, levels);
}

template <typename T, typename A, typename C>
KllSketch<T, A, C> KllSketch<T, A, C>::deserialize(
    const char* data,
    const A& allocator,
    uint32_t seed) {
  View view;
  view.deserialize(data);
  return fromView(view, allocator, seed);
}

// Each level corresponding to one bit in the binary representation of
// count, i.e. the size of level i is 1 if bit i is set, else 0.
template <typename T, typename A, typename C>
KllSketch<T, A, C> KllSketch<T, A, C>::fromRepeatedValue(
    T value,
    size_t count,
    uint32_t k,
    const A& allocator,
    uint32_t seed) {
  int numLevels = 0;
  for (auto x = count; x > 0; x >>= 1) {
    ++numLevels;
  }
  VELOX_CHECK_LE(numLevels, detail::kMaxLevel);
  KllSketch<T, A, C> ans(allocator, seed);
  ans.k_ = k;
  ans.n_ = count;
  ans.minValue_ = ans.maxValue_ = value;
  ans.isLevelZeroSorted_ = true;
  ans.levels_.resize(numLevels + 1);
  for (int i = 1; i <= numLevels; ++i) {
    ans.levels_[i] = ans.levels_[i - 1] + (count & 1);
    count >>= 1;
  }
  ans.items_.resize(ans.levels_.back(), value);
  VELOX_DCHECK_EQ(
      detail::sumSampleWeights(ans.numLevels(), ans.levels_.data()), ans.n_);
  return ans;
}

} // namespace facebook::velox::functions::kll
