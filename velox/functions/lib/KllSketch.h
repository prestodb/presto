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

#include <functional>
#include <random>

#include "folly/Random.h"
#include "folly/Range.h"

namespace facebook::velox::functions::kll {

constexpr uint32_t kDefaultK = 200;

/// Estimate the proper k value to ensure the error bound epsilon.
uint32_t kFromEpsilon(double epsilon);

/// Implementation of KLL sketch that can achieve nearly optimal
/// accuracy per retained item.
///
/// This sketch is configured with a parameter k, which affects the
/// size of the sketch and its estimation error.
///
/// The estimation error is commonly called epsilon and is a fraction
/// between zero and one.  Larger values of k result in smaller values
/// of epsilon.  Epsilon is always with respect to the quantile and
/// cannot be applied to the corresponding values.
///
/// The default k of 200 yields a "single-sided" epsilon of about
/// 1.33%.
///
/// See https://arxiv.org/abs/1603.05346v2 for more details.
template <
    typename T,
    typename Allocator = std::allocator<T>,
    typename Compare = std::less<T>>
struct KllSketch {
  KllSketch(
      uint32_t k = kll::kDefaultK,
      const Allocator& = Allocator(),
      uint32_t seed = folly::Random::rand32());

  /// Cannot be called after insert().
  void setK(uint32_t k);

  /// Add one new value to the sketch.
  void insert(T value);

  /// Call this before serialization can optimize the space used.
  void compact();

  /// Merge this sketch with values from multiple other sketches.
  /// @tparam Iter Iterator type dereferenceable to the same type as this sketch
  ///  (KllSketch<T, Allocator, Compare>)
  /// @param sketches Range of sketches to be merged to this one
  template <typename Iter>
  void merge(const folly::Range<Iter>& sketches);

  /// Merge with another sketch.
  void merge(const KllSketch<T, Allocator, Compare>& other);

  /// Must be called before estimateQuantile() or estimateQuantiles().
  void finish();

  /// Estimate the value of the given quantile.
  /// @param quantile Quantile in [0, 1] to be estimated
  T estimateQuantile(double quantile) const;

  /// Estimate the values of the given quantiles.  This is more
  /// efficient than calling estimateQuantile(double) repeatedly.
  /// @tparam Iter Iterator type dereferenceable to double
  /// @param quantiles Range of quantiles in [0, 1] to be estimated
  template <typename Iter>
  std::vector<T, Allocator> estimateQuantiles(
      const folly::Range<Iter>& quantiles) const;

  /// The total number of values being added to the sketch.
  size_t totalCount() const {
    return n_;
  }

  /// Calculate the size needed for serialization.
  size_t serializedByteSize() const;

  /// Serialize the sketch into bytes.
  /// @param out Pre-allocated memory at least serializedByteSize() in size
  void serialize(char* out) const;

  /// Deserialize a sketch from bytes.
  static KllSketch<T, Allocator, Compare> deserialize(
      const char* data,
      const Allocator& = Allocator(),
      uint32_t seed = folly::Random::rand32());

  /// Equivalent to calling insert(value) `count` times on a new
  /// sketch, but more efficient.
  static KllSketch<T, Allocator, Compare> fromRepeatedValue(
      T value,
      size_t count,
      uint32_t k = kll::kDefaultK,
      const Allocator& = Allocator(),
      uint32_t seed = folly::Random::rand32());

  /// Merge this sketch with values from multiple other deserialized sketches.
  /// @tparam Iter Iterator type dereferenceable to `const char*`, which
  ///  represents a deserialized sketch
  /// @param sketches Range of sketches to be merged to this one
  template <typename Iter>
  void mergeDeserialized(const folly::Range<Iter>& sketches);

  /// Merge with another deserialized sketch.  This is more efficient
  /// than deserialize then merge.
  void mergeDeserialized(const char* data);

  /// Get frequencies of items being tracked.  The result is sorted by item.
  std::vector<std::pair<T, uint64_t>> getFrequencies() const;

 private:
  KllSketch(const Allocator&, uint32_t seed);
  void doInsert(T);
  uint32_t insertPosition();
  int findLevelToCompact() const;
  void addEmptyTopLevelToCompletelyFullSketch();
  void shiftItems(uint32_t delta);

  template <typename Iter>
  void estimateQuantiles(const folly::Range<Iter>& fractions, T* out) const;

  uint8_t numLevels() const {
    return levels_.size() - 1;
  }

  uint32_t getNumRetained() const {
    return levels_.back() - levels_[0];
  }

  uint32_t safeLevelSize(uint8_t level) const {
    return level < numLevels() ? levels_[level + 1] - levels_[level] : 0;
  }

  struct View {
    uint32_t k;
    size_t n;
    T minValue;
    T maxValue;
    folly::Range<const T*> items;
    folly::Range<const uint32_t*> levels;

    uint8_t numLevels() const {
      return levels.size() - 1;
    }

    uint32_t safeLevelSize(uint8_t level) const {
      return level < numLevels() ? levels[level + 1] - levels[level] : 0;
    }

    void deserialize(const char*);
  };

  View toView() const;
  static KllSketch<T, Allocator, Compare>
  fromView(const View&, const Allocator&, uint32_t seed);
  void mergeViews(const folly::Range<const View*>& views);

  using AllocU32 = typename std::allocator_traits<
      Allocator>::template rebind_alloc<uint32_t>;

  uint32_t k_;
  Allocator allocator_;

  // mt19937 uses too much memory (up to 5000 bytes), we choose to use
  // default_random_engine here to sacrifice some randomness for memory.
  std::independent_bits_engine<std::default_random_engine, 1, uint32_t>
      randomBit_;

  size_t n_;
  T minValue_;
  T maxValue_;
  std::vector<T, Allocator> items_;
  std::vector<uint32_t, AllocU32> levels_;
  bool isLevelZeroSorted_;
};

} // namespace facebook::velox::functions::kll

#include "velox/functions/lib/KllSketch-inl.h"
