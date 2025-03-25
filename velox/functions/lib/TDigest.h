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

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/Portability.h"

#include <folly/Bits.h>

#include <numeric>

namespace facebook::velox::functions {

namespace tdigest {
constexpr double kDefaultCompression = 100;
}

/// Implementation of T-Digest that matches Presto Java behavior.  It has the
/// same error bound as Java version and the serialization format is same as
/// Java.
///
/// There are some improvements on runtime performance compared to Java version:
///
/// 1. The memory footprint is largely reduced compared to Java.  When we merge
/// new values, we keep the already merged values and unmerged values in the
/// same buffer and do the reordering and merging in-place, instead of keeping
/// the merged values in separate buffers like Java.  We also do not keep the
/// positions buffer inside this class, because these are temporary scratch
/// memory that can be reused across different objects and should not be stored
/// in row container.
///
/// 2. When we merging the deserialized digests, if the centroids are already
/// sorted (highly likely so), we no longer need to re-sort them and can
/// directly start merging the sorted centroids.
///
/// Java implementation can be found at
/// https://github.com/prestodb/presto/blob/master/presto-main/src/main/java/com/facebook/presto/tdigest/TDigest.java
template <typename Allocator = std::allocator<double>>
class TDigest {
 public:
  explicit TDigest(const Allocator& allocator = Allocator());

  /// Set the compression parameter of the T-Digest.  The value should be
  /// between 10 and 1000.  The larger the value, the more accurate this digest
  /// will be.  Default to tdigest::kDefaultCompression if this method is not
  /// called.
  void setCompression(double compression);

  /// Add a new value or multiple same values to the digest.
  ///
  /// @param positions Scratch memory used to keep the ordered positions of
  ///  centroids.  This buffer can and should be reused across different groups
  ///  of accumulators in an aggregate function.
  /// @param value The new value to be added.  Cannot be NaN.
  /// @param weight A positive number indicating how many copies of `value' to
  ///  be added.
  void add(std::vector<int16_t>& positions, double value, int64_t weight = 1);

  /// Compress the buffered values according to the compression parameter
  /// provided.  Must be called before doing any estimation or serialization.
  ///
  /// @param positions Scratch memory used to keep the ordered positions of
  ///  centroids.  This buffer can and should be reused across different groups
  ///  of accumulators in an aggregate function.
  void compress(std::vector<int16_t>& positions);

  /// Estimate the value of the given quantile.
  /// @param quantile Quantile in [0, 1] to be estimated.
  double estimateQuantile(double quantile) const;

  /// Calculate the size needed for serialization.
  int64_t serializedByteSize() const;

  /// Serialize the digest into bytes.  The serialzation is versioned, and newer
  /// version of code should be able to read all previous versions.  Presto Java
  /// can read output of this function.
  ///
  /// @param out Pre-allocated memory at least serializedByteSize() in size.
  void serialize(char* out) const;

  /// Merge this digest with values from another deserialized digest.
  /// Serialization produced by Presto Java can be used as input.
  ///
  /// @param positions Scratch memory used to keep the ordered positions of
  ///  centroids.  This buffer can and should be reused across different groups
  ///  of accumulators in an aggregate function.
  /// @param input The input serialization.
  void mergeDeserialized(std::vector<int16_t>& positions, const char* input);

  /// Returns the total sum of all values added to this digest.
  double sum() const;

  /// Returns the compression parameter.
  double compression() const {
    return compression_;
  }

  static constexpr int8_t kSerializationVersion = 1;
  static constexpr double kEpsilon = 1e-3;
  static constexpr double kRelativeErrorEpsilon = 1e-4;

 private:
  void mergeNewValues(std::vector<int16_t>& positions, double compression);

  void merge(
      double compression,
      const double* weights,
      const double* means,
      int count);

  template <bool kReverse>
  void mergeImpl(
      double compression,
      const double* weights,
      const double* means,
      int count);

  static double
  weightedAverageSorted(double x1, double w1, double x2, double w2) {
    VELOX_DCHECK_LE(x1, x2);
    double x = (x1 * w1 + x2 * w2) / (w1 + w2);
    return std::max(x1, std::min(x, x2));
  }

  // Performs sanity check for the sum: verifies that the given sum is close to
  // the calculated sum.
  static void
  checkTheSum(int32_t numEntries, double sum, double* weights, double* means);

  std::vector<double, Allocator> weights_;
  std::vector<double, Allocator> means_;
  double compression_;
  int maxBufferSize_;
  int32_t numMerged_ = 0;
  double min_ = INFINITY;
  double max_ = -INFINITY;
  bool reverseCompress_ = false;
};

template <typename A>
TDigest<A>::TDigest(const A& allocator)
    : weights_(allocator), means_(allocator) {
  setCompression(tdigest::kDefaultCompression);
}

template <typename A>
void TDigest<A>::setCompression(double compression) {
  VELOX_CHECK_GE(compression, 10);
  VELOX_CHECK_LE(compression, 1000);
  VELOX_CHECK(weights_.empty());
  compression_ = compression;
  maxBufferSize_ = 5 * std::ceil(2 * compression_ + 30);
}

template <typename A>
void TDigest<A>::add(
    std::vector<int16_t>& positions,
    double value,
    int64_t weight) {
  VELOX_CHECK(!std::isnan(value));
  VELOX_CHECK_GT(weight, 0);
  min_ = std::min(min_, value);
  max_ = std::max(max_, value);
  weights_.push_back(weight);
  means_.push_back(value);
  if (weights_.size() >= maxBufferSize_) {
    mergeNewValues(positions, 2 * compression_);
  }
}

template <typename A>
void TDigest<A>::compress(std::vector<int16_t>& positions) {
  if (!weights_.empty()) {
    mergeNewValues(positions, compression_);
  }
}

template <typename A>
void TDigest<A>::mergeNewValues(
    std::vector<int16_t>& positions,
    double compression) {
  if (numMerged_ < weights_.size()) {
    VELOX_CHECK_LE(weights_.size(), std::numeric_limits<int16_t>::max());
    positions.resize(weights_.size());
    std::iota(positions.begin(), positions.end(), 0);
    auto newBegin = positions.begin() + numMerged_;
    auto compare = [this](auto i, auto j) { return means_[i] < means_[j]; };
    if (!std::is_sorted(means_.begin() + numMerged_, means_.end())) {
      std::sort(newBegin, positions.end(), compare);
    }
    std::inplace_merge(positions.begin(), newBegin, positions.end(), compare);
    // Reorder weights_ and means_ according to positions.
    for (int i = 0; i < positions.size(); ++i) {
      if (i == positions[i]) {
        continue;
      }
      auto wi = weights_[i];
      auto mi = means_[i];
      auto j = i;
      for (;;) {
        auto k = positions[j];
        if (k == i) {
          break;
        }
        weights_[j] = weights_[k];
        means_[j] = means_[k];
        positions[j] = j;
        j = k;
      }
      weights_[j] = wi;
      means_[j] = mi;
      positions[j] = j;
    }
    VELOX_DCHECK(std::is_sorted(means_.begin(), means_.end()));
  }
  merge(compression, weights_.data(), means_.data(), weights_.size());
}

template <typename A>
void TDigest<A>::merge(
    double compression,
    const double* weights,
    const double* means,
    int count) {
  VELOX_CHECK_GT(count, 0);
  VELOX_CHECK_GE(weights_.size(), count);
  if (reverseCompress_) {
    // Run the merge in reverse every other merge to avoid left-to-right
    // bias.
    mergeImpl<true>(compression, weights, means, count);
  } else {
    mergeImpl<false>(compression, weights, means, count);
  }
  reverseCompress_ = !reverseCompress_;
}

template <typename A>
template <bool kReverse>
void TDigest<A>::mergeImpl(
    double compression,
    const double* weights,
    const double* means,
    int count) {
  const auto totalWeight = std::accumulate(weights, weights + count, 0.0);
  const auto invTotalWeight = 1 / totalWeight;
  const auto normalizer =
      (4 * std::log(totalWeight / compression) + 24) / compression;
  auto maxSize = [normalizer](double q) { return q * (1 - q) * normalizer; };
  double weightSoFar = 0;
  numMerged_ = 0;
  const int begin = kReverse ? count - 1 : 0;
  auto notEnd = [&](auto i) INLINE_LAMBDA {
    if constexpr (kReverse) {
      return i >= 0;
    } else {
      return i < count;
    }
  };
  constexpr int kStep = kReverse ? -1 : 1;
  int j = begin;
  weights_[j] = weights[begin];
  means_[j] = means[begin];
  for (int i = begin + kStep; notEnd(i); i += kStep) {
    auto proposedWeight = weights_[j] + weights[i];
    auto q0 = weightSoFar * invTotalWeight;
    auto q2 = (weightSoFar + proposedWeight) * invTotalWeight;
    if (proposedWeight <= totalWeight * std::min(maxSize(q0), maxSize(q2))) {
      weights_[j] += weights[i];
      means_[j] += (means[i] - means_[j]) * weights[i] / weights_[j];
    } else {
      weightSoFar += weights_[j];
      ++numMerged_;
      j += kStep;
      weights_[j] = weights[i];
      means_[j] = means[i];
    }
  }
  weightSoFar += weights_[j];
  ++numMerged_;
  VELOX_CHECK_LT(std::abs(weightSoFar - totalWeight), kEpsilon);
  if constexpr (kReverse) {
    std::copy(weights_.begin() + j, weights_.end(), weights_.begin());
    std::copy(means_.begin() + j, means_.end(), means_.begin());
  }
  weights_.resize(numMerged_);
  means_.resize(numMerged_);
  min_ = std::min(min_, means_.front());
  max_ = std::max(max_, means_.back());
}

template <typename A>
double TDigest<A>::estimateQuantile(double quantile) const {
  VELOX_CHECK(0 <= quantile && quantile <= 1);
  VELOX_CHECK_EQ(numMerged_, weights_.size());
  if (numMerged_ == 0) {
    return NAN;
  }
  if (numMerged_ == 1) {
    return means_[0];
  }
  auto totalWeight = std::accumulate(weights_.begin(), weights_.end(), 0.0);
  const auto index = quantile * totalWeight;
  if (index < 1) {
    return min_;
  }
  // If the left centroid has more than one sample, we still know that one
  // sample occurred at min so we can do some interpolation.
  if (weights_.front() > 1 && index < weights_.front() / 2) {
    // There is a single sample at min so we interpolate with less weight.
    return min_ +
        (index - 1) / (weights_.front() / 2 - 1) * (means_.front() - min_);
  }
  if (index > totalWeight - 1) {
    return max_;
  }
  // If the right-most centroid has more than one sample, we still know that one
  // sample occurred at max so we can do some interpolation.
  if (weights_.back() > 1 && totalWeight - index <= weights_.back() / 2) {
    return max_ -
        (totalWeight - index - 1) / (weights_.back() / 2 - 1) *
        (max_ - means_.back());
  }
  // In between extremes we interpolate between centroids.
  auto weightSoFar = weights_[0] / 2;
  for (int i = 1; i < numMerged_; ++i) {
    // Centroids i-1 and i bracket our current point.
    auto dw = (weights_[i - 1] + weights_[i]) / 2;
    if (weightSoFar + dw <= index) {
      weightSoFar += dw;
      continue;
    }
    // Check for unit weight.
    double leftUnit = 0;
    if (weights_[i - 1] == 1) {
      if (index - weightSoFar < 0.5) {
        // Within the singleton's sphere.
        return means_[i - 1];
      }
      leftUnit = 0.5;
    }
    double rightUnit = 0;
    if (weights_[i] == 1) {
      if (weightSoFar + dw - index <= 0.5) {
        // Within the singleton's sphere.
        return means_[i];
      }
      rightUnit = 0.5;
    }
    auto z1 = index - weightSoFar - leftUnit;
    auto z2 = weightSoFar + dw - index - rightUnit;
    return weightedAverageSorted(means_[i - 1], z2, means_[i], z1);
  }
  VELOX_CHECK_GT(weights_.back(), 1);
  VELOX_CHECK_LE(index, totalWeight);
  VELOX_CHECK_GE(index, totalWeight - weights_.back() / 2);
  // weightSoFar is very close to totalWeight - weight[n - 1] / 2 so we
  // interpolate out to max value ever seen.
  auto z1 = index - totalWeight - weights_.back() / 2;
  auto z2 = weights_.back() / 2 - z1;
  return weightedAverageSorted(means_.back(), z1, max_, z2);
}

namespace tdigest::detail {

static_assert(folly::kIsLittleEndian);

template <typename T>
void write(T value, char*& out) {
  folly::storeUnaligned(out, value);
  out += sizeof(T);
}

template <typename T>
void write(const T* values, int count, char*& out) {
  auto size = sizeof(T) * count;
  memcpy(out, values, size);
  out += size;
}

template <typename T>
void read(const char*& input, T& value) {
  value = folly::loadUnaligned<T>(input);
  input += sizeof(T);
}

template <typename T>
void read(const char*& input, T* values, int count) {
  auto size = sizeof(T) * count;
  memcpy(values, input, size);
  input += size;
}

} // namespace tdigest::detail

template <typename A>
int64_t TDigest<A>::serializedByteSize() const {
  VELOX_CHECK_EQ(numMerged_, weights_.size());
  return sizeof(kSerializationVersion) + 1 /*data type*/ + sizeof(min_) +
      sizeof(max_) + sizeof(double) /*sum*/ + sizeof(compression_) +
      sizeof(double) /*total weight*/ + sizeof(numMerged_) +
      2 * numMerged_ * sizeof(double);
}

template <typename A>
void TDigest<A>::serialize(char* out) const {
  VELOX_CHECK_EQ(numMerged_, weights_.size());
  auto totalWeight = std::accumulate(weights_.begin(), weights_.end(), 0.0);
  const char* oldOut = out;
  tdigest::detail::write(kSerializationVersion, out);
  tdigest::detail::write<int8_t>(0, out);
  tdigest::detail::write(min_, out);
  tdigest::detail::write(max_, out);
  tdigest::detail::write(sum(), out);
  tdigest::detail::write(compression_, out);
  tdigest::detail::write(totalWeight, out);
  tdigest::detail::write(numMerged_, out);
  if (numMerged_ > 0) {
    tdigest::detail::write(weights_.data(), numMerged_, out);
    tdigest::detail::write(means_.data(), numMerged_, out);
  }
  VELOX_CHECK_EQ(out - oldOut, serializedByteSize());
}

template <typename A>
void TDigest<A>::mergeDeserialized(
    std::vector<int16_t>& positions,
    const char* input) {
  int8_t version;
  tdigest::detail::read(input, version);
  VELOX_CHECK_GE(version, 0);
  VELOX_CHECK_LE(version, kSerializationVersion);
  int8_t type;
  tdigest::detail::read(input, type);
  VELOX_CHECK_EQ(type, 0);
  double min, max, sum, compression, totalWeight;
  tdigest::detail::read(input, min);
  tdigest::detail::read(input, max);
  if (version >= 1) {
    tdigest::detail::read(input, sum);
  }
  tdigest::detail::read(input, compression);
  // If the TDigest is empty, set compression from TDigest being merged.
  if (weights_.empty()) {
    setCompression(compression);
  }
  tdigest::detail::read(input, totalWeight);
  int32_t numNew;
  tdigest::detail::read(input, numNew);
  if (numNew > 0) {
    VELOX_CHECK_EQ(compression, compression_);
    auto numOld = weights_.size();
    weights_.resize(numOld + numNew);
    auto* weights = weights_.data() + numOld;
    tdigest::detail::read(input, weights, numNew);
    for (auto i = 0; i < numNew; ++i) {
      VELOX_CHECK_GT(weights[i], 0);
    }
    means_.resize(numOld + numNew);
    auto* means = means_.data() + numOld;
    tdigest::detail::read(input, means, numNew);
    for (auto i = 0; i < numNew; ++i) {
      VELOX_CHECK(!std::isnan(means[i]));
    }

    if (version >= 1) {
      checkTheSum(numNew, sum, weights, means);
    }

    double actualTotalWeight = std::accumulate(weights, weights + numNew, 0.0);
    VELOX_CHECK_LT(std::abs(actualTotalWeight - totalWeight), kEpsilon);
  } else {
    VELOX_CHECK_LT(std::abs(sum), kEpsilon);
    VELOX_CHECK_LT(std::abs(totalWeight), kEpsilon);
  }
  if (weights_.size() >= maxBufferSize_) {
    mergeNewValues(positions, 2 * compression_);
  }
}

template <typename A>
void TDigest<A>::checkTheSum(
    int32_t numEntries,
    double sum,
    double* weights,
    double* means) {
  double actualSum = 0;
  for (auto i = 0; i < numEntries; ++i) {
    actualSum += weights[i] * means[i];
  }

  // Simple check for sums matching first.
  const double difference = std::abs(sum - actualSum);
  if (FOLLY_UNLIKELY(difference >= kEpsilon)) {
    // According to http://floating-point-gui.de/errors/comparison/
    const double controlMean = sum / numEntries;
    const double testMean = actualSum / numEntries;
    bool sumIsGood = true;
    if (std::abs(controlMean) < kEpsilon || std::abs(testMean) < kEpsilon) {
      sumIsGood =
          std::abs(controlMean) < kEpsilon && std::abs(testMean) < kEpsilon;
    } else {
      const double relativeError = difference /
          std::min((std::abs(sum) + std::abs(actualSum)) / 2,
                   std::numeric_limits<double>::max());
      sumIsGood = relativeError < kRelativeErrorEpsilon;
    }
    VELOX_CHECK(
        sumIsGood,
        "TDigest declared sum: {} and actual sum: {} differ too much: {}",
        sum,
        actualSum,
        difference);
  }
}

template <typename A>
double TDigest<A>::sum() const {
  VELOX_CHECK_EQ(numMerged_, weights_.size());
  double result = 0;
  for (int i = 0; i < numMerged_; ++i) {
    result += weights_[i] * means_[i];
  }
  return result;
}

} // namespace facebook::velox::functions
