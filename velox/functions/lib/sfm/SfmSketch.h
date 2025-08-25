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

#define XXH_INLINE_ALL
#include <xxhash.h> // @manual=third-party//xxHash:xxhash

#include <folly/Range.h>
#include <cstdint>
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/functions/lib/sfm/MersenneTwisterRandomizationStrategy.h"

namespace facebook::velox::functions::sfm {

/// SFM sketch is used for estimating distinct count, very similar to
/// HyperLogLog. This sketch is introduced in the paper Sketch-Flip-Merge:
/// Mergeable Sketches for Private Distinct Counting
/// <https://arxiv.org/pdf/2302.02056.pdf>.

/// The primary differences between SFM sketch and HyperLogLog are that
///  (a) SFM sketch supports noise addition.
///  (b) SFM sketch tracks a larger number of hash statistics (and is therefore
///  larger).

/// The SFM sketch data structure is initially created without noise.
/// Noise is added by randomizing the bits of the sketch via the
/// enablePrivacy() function. Once noise has been added, further items
/// cannot be added to the sketch. However, two or more noisy sketches may be
/// merged together. A typical work flow is: initialize -> add -> merge/
/// enablePrivacy -> cardinality.

/// The noise level is quantified by the parameter 'epsilon', a positive real
/// number. A smaller value of 'epsilon' corresponds to a more noisy sketch.
/// An 'epsilon' of 0 would yield a summary that is entirely noise, while an
/// 'epsilon' that is infinite yields no noise. The estimated cardinality
/// becomes less accurate with the addition of more noise (i.e., smaller
/// 'epsilon' yield less accurate results. bits of the sketch are randomized
/// with probability 1 / (1 + exp(epsilon)), an epsilon of 1 will flip about
/// 1 / (1 + exp(1)) = 27% of the bits of the sketch).
class SfmSketch {
 public:
  /// Creates an uninitialized instance. The caller must call initialize()
  /// before using this instance. If seed is provided, the instance will
  /// initialize a deterministic randomization strategy, which is useful for
  /// testing. The deterministic randomization strategy is not retained in the
  /// serialized sketch.
  SfmSketch(HashStringAllocator* allocator, std::optional<int32_t> seed = {});

  /// Specify the sketch size. The size is determined by the number of buckets
  /// and precision. The sketch uses buckets * precision bits or (buckets *
  /// precision) / 8 bytes.

  /// When adding items to the sketch, we compute a 64-bit hash of the item,
  /// then use first n bits as a bucket index and last precision bits to count
  /// zeros. n = log2(buckets) so that buckets = 2^n. Hence, buckets must be a
  /// power of two and n + precision must be <= 64.

  /// This function should be called before any other functions.

  /// @param buckets The number of buckets, should be a power of 2:
  /// buckets = 2 ^ n. Later, n is referred to as numIndexBits.
  /// @param precision The number of bits to count trailing zeros in a hash.
  /// precision + n must be <= 64.
  void initialize(int32_t buckets, int32_t precision);

  /// @tparam T StringView, bigint, or double.
  template <typename T>
  void add(T value) {
    VELOX_CHECK(isInitialized(), "Sketch is not initialized.");
    addHash(XXH64(
        reinterpret_cast<const void*>(&value),
        sizeof(value),
        /*seed*/ 0));
  }

  /// Another way to add an element to the sketch. Explicitly toggle the bit at
  /// 'bucketIndex' and 'zeros' position.
  void addIndexAndZeros(int32_t bucketIndex, int32_t zeros);

  /// Compute the bit index with the given hash and numIndexBits.
  static int32_t computeIndex(uint64_t hash, int32_t numIndexBits);

  /// Compute the number of buckets given the numIndexBits.
  /// @return 2 ^ numIndexBits.
  static int32_t numBuckets(int32_t numIndexBits);

  /// Compute the number of bits needed to represent the index of a bucket.
  /// Range of numIndexBits is [1, 16].
  /// @return log2(buckets).
  static int32_t numIndexBits(int32_t buckets);

  /// Merge another sketch into the current sketch. Items can be added to the
  /// sketch after merging as long as the sketch is not private.
  void mergeWith(const SfmSketch& other);

  /// Make a non-private sketch private. This function is called
  /// after adding all the items to the sketch to add noise to the sketch and
  /// before calling cardinality().
  /// @param epsilon The privacy parameter, the larger the epsilon, the less
  /// noise will be added. The range of epsilon is (0, inf].
  void enablePrivacy(double epsilon);

  /// Estimate the number of distinct items added to the sketch via maximum
  /// pseudolikelihood (Newton's method).
  int64_t cardinality() const;

  /// Return the size of the serialized sketch in bytes.
  int32_t serializedSize() const;

  /// Serialize the sketch into pre-allocated buffer of at least
  /// 'serializedSize()' bytes. The Java serialization format is used in
  /// production, so we need to ensure that the C++ implementation can read and
  /// write the same format.
  /// The Java serialization format is as follows:
  /// 1. 1 byte for the FORMAT_TAG = 7, constant.
  /// 2. 4 bytes for the numIndexBits.
  /// 3. 4 bytes for the precision.
  /// 4. 8 bytes for the randomized response probability.
  /// 5. 4 bytes for the actual number of bytes in the serialized bits.
  /// 6. The bits data.
  void serialize(char* out) const;

  /// Deserialize the sketch from a pre-allocated buffer of at least
  /// 'serializedSize()' bytes.
  static SfmSketch deserialize(const char* in, HashStringAllocator* allocator);

  int32_t numIndexBits() const {
    return numIndexBits_;
  }

  int32_t precision() const {
    return precision_;
  }

  bool privacyEnabled() const {
    return randomizedResponseProbability_ > 0;
  }

  // Check if the sketch is initialized.
  bool isInitialized() const {
    return numIndexBits_ > 0;
  }

 private:
  // Epsilon for non-private sketch.
  static constexpr double kNonPrivateEpsilon =
      std::numeric_limits<double>::infinity();

  // Maximum number of iterations for Newton's method.
  static constexpr int32_t kMaxIteration = 1000;

  // Java implementation use a format_tag to identify the sketch.
  // We need this tag to be able to deserialize the sketch from Java.
  static constexpr int8_t kFormatTag = 7;

  // Calculate the RandomizedResponseProbabilities for merged sketches.
  // For math details, see Theorem 4.8,
  // <a href="https://arxiv.org/pdf/2302.02056.pdf">arXiv:2302.02056</a>.
  static double mergeRandomizedResponseProbabilities(double p1, double p2) {
    return (p1 + p2 - 3 * p1 * p2) / (1 - 2 * p1 * p2);
  }

  // Calculate the randomized response probability given epsilon.
  static double calculateRandomizedResponseProbability(double epsilon);

  // Add a hash to the sketch.
  void addHash(uint64_t hash);

  // Probability of a 1-bit remaining a 1-bit under randomized response.
  double onProbability() const {
    return 1 - randomizedResponseProbability_;
  }

  // Get the number of bits that are set to 1.
  int32_t countBits() const;

  // Return the size of the bits in bytes after dropping the trailing zeros.
  int32_t compactBitSize() const;

  // Probability of observing a run of zeros of length level in any single
  // bucket.
  double observationProbability(int32_t level) const;

  // Set the bit at given bucketIndex and zeros position.
  void setBitTrue(int32_t bucketIndex, int32_t zeros);

  // Helper functions for cardinality estimation using Newton's method.
  double logLikelihoodFirstDerivative(double n) const;
  double logLikelihoodTermFirstDerivative(int32_t level, bool on, double n)
      const;
  double logLikelihoodSecondDerivative(double n) const;
  double logLikelihoodTermSecondDerivative(int32_t level, bool on, double n)
      const;

  // Number of buckets in the sketch.
  int32_t numBuckets_{0};

  // Number of bits to represent the index of a bucket. We use first
  // numIndexBits bits of a hash to represent the index of a bucket, meaning
  // that total number of buckets is 2 ^ numIndexBits, and max index is
  // 2 ^ numIndexBits - 1.
  int32_t numIndexBits_{0};

  // Number of bits to count trailing zeros in a hash value.
  int32_t precision_{0};

  // Probability of a bit being flipped.
  double randomizedResponseProbability_{0.0};

  // The underline bit representation of the sketch.
  std::vector<int8_t, facebook::velox::StlAllocator<int8_t>> bits_;

  // Pointer to the raw bits.
  uint64_t* rawBits_{nullptr};

  // Optional randomization strategy. If seed is provided at construction, the
  // instance will initialize a deterministic randomization strategy.
  // Otherwise the randomization strategy will be initialized unseeded when
  // merging private sketches.
  std::optional<MersenneTwisterRandomizationStrategy> randomizationStrategy_;
};
} // namespace facebook::velox::functions::sfm
