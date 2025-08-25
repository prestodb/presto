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

#include "velox/functions/lib/sfm/SfmSketch.h"
#include <cmath>
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/IOUtils.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/functions/lib/sfm/MersenneTwisterRandomizationStrategy.h"
#include "velox/functions/lib/sfm/SecureRandomizationStrategy.h"

namespace facebook::velox::functions::sfm {

namespace {
void validateNumIndexBits(int32_t numIndexBits) {
  VELOX_CHECK_LE(numIndexBits, 16, "numIndexBits must be <= 16.");
  VELOX_CHECK_GE(numIndexBits, 1, "numIndexBits must be >= 1.");
}

int32_t numberOfTrailingZeros(uint64_t hash, int32_t numIndexBits) {
  // Set the lowest bit in the prefix to ensure value is non-zero.
  constexpr int32_t kBitWidth = sizeof(uint64_t) * 8;
  const uint64_t value = hash | (1ULL << (kBitWidth - numIndexBits));

  // Safe to call because value is guaranteed non-zero.
  return __builtin_ctzll(value);
}
} // namespace

using Allocator = StlAllocator<int8_t>;

SfmSketch::SfmSketch(
    HashStringAllocator* allocator,
    std::optional<int32_t> seed)
    : bits_{Allocator(allocator)} {
  if (seed.has_value()) {
    randomizationStrategy_ = MersenneTwisterRandomizationStrategy(*seed);
  }
}

void SfmSketch::initialize(int32_t buckets, int32_t precision) {
  const auto numIndexBits = SfmSketch::numIndexBits(buckets);
  validateNumIndexBits(numIndexBits);

  VELOX_CHECK_LE(
      precision + numIndexBits,
      64,
      "Precision + numIndexBits cannot exceed 64");
  numBuckets_ = buckets;
  numIndexBits_ = numIndexBits;
  precision_ = precision;

  bits_.resize(bits::nbytes(buckets * precision));
  rawBits_ = reinterpret_cast<uint64_t*>(bits_.data());
}

void SfmSketch::addHash(uint64_t hash) {
  const auto bucketIndex = computeIndex(hash, numIndexBits_);
  // Cap the number of trailing zeros to precision - 1, to avoid out of
  // bounds.
  const auto zeros =
      std::min(precision_ - 1, numberOfTrailingZeros(hash, numIndexBits_));
  setBitTrue(bucketIndex, zeros);
}

void SfmSketch::addIndexAndZeros(int32_t bucketIndex, int32_t zeros) {
  VELOX_CHECK_GE(bucketIndex, 0, "Bucket index out of range.");
  VELOX_CHECK_LT(bucketIndex, numBuckets_, "Bucket index out of range.");
  VELOX_CHECK_GE(zeros, 0, "Zeros must be greater than or equal to 0.");
  VELOX_CHECK_LE(zeros, 64, "Zeros must be less than or equal to 64.");
  VELOX_CHECK(isInitialized(), "Sketch is not initialized.");

  // Count of zeros in range [0, precision - 1].
  zeros = std::min(precision_ - 1, zeros);
  setBitTrue(bucketIndex, zeros);
}

// static
int32_t SfmSketch::computeIndex(uint64_t hash, int32_t numIndexBits) {
  validateNumIndexBits(numIndexBits);
  constexpr int32_t kBitWidth = 64; // The hash is 64 bits.
  return static_cast<int32_t>(hash >> (kBitWidth - numIndexBits));
}

// static
int32_t SfmSketch::numBuckets(int32_t numIndexBits) {
  validateNumIndexBits(numIndexBits);
  return 1 << numIndexBits;
}

// static
int32_t SfmSketch::numIndexBits(int32_t buckets) {
  VELOX_CHECK_GT(buckets, 0, "Number of buckets must be greater than 0.");
  VELOX_CHECK_EQ(
      buckets & (buckets - 1), 0, "Number of buckets must be power of 2.");
  return static_cast<int32_t>(std::log2(buckets));
}

// static
double SfmSketch::calculateRandomizedResponseProbability(double epsilon) {
  if (epsilon == kNonPrivateEpsilon) {
    return 0.0;
  }
  return 1.0 / (1.0 + exp(epsilon));
}

void SfmSketch::mergeWith(const SfmSketch& other) {
  VELOX_CHECK_EQ(
      precision_,
      other.precision_,
      "Cannot merge two SFM sketches with different precision");
  VELOX_CHECK_EQ(
      numIndexBits_,
      other.numIndexBits_,
      "Cannot merge two SFM sketches with different numIndexBits");
  VELOX_CHECK(isInitialized(), "Sketch is not initialized.");
  const auto numBits = precision_ * numBuckets_;

  // If neither sketch is private, we just take the OR of the sketches.
  if (!privacyEnabled() && !other.privacyEnabled()) {
    bits::orBits(rawBits_, other.rawBits_, 0, numBits);
  } else {
    // If either sketch is private, we combine using a randomized merge.
    const double p1 = randomizedResponseProbability_;
    const double p2 = other.randomizedResponseProbability_;
    const double p = mergeRandomizedResponseProbabilities(p1, p2);
    const double normalizer = (1 - 2 * p) / ((1 - 2 * p1) * (1 - 2 * p2));

    if (!randomizationStrategy_.has_value()) {
      randomizationStrategy_ = MersenneTwisterRandomizationStrategy();
    }

    for (int32_t i = 0; i < numBits; i++) {
      const double bit1 = bits::isBitSet(rawBits_, i) ? 1.0 : 0.0;
      const double bit2 = bits::isBitSet(other.rawBits_, i) ? 1.0 : 0.0;
      const double x =
          1 - 2 * p - normalizer * (1 - p1 - bit1) * (1 - p2 - bit2);
      double probability = p + normalizer * x;
      probability = std::min(1.0, std::max(0.0, probability));

      bits::setBit(
          rawBits_, i, randomizationStrategy_->nextBoolean(probability));
    }
  }

  randomizedResponseProbability_ = mergeRandomizedResponseProbabilities(
      randomizedResponseProbability_, other.randomizedResponseProbability_);
}

void SfmSketch::enablePrivacy(double epsilon) {
  VELOX_CHECK(isInitialized(), "Sketch is not initialized.");
  VELOX_CHECK(!privacyEnabled(), "Privacy is already enabled.");
  VELOX_CHECK_GT(
      epsilon, 0, "Epsilon must be greater than zero or equal to infinity");
  randomizedResponseProbability_ =
      calculateRandomizedResponseProbability(epsilon);

  auto applyRandomization = [this](auto& randomizationStrategy) {
    // Toggle each bit with a randomly generated probability.
    // If seed is provided, use it to generate a deterministic randomization.
    // Otherwise, use a secure randomization strategy.
    const auto numBits = precision_ * numBuckets_;
    for (int32_t i = 0; i < numBits; ++i) {
      if (randomizationStrategy.nextBoolean(randomizedResponseProbability_)) {
        bits::negateBit(bits_.data(), i);
      }
    }
  };

  // If randomizationStrategy_ is now set, it's guaranteed to be a deterministic
  // randomization strategy set in constructor, otherwise it would have
  // been set in private sketch merge which only happens after enablePrivacy().
  if (randomizationStrategy_.has_value()) {
    applyRandomization(*randomizationStrategy_);
  } else {
    SecureRandomizationStrategy randomizationStrategy;
    applyRandomization(randomizationStrategy);
  }
}

int32_t SfmSketch::countBits() const {
  return bits::countBits(rawBits_, 0, precision_ * numBuckets_);
}

int64_t SfmSketch::cardinality() const {
  VELOX_CHECK(isInitialized(), "Sketch is not initialized.");
  // Handle empty sketch case.
  if (countBits() == 0) {
    return 0;
  }

  double guess = 1.0;
  double changeInGuess = std::numeric_limits<double>::infinity();
  int32_t iterations = 0;

  while (std::abs(changeInGuess) > 0.1 && iterations < kMaxIteration) {
    changeInGuess = -logLikelihoodFirstDerivative(guess) /
        logLikelihoodSecondDerivative(guess);
    guess += changeInGuess;
    iterations++;
  }

  // Handle NaN values, which is unlikely from any real sketch but can happen.
  // We add the check here to avoid undefined behavior.
  if (std::isnan(guess)) {
    return 0;
  }

  // Clamp negative values to 0 before rounding to avoid undefined behavior
  // when casting negative values to unsigned types.
  const double clampedGuess = std::max(0.0, guess);
  const double roundedGuess = std::round(clampedGuess);
  VELOX_CHECK_LT(
      roundedGuess, static_cast<double>(std::numeric_limits<int64_t>::max()));
  return static_cast<int64_t>(roundedGuess);
}

// Java-compatible format: FORMAT_TAG + numIndexBits + precision +
// randomizedResponseProbability + compactBitSize + bitsData.
int32_t SfmSketch::serializedSize() const {
  return static_cast<int32_t>(
      sizeof(int8_t) + sizeof(int32_t) + sizeof(int32_t) + sizeof(double) +
      sizeof(int32_t) + compactBitSize());
}

void SfmSketch::serialize(char* out) const {
  common::OutputByteStream stream(out);
  stream.appendOne(kFormatTag);
  stream.appendOne(numIndexBits_);
  stream.appendOne(precision_);
  stream.appendOne(randomizedResponseProbability_);
  const auto compactBitSize = this->compactBitSize();
  stream.appendOne(compactBitSize);

  // Only append data if the vector is not empty to avoid null pointer issues.
  if (compactBitSize > 0) {
    stream.append(reinterpret_cast<const char*>(bits_.data()), compactBitSize);
  }
}

SfmSketch SfmSketch::deserialize(
    const char* in,
    HashStringAllocator* allocator) {
  common::InputByteStream stream(in);
  const auto formatTag = stream.read<int8_t>();
  const auto numIndexBits = stream.read<int32_t>();
  const auto precision = stream.read<int32_t>();
  const auto randomizedResponseProbability = stream.read<double>();
  const auto compactBitSize = stream.read<int32_t>();

  // Validate format tag.
  VELOX_CHECK_EQ(formatTag, kFormatTag, "Invalid format tag");

  // Validate numIndexBits before calling numBuckets.
  validateNumIndexBits(numIndexBits);

  // Create the sketch.
  SfmSketch sketch(allocator);
  const auto buckets = numBuckets(numIndexBits);
  sketch.initialize(buckets, precision);

  // Set the randomized response probability.
  sketch.randomizedResponseProbability_ = randomizedResponseProbability;

  // Read the serialized bitmap data directly into the sketch's bitset.
  // The sketch's bitset is already the correct size and zero-initialized.
  // We just need to copy the serialized data (which may be truncated).
  stream.copyTo(sketch.bits_.data(), compactBitSize);

  return sketch;
}

double SfmSketch::observationProbability(int32_t level) const {
  return std::pow(2.0, -(level + 1.0)) / numBuckets_;
}

int32_t SfmSketch::compactBitSize() const {
  // Find the last non-zero byte.
  const int32_t lastSetBit =
      bits::findLastBit(rawBits_, 0, precision_ * numBuckets_);

  if (lastSetBit < 0) {
    // No bits are set.
    return 0;
  }
  return bits::nbytes(lastSetBit + 1);
}

double SfmSketch::logLikelihoodFirstDerivative(double n) const {
  double result = 0.0;
  for (int32_t level = 0; level < precision_; level++) {
    const double termOn = logLikelihoodTermFirstDerivative(level, true, n);
    const double termOff = logLikelihoodTermFirstDerivative(level, false, n);
    for (int32_t bucket = 0; bucket < numBuckets_; bucket++) {
      int32_t bitPosition = level * numBuckets_ + bucket;
      result += bits::isBitSet(bits_.data(), bitPosition) ? termOn : termOff;
    }
  }
  return result;
}

double SfmSketch::logLikelihoodTermFirstDerivative(
    int32_t level,
    bool on,
    double n) const {
  const double p = observationProbability(level);
  const int32_t sign = on ? -1 : 1;
  const double c1 = on ? onProbability() : 1 - onProbability();
  const double c2 = onProbability() - randomizedResponseProbability_;
  return std::log1p(-p) * (1 - c1 / (c1 + sign * c2 * std::pow(1 - p, n)));
}

double SfmSketch::logLikelihoodSecondDerivative(double n) const {
  double result = 0.0;
  for (int32_t level = 0; level < precision_; level++) {
    const double termOn = logLikelihoodTermSecondDerivative(level, true, n);
    const double termOff = logLikelihoodTermSecondDerivative(level, false, n);
    for (int32_t bucket = 0; bucket < numBuckets_; bucket++) {
      const int32_t bitPosition = level * numBuckets_ + bucket;
      result += bits::isBitSet(bits_.data(), bitPosition) ? termOn : termOff;
    }
  }
  return result;
}

double SfmSketch::logLikelihoodTermSecondDerivative(
    int32_t level,
    bool on,
    double n) const {
  const double p = observationProbability(level);
  const int32_t sign = on ? -1 : 1;
  const double c1 = on ? onProbability() : 1 - onProbability();
  const double c2 = onProbability() - randomizedResponseProbability_;
  return sign * c1 * c2 * std::pow(std::log1p(-p), 2) * std::pow(1 - p, n) *
      std::pow(c1 + sign * c2 * std::pow(1 - p, n), -2);
}

void SfmSketch::setBitTrue(int32_t bucketIndex, int32_t zeros) {
  VELOX_CHECK(!privacyEnabled(), "Private sketch is immutable.");
  // It's more likely to have a hash with less zeros than more zeros.
  // We keep the bitmap in the form of a bit matrix, where each row is a zero
  // level instead of a bucket.
  // In this way, we can save space by dropping the trailing zeros in
  // serialization.
  const auto bitPosition = zeros * numBuckets_ + bucketIndex;
  bits::setBit(bits_.data(), bitPosition, true);
}

} // namespace facebook::velox::functions::sfm
