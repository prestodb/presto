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
#include "velox/common/base/CheckedArithmetic.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/IOUtils.h"

namespace facebook::velox::functions::aggregate {

class NoisyCountSumAvgAccumulator {
 public:
  NoisyCountSumAvgAccumulator() = default;
  NoisyCountSumAvgAccumulator(
      double sum,
      uint64_t count,
      double noiseScale,
      std::optional<int64_t> randomSeed,
      std::optional<double> lowerBound,
      std::optional<double> upperBound)
      : sum_{sum},
        count_{count},
        noiseScale_{noiseScale},
        randomSeed_(randomSeed),
        lowerBound_(lowerBound),
        upperBound_(upperBound) {}

  void updateCount(uint64_t value) {
    count_ = facebook::velox::checkedPlus<uint64_t>(count_, value);
  }

  void updateSum(double value) {
    sum_ += value;
  }

  void clipUpdateSum(double value) {
    if (lowerBound_.has_value() && upperBound_.has_value()) {
      auto clippedValue = std::max(*lowerBound_, std::min(*upperBound_, value));
      this->sum_ += clippedValue;
    } else {
      this->sum_ += value;
    }
  }

  void checkAndSetNoiseScale(double newNoiseScale) {
    VELOX_USER_CHECK_GE(
        newNoiseScale, 0, "Noise scale must be a non-negative value.");
    noiseScale_ = newNoiseScale;
  }

  void checkAndSetBounds(double lowerBound, double upperBound) {
    VELOX_USER_CHECK_LE(
        lowerBound,
        upperBound,
        "Lower bound must be less than or equal to upper bound.");
    this->lowerBound_ = lowerBound;
    this->upperBound_ = upperBound;
  }

  void setRandomSeed(int64_t randomSeed) {
    this->randomSeed_ = randomSeed;
  }

  double getSum() const {
    return sum_;
  }

  uint64_t getCount() const {
    return count_;
  }

  double getNoiseScale() const {
    return noiseScale_;
  }

  std::optional<double> getLowerBound() const {
    return lowerBound_;
  }

  std::optional<double> getUpperBound() const {
    return upperBound_;
  }

  std::optional<int64_t> getRandomSeed() const {
    return randomSeed_;
  }

  // sizeof(double) for sum_
  // sizeof(uint64_t) for count_
  // sizeof(double) for noiseScale_
  // sizeof(bool) for has_bound flag
  // sizeof(double) for lowerBound_ value
  // sizeof(double) for upperBound_ value
  // sizeof(bool) for randomSeed_ has_value flag
  // sizeof(int32_t) for randomSeed_ value
  static size_t serializedSize() {
    return sizeof(double) + sizeof(uint64_t) + sizeof(double) + sizeof(bool) +
        sizeof(double) + sizeof(double) + sizeof(bool) + sizeof(int64_t);
  }

  void serialize(char* buffer) const {
    common::OutputByteStream stream(buffer);
    stream.appendOne(sum_);
    stream.appendOne(count_);
    stream.appendOne(noiseScale_);
    // Serialize lowerBound_ and upperBound_(append 0 if has_value is false).
    stream.appendOne(lowerBound_.has_value());
    stream.appendOne(lowerBound_.has_value() ? *lowerBound_ : 0.0);
    stream.appendOne(upperBound_.has_value() ? *upperBound_ : 0.0);

    // Serialize randomSeed_(append 0 if has_value is false).
    stream.appendOne(randomSeed_.has_value());
    stream.appendOne(randomSeed_.has_value() ? *randomSeed_ : 0);
  }

  static NoisyCountSumAvgAccumulator deserialize(const char* buffer) {
    common::InputByteStream stream(buffer);
    double sum = stream.read<double>();
    uint64_t count = stream.read<uint64_t>();
    double noiseScale = stream.read<double>();
    bool hasBounds = stream.read<bool>();
    std::optional<double> lowerBound = stream.read<double>();
    std::optional<double> upperBound = stream.read<double>();
    bool hasRandomSeed = stream.read<bool>();
    std::optional<int32_t> randomSeed = stream.read<int64_t>();
    return NoisyCountSumAvgAccumulator(
        sum,
        count,
        noiseScale,
        hasRandomSeed ? randomSeed : std::nullopt,
        hasBounds ? lowerBound : std::nullopt,
        hasBounds ? upperBound : std::nullopt);
  }

 private:
  double sum_{0};
  uint64_t count_{0};
  double noiseScale_{-1};
  std::optional<int64_t> randomSeed_{std::nullopt};
  std::optional<double> lowerBound_{std::nullopt};
  std::optional<double> upperBound_{std::nullopt};
};

} // namespace facebook::velox::functions::aggregate
