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
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/IOUtils.h"

namespace facebook::velox::functions::aggregate {

class NoisySumAccumulator {
 public:
  NoisySumAccumulator(
      double sum,
      double noiseScale,
      std::optional<double> lowerBound,
      std::optional<double> upperBound,
      std::optional<int32_t> randomSeed)
      : sum_{sum},
        noiseScale_{noiseScale},
        lowerBound_{lowerBound},
        upperBound_{upperBound},
        randomSeed_{randomSeed} {}

  NoisySumAccumulator() = default;

  void checkAndSetNoiseScale(double noiseScale) {
    VELOX_USER_CHECK_GE(
        noiseScale, 0.0, "Noise scale must be non-negative value.");
    this->noiseScale_ = noiseScale;
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

  // This function is used to update the sum with a new input value.
  void clipUpdate(double value) {
    if (lowerBound_.has_value() && upperBound_.has_value()) {
      auto clippedValue = std::max(*lowerBound_, std::min(*upperBound_, value));
      this->sum_ += clippedValue;
    } else {
      this->sum_ += value;
    }
  }

  // This function is used to update the sum with intermediate accumulator.
  void update(double value) {
    this->sum_ += value;
  }

  double getSum() const {
    return this->sum_;
  }

  double getNoiseScale() const {
    return this->noiseScale_;
  }

  std::optional<int64_t> getRandomSeed() const {
    return this->randomSeed_;
  }

  std::optional<double> getLowerBound() const {
    return this->lowerBound_;
  }

  std::optional<double> getUpperBound() const {
    return this->upperBound_;
  }

  static size_t serializedSize() {
    // The serialized size should be the sum of:
    // - sizeof(double) for sum_
    // - sizeof(double) for noiseScale_
    // - sizeof(bool) for has_bound flag
    // - sizeof(double) for lowerBound_ value
    // - sizeof(double) for upperBound_ value
    // - sizeof(bool) for randomSeed_ has_value flag
    // - sizeof(int64_t) for randomSeed_ value
    return sizeof(double) + sizeof(double) + sizeof(bool) + sizeof(double) +
        sizeof(double) + sizeof(bool) + sizeof(int64_t);
  }

  void serialize(char* buffer) {
    common::OutputByteStream stream(buffer);
    stream.appendOne(sum_);
    stream.appendOne(noiseScale_);

    // Serialize lowerBound_ and upperBound_(append 0 if has_value is false).
    bool hasBounds = lowerBound_.has_value() && upperBound_.has_value();
    stream.appendOne(hasBounds);
    stream.appendOne(hasBounds ? *lowerBound_ : 0.0);
    stream.appendOne(hasBounds ? *upperBound_ : 0.0);

    // Serialize randomSeed_(append 0 if has_value is false).
    stream.appendOne(randomSeed_.has_value());
    stream.appendOne(randomSeed_.has_value() ? randomSeed_.value() : 0);
  }

  static NoisySumAccumulator deserialize(const char* intermediate) {
    common::InputByteStream stream(intermediate);
    auto sum = stream.read<double>();
    auto noiseScale = stream.read<double>();

    // Deserialize lowerBound_ and upperBound_.
    bool hasBounds = stream.read<bool>();
    std::optional<double> lowerBound = stream.read<double>();
    std::optional<double> upperBound = stream.read<double>();

    // Deserialize randomSeed_
    bool hasRandomSeed = stream.read<bool>();
    std::optional<int64_t> randomSeed = stream.read<int64_t>();

    return NoisySumAccumulator(
        sum,
        noiseScale,
        hasBounds ? lowerBound : std::nullopt,
        hasBounds ? upperBound : std::nullopt,
        hasRandomSeed ? randomSeed : std::nullopt);
  }

 private:
  double sum_{0.0};
  // Initial noise scale is an invalid noise scale,
  // indicating that we have not updated it yet
  double noiseScale_{-1.0};
  std::optional<double> lowerBound_{std::nullopt};
  std::optional<double> upperBound_{std::nullopt};
  std::optional<int64_t> randomSeed_{std::nullopt};
};

} // namespace facebook::velox::functions::aggregate
