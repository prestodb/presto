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

class NoisyAvgAccumulator {
 public:
  NoisyAvgAccumulator() = default;
  NoisyAvgAccumulator(double sum, uint64_t count, double noiseScale)
      : sum_{sum}, count_{count}, noiseScale_{noiseScale} {}

  void updateCount(uint64_t value) {
    count_ = facebook::velox::checkedPlus<uint64_t>(count_, value);
  }

  void updateSum(double value) {
    sum_ += value;
  }

  void checkAndSetNoiseScale(double newNoiseScale) {
    VELOX_USER_CHECK_GE(
        newNoiseScale, 0, "Noise scale must be a non-negative value.");
    noiseScale_ = newNoiseScale;
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

  static size_t serializedSize() {
    return sizeof(double) + sizeof(uint64_t) + sizeof(double);
  }

  void serialize(char* buffer) const {
    common::OutputByteStream stream(buffer);
    stream.appendOne(sum_);
    stream.appendOne(count_);
    stream.appendOne(noiseScale_);
  }

  static NoisyAvgAccumulator deserialize(const char* buffer) {
    common::InputByteStream stream(buffer);
    double sum = stream.read<double>();
    uint64_t count = stream.read<uint64_t>();
    double noiseScale = stream.read<double>();
    return NoisyAvgAccumulator(sum, count, noiseScale);
  }

 private:
  double sum_{0};
  uint64_t count_{0};
  double noiseScale_{-1};
};

} // namespace facebook::velox::functions::aggregate
