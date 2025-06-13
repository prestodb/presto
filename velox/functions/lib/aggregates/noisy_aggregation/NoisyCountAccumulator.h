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

struct NoisyCountAccumulator {
  uint64_t count{0};
  // Initial noise scale is an invalid noise scale,
  // indicating that we have not updated it yet
  double noiseScale{-1.0};

  // Add a field to store random seed
  std::optional<int64_t> randomSeed{std::nullopt};

  void setRandomSeed(int64_t seed) {
    randomSeed = seed;
  }

  void increaseCount(uint64_t value) {
    count = facebook::velox::checkedPlus<uint64_t>(count, value);
  }

  void checkAndSetNoiseScale(double newNoiseScale) {
    VELOX_USER_CHECK_GE(
        newNoiseScale, 0, "Noise scale must be a non-negative value");

    noiseScale = newNoiseScale;
  }

  static size_t serializedSize() {
    return sizeof(uint64_t) + sizeof(double) +
        sizeof(bool) /** has_random_seed flag */ + sizeof(int64_t);
  }

  void serialize(char* output) {
    common::OutputByteStream stream(output);
    stream.appendOne(count);
    stream.appendOne(noiseScale);
    stream.appendOne(randomSeed.has_value());
    stream.appendOne(randomSeed.has_value() ? randomSeed.value() : 0);
  }

  static NoisyCountAccumulator deserialize(const char* serialized) {
    common::InputByteStream stream(serialized);

    auto count = stream.read<uint64_t>();
    auto noiseScale = stream.read<double>();
    auto hasRandomSeed = stream.read<bool>();
    auto randomSeed = stream.read<int64_t>();
    if (hasRandomSeed) {
      return NoisyCountAccumulator{count, noiseScale, randomSeed};
    }

    return NoisyCountAccumulator{count, noiseScale, std::nullopt};
  }
};

} // namespace facebook::velox::functions::aggregate
