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

#include "velox/functions/Macros.h"

namespace facebook::velox::functions::sparksql {

template <typename T>
struct RandFunction {
  static constexpr bool is_deterministic = false;

  FOLLY_ALWAYS_INLINE void call(double& result) {
    result = folly::Random::randDouble01();
  }

  FOLLY_ALWAYS_INLINE void callNullable(
      double& result,
      const int32_t* seed,
      const int32_t* partitionIndex) {
    initializeGenerator(seed, partitionIndex);
    result = folly::Random::randDouble01(*generator_);
  }

  // To differentiate generator for each thread, seed plus partitionIndex is
  // the actual seed used for generator.
  FOLLY_ALWAYS_INLINE void callNullable(
      double& result,
      const int64_t* seed,
      const int32_t* partitionIndex) {
    initializeGenerator(seed, partitionIndex);
    result = folly::Random::randDouble01(*generator_);
  }

  // For NULL constant input of unknown type.
  FOLLY_ALWAYS_INLINE void callNullable(
      double& result,
      const UnknownValue* /*seed*/,
      const int32_t* partitionIndex) {
    initializeGenerator<int64_t>(nullptr, partitionIndex);
    result = folly::Random::randDouble01(*generator_);
  }

 private:
  template <typename TSeed>
  FOLLY_ALWAYS_INLINE void initializeGenerator(
      const TSeed* seed,
      const int32_t* partitionIndex) {
    VELOX_USER_CHECK_NOT_NULL(partitionIndex, "partitionIndex cannot be null.");
    if (!generator_.has_value()) {
      generator_ = std::mt19937{};
      if (seed != nullptr) {
        generator_->seed((int64_t)*seed + *partitionIndex);
      } else {
        // For null seed, partitionIndex is the seed, consistent with Spark.
        generator_->seed(*partitionIndex);
      }
    }
  }

  std::optional<std::mt19937> generator_;
};
} // namespace facebook::velox::functions::sparksql
