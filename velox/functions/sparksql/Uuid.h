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

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "velox/core/QueryConfig.h"
#include "velox/functions/Macros.h"

namespace facebook::velox::functions::sparksql {

template <typename T>
struct UuidFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr bool is_deterministic = false;

  // Spark would set the seed with 'random.nextLong()' by 'ResolveRandomSeed'
  // rule.
  void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const int64_t* seed) {
    VELOX_CHECK_NOT_NULL(seed, "seed argument must be constant");
    const int32_t partitionId = config.sparkPartitionId();
    generator_.seed((*seed) + partitionId);
  }

  FOLLY_ALWAYS_INLINE void call(out_type<Varchar>& result, int64_t /*seed*/) {
    int64_t mostSigBits =
        (nextLong() & 0xFFFFFFFFFFFF0FFFL) | 0x0000000000004000L;
    int64_t leastSigBits =
        (nextLong() | 0x8000000000000000L) & 0xBFFFFFFFFFFFFFFFL;
    for (int32_t i = 0; i < 8; ++i) {
      uuid_.data[i] = (mostSigBits >> ((7 - i) * 8)) & 0xFF;
      uuid_.data[i + 8] = (leastSigBits >> ((7 - i) * 8)) & 0xFF;
    }
    constexpr int32_t kUuidStringSize = 36;
    result.resize(kUuidStringSize);
    boost::uuids::to_chars(
        uuid_, result.data(), result.data() + kUuidStringSize);
  }

 private:
  FOLLY_ALWAYS_INLINE int64_t nextLong() {
    // Follow the org.apache.commons.math3.random.BitsStreamGenerator
    // implementation.
    int64_t high = (int64_t)generator_() << 32;
    int64_t low = (int64_t)generator_() & 4294967295L;
    return high | low;
  }

  std::mt19937 generator_;
  boost::uuids::uuid uuid_;
};
} // namespace facebook::velox::functions::sparksql
