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

#include <cmath>
#include <vector>
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::common::hll {

constexpr double kLowestMaxStandardError = 0.0040625;
constexpr double kHighestMaxStandardError = 0.26000;
constexpr double kDefaultApproxDistinctStandardError = 0.023;
constexpr double kDefaultApproxSetStandardError = 0.01625;

const int8_t kPrestoSparseV2 = 2;
const int8_t kPrestoDenseV2 = 3;

inline void checkMaxStandardError(double error) {
  VELOX_USER_CHECK_GE(
      error,
      kLowestMaxStandardError,
      "Max standard error must be in [{}, {}] range",
      kLowestMaxStandardError,
      kHighestMaxStandardError);
  VELOX_USER_CHECK_LE(
      error,
      kHighestMaxStandardError,
      "Max standard error must be in [{}, {}] range",
      kLowestMaxStandardError,
      kHighestMaxStandardError);
}

inline int8_t toIndexBitLength(double maxStandardError) {
  int buckets = std::ceil(1.0816 / (maxStandardError * maxStandardError));
  return 8 * sizeof(int) - __builtin_clz(buckets - 1);
}

/// Returns first 'indexBitLength' bits of a hash.
inline uint32_t computeIndex(uint64_t hash, int indexBitLength) {
  return hash >> (64 - indexBitLength);
}

/// Returns number of contiguous zeros after 'indexBitLength' bits in the
/// 'hash'.
inline int numberOfLeadingZeros(uint64_t hash, int indexBitLength) {
  // Place a 1 in the LSB to preserve the original number of leading zeros if
  // the hash happens to be 0.
  return __builtin_clzl(
      (hash << indexBitLength) | (1L << (indexBitLength - 1)));
}

/// Estimates cardinality using Linear Counting algorithm.
inline double linearCounting(int zeroBuckets, int totalBuckets) {
  return totalBuckets * std::log(totalBuckets * 1.0 / zeroBuckets);
}
} // namespace facebook::velox::common::hll
