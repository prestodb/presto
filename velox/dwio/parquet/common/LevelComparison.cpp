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

// Adapted from Apache Arrow.

#include "velox/dwio/parquet/common/LevelComparison.h"

#include <limits>

#include "folly/lang/Bits.h"

namespace facebook::velox::parquet {
namespace {
template <typename Predicate>
inline uint64_t
LevelsToBitmap(const int16_t* levels, int64_t numLevels, Predicate predicate) {
  // Both clang and GCC can vectorize this automatically with SSE4/AVX2.
  uint64_t mask = 0;
  for (int x = 0; x < numLevels; x++) {
    mask |= static_cast<uint64_t>(predicate(levels[x]) ? 1 : 0) << x;
  }
  return folly::Endian::little(mask);
}

} // namespace

uint64_t
GreaterThanBitmap(const int16_t* levels, int64_t numLevels, int16_t rhs) {
  return LevelsToBitmap(
      levels, numLevels, [rhs](int16_t value) { return value > rhs; });
}

MinMax FindMinMax(const int16_t* levels, int64_t numLevels) {
  MinMax out{
      std::numeric_limits<int16_t>::max(), std::numeric_limits<int16_t>::min()};
  for (int x = 0; x < numLevels; x++) {
    out.min = std::min(levels[x], out.min);
    out.max = std::max(levels[x], out.max);
  }
  return out;
}

} // namespace facebook::velox::parquet
