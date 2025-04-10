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

#include <codecvt>

#include <folly/Random.h>

#include <boost/random/uniform_01.hpp>
#include <boost/random/uniform_int_distribution.hpp>
#include <boost/random/uniform_real_distribution.hpp>

#include "velox/common/base/Exceptions.h"
#include "velox/type/Type.h"

namespace facebook::velox::fuzzer {

using FuzzerGenerator = folly::detail::DefaultGenerator;

enum UTF8CharList {
  ASCII = 0, // Ascii character set.
  UNICODE_CASE_SENSITIVE = 1, // Unicode scripts that support case.
  EXTENDED_UNICODE = 2, // Extended Unicode: Arabic, Devanagiri etc
  MATHEMATICAL_SYMBOLS = 3 // Mathematical Symbols.
};

bool coinToss(FuzzerGenerator& rng, double threshold);

/// Generate a random type with given scalar types. The level of nesting is up
/// to maxDepth. If keyTypes is non-empty, choosing from keyTypes when
/// determining the types of map keys. If keyTypes is empty, choosing from
/// scalarTypes for the types of map keys. Similar for valueTypes.
TypePtr randType(
    FuzzerGenerator& rng,
    const std::vector<TypePtr>& scalarTypes,
    int maxDepth,
    const std::vector<TypePtr>& mapKeyTypes = {},
    const std::vector<TypePtr>& mapValueTypes = {});

/// Similar to randType but generates a random map type.
TypePtr randMapType(
    FuzzerGenerator& rng,
    const std::vector<TypePtr>& scalarTypes,
    int maxDepth,
    const std::vector<TypePtr>& mapKeyTypes = {},
    const std::vector<TypePtr>& mapValueTypes = {});

/// Similar to randType but generates a random row type.
RowTypePtr randRowType(
    FuzzerGenerator& rng,
    const std::vector<TypePtr>& scalarTypes,
    int maxDepth,
    const std::vector<TypePtr>& mapKeyTypes = {},
    const std::vector<TypePtr>& mapValueTypes = {});

struct DataSpec {
  bool includeNaN;
  bool includeInfinity;
};

enum class FuzzerTimestampPrecision : int8_t {
  kNanoSeconds = 0,
  kMicroSeconds = 1,
  kMilliSeconds = 2,
  kSeconds = 3,
};

// Generate random values for the different supported types.
template <typename T>
inline T rand(
    FuzzerGenerator& /*rng*/,
    DataSpec /*dataSpec*/ = {false, false}) {
  VELOX_NYI();
}

template <>
inline int8_t rand(FuzzerGenerator& rng, DataSpec /*dataSpec*/) {
  return boost::random::uniform_int_distribution<int8_t>()(rng);
}

template <>
inline int16_t rand(FuzzerGenerator& rng, DataSpec /*dataSpec*/) {
  return boost::random::uniform_int_distribution<int16_t>()(rng);
}

template <>
inline int32_t rand(FuzzerGenerator& rng, DataSpec /*dataSpec*/) {
  return boost::random::uniform_int_distribution<int32_t>()(rng);
}

template <>
inline int64_t rand(FuzzerGenerator& rng, DataSpec /*dataSpec*/) {
  return boost::random::uniform_int_distribution<int64_t>()(rng);
}

template <>
inline double rand(FuzzerGenerator& rng, DataSpec dataSpec) {
  if (dataSpec.includeNaN && coinToss(rng, 0.05)) {
    return std::nan("");
  }

  if (dataSpec.includeInfinity && coinToss(rng, 0.05)) {
    return std::numeric_limits<double>::infinity();
  }

  return boost::random::uniform_01<double>()(rng);
}

template <>
inline float rand(FuzzerGenerator& rng, DataSpec dataSpec) {
  if (dataSpec.includeNaN && coinToss(rng, 0.05)) {
    return std::nanf("");
  }

  if (dataSpec.includeInfinity && coinToss(rng, 0.05)) {
    return std::numeric_limits<float>::infinity();
  }

  return boost::random::uniform_01<float>()(rng);
}

template <>
inline bool rand(FuzzerGenerator& rng, DataSpec /*dataSpec*/) {
  return boost::random::uniform_int_distribution<uint32_t>(0, 1)(rng);
}

template <>
inline uint32_t rand(FuzzerGenerator& rng, DataSpec /*dataSpec*/) {
  return boost::random::uniform_int_distribution<uint32_t>()(rng);
}

template <>
inline uint64_t rand(FuzzerGenerator& rng, DataSpec /*dataSpec*/) {
  return boost::random::uniform_int_distribution<uint64_t>()(rng);
}

template <>
inline int128_t rand(FuzzerGenerator& rng, DataSpec /*dataSpec*/) {
  return HugeInt::build(rand<int64_t>(rng), rand<uint64_t>(rng));
}

Timestamp randTimestamp(
    FuzzerGenerator& rng,
    FuzzerTimestampPrecision timestampPrecision);

template <>
inline Timestamp rand(FuzzerGenerator& rng, DataSpec /*dataSpec*/) {
  // TODO: support other timestamp precisions.
  return randTimestamp(rng, FuzzerTimestampPrecision::kMicroSeconds);
}

int32_t randDate(FuzzerGenerator& rng);

template <
    typename T,
    typename std::enable_if_t<std::is_arithmetic_v<T>, int> = 0>
inline T rand(FuzzerGenerator& rng, T min, T max) {
  if constexpr (std::is_integral_v<T>) {
    return boost::random::uniform_int_distribution<T>(min, max)(rng);
  } else {
    return boost::random::uniform_real_distribution<T>(min, max)(rng);
  }
}

/// Generates a random string in buf with characters of encodings. Return buf at
/// the end.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
std::string randString(
    FuzzerGenerator& rng,
    size_t length,
    const std::vector<UTF8CharList>& encodings,
    std::string& buf,
    std::wstring_convert<std::codecvt_utf8<char16_t>, char16_t>& converter);
#pragma GCC diagnostic pop

} // namespace facebook::velox::fuzzer
