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

#include <optional>
#include "velox/type/Timestamp.h"

namespace facebook::velox::functions {
namespace {
constexpr double kNanosecondsInSecond = 1'000'000'000;
constexpr int64_t kNanosecondsInMillisecond = 1'000'000;
constexpr int64_t kMillisecondsInSecond = 1'000;
} // namespace

FOLLY_ALWAYS_INLINE double toUnixtime(const Timestamp& timestamp) {
  double result = timestamp.getSeconds();
  result += static_cast<double>(timestamp.getNanos()) / kNanosecondsInSecond;
  return result;
}

FOLLY_ALWAYS_INLINE std::optional<Timestamp> fromUnixtime(double unixtime) {
  if (UNLIKELY(std::isnan(unixtime))) {
    return Timestamp(0, 0);
  }

  static const int64_t kMax = std::numeric_limits<int64_t>::max();
  static const int64_t kMin = std::numeric_limits<int64_t>::min();

  static const Timestamp kMaxTimestamp(
      kMax / 1000, kMax % 1000 * kNanosecondsInMillisecond);
  static const Timestamp kMinTimestamp(
      kMin / 1000 - 1, (kMin % 1000 + 1000) * kNanosecondsInMillisecond);

  if (UNLIKELY(unixtime >= kMax)) {
    return kMaxTimestamp;
  }

  if (UNLIKELY(unixtime <= kMin)) {
    return kMinTimestamp;
  }

  if (UNLIKELY(std::isinf(unixtime))) {
    return unixtime < 0 ? kMinTimestamp : kMaxTimestamp;
  }

  auto seconds = std::floor(unixtime);
  auto nanos = unixtime - seconds;
  return Timestamp(seconds, nanos * kNanosecondsInSecond);
}
} // namespace facebook::velox::functions
