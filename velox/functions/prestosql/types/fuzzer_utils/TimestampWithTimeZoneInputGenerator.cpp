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

#include "velox/functions/prestosql/types/fuzzer_utils/TimestampWithTimeZoneInputGenerator.h"

#include "velox/common/fuzzer/Utils.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/type/Variant.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::fuzzer {
TimestampWithTimeZoneInputGenerator::TimestampWithTimeZoneInputGenerator(
    const size_t seed,
    const double nullRatio)
    : AbstractInputGenerator(
          seed,
          TIMESTAMP_WITH_TIME_ZONE(),
          nullptr,
          nullRatio),
      timeZoneIds_(tz::getTimeZoneIDs()) {}

variant TimestampWithTimeZoneInputGenerator::generate() {
  if (coinToss(rng_, nullRatio_)) {
    return variant::null(type_->kind());
  }

  int16_t timeZoneId =
      timeZoneIds_[rand<size_t>(rng_, 0, timeZoneIds_.size() - 1)];

  return pack(rand<int64_t>(rng_, kMinMillisUtc, kMaxMillisUtc), timeZoneId);
}
} // namespace facebook::velox::fuzzer
