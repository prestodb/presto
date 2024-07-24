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

#include "velox/functions/lib/aggregates/Compare.h"

namespace facebook::velox::functions::aggregate {

int32_t compare(
    const SingleValueAccumulator* accumulator,
    const DecodedVector& decoded,
    vector_size_t index,
    CompareFlags::NullHandlingMode nullHandlingMode) {
  static const CompareFlags kCompareFlags{
      true, // nullsFirst
      true, // ascending
      false, // equalsOnly
      nullHandlingMode};

  auto result = accumulator->compare(decoded, index, kCompareFlags);
  VELOX_USER_CHECK(
      result.has_value(),
      fmt::format(
          "{} comparison not supported for values that contain nulls",
          mapTypeKindToName(decoded.base()->typeKind())));
  return result.value();
}
} // namespace facebook::velox::functions::aggregate
