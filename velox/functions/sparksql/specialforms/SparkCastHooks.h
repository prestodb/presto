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

#include "velox/expression/CastHooks.h"

namespace facebook::velox::functions::sparksql {

// This class provides cast hooks following Spark semantics.
class SparkCastHooks : public exec::CastHooks {
 public:
  // TODO: Spark hook allows more string patterns than Presto.
  Timestamp castStringToTimestamp(const StringView& view) const override;

  /// 1) Removes all leading and trailing UTF8 white-spaces before cast. 2) Uses
  /// non-standard cast mode to cast from string to date.
  int32_t castStringToDate(const StringView& dateString) const override;

  // Returns false.
  bool legacy() const override;

  /// When casting from string to integral, floating-point, decimal, date, and
  /// timestamp types, Spark hook trims all leading and trailing UTF8
  /// whitespaces before cast.
  StringView removeWhiteSpaces(const StringView& view) const override;

  /// 1) Does not follow 'isLegacyCast' and session timezone. 2) The conversion
  /// precision is microsecond. 3) Does not append trailing zeros. 4) Adds a
  /// positive sign at first if the year exceeds 9999.
  const TimestampToStringOptions& timestampToStringOptions() const override;

  // Returns true.
  bool truncate() const override;
};
} // namespace facebook::velox::functions::sparksql
