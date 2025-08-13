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

#include "velox/core/QueryCtx.h"
#include "velox/expression/CastHooks.h"

namespace facebook::velox::functions::sparksql {

// This class provides cast hooks following Spark semantics.
class SparkCastHooks : public exec::CastHooks {
 public:
  explicit SparkCastHooks(
      const velox::core::QueryConfig& config,
      bool allowOverflow);

  // TODO: Spark hook allows more string patterns than Presto.
  Expected<Timestamp> castStringToTimestamp(
      const StringView& view) const override;

  /// When casting integral value as timestamp, the input is treated as the
  /// number of seconds since the epoch (1970-01-01 00:00:00 UTC).
  Expected<Timestamp> castIntToTimestamp(int64_t seconds) const override;

  Expected<int64_t> castTimestampToInt(Timestamp timestamp) const override;

  /// When casting double as timestamp, the input is treated as
  /// the number of seconds since the epoch (1970-01-01 00:00:00 UTC).
  Expected<std::optional<Timestamp>> castDoubleToTimestamp(
      double value) const override;

  /// 1) Removes all leading and trailing UTF8 white-spaces before cast. 2) Uses
  /// non-standard cast mode to cast from string to date.
  Expected<int32_t> castStringToDate(
      const StringView& dateString) const override;

  // Allows casting 'NaN', 'Infinity', '-Infinity', 'Inf', '-Inf', and these
  // strings with different letter cases to real.
  Expected<float> castStringToReal(const StringView& data) const override;

  // Allows casting 'NaN', 'Infinity', '-Infinity', 'Inf', '-Inf', and these
  // strings with different letter cases to double.
  Expected<double> castStringToDouble(const StringView& data) const override;

  /// When casting from string to integral, floating-point, decimal, date, and
  /// timestamp types, Spark hook trims all leading and trailing UTF8
  /// whitespaces before cast.
  StringView removeWhiteSpaces(const StringView& view) const override;

  // Supports Spark boolean to timestamp cast.
  Expected<Timestamp> castBooleanToTimestamp(bool seconds) const override;

  const TimestampToStringOptions& timestampToStringOptions() const override {
    return timestampToStringOptions_;
  }

  bool truncate() const override {
    return allowOverflow_;
  }

  bool applyTryCastRecursively() const override {
    return true;
  }

  exec::PolicyType getPolicy() const override;

 private:
  // Casts a number to a timestamp. The number is treated as the number of
  // seconds since the epoch (1970-01-01 00:00:00 UTC).
  // Supports integer and floating-point types.
  template <typename T>
  Expected<Timestamp> castNumberToTimestamp(T seconds) const;

  const core::QueryConfig& config_;

  // If true, the cast will truncate the overflow value to fit the target type.
  const bool allowOverflow_;

  /// 1) Does not follow 'isLegacyCast'. 2) The conversion precision is
  /// microsecond. 3) Does not append trailing zeros. 4) Adds a positive
  /// sign at first if the year exceeds 9999. 5) Respects the configured
  /// session timezone.
  TimestampToStringOptions timestampToStringOptions_ = {
      .precision = TimestampToStringOptions::Precision::kMicroseconds,
      .leadingPositiveSign = true,
      .skipTrailingZeros = true,
      .zeroPaddingYear = true,
      .dateTimeSeparator = ' '};
};
} // namespace facebook::velox::functions::sparksql
