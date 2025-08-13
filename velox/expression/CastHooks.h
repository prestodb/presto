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

#include "velox/expression/StringWriter.h"
#include "velox/type/Timestamp.h"

namespace facebook::velox::exec {

enum PolicyType {
  LegacyCastPolicy = 1,
  PrestoCastPolicy,
  SparkCastPolicy,
  SparkTryCastPolicy
};

fmt::underlying_t<PolicyType> format_as(PolicyType f);

/// This class provides cast hooks to allow different behaviors of CastExpr and
/// SparkCastExpr. The main purpose is to create customized cast implementation
/// by taking full usage of existing cast expression.
class CastHooks {
 public:
  virtual ~CastHooks() = default;

  virtual Expected<Timestamp> castStringToTimestamp(
      const StringView& view) const = 0;

  virtual Expected<Timestamp> castIntToTimestamp(int64_t seconds) const = 0;

  virtual Expected<int64_t> castTimestampToInt(Timestamp timestamp) const = 0;

  virtual Expected<std::optional<Timestamp>> castDoubleToTimestamp(
      double seconds) const = 0;

  virtual Expected<int32_t> castStringToDate(
      const StringView& dateString) const = 0;

  /// 'data' is guaranteed to be non-empty and has been processed by
  /// removeWhiteSpaces.
  virtual Expected<float> castStringToReal(const StringView& data) const = 0;

  /// 'data' is guaranteed to be non-empty and has been processed by
  /// removeWhiteSpaces.
  virtual Expected<double> castStringToDouble(const StringView& data) const = 0;

  /// Trims all leading and trailing UTF8 whitespaces.
  virtual StringView removeWhiteSpaces(const StringView& view) const = 0;

  /// Returns the options to cast from timestamp to string.
  virtual const TimestampToStringOptions& timestampToStringOptions() const = 0;

  /// Returns whether to cast to int by truncate.
  virtual bool truncate() const = 0;

  /// Returns whether to apply try_cast recursively rather than only at the top
  /// level. E.g. if true, an element inside an array would be null rather than
  /// the entire array if the cast of that element fails.
  virtual bool applyTryCastRecursively() const = 0;

  virtual PolicyType getPolicy() const = 0;

  /// Converts boolean to timestamp type.
  virtual Expected<Timestamp> castBooleanToTimestamp(bool seconds) const = 0;
};
} // namespace facebook::velox::exec
