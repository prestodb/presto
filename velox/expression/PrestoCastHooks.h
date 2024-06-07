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
#include "velox/expression/EvalCtx.h"

namespace facebook::velox::exec {

// This class provides cast hooks following Presto semantics.
class PrestoCastHooks : public CastHooks {
 public:
  explicit PrestoCastHooks(const core::QueryConfig& config);

  // Uses the default implementation of 'castFromDateString'.
  Expected<Timestamp> castStringToTimestamp(
      const StringView& view) const override;

  // Uses standard cast mode to cast from string to date.
  Expected<int32_t> castStringToDate(
      const StringView& dateString) const override;

  // Allows casting 'NaN', 'Infinity', and '-Infinity' to real, but not 'Inf' or
  // these strings with different letter cases.
  Expected<float> castStringToReal(const StringView& data) const override;

  // Allows casting 'NaN', 'Infinity', and '-Infinity' to double, but not 'Inf'
  // or these strings with different letter cases.
  Expected<double> castStringToDouble(const StringView& data) const override;

  bool legacy() const override {
    return legacyCast_;
  }

  // Returns the input as is.
  StringView removeWhiteSpaces(const StringView& view) const override;

  // Returns cast options following 'isLegacyCast' and session timezone.
  const TimestampToStringOptions& timestampToStringOptions() const override;

  bool truncate() const override {
    return false;
  }

 private:
  const bool legacyCast_;
  TimestampToStringOptions options_ = {
      .precision = TimestampToStringOptions::Precision::kMilliseconds};
};
} // namespace facebook::velox::exec
