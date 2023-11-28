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

#include "velox/expression/FunctionCallToSpecialForm.h"

namespace facebook::velox::functions::sparksql {

class DecimalRoundCallToSpecialForm : public exec::FunctionCallToSpecialForm {
 public:
  // Throws not supported exception.
  TypePtr resolveType(const std::vector<TypePtr>& argTypes) override;

  /// @brief Returns an expression for decimal_round special form. The
  /// expression is a regular expression based on a custom VectorFunction
  /// implementation.
  /// @param type Result type. Must be short or long decimal.
  /// @param args One or two inputs. First input must be decimal. Second
  /// optional input is the new scale to be rounded to, and must be constant
  /// INTEGER.
  exec::ExprPtr constructSpecialForm(
      const TypePtr& type,
      std::vector<exec::ExprPtr>&& args,
      bool trackCpuUsage,
      const core::QueryConfig& config) override;

  /// Returns the result precision and scale after rounding from the input
  /// precision and scale to a new scale. The calculation logic is consistent
  /// with Spark version after 3.3.
  static std::pair<uint8_t, uint8_t>
  getResultPrecisionScale(uint8_t precision, uint8_t scale, int32_t roundScale);

  static constexpr const char* kRoundDecimal = "decimal_round";
};
} // namespace facebook::velox::functions::sparksql
