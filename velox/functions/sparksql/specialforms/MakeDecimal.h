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
class MakeDecimalCallToSpecialForm : public exec::FunctionCallToSpecialForm {
 public:
  // Throws not supported exception.
  TypePtr resolveType(const std::vector<TypePtr>& argTypes) override;

  /// @brief Returns an expression for make_decimal special form. The expression
  /// is a regular expression based on a custom VectorFunction implementation.
  /// @param type Result type. Must be short or long decimal.
  /// @param args One or two inputs. First input must be BIGINT (unscaled
  /// value). Second optional input must be constant BOOLEAN (indicates whether
  /// to return null or throw if unscaled value is too large for the result
  /// precision).
  exec::ExprPtr constructSpecialForm(
      const TypePtr& type,
      std::vector<exec::ExprPtr>&& args,
      bool trackCpuUsage,
      const core::QueryConfig& config) override;

  static constexpr const char* kMakeDecimal = "make_decimal";
};
} // namespace facebook::velox::functions::sparksql
