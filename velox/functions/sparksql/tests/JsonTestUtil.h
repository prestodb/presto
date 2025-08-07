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

#include "velox/core/Expressions.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace facebook::velox::functions::sparksql::test {

constexpr float kNaNFloat = std::numeric_limits<float>::quiet_NaN();
constexpr float kInfFloat = std::numeric_limits<float>::infinity();
constexpr double kNaNDouble = std::numeric_limits<double>::quiet_NaN();
constexpr double kInfDouble = std::numeric_limits<double>::infinity();

inline core::CallTypedExprPtr createFromJson(const TypePtr& outputType) {
  std::vector<core::TypedExprPtr> inputs = {
      std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "c0")};

  return std::make_shared<const core::CallTypedExpr>(
      outputType, std::move(inputs), "from_json");
}

inline core::CallTypedExprPtr createToJson(
    const TypePtr& inputType,
    const std::optional<std::string>& timezone = std::nullopt) {
  auto input = std::make_shared<core::FieldAccessTypedExpr>(inputType, "c0");
  std::vector<core::TypedExprPtr> inputs = {input};
  if (timezone) {
    auto tz = std::make_shared<core::ConstantTypedExpr>(VARCHAR(), *timezone);
    inputs.emplace_back(tz);
  }
  return std::make_shared<const core::CallTypedExpr>(
      VARCHAR(), std::move(inputs), "to_json");
}
} // namespace facebook::velox::functions::sparksql::test
