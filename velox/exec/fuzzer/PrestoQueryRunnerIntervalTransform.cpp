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

#include "velox/exec/fuzzer/PrestoQueryRunnerIntervalTransform.h"
#include "velox/parse/Expressions.h"

namespace facebook::velox::exec::test {

core::ExprPtr IntervalDayTimeTransform::projectToTargetType(
    const core::ExprPtr& inputExpr,
    const std::string& columnAlias) const {
  return std::make_shared<core::CallExpr>(
      "try",
      std::vector<core::ExprPtr>{std::make_shared<core::CallExpr>(
          "to_milliseconds",
          std::vector<core::ExprPtr>{inputExpr},
          std::nullopt)},
      columnAlias);
}

// The below transform executes the following Presto SQL:
// try(parse_duration(concat(CAST(inputExpr AS VARCHAR), 'ms')))
//
// This casts the inputted bigint as a string and concatenates it with 'ms'
// to generate a valid millisecond duration as a string, which will be parsed
// as a valid interval and easily re-converted using to_milliseconds.
core::ExprPtr IntervalDayTimeTransform::projectToIntermediateType(
    const core::ExprPtr& inputExpr,
    const std::string& columnAlias) const {
  return std::make_shared<core::CallExpr>(
      "try",
      std::vector<core::ExprPtr>{std::make_shared<core::CallExpr>(
          "parse_duration",
          std::vector<core::ExprPtr>{std::make_shared<core::CallExpr>(
              "concat",
              std::vector<core::ExprPtr>{
                  std::make_shared<core::CastExpr>(
                      VARCHAR(), inputExpr, false, columnAlias),
                  std::make_shared<core::ConstantExpr>(
                      VARCHAR(),
                      variant::create<TypeKind::VARCHAR>("ms"),
                      std::nullopt)},
              std::nullopt)},
          std::nullopt)},
      columnAlias);
}

} // namespace facebook::velox::exec::test
