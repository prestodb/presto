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

#include "velox/exec/fuzzer/PrestoQueryRunnerJsonTransform.h"
#include "velox/parse/Expressions.h"

namespace facebook::velox::exec::test {

core::ExprPtr JsonTransform::projectToTargetType(
    const core::ExprPtr& inputExpr,
    const std::string& columnAlias) const {
  return std::make_shared<core::CallExpr>(
      "try",
      std::vector<core::ExprPtr>{std::make_shared<core::CallExpr>(
          "json_format", std::vector<core::ExprPtr>{inputExpr}, std::nullopt)},
      columnAlias);
}

core::ExprPtr JsonTransform::projectToIntermediateType(
    const core::ExprPtr& inputExpr,
    const std::string& columnAlias) const {
  return std::make_shared<core::CallExpr>(
      "try",
      std::vector<core::ExprPtr>{std::make_shared<core::CallExpr>(
          "json_parse", std::vector<core::ExprPtr>{inputExpr}, std::nullopt)},
      columnAlias);
}

} // namespace facebook::velox::exec::test
