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

#include "velox/experimental/codegen/ast/UnaryExpressions.h"
#include "velox/experimental/codegen/ast/CodegenUtils.h"

namespace facebook {
namespace velox {
namespace codegen {

CodeSnippet IsNullExpr::generateCode(
    CodegenCtx& exprCodegenCtx,
    const std::string& outputVarName) const {
  /// FIXME: the below optimization is only correct if child expr has no side
  /// effect Need to add a side effect detection property and fix the code
  if (!child()->maybeNull()) {
    /// here we don't need the code of its child because we can directly return
    /// false, but we still need to traverse down so it sees all the input
    /// variables, this matters because of filter default null since at runtime
    /// it needs to enumerate all input and deselect null
    auto childCodeSnippet =
        child()->generateCode(exprCodegenCtx, "place_holder");
    return CodeSnippet(
        outputVarName, codegenUtils::codegenAssignment(outputVarName, "false"));
  }

  std::stringstream code;
  auto inputVar = exprCodegenCtx.getUniqueTemp("input");
  code << codegenUtils::codegenTempVarDecl(child()->type(), inputVar);
  auto childCodeSnippet = child()->generateCode(exprCodegenCtx, inputVar);
  code << childCodeSnippet.code();

  std::string inputNotNull = fmt::format("{}.has_value()", inputVar);
  code << codegenUtils::codegenAssignment(
      outputVarName,
      fmt::format(
          "{inputNotNull}?false:true", fmt::arg("inputNotNull", inputNotNull)));

  return CodeSnippet(outputVarName, code.str());
}

CodeSnippet NotExpr::generateCode(
    CodegenCtx& exprCodegenCtx,
    const std::string& outputVarName) const {
  std::stringstream code;
  auto inputVar = exprCodegenCtx.getUniqueTemp("input");
  code << codegenUtils::codegenTempVarDecl(child()->type(), inputVar);
  auto childCodeSnippet = child()->generateCode(exprCodegenCtx, inputVar);
  code << childCodeSnippet.code();

  std::string inputNotNull =
      child()->maybeNull() ? fmt::format("{}.has_value()", inputVar) : "true";
  std::string notInput = fmt::format("std::make_optional(!*{})", inputVar);

  code << codegenUtils::codegenAssignment(
      outputVarName,
      fmt::format(
          "{inputNotNull}?{notInput}:std::nullopt",
          fmt::arg("inputNotNull", inputNotNull),
          fmt::arg("notInput", notInput)));

  return CodeSnippet(outputVarName, code.str());
}
} // namespace codegen
} // namespace velox
} // namespace facebook
