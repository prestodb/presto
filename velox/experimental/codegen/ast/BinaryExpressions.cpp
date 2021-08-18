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

#include "velox/experimental/codegen/ast/BinaryExpressions.h"
#include "velox/experimental/codegen/ast/CodegenUtils.h"

namespace facebook {
namespace velox {
namespace codegen {

// Presto reference semantics
// Reference semantics https://prestodb.io/docs/current/functions/logical.html

CodeSnippet LogicalAnd::generateCode(
    CodegenCtx& exprCodegenCtx,
    const std::string& outputVarName) const {
  std::stringstream code;

  // Execute lhs
  auto lhsResultVar = exprCodegenCtx.getUniqueTemp("lhsAnd");
  code << codegenUtils::codegenTempVarDecl(lhs()->type(), lhsResultVar);
  auto lhsCodeSnippet = lhs()->generateCode(exprCodegenCtx, lhsResultVar);

  // Execute rhs
  auto rhsResultVar = exprCodegenCtx.getUniqueTemp("rhsAnd");
  code << codegenUtils::codegenTempVarDecl(rhs()->type(), rhsResultVar);
  auto rhsCodeSnippet = rhs()->generateCode(exprCodegenCtx, rhsResultVar);

  // Turn off dynamic check for nullability when we can
  std::string rhsHasValue =
      codegenUtils::codegenHasValueCheck(rhsResultVar, rhs()->maybeNull());
  std::string lhsHasValue =
      codegenUtils::codegenHasValueCheck(lhsResultVar, lhs()->maybeNull());

  code << fmt::format(
      "{lhsCode} {rhsCode}"
      "" // Both true return true
      "if({lhsHasValue} && *{lhs} && {rhsHasValue} && *{rhs}) {{"
      "   {outVar} = true; "
      "}}"
      "" // Any false return false
      "else if({lhsHasValue} && !*{lhs}) {{"
      "   {outVar} = false;"
      "}}"
      ""
      "else if({rhsHasValue} && !*{rhs}) {{"
      "   {outVar} = false;"
      "}}"
      "" // Return null
      "else {{"
      "  {outVar} = std::nullopt;"
      "}}",
      fmt::arg("lhsCode", lhsCodeSnippet.code()),
      fmt::arg("rhsCode", rhsCodeSnippet.code()),
      fmt::arg("lhs", lhsResultVar),
      fmt::arg("rhs", rhsResultVar),
      fmt::arg("rhsHasValue", rhsHasValue),
      fmt::arg("lhsHasValue", lhsHasValue),
      fmt::arg("outVar", outputVarName));

  return CodeSnippet(outputVarName, code.str());
}

CodeSnippet LogicalOr::generateCode(
    CodegenCtx& exprCodegenCtx,
    const std::string& outputVarName) const {
  std::stringstream code;
  // Execute lhs
  auto lhsResultVar = exprCodegenCtx.getUniqueTemp("lhsOr");
  code << codegenUtils::codegenTempVarDecl(lhs()->type(), lhsResultVar);
  auto lhsCodeSnippet = lhs()->generateCode(exprCodegenCtx, lhsResultVar);

  // Execute rhs
  auto rhsResultVar = exprCodegenCtx.getUniqueTemp("rhsOr");
  code << codegenUtils::codegenTempVarDecl(rhs()->type(), rhsResultVar);
  auto rhsCodeSnippet = rhs()->generateCode(exprCodegenCtx, rhsResultVar);

  // Turn off dynamic check for nullability when we can
  std::string rhsHasValue =
      codegenUtils::codegenHasValueCheck(rhsResultVar, rhs()->maybeNull());
  std::string lhsHasValue =
      codegenUtils::codegenHasValueCheck(lhsResultVar, lhs()->maybeNull());

  code << fmt::format(
      "{lhsCode} {rhsCode}"
      "" // Both false return false
      "if({lhsHasValue} && !*{lhs} && {rhsHasValue} && !*{rhs}) {{"
      "   {outVar} = false; "
      "}}"
      "" // Any true return true
      "else if({lhsHasValue} && *{lhs}) {{"
      "   {outVar} = true;"
      "}}"
      ""
      "else if({rhsHasValue} && *{rhs}) {{"
      "   {outVar} = true;"
      "}}"
      "" // Return null
      "else {{"
      "  {outVar} = std::nullopt;"
      "}}",
      fmt::arg("lhsCode", lhsCodeSnippet.code()),
      fmt::arg("rhsCode", rhsCodeSnippet.code()),
      fmt::arg("lhs", lhsResultVar),
      fmt::arg("rhs", rhsResultVar),
      fmt::arg("rhsHasValue", rhsHasValue),
      fmt::arg("lhsHasValue", lhsHasValue),
      fmt::arg("outVar", outputVarName));

  return CodeSnippet(outputVarName, code.str());
}

} // namespace codegen
} // namespace velox
} // namespace facebook
