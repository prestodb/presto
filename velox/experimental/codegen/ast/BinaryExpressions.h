/*
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
#include "velox/experimental/codegen/ast/ASTNode.h"
#include "velox/experimental/codegen/ast/CodegenUtils.h"

namespace facebook::velox::codegen {

/// Abstract class that represents binary expression.
class BinaryExpr : public ASTNode {
 public:
  BinaryExpr(const TypePtr& type, const ASTNodePtr& lhs, const ASTNodePtr& rhs)
      : ASTNode(type), lhs_(lhs), rhs_(rhs) {}

  ~BinaryExpr() override {}

  virtual CodeSnippet generateCode(
      CodegenCtx& exprCodegenCtx,
      const std::string& outputVarName) const override = 0;

  const std::vector<ASTNodePtr> children() const override {
    return {lhs_, rhs_};
  }

  ASTNodePtr lhs() const {
    return lhs_;
  }

  ASTNodePtr rhs() const {
    return rhs_;
  }

 private:
  // LHS child expression
  ASTNodePtr lhs_;

  /// RHS child expression
  ASTNodePtr rhs_;
};

/// A template class to used to implement SymbolBinaryExpr, templated on a
/// binary symbol. Null mode is NullInNullOut for such expressions implemented
/// using this class.
template <const char* symbol>
class SymbolBinaryExpr final : public BinaryExpr {
 public:
  SymbolBinaryExpr(
      const TypePtr& type,
      const ASTNodePtr& lhs,
      const ASTNodePtr& rhs)
      : BinaryExpr(type, lhs, rhs) {}

 private:
  // Default code generation for symbol binary expression
  CodeSnippet generateCode(
      CodegenCtx& exprCodegenCtx,
      const std::string& outputVarName) const override;

  ExpressionNullMode getNullMode() const override {
    return ExpressionNullMode::NullInNullOut;
  }

  void validate() const override {
    if (getNullMode() != ExpressionNullMode::NullInNullOut) {
      throw ASTValidationException(
          "expecting NullInNullOut for default symbol binary expression, if getNullMode is overridden validate should be overridden as well");
    }
    validateTyped();
  }
};

class LogicalAnd final : public BinaryExpr {
 public:
  LogicalAnd(const ASTNodePtr& lhs, const ASTNodePtr& rhs)
      : BinaryExpr(BOOLEAN(), lhs, rhs) {}

 private:
  CodeSnippet generateCode(
      CodegenCtx& exprCodegenCtx,
      const std::string& outputs) const override;

  ExpressionNullMode getNullMode() const override {
    return ExpressionNullMode::NullableInNullableOut;
  }

  void validate() const override {
    validateTyped();
  }
};

class LogicalOr final : public BinaryExpr {
 public:
  LogicalOr(const ASTNodePtr& lhs, const ASTNodePtr& rhs)
      : BinaryExpr(BOOLEAN(), lhs, rhs) {}

 private:
  CodeSnippet generateCode(
      CodegenCtx& exprCodegenCtx,
      const std::string& outputVarName) const override;

  ExpressionNullMode getNullMode() const override {
    return ExpressionNullMode::NullableInNullableOut;
  }

  void validate() const override {
    validateTyped();
  }
};

// Default code generation for symbol binary expressions, it assumes null in
// null out semantics
template <const char* symbol>
CodeSnippet SymbolBinaryExpr<symbol>::generateCode(
    CodegenCtx& exprCodegenCtx,
    const std::string& outputVarName) const {
  std::stringstream code;
  auto lhsResultVar = exprCodegenCtx.getUniqueTemp("symbol_lhs_arg");
  auto rhsResultVar = exprCodegenCtx.getUniqueTemp("symbol_rhs_arg");

  // Execute lhs
  code << codegenUtils::codegenTempVarDecl(lhs()->type(), lhsResultVar);
  auto lhsCodeSnippet = lhs()->generateCode(exprCodegenCtx, lhsResultVar);
  code << lhsCodeSnippet.code();

  // Execute rhs
  code << codegenUtils::codegenTempVarDecl(rhs()->type(), rhsResultVar);
  auto rhsCodeSnippet = rhs()->generateCode(exprCodegenCtx, rhsResultVar);
  code << rhsCodeSnippet.code();

  auto assignmentRHS =
      codegenUtils::codegenBinarySymbolCall(symbol, lhsResultVar, rhsResultVar);
  auto evalWork = codegenUtils::codegenAssignment(outputVarName, assignmentRHS);

  std::vector<std::string> nullableArgs;

  if (lhs()->maybeNull()) {
    nullableArgs.push_back(lhsResultVar);
  }

  if (rhs()->maybeNull()) {
    nullableArgs.push_back(rhsResultVar);
  }

  code << codegenUtils::codegenNullInNullOut(
      evalWork, nullableArgs, outputVarName);
  return CodeSnippet(outputVarName, code.str());
}

const char kAddSymbol[] = "+";
using AddExpr = SymbolBinaryExpr<kAddSymbol>;

const char kSubtractSymbol[] = "-";
using SubtractExpr = SymbolBinaryExpr<kSubtractSymbol>;

const char kMultiplySymbol[] = "*";
using MultiplyExpr = SymbolBinaryExpr<kMultiplySymbol>;

const char kGreaterThantSymbol[] = ">";
using GreaterThan = SymbolBinaryExpr<kGreaterThantSymbol>;

const char kGreaterThanEqualSymbol[] = ">=";
using GreaterThanEquel = SymbolBinaryExpr<kGreaterThanEqualSymbol>;

const char kLessThantSymbol[] = "<";
using LessThan = SymbolBinaryExpr<kLessThantSymbol>;

const char kLessThanEqualSymbol[] = "<=";
using LessThanEqual = SymbolBinaryExpr<kLessThanEqualSymbol>;

const char kEqualSymbol[] = "==";
using Equal = SymbolBinaryExpr<kEqualSymbol>;

const char kNotEqualSymbol[] = "!=";
using NotEqual = SymbolBinaryExpr<kNotEqualSymbol>;

} // namespace facebook::velox::codegen
