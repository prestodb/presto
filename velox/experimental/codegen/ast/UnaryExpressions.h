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
#include "velox/common/base/Exceptions.h"
#include "velox/experimental/codegen/ast/ASTNode.h"

namespace facebook::velox::codegen {

/// Abstract class that represents unary expression.
class UnaryExpr : public ASTNode {
 public:
  UnaryExpr(const TypePtr& type, const ASTNodePtr& child)
      : ASTNode(type), child_(child) {}

  ~UnaryExpr() override {}

  virtual CodeSnippet generateCode(
      CodegenCtx& exprCodegenCtx,
      const std::string& outputVarName) const override = 0;

  const std::vector<ASTNodePtr> children() const override {
    return {child_};
  }

  ASTNodePtr child() const {
    return child_;
  }

 private:
  /// Child expression
  ASTNodePtr child_;
};

class IsNullExpr final : public UnaryExpr {
 public:
  IsNullExpr(const TypePtr& type, const ASTNodePtr& child)
      : UnaryExpr(type, child) {}

  void validate() const override {
    validateTyped();
  }

  CodeSnippet generateCode(
      CodegenCtx& exprCodegenCtx,
      const std::string& outputVarName) const override;

  virtual ExpressionNullMode getNullMode() const override {
    return ExpressionNullMode::NotNull;
  }
};

class NotExpr final : public UnaryExpr {
 public:
  NotExpr(const TypePtr& type, const ASTNodePtr& child)
      : UnaryExpr(type, child) {}

  void validate() const override {
    validateTyped();
  }

  CodeSnippet generateCode(
      CodegenCtx& exprCodegenCtx,
      const std::string& outputVarName) const override;

  virtual ExpressionNullMode getNullMode() const override {
    return ExpressionNullMode::NullInNullOut;
  }
};

} // namespace facebook::velox::codegen
