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

#include "velox/experimental/codegen/ast/ASTNode.h"
#include <sstream>
#include "velox/common/base/Exceptions.h"
#include "velox/experimental/codegen/ast/CodegenUtils.h"
#include "velox/experimental/codegen/ast/UnaryExpressions.h"

namespace facebook::velox::codegen {

ASTNode::~ASTNode(){};

void ASTNode::markAllInputsNotNullable() {
  if (auto* inputRefExpr = this->as<InputRefExpr>()) {
    inputRefExpr->setMaybeNull(false);
  }
  for (auto& child : children()) {
    child->markAllInputsNotNullable();
  }
}

// Implements codegen for inputRef expression
CodeSnippet InputRefExpr::generateCode(
    CodegenCtx& exprCodegenCtx,
    const std::string& outputVarName) const {
  std::string code;
  auto inputRowIndex = exprCodegenCtx.computeInputTypeDuringCodegen()
      ? exprCodegenCtx.computeIndexForColumn(name(), typePtr())
      : this->index();
  auto inputArgAccess =
      codegenUtils::codegenTupleAccess("inputTuple", inputRowIndex);

  code = codegenUtils::codegenAssignment(outputVarName, inputArgAccess);

  return CodeSnippet(outputVarName, code);
}

// Implements codegen for output expression
CodeSnippet MakeRowExpression::generateCode(
    CodegenCtx& exprCodegenCtx,
    const std::string& outputVarName) const {
  std::stringstream code;
  for (auto i = 0; i < children_.size(); i++) {
    // Write results in output tuple fields directly
    auto childCodeSnippet = children_[i]->generateCode(
        exprCodegenCtx, codegenUtils::codegenTupleAccess(outputVarName, i));
    code << childCodeSnippet.code();
  }

  return CodeSnippet(outputVarName, code.str());
}

CodeSnippet IfExpression::generateCode(
    CodegenCtx& exprCodegenCtx,
    const std::string& outputVarName) const {
  std::stringstream code;
  auto conditionOutputVariable = exprCodegenCtx.getUniqueTemp("condition");
  code << codegenUtils::codegenTempVarDecl(
      BooleanType(), conditionOutputVariable);
  auto conditionExprCodeSnippet =
      condition_->generateCode(exprCodegenCtx, conditionOutputVariable);

  auto thenExprCodeSnippet =
      thenPart_->generateCode(exprCodegenCtx, outputVarName);

  CodeSnippet elseExprCodeSnippet;
  if (elsePart_) {
    elseExprCodeSnippet =
        elsePart_->generateCode(exprCodegenCtx, outputVarName);
  } else {
    // Default is null
    auto outputVar = exprCodegenCtx.getUniqueTemp();

    elseExprCodeSnippet = CodeSnippet(
        outputVarName,
        codegenUtils::codegenAssignment(outputVarName, "std::nullopt"));
  }

  std::string condition;
  if (!condition_->maybeNull()) {
    condition = fmt::format("*{}", conditionOutputVariable);
  } else {
    condition = fmt::format("{0}.has_value() && *{0}", conditionOutputVariable);
  }

  // TODO: support eager format
  if (isEager_) {
    throw CodegenNotSupported("eager if not supported");
  }

  constexpr auto lazyFormat =
      "{conditionCode} if ({condition}) "
      "{{ {thenCode}  }} else {{ {elseCode} }}";

  code << fmt::format(
      lazyFormat,
      fmt::arg("conditionCode", conditionExprCodeSnippet.code()),
      fmt::arg("elseCode", elseExprCodeSnippet.code()),
      fmt::arg("thenCode", thenExprCodeSnippet.code()),
      fmt::arg("condition", condition));

  return CodeSnippet(outputVarName, code.str());
}

void IfExpression::propagateNullability() {
  condition_->propagateNullability();
  thenPart_->propagateNullability();
  if (elsePart_) {
    elsePart_->propagateNullability();
  }

  // TODO: double check if we need to check on eagerness here.
  setMaybeNull(
      thenPart_->maybeNull() || elsePart_ == nullptr || elsePart_->maybeNull());
}

void IfExpression::validate() const {
  validateTyped();
}

CodeSnippet SwitchExpression::generateCode(
    CodegenCtx& exprCodegenCtx,
    const std::string& outputVarName) const {
  std::stringstream code;

  for (int i = 0; i < inputs_.size() / 2; i++) {
    auto conditionOutputVariable = exprCodegenCtx.getUniqueTemp("condition");
    auto whenPart = inputs_[2 * i];
    auto thenPart = inputs_[2 * i + 1];

    auto conditionExprCodeSnippet =
        whenPart->generateCode(exprCodegenCtx, conditionOutputVariable);
    auto thenExprCodeSnippet =
        thenPart->generateCode(exprCodegenCtx, outputVarName);

    std::string condition;
    if (!whenPart->maybeNull()) {
      condition = fmt::format("*{}", conditionOutputVariable);
    } else {
      condition =
          fmt::format("{0}.has_value() && *{0}", conditionOutputVariable);
    }

    constexpr auto lazyFormat =
        "{maybeElse}if (({{ {conditionVar} {conditionCode}{condition}; }})) "
        "{{ {thenCode}  }}";

    code << fmt::format(
        lazyFormat,
        fmt::arg("maybeElse", (i > 0) ? "else " : ""),
        fmt::arg(
            "conditionVar",
            codegenUtils::codegenTempVarDecl(
                BooleanType(), conditionOutputVariable)),
        fmt::arg("conditionCode", conditionExprCodeSnippet.code()),
        fmt::arg("thenCode", thenExprCodeSnippet.code()),
        fmt::arg("condition", condition));
  }

  CodeSnippet elseExprCodeSnippet;
  if (inputs_.size() & 1) { // has ELSE
    elseExprCodeSnippet =
        inputs_.back()->generateCode(exprCodegenCtx, outputVarName);
  } else {
    // Default is null
    auto outputVar = exprCodegenCtx.getUniqueTemp();

    elseExprCodeSnippet = CodeSnippet(
        outputVarName,
        codegenUtils::codegenAssignment(outputVarName, "std::nullopt"));
  }

  constexpr auto lazyFormat = "else {{ {elseCode} }}";

  code << fmt::format(
      lazyFormat, fmt::arg("elseCode", elseExprCodeSnippet.code()));

  return CodeSnippet(outputVarName, code.str());
}

void SwitchExpression::propagateNullability() {
  bool maybeNull = false;
  for (int i = 0; i < inputs_.size() / 2; i++) {
    auto whenPart = inputs_[2 * i];
    auto thenPart = inputs_[2 * i + 1];
    whenPart->propagateNullability();
    thenPart->propagateNullability();
    maybeNull |= thenPart->maybeNull();
  }

  if (inputs_.size() & 1) { // has ELSE
    auto elsePart = inputs_.back();
    elsePart->maybeNull();
    maybeNull |= elsePart->maybeNull();
  } else {
    // FIXME: Unless query optimizer took care of this already, othersie this
    // isn't a careful enough check, if the cases cover all possibilities, then
    // it won't NULL even without ELSE.
    maybeNull = true;
  }

  setMaybeNull(maybeNull);
}

void SwitchExpression::validate() const {
  validateTyped();
}

// UDF Call expression
void UDFCallExpr::validate() const {
  if (getNullMode() == ExpressionNullMode::Custom) {
    throw CodegenNotSupported(
        "udf does not support custom nullable propagation");
  }
  validateTyped();
}

CodeSnippet UDFCallExpr::generateCode(
    CodegenCtx& exprCodegenCtx,
    const std::string& outputVarName) const {
  for (auto& header : getHeaderFiles()) {
    exprCodegenCtx.addHeaderPath(header);
  }

  exprCodegenCtx.addLibs(getLibs());

  std::stringstream code;
  std::stringstream inputArguments;

  bool first = true;
  std::vector<std::string> nullableArgs;
  size_t i = 0;
  for (auto child : children()) {
    if (!first) {
      inputArguments << ", ";
    }

    if (first) {
      first = false;
    }

    // UDF arguments are always materialized even for complex types,
    // if things can benefit from a lazy eval, they should be implemented as
    // built in.
    // In theory we can allow UDFs to accept lambdas with a lazy arg configs.
    auto childResultVariable =
        exprCodegenCtx.getUniqueTemp(fmt::format("udf_arg_{}", i++));

    code << codegenUtils::codegenTempVarDecl(
        child->type(), childResultVariable);
    if (child->maybeNull()) {
      nullableArgs.push_back(childResultVariable);
    }

    auto childCode = child->generateCode(exprCodegenCtx, childResultVariable);
    code << childCode.code();

    // TODO: this is a work around for UDFs that do not respect the templated
    // interface as input. i.e: cast udfs with string inputs.
    inputArguments
        << (udfInformation_.isOptionalArguments()
                ? childResultVariable
                : codegenUtils::codegenOptionalAccess(childResultVariable));
  }

  std::string work;
  if (udfInformation_.getOutputForm() == OutputForm::Return) {
    VELOX_CHECK(type().kind() != TypeKind::VARCHAR);
    std::string functionCall;

    if (udfInformation_.isOptionalOutput()) {
      functionCall =
          fmt::format("{}({})", getFunctionName(), inputArguments.str());
    } else {
      functionCall = fmt::format(
          "{}{{ {}({}) }}",
          codegenUtils::codegenNullableNativeType(
              type(), codegenUtils::TypePlacement::Temp),
          getFunctionName(),
          inputArguments.str());
    }
    work = codegenUtils::codegenAssignment(outputVarName, functionCall);
  } else if (udfInformation_.getOutputForm() == OutputForm::InOut) {
    // Generate output argument
    std::string outputInOutArg;
    if (udfInformation_.isOptionalOutput()) {
      VELOX_CHECK(type().kind() != TypeKind::VARCHAR);
      outputInOutArg = outputVarName;
    } else {
      outputInOutArg = fmt::format("*{}", outputVarName);
    }

    // Add output argument to the beginning
    auto allArguments = inputArguments.str();
    if (allArguments.size() == 0) {
      allArguments = outputInOutArg;
    } else {
      allArguments = fmt::format("{},{}", outputInOutArg, allArguments);
    }

    // Generate function call
    work = fmt::format("{}({});", getFunctionName(), allArguments);
    // Call finalize for strings
    if (udfInformation_.getOutputForm() == OutputForm::InOut &&
        type().kind() == TypeKind::VARCHAR) {
      work += fmt::format("({}).finalize();", outputInOutArg);
    }
  }

  if (udfInformation_.codegenNullInNullOutChecks()) {
    code << codegenUtils::codegenNullInNullOut(
        work, nullableArgs, outputVarName);
  } else {
    code << work;
  }
  return CodeSnippet(outputVarName, code.str());
}

void CoalesceExpr::propagateNullability() {
  for (auto& child : children()) {
    child->propagateNullability();
  }

  // If any is not nullable output is not nullable
  for (auto& child : children()) {
    if (!child->maybeNull()) {
      setMaybeNull(false);
      return;
    }
  }

  // All  inputs maybe null
  setMaybeNull(true);
}

CodeSnippet CoalesceExpr::generateCode(
    CodegenCtx& exprCodegenCtx,
    const std::string& outputVarName) const {
  std::stringstream code;

  auto children_ = children();
  std::function<void(int)> generateChildrenCode = [&](int idx) {
    if (idx == children_.size()) {
      // write null to output
      code << codegenUtils::codegenAssignment(outputVarName, "std::nullopt");
      return;
    }

    auto& child = children_[idx];

    // We need to make sure we can shallow copy the writers of complex types in
    // efficient way.
    auto childCodeSnippet = child->generateCode(exprCodegenCtx, outputVarName);
    code << childCodeSnippet.code() << "\n";

    code << fmt::format(
        "if (!{condition}) {{ ",
        fmt::arg(
            "condition",
            child->maybeNull()
                ? codegenUtils::codegenOptionalValueCheck(outputVarName)
                : "true"));
    generateChildrenCode(idx + 1);
    // Close else
    code << "}";
  };

  generateChildrenCode(0);
  return CodeSnippet(outputVarName, code.str());
}
} // namespace facebook::velox::codegen
