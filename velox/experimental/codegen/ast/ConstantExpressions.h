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
#include <string>

#include "velox/experimental/codegen/CodegenExceptions.h"
#include "velox/experimental/codegen/ast/ASTNode.h"
#include "velox/experimental/codegen/ast/CodegenUtils.h"

namespace facebook {
namespace velox {
namespace codegen {

namespace {
// Interesting discussion
// https://stackoverflow.com/questions/208433/how-do-i-write-a-short-literal-in-c
// the compiler optimizes the static cast away and const fold it

/// Converts a value to its code string representation
/// Only this needed to be implemented to extend constant expression to a
/// new type
template <typename T>
std::string codegenValue([[maybe_unused]] const T& value) {
  throw CodegenNotSupported(
      fmt::format("unsupported type {} for codegenValue", typeid(T).name()));
}

template <>
inline std::string codegenValue<bool>(const bool& value) {
  if (value) {
    return "true";
  } else {
    return "false";
  }
}

template <>
inline std::string codegenValue<int8_t>(const int8_t& value) {
  return fmt::format("static_cast<int8_t>({})", value);
}

template <>
inline std::string codegenValue<int16_t>(const int16_t& value) {
  return fmt::format("static_cast<int16_t>({})", value);
}

template <>
inline std::string codegenValue<int32_t>(const int32_t& value) {
  return fmt::format("static_cast<int32_t>({})", value);
}

template <>
inline std::string codegenValue<int64_t>(const int64_t& value) {
  return fmt::format("static_cast<int64_t>({})", value);
}

template <>
inline std::string codegenValue<float>(const float& value) {
  return fmt::format("static_cast<float>({})", value);
}

template <>
inline std::string codegenValue<double>(const double& value) {
  return fmt::format("static_cast<double>({})", value);
}

template <>
inline std::string codegenValue<std::string>(const std::string& value) {
  return value;
}
} // namespace

/// Template class that used to implement constant expressions
template <typename T>
class ConstantExpression final : public ASTNode {
 public:
  ConstantExpression(const TypePtr& type, const std::optional<T>& value)
      : ASTNode(type), value_(value) {
    setMaybeNull(isNull());
  }

  ~ConstantExpression() override {}

  bool isConstantExpression() const override {
    return true;
  }

  CodeSnippet generateCode(
      CodegenCtx& exprCodegenCtx,
      const std::string& outputVarName) const override {
    exprCodegenCtx.addHeaderPaths(getHeaderFiles());

    auto constantValue =
        isNull() ? "std::nullopt" : codegenValue<T>(value_.value());
    auto code = codegenUtils::codegenAssignment(
        codegenUtils::codegenOptionalAccess(outputVarName), constantValue);
    return CodeSnippet(outputVarName, code);
  }

  const std::vector<ASTNodePtr> children() const override {
    return {};
  }

  std::vector<std::string> getHeaderFiles() const override {
    return {};
  }

  ExpressionNullMode getNullMode() const override {
    return ExpressionNullMode::Custom;
  }

  bool isNull() const {
    return value_ == std::nullopt;
  }

  void propagateNullability() override {
    if (isNull()) {
      setMaybeNull(true);
    } else {
      setMaybeNull(false);
    }
  }

  void validate() const override {
    validateTyped();
  }

 private:
  /// Child expression
  const std::optional<T> value_;
}; // namespace codegen

using Int8Literal = ConstantExpression<int8_t>;
using Int16Literal = ConstantExpression<int16_t>;
using Int32Literal = ConstantExpression<int32_t>;
using Int64Literal = ConstantExpression<int64_t>;
using Int32Literal = ConstantExpression<int32_t>;
using Int32Literal = ConstantExpression<int32_t>;

using FloatLiteral = ConstantExpression<float>;
using DoubleLiteral = ConstantExpression<double>;

using StringLiteral = ConstantExpression<std::string>;

template <>
std::vector<std::string> ConstantExpression<std::string>::getHeaderFiles()
    const;

template <>
CodeSnippet ConstantExpression<std::string>::generateCode(
    CodegenCtx& exprCodegenCtx,
    const std::string& outputVarName) const;

} // namespace codegen
} // namespace velox
} // namespace facebook
