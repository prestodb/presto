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

#include "velox/common/base/Exceptions.h"
#include "velox/core/Expressions.h"
#include "velox/parse/IExpr.h"
#include "velox/type/Variant.h"

namespace facebook::velox::core {

class CallExpr;
class LambdaExpr;

class Expressions {
 public:
  using TypeResolverHook = std::function<TypePtr(
      const std::vector<core::TypedExprPtr>& inputs,
      const std::shared_ptr<const core::CallExpr>& expr,
      bool nullOnFailure)>;

  static TypedExprPtr inferTypes(
      const std::shared_ptr<const IExpr>& expr,
      const TypePtr& input,
      memory::MemoryPool* pool,
      const VectorPtr& complexConstants = nullptr);

  static TypePtr getInputRowType(const TypedExprPtr& expr);

  static void setTypeResolverHook(TypeResolverHook hook) {
    resolverHook_ = hook;
  }

  static TypeResolverHook getResolverHook() {
    return resolverHook_;
  }

  static TypedExprPtr inferTypes(
      const std::shared_ptr<const IExpr>& expr,
      const TypePtr& input,
      const std::vector<TypePtr>& lambdaInputTypes,
      memory::MemoryPool* pool,
      const VectorPtr& complexConstants = nullptr);

 private:
  static TypedExprPtr resolveLambdaExpr(
      const std::shared_ptr<const core::LambdaExpr>& lambdaExpr,
      const TypePtr& inputRow,
      const std::vector<TypePtr>& lambdaInputTypes,
      memory::MemoryPool* pool,
      const VectorPtr& complexConstants = nullptr);

  static TypedExprPtr tryResolveCallWithLambdas(
      const std::shared_ptr<const CallExpr>& expr,
      const TypePtr& input,
      memory::MemoryPool* pool,
      const VectorPtr& complexConstants = nullptr);

  static TypeResolverHook resolverHook_;
};

class InputExpr : public core::IExpr {
 public:
  InputExpr() {}

  std::string toString() const override {
    return "ROW";
  }

  const std::vector<std::shared_ptr<const IExpr>>& getInputs() const override {
    return EMPTY();
  }

  VELOX_DEFINE_CLASS_NAME(InputExpr)
};

class FieldAccessExpr : public core::IExpr {
 public:
  explicit FieldAccessExpr(
      const std::string& name,
      std::optional<std::string> alias,
      std::vector<std::shared_ptr<const IExpr>>&& inputs =
          std::vector<std::shared_ptr<const IExpr>>{
              std::make_shared<const InputExpr>()})
      : IExpr{std::move(alias)}, name_{name}, inputs_{std::move(inputs)} {
    CHECK_EQ(inputs_.size(), 1);
  }

  const std::string& getFieldName() const {
    return name_;
  }

  bool isRootColumn() const {
    if (UNLIKELY(inputs_.empty())) {
      return false;
    }
    return dynamic_cast<const core::InputExpr*>(inputs_.front().get()) !=
        nullptr;
  }

  std::string toString() const override {
    if (UNLIKELY(inputs_.empty())) {
      return appendAliasIfExists(toStringForRootColumn());
    }
    return appendAliasIfExists(
        isRootColumn() ? toStringForRootColumn() : toStringForMemberAccess());
  }

  const std::vector<std::shared_ptr<const IExpr>>& getInputs() const override {
    return inputs_;
  }

 private:
  std::string toStringForRootColumn() const {
    return "\"" + getEscapedName() + "\"";
  }

  std::string toStringForMemberAccess() const {
    return "dot(" + inputs_.front()->toString() + ",\"" + getEscapedName() +
        "\")";
  }

  std::string getEscapedName() const {
    return folly::cEscape<std::string>(name_);
  }

  const std::string name_;
  const std::vector<std::shared_ptr<const IExpr>> inputs_;

  VELOX_DEFINE_CLASS_NAME(FieldAccessExpr)
};

class CallExpr : public core::IExpr {
 public:
  CallExpr(
      std::string&& funcName,
      std::vector<std::shared_ptr<const IExpr>>&& inputs,
      std::optional<std::string> alias)
      : core::IExpr{std::move(alias)},
        name_{std::move(funcName)},
        inputs_{std::move(inputs)} {
    VELOX_CHECK(!name_.empty());
  }

  const std::string& getFunctionName() const {
    return name_;
  }

  std::string toString() const override {
    std::string buf{name_ + "("};
    bool first = true;
    for (auto& f : getInputs()) {
      if (!first) {
        buf += ",";
      }
      buf += f->toString();
      first = false;
    }
    buf += ")";
    return appendAliasIfExists(buf);
  }

  const std::vector<std::shared_ptr<const IExpr>>& getInputs() const override {
    return inputs_;
  }

 private:
  const std::string name_;
  const std::vector<std::shared_ptr<const IExpr>> inputs_;

  VELOX_DEFINE_CLASS_NAME(CallExpr)
};

class ConstantExpr : public IExpr,
                     public std::enable_shared_from_this<ConstantExpr> {
 public:
  explicit ConstantExpr(
      TypePtr type,
      variant value,
      std::optional<std::string> alias)
      : IExpr{std::move(alias)},
        type_{std::move(type)},
        value_{std::move(value)} {}

  std::string toString() const override {
    return appendAliasIfExists(variant{value_}.toJson(type_));
  }

  const variant& value() const {
    return value_;
  }

  const TypePtr& type() const {
    return type_;
  }

  const std::vector<std::shared_ptr<const IExpr>>& getInputs() const override {
    return EMPTY();
  }

  VELOX_DEFINE_CLASS_NAME(ConstantExpr)

 private:
  const TypePtr type_;
  const variant value_;
};

class CastExpr : public IExpr, public std::enable_shared_from_this<CastExpr> {
 private:
  const TypePtr type_;
  const std::shared_ptr<const IExpr> expr_;
  const std::vector<std::shared_ptr<const IExpr>> inputs_;
  bool nullOnFailure_;

 public:
  explicit CastExpr(
      const TypePtr& type,
      const std::shared_ptr<const IExpr>& expr,
      bool nullOnFailure,
      std::optional<std::string> alias)
      : IExpr{std::move(alias)},
        type_(type),
        expr_(expr),
        inputs_({expr}),
        nullOnFailure_(nullOnFailure) {}

  std::string toString() const override {
    return appendAliasIfExists(
        "cast(" + expr_->toString() + ", " + type_->toString() + ")");
  }

  const std::vector<std::shared_ptr<const IExpr>>& getInputs() const override {
    return inputs_;
  }

  const TypePtr& type() const {
    return type_;
  }

  const std::shared_ptr<const IExpr>& expr() const {
    return expr_;
  }

  bool nullOnFailure() const {
    return nullOnFailure_;
  }

  VELOX_DEFINE_CLASS_NAME(CastExpr)
};

/// Represents lambda expression as a list of inputs and the body expression.
/// For example, the expression
///     (k, v) -> k + v
/// is represented using [k, v] as inputNames and k + v as body.
class LambdaExpr : public IExpr,
                   public std::enable_shared_from_this<LambdaExpr> {
 public:
  LambdaExpr(
      std::vector<std::string> inputNames,
      std::shared_ptr<const IExpr> body)
      : inputNames_{std::move(inputNames)}, body_{{std::move(body)}} {
    VELOX_CHECK(!inputNames_.empty());
  }

  const std::vector<std::string>& inputNames() const {
    return inputNames_;
  }

  const std::shared_ptr<const IExpr>& body() const {
    return body_[0];
  }

  std::string toString() const override {
    std::ostringstream out;
    if (inputNames_.size() > 1) {
      out << "(";
      for (auto i = 0; i < inputNames_.size(); ++i) {
        if (i > 0) {
          out << ", ";
        }
        out << inputNames_[i];
      }
      out << ")";
    } else {
      out << inputNames_[0];
    }
    out << " -> " << body_[0]->toString();
    return out.str();
  }

  const std::vector<std::shared_ptr<const IExpr>>& getInputs() const override {
    return body_;
  }

 private:
  std::vector<std::string> inputNames_;
  std::vector<std::shared_ptr<const IExpr>> body_;
};
} // namespace facebook::velox::core
