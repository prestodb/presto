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
#include "velox/parse/IExpr.h"
#include "velox/type/Variant.h"

namespace facebook::velox::core {

class InputExpr : public IExpr {
 public:
  InputExpr() : IExpr({}) {}

  std::string toString() const override {
    return "ROW";
  }

  VELOX_DEFINE_CLASS_NAME(InputExpr)
};

class FieldAccessExpr : public IExpr {
 public:
  FieldAccessExpr(
      const std::string& name,
      std::optional<std::string> alias,
      std::vector<ExprPtr>&& inputs =
          std::vector<ExprPtr>{std::make_shared<const InputExpr>()})
      : IExpr{std::move(inputs), std::move(alias)}, name_{name} {
    VELOX_USER_CHECK_EQ(IExpr::inputs().size(), 1);
  }

  const std::string& name() const {
    return name_;
  }

  bool isRootColumn() const {
    return dynamic_cast<const InputExpr*>(input().get()) != nullptr;
  }

  std::string toString() const override;

  VELOX_DEFINE_CLASS_NAME(FieldAccessExpr)

 private:
  const std::string name_;
};

class CallExpr : public IExpr {
 public:
  CallExpr(
      std::string&& funcName,
      std::vector<ExprPtr>&& inputs,
      std::optional<std::string> alias)
      : IExpr{std::move(inputs), std::move(alias)}, name_{std::move(funcName)} {
    VELOX_USER_CHECK(!name_.empty());
  }

  const std::string& name() const {
    return name_;
  }

  std::string toString() const override;

  VELOX_DEFINE_CLASS_NAME(CallExpr)

 private:
  const std::string name_;
};

class ConstantExpr : public IExpr,
                     public std::enable_shared_from_this<ConstantExpr> {
 public:
  ConstantExpr(TypePtr type, variant value, std::optional<std::string> alias)
      : IExpr{{}, std::move(alias)},
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

  VELOX_DEFINE_CLASS_NAME(ConstantExpr)

 private:
  const TypePtr type_;
  const variant value_;
};

class CastExpr : public IExpr, public std::enable_shared_from_this<CastExpr> {
 public:
  CastExpr(
      const TypePtr& type,
      const ExprPtr& expr,
      bool isTryCast,
      std::optional<std::string> alias)
      : IExpr{{expr}, std::move(alias)}, type_(type), isTryCast_(isTryCast) {}

  std::string toString() const override;

  const TypePtr& type() const {
    return type_;
  }

  bool isTryCast() const {
    return isTryCast_;
  }

  VELOX_DEFINE_CLASS_NAME(CastExpr)

 private:
  const TypePtr type_;
  const bool isTryCast_;
};

/// Represents lambda expression as a list of inputs and the body expression.
/// For example, the expression
///     (k, v) -> k + v
/// is represented using [k, v] as inputNames and k + v as body.
class LambdaExpr : public IExpr,
                   public std::enable_shared_from_this<LambdaExpr> {
 public:
  LambdaExpr(std::vector<std::string> arguments, ExprPtr body)
      : IExpr({}), arguments_{std::move(arguments)}, body_{std::move(body)} {
    VELOX_USER_CHECK(!arguments_.empty());
  }

  const std::vector<std::string>& arguments() const {
    return arguments_;
  }

  const ExprPtr& body() const {
    return body_;
  }

  std::string toString() const override;

 private:
  const std::vector<std::string> arguments_;
  const ExprPtr body_;
};
} // namespace facebook::velox::core
