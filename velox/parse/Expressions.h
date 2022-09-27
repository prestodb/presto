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

#include <functional>
#include <utility>

#include <folly/Singleton.h>

#include "IExpr.h"
#include "velox/common/base/Exceptions.h"
#include "velox/core/Expressions.h"
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
      memory::MemoryPool* pool);

  static TypePtr getInputRowType(const TypedExprPtr& expr);

  static void setTypeResolverHook(TypeResolverHook hook) {
    resolverHook_ = hook;
  }

  static TypeResolverHook getResolverHook() {
    return resolverHook_;
  }

 private:
  static TypedExprPtr inferTypes(
      const std::shared_ptr<const IExpr>& expr,
      const TypePtr& input,
      const std::vector<TypePtr>& lambdaInputTypes,
      memory::MemoryPool* pool);

  static TypedExprPtr resolveLambdaExpr(
      const std::shared_ptr<const core::LambdaExpr>& lambdaExpr,
      const TypePtr& inputRow,
      const std::vector<TypePtr>& lambdaInputTypes,
      memory::MemoryPool* pool);

  static TypedExprPtr tryResolveCallWithLambdas(
      const std::shared_ptr<const CallExpr>& expr,
      const TypePtr& input,
      memory::MemoryPool* pool);

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

  std::shared_ptr<const IExpr> withInputs(
      std::vector<std::shared_ptr<const IExpr>> /* unused */) const override {
    return INSTANCE();
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = InputExpr::getClassName();
    return obj;
  }

  static std::shared_ptr<const IExpr> create(const folly::dynamic& obj) {
    VELOX_USER_CHECK_EQ(obj["name"], InputExpr::getClassName());

    return std::make_shared<const InputExpr>();
  }

  bool equalsNonRecursive(const IExpr& other) const override {
    return dynamic_cast<const InputExpr*>(&other) != nullptr;
  }

  VELOX_DEFINE_CLASS_NAME(InputExpr)
 private:
  static const std::shared_ptr<const IExpr> INSTANCE() {
    static const folly::Singleton<InputExpr> instance_;
    return instance_.try_get();
  }
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

  std::shared_ptr<const IExpr> withInputs(
      std::vector<std::shared_ptr<const IExpr>> inputs) const override {
    return std::make_shared<FieldAccessExpr>(
        std::string{name_}, alias_, std::move(inputs));
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

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = FieldAccessExpr::getClassName();
    obj["inputs"] = ISerializable::serialize(inputs_);
    obj["fieldName"] = name_;
    return obj;
  }

  const std::vector<std::shared_ptr<const IExpr>>& getInputs() const override {
    return inputs_;
  }

  static std::shared_ptr<const IExpr> create(const folly::dynamic& obj) {
    VELOX_USER_CHECK_EQ(obj["name"], FieldAccessExpr::getClassName());
    auto fieldName = ISerializable::deserialize<std::string>(obj["fieldName"]);
    auto inputs = ISerializable::deserialize<std::vector<IExpr>>(obj["inputs"]);

    return std::make_shared<const FieldAccessExpr>(
        std::move(fieldName), std::nullopt, std::move(inputs));
  }

  static std::shared_ptr<const FieldAccessExpr> column(std::string columnName) {
    return std::make_shared<const FieldAccessExpr>(
        std::move(columnName), std::nullopt);
  }

  bool equalsNonRecursive(const IExpr& other) const override {
    auto o = dynamic_cast<const FieldAccessExpr*>(&other);
    return o && name_ == o->name_;
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

class SortExpr : public core::IExpr {
 public:
  SortExpr(
      std::vector<bool>&& orders,
      std::vector<std::shared_ptr<const IExpr>>&& inputs)
      : orders_(std::move(orders)), inputs_{std::move(inputs)} {
    CHECK_EQ(inputs_.size(), orders_.size());
  }

  std::shared_ptr<const IExpr> withInputs(
      std::vector<std::shared_ptr<const IExpr>> inputs) const override {
    return std::make_shared<SortExpr>(
        std::vector<bool>(this->orders_), std::move(inputs));
  }

  const std::vector<bool>& getOrders() const {
    return orders_;
  }

  std::string toString() const override {
    std::string buf{"sort("};
    for (size_t i = 0; i < inputs_.size(); ++i) {
      if (i != 0) {
        buf += ",";
      }
      buf += inputs_[i]->toString() + " " + (orders_[i] ? "ASC" : "DESC");
    }
    buf += ")";
    return buf;
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = SortExpr::getClassName();
    obj["orders"] = ISerializable::serialize(orders_);
    obj["inputs"] = ISerializable::serialize(inputs_);
    return obj;
  }

  const std::vector<std::shared_ptr<const IExpr>>& getInputs() const override {
    return inputs_;
  }

  static std::shared_ptr<const IExpr> create(const folly::dynamic& obj) {
    VELOX_USER_CHECK_EQ(obj["name"], SortExpr::getClassName());
    return std::make_shared<const SortExpr>(
        ISerializable::deserialize<std::vector<bool>>(obj["orders"]),
        ISerializable::deserialize<std::vector<IExpr>>(obj["inputs"]));
  }

 private:
  const std::vector<bool> orders_;
  const std::vector<std::shared_ptr<const IExpr>> inputs_;

  VELOX_DEFINE_CLASS_NAME(SortExpr)
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

  std::shared_ptr<const IExpr> withInputs(
      std::vector<std::shared_ptr<const IExpr>> inputs) const override {
    return std::make_shared<CallExpr>(
        std::string{name_}, std::move(inputs), alias_);
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

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = CallExpr::getClassName();
    obj["inputs"] = ISerializable::serialize(inputs_);
    obj["functionName"] = name_;
    return obj;
  }

  static std::shared_ptr<const IExpr> create(const folly::dynamic& obj) {
    VELOX_USER_CHECK_EQ(obj["name"], CallExpr::getClassName());
    auto functionName =
        ISerializable::deserialize<std::string>(obj["functionName"]);
    auto inputs = ISerializable::deserialize<std::vector<IExpr>>(obj["inputs"]);

    return std::make_shared<const CallExpr>(
        std::move(functionName), std::move(inputs), std::nullopt);
  }

  template <typename... T>
  static auto createCall(std::string&& funcName, T... args) {
    std::vector<std::shared_ptr<const IExpr>> argsv{std::move(args)...};
    return std::make_shared<const CallExpr>(
        std::move(funcName), std::move(argsv));
  }

  bool equalsNonRecursive(const IExpr& other) const override {
    auto o = dynamic_cast<const CallExpr*>(&other);
    return o && name_ == o->name_;
  }

 private:
  const std::string name_;
  const std::vector<std::shared_ptr<const IExpr>> inputs_;

  VELOX_DEFINE_CLASS_NAME(CallExpr)
};

class ConstantExpr : public IExpr,
                     public std::enable_shared_from_this<ConstantExpr> {
 private:
  TypePtr type_;
  const variant value_;

 public:
  explicit ConstantExpr(variant value, std::optional<std::string> alias)
      : IExpr{std::move(alias)},
        type_{value.inferType()},
        value_{std::move(value)} {}

  std::string toString() const override {
    return appendAliasIfExists(variant{value_}.toJson());
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

  std::shared_ptr<const IExpr> withInputs(
      std::vector<std::shared_ptr<const IExpr>> /* unused */) const override {
    return shared_from_this();
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = ConstantExpr::getClassName();
    obj["value"] = ISerializable::serialize(value_);

    return obj;
  }

  static std::shared_ptr<const IExpr> create(const folly::dynamic& obj) {
    VELOX_USER_CHECK_EQ(obj["name"], ConstantExpr::getClassName());
    auto v = ISerializable::deserialize<variant>(obj["value"]);

    return std::make_shared<const ConstantExpr>(v, std::nullopt);
  }

  VELOX_DEFINE_CLASS_NAME(ConstantExpr)
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

  std::shared_ptr<const IExpr> withInputs(
      std::vector<std::shared_ptr<const IExpr>> /* unused */) const override {
    return std::make_shared<CastExpr>(type_, expr_, nullOnFailure_, alias_);
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["type"] = type_->toString();
    obj["expr"] = ISerializable::serialize(expr_);
    obj["nullOnFailure"] = nullOnFailure_;

    return obj;
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

  std::shared_ptr<const IExpr> withInputs(
      std::vector<std::shared_ptr<const IExpr>> /* unused */) const override {
    VELOX_NYI();
  }

  folly::dynamic serialize() const override {
    VELOX_NYI();
  }

 private:
  std::vector<std::string> inputNames_;
  std::vector<std::shared_ptr<const IExpr>> body_;
};
} // namespace facebook::velox::core
