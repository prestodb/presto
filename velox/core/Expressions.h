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
#include "velox/core/ITypedExpr.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::core {

class InputTypedExpr : public ITypedExpr {
 public:
  explicit InputTypedExpr(TypePtr type)
      : ITypedExpr{ExprKind::kInput, std::move(type)} {}

  bool operator==(const ITypedExpr& other) const final {
    const auto* casted = dynamic_cast<const InputTypedExpr*>(&other);
    return casted != nullptr;
  }

  std::string toString() const override {
    return "ROW";
  }

  size_t localHash() const override {
    static const size_t kBaseHash = std::hash<const char*>()("InputTypedExpr");
    return kBaseHash;
  }

  TypedExprPtr rewriteInputNames(
      const std::unordered_map<std::string, TypedExprPtr>& /*mapping*/)
      const override {
    return std::make_shared<InputTypedExpr>(type());
  }

  void accept(
      const ITypedExprVisitor& visitor,
      ITypedExprVisitorContext& context) const override;

  folly::dynamic serialize() const override;

  static TypedExprPtr create(const folly::dynamic& obj, void* context);
};

class ConstantTypedExpr : public ITypedExpr {
 public:
  // Creates constant expression. For complex types, only
  // Variant::null() value is supported.
  ConstantTypedExpr(TypePtr type, Variant value)
      : ITypedExpr{ExprKind::kConstant, std::move(type)},
        value_{std::move(value)} {}

  // Creates constant expression of scalar or complex type. The value comes from
  // index zero.
  explicit ConstantTypedExpr(const VectorPtr& value)
      : ITypedExpr{ExprKind::kConstant, value->type()},
        valueVector_{
            value->isConstantEncoding()
                ? value
                : BaseVector::wrapInConstant(1, 0, value)} {}

  std::string toString() const override;

  size_t localHash() const override;

  bool hasValueVector() const {
    return valueVector_ != nullptr;
  }

  /// Returns scalar value as Variant if hasValueVector() is false.
  const Variant& value() const {
    return value_;
  }

  /// Return constant value vector if hasValueVector() is true. Returns null
  /// otherwise.
  const VectorPtr& valueVector() const {
    return valueVector_;
  }

  bool isNull() const {
    if (hasValueVector()) {
      return valueVector_->isNullAt(0);
    }
    return value_.isNull();
  }

  VectorPtr toConstantVector(memory::MemoryPool* pool) const {
    if (valueVector_) {
      return valueVector_;
    }
    if (value_.isNull()) {
      return BaseVector::createNullConstant(type(), 1, pool);
    }
    return BaseVector::createConstant(type(), value_, 1, pool);
  }

  const std::vector<TypedExprPtr>& inputs() const {
    static const std::vector<TypedExprPtr> kEmpty{};
    return kEmpty;
  }

  TypedExprPtr rewriteInputNames(
      const std::unordered_map<std::string, TypedExprPtr>& /*mapping*/)
      const override {
    if (hasValueVector()) {
      return std::make_shared<ConstantTypedExpr>(valueVector_);
    } else {
      return std::make_shared<ConstantTypedExpr>(type(), value_);
    }
  }

  void accept(
      const ITypedExprVisitor& visitor,
      ITypedExprVisitorContext& context) const override;

  bool equals(const ITypedExpr& other) const;

  bool operator==(const ITypedExpr& other) const final {
    return this->equals(other);
  }

  bool operator==(const ConstantTypedExpr& other) const {
    return this->equals(other);
  }

  folly::dynamic serialize() const override;

  static TypedExprPtr create(const folly::dynamic& obj, void* context);

 private:
  const Variant value_;
  const VectorPtr valueVector_;
};

using ConstantTypedExprPtr = std::shared_ptr<const ConstantTypedExpr>;

/// Evaluates a scalar function or a special form.
///
/// Supported special forms are: and, or, cast, try_cast, coalesce, if,
/// switch, try. See registerFunctionCallToSpecialForms in
/// expression/RegisterSpecialForm.h for the up-to-date list.
///
/// Regular functions have the following properties: (1) return type is fully
/// defined by function name and input types; (2) during evaluation all
/// function arguments are evaluated first before the function itself is
/// evaluated on the results, a failure to evaluate function argument prevents
/// the function from being evaluated.
///
/// Special forms are different from regular scalar functions as they do not
/// always have the above properties.
///
/// - CAST doesn't have (1): return type is not defined by input type as it is
/// possible to cast VARCHAR to INTEGER, BOOLEAN, and many other types.
/// - Conjuncts AND, OR don't have (2): these have logic to stop evaluating
/// arguments if the outcome is already decided. For example, a > 10 AND b < 3
/// applied to a = 0 and b = 0 is fully decided after evaluating a > 10. The
/// result is FALSE. This is important not only from efficiency standpoint,
/// but semantically as well. Not evaluating unnecessary arguments implicitly
/// suppresses the errors that might have happened if evaluation proceeded.
/// For example, a > 10 AND b / a > 1 would fail if both expressions were
/// evaluated on a = 0.
/// - Coalesce, if, switch also don't have (2): these also have logic to stop
/// evaluating arguments if the outcome is already decided.
/// - TRY doesn't have (2) either: it needs to capture and suppress errors
/// received while evaluating the input.
class CallTypedExpr : public ITypedExpr {
 public:
  /// @param type Return type.
  /// @param inputs List of input expressions. May be empty.
  /// @param name Name of the function or special form.
  CallTypedExpr(
      TypePtr type,
      std::vector<TypedExprPtr> inputs,
      std::string name)
      : ITypedExpr{ExprKind::kCall, std::move(type), std::move(inputs)},
        name_(std::move(name)) {}

  /// @param type Return type.
  /// @param name Name of the function or special form.
  /// @param inputs List of input expressions.
  template <typename... TypedExprs>
  CallTypedExpr(TypePtr type, std::string name, TypedExprs... inputs)
      : CallTypedExpr(
            std::move(type),
            std::vector<TypedExprPtr>{std::forward<TypedExprs>(inputs)...},
            std::move(name)) {}

  virtual const std::string& name() const {
    return name_;
  }

  TypedExprPtr rewriteInputNames(
      const std::unordered_map<std::string, TypedExprPtr>& mapping)
      const override {
    return std::make_shared<CallTypedExpr>(
        type(), rewriteInputsRecursive(mapping), name_);
  }

  std::string toString() const override;

  size_t localHash() const override {
    static const size_t kBaseHash = std::hash<const char*>()("CallTypedExpr");
    return bits::hashMix(kBaseHash, std::hash<std::string>()(name_));
  }

  void accept(
      const ITypedExprVisitor& visitor,
      ITypedExprVisitorContext& context) const override;

  bool operator==(const ITypedExpr& other) const override {
    const auto* casted = dynamic_cast<const CallTypedExpr*>(&other);
    if (!casted) {
      return false;
    }
    return operator==(*casted);
  }

  bool operator==(const CallTypedExpr& other) const {
    if (other.name() != this->name()) {
      return false;
    }
    if (*other.type() != *this->type()) {
      return false;
    }
    return std::equal(
        this->inputs().begin(),
        this->inputs().end(),
        other.inputs().begin(),
        other.inputs().end(),
        [](const auto& p1, const auto& p2) { return *p1 == *p2; });
  }

  folly::dynamic serialize() const override;

  static TypedExprPtr create(const folly::dynamic& obj, void* context);

 private:
  const std::string name_;
};

using CallTypedExprPtr = std::shared_ptr<const CallTypedExpr>;

/// Represents a leaf in an expression tree specifying input column by name.
class FieldAccessTypedExpr : public ITypedExpr {
 public:
  /// Used as a leaf in an expression tree specifying input column by name.
  FieldAccessTypedExpr(TypePtr type, std::string name)
      : ITypedExpr{ExprKind::kFieldAccess, std::move(type)},
        name_(std::move(name)),
        isInputColumn_(true) {}

  /// Used as a dereference expression which selects a subfield in a struct by
  /// name.
  FieldAccessTypedExpr(TypePtr type, TypedExprPtr input, std::string name)
      : ITypedExpr{ExprKind::kFieldAccess, std::move(type), {std::move(input)}},
        name_(std::move(name)),
        isInputColumn_(dynamic_cast<const InputTypedExpr*>(inputs()[0].get())) {
  }

  const std::string& name() const {
    return name_;
  }

  TypedExprPtr rewriteInputNames(
      const std::unordered_map<std::string, TypedExprPtr>& mapping)
      const override;

  std::string toString() const override;

  size_t localHash() const override {
    static const size_t kBaseHash =
        std::hash<const char*>()("FieldAccessTypedExpr");
    return bits::hashMix(kBaseHash, std::hash<std::string>()(name_));
  }

  void accept(
      const ITypedExprVisitor& visitor,
      ITypedExprVisitorContext& context) const override;

  bool operator==(const ITypedExpr& other) const final {
    const auto* casted = dynamic_cast<const FieldAccessTypedExpr*>(&other);
    if (!casted) {
      return false;
    }
    return operator==(*casted);
  }

  bool operator==(const FieldAccessTypedExpr& other) const {
    if (other.name_ != this->name_) {
      return false;
    }
    if (*other.type() != *this->type()) {
      return false;
    }
    return std::equal(
        this->inputs().begin(),
        this->inputs().end(),
        other.inputs().begin(),
        other.inputs().end(),
        [](const auto& p1, const auto& p2) { return *p1 == *p2; });
  }

  /// Is this FieldAccess accessing an input column or a field in a struct.
  bool isInputColumn() const {
    return isInputColumn_;
  }

  folly::dynamic serialize() const override;

  static TypedExprPtr create(const folly::dynamic& obj, void* context);

 private:
  const std::string name_;
  const bool isInputColumn_;
};

using FieldAccessTypedExprPtr = std::shared_ptr<const FieldAccessTypedExpr>;

/// Represents a dereference expression which selects a subfield in a struct
/// by name.
class DereferenceTypedExpr : public ITypedExpr {
 public:
  DereferenceTypedExpr(TypePtr type, TypedExprPtr input, uint32_t index)
      : ITypedExpr{ExprKind::kDereference, std::move(type), {std::move(input)}},
        index_(index) {
    // Make sure this isn't being used to access a top level column.
    VELOX_USER_CHECK_NULL(
        std::dynamic_pointer_cast<const InputTypedExpr>(inputs()[0]));
  }

  uint32_t index() const {
    return index_;
  }

  const std::string& name() const {
    return inputs()[0]->type()->asRow().nameOf(index_);
  }

  TypedExprPtr rewriteInputNames(
      const std::unordered_map<std::string, TypedExprPtr>& mapping)
      const override {
    auto newInputs = rewriteInputsRecursive(mapping);
    VELOX_CHECK_EQ(1, newInputs.size());

    return std::make_shared<DereferenceTypedExpr>(type(), newInputs[0], index_);
  }

  std::string toString() const override {
    return fmt::format("{}[{}]", inputs()[0]->toString(), name());
  }

  size_t localHash() const override {
    static const size_t kBaseHash =
        std::hash<const char*>()("DereferenceTypedExpr");
    return bits::hashMix(kBaseHash, index_);
  }

  void accept(
      const ITypedExprVisitor& visitor,
      ITypedExprVisitorContext& context) const override;

  bool operator==(const ITypedExpr& other) const final {
    const auto* casted = dynamic_cast<const DereferenceTypedExpr*>(&other);
    if (!casted) {
      return false;
    }
    return operator==(*casted);
  }

  bool operator==(const DereferenceTypedExpr& other) const {
    if (other.index_ != this->index_) {
      return false;
    }
    return std::equal(
        this->inputs().begin(),
        this->inputs().end(),
        other.inputs().begin(),
        other.inputs().end(),
        [](const auto& p1, const auto& p2) { return *p1 == *p2; });
  }

  folly::dynamic serialize() const override;

  static TypedExprPtr create(const folly::dynamic& obj, void* context);

 private:
  const uint32_t index_;
};

using DereferenceTypedExprPtr = std::shared_ptr<const DereferenceTypedExpr>;

/// Evaluates a list of expressions to produce a row.
class ConcatTypedExpr : public ITypedExpr {
 public:
  ConcatTypedExpr(
      const std::vector<std::string>& names,
      const std::vector<TypedExprPtr>& inputs);

  TypedExprPtr rewriteInputNames(
      const std::unordered_map<std::string, TypedExprPtr>& mapping)
      const override {
    return std::make_shared<ConcatTypedExpr>(
        type()->asRow().names(), rewriteInputsRecursive(mapping));
  }

  std::string toString() const override;

  size_t localHash() const override {
    static const size_t kBaseHash = std::hash<const char*>()("ConcatTypedExpr");
    return kBaseHash;
  }

  void accept(
      const ITypedExprVisitor& visitor,
      ITypedExprVisitorContext& context) const override;

  bool operator==(const ITypedExpr& other) const override {
    const auto* casted = dynamic_cast<const ConcatTypedExpr*>(&other);
    if (!casted) {
      return false;
    }
    return operator==(*casted);
  }

  bool operator==(const ConcatTypedExpr& other) const {
    if (*other.type() != *this->type()) {
      return false;
    }
    return std::equal(
        this->inputs().begin(),
        this->inputs().end(),
        other.inputs().begin(),
        other.inputs().end(),
        [](const auto& p1, const auto& p2) { return *p1 == *p2; });
  }

  folly::dynamic serialize() const override;

  static TypedExprPtr create(const folly::dynamic& obj, void* context);
};

using ConcatTypedExprPtr = std::shared_ptr<const ConcatTypedExpr>;

class LambdaTypedExpr : public ITypedExpr {
 public:
  LambdaTypedExpr(RowTypePtr signature, TypedExprPtr body)
      : ITypedExpr(
            ExprKind::kLambda,
            std::make_shared<FunctionType>(
                std::vector<TypePtr>(signature->children()),
                body->type())),
        signature_(std::move(signature)),
        body_(std::move(body)) {}

  const RowTypePtr& signature() const {
    return signature_;
  }

  const TypedExprPtr& body() const {
    return body_;
  }

  TypedExprPtr rewriteInputNames(
      const std::unordered_map<std::string, TypedExprPtr>& mapping)
      const override;

  std::string toString() const override {
    return fmt::format(
        "lambda {} -> {}", signature_->toString(), body_->toString());
  }

  size_t localHash() const override {
    static const size_t kBaseHash = std::hash<const char*>()("LambdaTypedExpr");
    return bits::hashMix(kBaseHash, body_->hash());
  }

  void accept(
      const ITypedExprVisitor& visitor,
      ITypedExprVisitorContext& context) const override;

  bool operator==(const ITypedExpr& other) const override {
    const auto* casted = dynamic_cast<const LambdaTypedExpr*>(&other);
    if (!casted) {
      return false;
    }
    return operator==(*casted);
  }

  bool operator==(const LambdaTypedExpr& other) const {
    if (*type() != *other.type()) {
      return false;
    }
    return *signature_ == *other.signature_ && *body_ == *other.body_;
  }

  folly::dynamic serialize() const override;

  static TypedExprPtr create(const folly::dynamic& obj, void* context);

 private:
  const RowTypePtr signature_;
  const TypedExprPtr body_;
};

using LambdaTypedExprPtr = std::shared_ptr<const LambdaTypedExpr>;

/// Converts input values to specified type.
class CastTypedExpr : public ITypedExpr {
 public:
  /// @param type Type to convert to. This is the return type of the CAST
  /// expresion.
  /// @param input Single input. The type of input is referred to as from-type
  /// and expected to be different from to-type.
  /// @param isTryCast Whether this expression is used for `try_cast`.
  CastTypedExpr(const TypePtr& type, const TypedExprPtr& input, bool isTryCast)
      : ITypedExpr{ExprKind::kCast, type, {input}}, isTryCast_(isTryCast) {}

  CastTypedExpr(
      const TypePtr& type,
      const std::vector<TypedExprPtr>& inputs,
      bool isTryCast)
      : ITypedExpr{ExprKind::kCast, type, inputs}, isTryCast_(isTryCast) {
    VELOX_USER_CHECK_EQ(
        1, inputs.size(), "Cast expression requires exactly one input");
  }

  TypedExprPtr rewriteInputNames(
      const std::unordered_map<std::string, TypedExprPtr>& mapping)
      const override {
    return std::make_shared<CastTypedExpr>(
        type(), rewriteInputsRecursive(mapping), isTryCast_);
  }

  std::string toString() const override;

  size_t localHash() const override {
    static const size_t kBaseHash = std::hash<const char*>()("CastTypedExpr");
    return bits::hashMix(kBaseHash, std::hash<bool>()(isTryCast_));
  }

  void accept(
      const ITypedExprVisitor& visitor,
      ITypedExprVisitorContext& context) const override;

  bool operator==(const ITypedExpr& other) const override {
    const auto* otherCast = dynamic_cast<const CastTypedExpr*>(&other);
    if (!otherCast) {
      return false;
    }
    if (inputs().empty()) {
      return type() == otherCast->type() && otherCast->inputs().empty() &&
          isTryCast_ == otherCast->isTryCast();
    }
    return *type() == *otherCast->type() &&
        *inputs()[0] == *otherCast->inputs()[0] &&
        isTryCast_ == otherCast->isTryCast();
  }

  bool isTryCast() const {
    return isTryCast_;
  }

  folly::dynamic serialize() const override;

  static TypedExprPtr create(const folly::dynamic& obj, void* context);

 private:
  // Whether this expression is used for `try_cast`. When true, Presto cast
  // suppresses exception and return null on failure to case.
  const bool isTryCast_;
};

using CastTypedExprPtr = std::shared_ptr<const CastTypedExpr>;

/// A collection of convenience methods for working with expressions.
class TypedExprs {
 public:
  /// Returns true if 'expr' is a field access expression.
  static bool isFieldAccess(const TypedExprPtr& expr) {
    return dynamic_cast<const FieldAccessTypedExpr*>(expr.get()) != nullptr;
  }

  /// Returns 'expr' as FieldAccessTypedExprPtr or null if not field access
  /// expression.
  static FieldAccessTypedExprPtr asFieldAccess(const TypedExprPtr& expr) {
    return std::dynamic_pointer_cast<const FieldAccessTypedExpr>(expr);
  }

  /// Returns true if 'expr' is a constant expression.
  static bool isConstant(const TypedExprPtr& expr) {
    return dynamic_cast<const ConstantTypedExpr*>(expr.get()) != nullptr;
  }

  /// Returns 'expr' as ConstantTypedExprPtr or null if not a constant
  /// expression.
  static ConstantTypedExprPtr asConstant(const TypedExprPtr& expr) {
    return std::dynamic_pointer_cast<const ConstantTypedExpr>(expr);
  }

  /// Returns true if 'expr' is a lambda expression.
  static bool isLambda(const TypedExprPtr& expr) {
    return dynamic_cast<const LambdaTypedExpr*>(expr.get()) != nullptr;
  }

  /// Returns 'expr' as LambdaTypedExprPtr or null if not a lambda expression.
  static LambdaTypedExprPtr asLambda(const TypedExprPtr& expr) {
    return std::dynamic_pointer_cast<const LambdaTypedExpr>(expr);
  }
};

class ITypedExprVisitorContext {
 public:
  virtual ~ITypedExprVisitorContext() = default;
};

class ITypedExprVisitor {
 public:
  virtual ~ITypedExprVisitor() = default;

  virtual void visit(const CallTypedExpr& expr, ITypedExprVisitorContext& ctx)
      const = 0;

  virtual void visit(const CastTypedExpr& expr, ITypedExprVisitorContext& ctx)
      const = 0;

  virtual void visit(const ConcatTypedExpr& expr, ITypedExprVisitorContext& ctx)
      const = 0;

  virtual void visit(
      const ConstantTypedExpr& expr,
      ITypedExprVisitorContext& ctx) const = 0;

  virtual void visit(
      const DereferenceTypedExpr& expr,
      ITypedExprVisitorContext& ctx) const = 0;

  virtual void visit(
      const FieldAccessTypedExpr& expr,
      ITypedExprVisitorContext& ctx) const = 0;

  virtual void visit(const InputTypedExpr& expr, ITypedExprVisitorContext& ctx)
      const = 0;

  virtual void visit(const LambdaTypedExpr& expr, ITypedExprVisitorContext& ctx)
      const = 0;

 protected:
  void visitInputs(const ITypedExpr& expr, ITypedExprVisitorContext& ctx)
      const {
    for (auto& child : expr.inputs()) {
      child->accept(*this, ctx);
    }
  }
};

/// An implementation of ITypedExprVisitor that can be used to build a visitor
/// that handles only specific types of expressions.
class DefaultTypedExprVisitor : public ITypedExprVisitor {
 public:
  void visit(const CallTypedExpr& expr, ITypedExprVisitorContext& ctx)
      const override {
    visitInputs(expr, ctx);
  }

  void visit(const CastTypedExpr& expr, ITypedExprVisitorContext& ctx)
      const override {
    visitInputs(expr, ctx);
  }

  void visit(const ConcatTypedExpr& expr, ITypedExprVisitorContext& ctx)
      const override {
    visitInputs(expr, ctx);
  }

  void visit(const ConstantTypedExpr& expr, ITypedExprVisitorContext& ctx)
      const override {
    visitInputs(expr, ctx);
  }

  void visit(const DereferenceTypedExpr& expr, ITypedExprVisitorContext& ctx)
      const override {
    visitInputs(expr, ctx);
  }

  void visit(const FieldAccessTypedExpr& expr, ITypedExprVisitorContext& ctx)
      const override {
    visitInputs(expr, ctx);
  }

  void visit(const InputTypedExpr& expr, ITypedExprVisitorContext& ctx)
      const override {
    visitInputs(expr, ctx);
  }

  void visit(const LambdaTypedExpr& expr, ITypedExprVisitorContext& ctx)
      const override {
    visitInputs(expr, ctx);
  }
};

} // namespace facebook::velox::core
