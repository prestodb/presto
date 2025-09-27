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

#include "velox/type/Type.h"

namespace facebook::velox::core {

enum class ExprKind : int32_t {
  kInput = 0,
  kFieldAccess = 1,
  kDereference = 2,
  kCall = 3,
  kCast = 5,
  kConstant = 6,
  kConcat = 7,
  kLambda = 8,
};

VELOX_DECLARE_ENUM_NAME(ExprKind);

class ITypedExpr;
class ITypedExprVisitor;
class ITypedExprVisitorContext;

using TypedExprPtr = std::shared_ptr<const ITypedExpr>;

/// Strongly-typed expression, e.g. literal, function call, etc.
class ITypedExpr : public ISerializable {
 public:
  ITypedExpr(ExprKind kind, TypePtr type)
      : kind_{kind}, type_{std::move(type)}, inputs_{} {}

  ITypedExpr(ExprKind kind, TypePtr type, std::vector<TypedExprPtr> inputs)
      : kind_{kind}, type_{std::move(type)}, inputs_{std::move(inputs)} {}

  virtual ~ITypedExpr() = default;

  ExprKind kind() const {
    return kind_;
  }
  //
  //  virtual const std::string& name() const {
  //    VELOX_UNSUPPORTED("name() is not supported for this expression");
  //  }

  const TypePtr& type() const {
    return type_;
  }
  //
  //  void setType(const TypePtr& newType) {
  //    type_.reset(newType.get());
  //  }

  const std::vector<TypedExprPtr>& inputs() const {
    return inputs_;
  }

  bool isInputKind() const {
    return kind_ == ExprKind::kInput;
  }

  bool isFieldAccessKind() const {
    return kind_ == ExprKind::kFieldAccess;
  }

  bool isDereferenceKind() const {
    return kind_ == ExprKind::kDereference;
  }

  bool isCallKind() const {
    return kind_ == ExprKind::kCall;
  }

  bool isCastKind() const {
    return kind_ == ExprKind::kCast;
  }

  bool isConstantKind() const {
    return kind_ == ExprKind::kConstant;
  }

  bool isConcatKind() const {
    return kind_ == ExprKind::kConcat;
  }

  bool isLambdaKind() const {
    return kind_ == ExprKind::kLambda;
  }

  template <typename T>
  const T* asUnchecked() const {
    return dynamic_cast<const T*>(this);
  }

  /// Returns a copy of this expression with input fields replaced according
  /// to specified 'mapping'. Fields specified in the 'mapping' are replaced
  /// by the corresponding expression in 'mapping'.
  /// Fields not present in 'mapping' are left unmodified.
  ///
  /// Used to bind inputs to lambda functions.
  virtual TypedExprPtr rewriteInputNames(
      const std::unordered_map<std::string, TypedExprPtr>& mapping) const = 0;

  /// Part of the visitor pattern. Calls visitor.vist(*this, context) with the
  /// "right" type of the first argument.
  virtual void accept(
      const ITypedExprVisitor& visitor,
      ITypedExprVisitorContext& context) const = 0;

  virtual std::string toString() const = 0;

  virtual size_t localHash() const = 0;

  size_t hash() const {
    size_t hash = bits::hashMix(type_->hashKind(), localHash());
    for (size_t i = 0; i < inputs_.size(); ++i) {
      hash = bits::hashMix(hash, inputs_[i]->hash());
    }
    return hash;
  }

  virtual bool operator==(const ITypedExpr& other) const = 0;

  static void registerSerDe();

 protected:
  folly::dynamic serializeBase(std::string_view name) const;

  std::vector<TypedExprPtr> rewriteInputsRecursive(
      const std::unordered_map<std::string, TypedExprPtr>& mapping) const {
    std::vector<TypedExprPtr> newInputs;
    newInputs.reserve(inputs().size());
    for (const auto& input : inputs()) {
      newInputs.emplace_back(input->rewriteInputNames(mapping));
    }
    return newInputs;
  }

 private:
  const ExprKind kind_;
  const TypePtr type_;
  const std::vector<TypedExprPtr> inputs_;
};

} // namespace facebook::velox::core
