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

#include "velox/expression/ControlExpr.h"

namespace facebook::velox::exec {

constexpr folly::StringPiece kCast = "cast";

/// Custom operator for casts from and to custom types.
class CastOperator {
 public:
  virtual ~CastOperator() = default;

  /// Determines whether the cast operator supports casting to the custom type
  /// from the other type.
  virtual bool isSupportedFromType(const TypePtr& other) const = 0;

  /// Determines whether the cast operator supports casting from the custom type
  /// to the other type.
  virtual bool isSupportedToType(const TypePtr& other) const = 0;

  /// Casts an input vector to the custom type.
  /// @param input The flat or constant input vector
  /// @param context The context
  /// @param rows Non-null rows of input
  /// @param nullOnFailure Whether this is a cast or try_cast operation
  /// @param result The writable output vector of the custom type
  virtual void castTo(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      bool nullOnFailure,
      BaseVector& result) const = 0;

  /// Casts a vector of the custom type to another type.
  /// @param input The flat or constant input vector
  /// @param context The context
  /// @param rows Non-null rows of input
  /// @param nullOnFailure Whether this is a cast or try_cast operation
  /// @param result The writable output vector of the destination type
  virtual void castFrom(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      bool nullOnFailure,
      BaseVector& result) const = 0;
};

class CastExpr : public SpecialForm {
 public:
  /// @param type The target type of the cast expression
  /// @param expr The expression to cast
  /// @param trackCpuUsage Whether to track CPU usage
  /// @param nullOnFailure Whether to throw or return null if cast is not
  /// possible
  CastExpr(TypePtr type, ExprPtr&& expr, bool trackCpuUsage, bool nullOnFailure)
      : SpecialForm(
            type,
            std::vector<ExprPtr>({expr}),
            kCast.data(),
            trackCpuUsage),
        nullOnFailure_(nullOnFailure) {
    auto fromType = inputs_[0]->type();
    castFromOperator_ = getCastOperator(fromType->toString());
    if (castFromOperator_ && !castFromOperator_->isSupportedToType(type)) {
      VELOX_FAIL(
          "Cannot cast {} to {}.", fromType->toString(), type->toString());
    }

    castToOperator_ = getCastOperator(type->toString());
    if (castToOperator_ && !castToOperator_->isSupportedFromType(fromType)) {
      VELOX_FAIL(
          "Cannot cast {} to {}.", fromType->toString(), type->toString());
    }
  }

  void evalSpecialForm(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override;

  std::string toString(bool recursive = true) const override;

 private:
  /// @tparam To The cast target type
  /// @tparam From The expression type
  /// @param rows The list of rows being processed
  /// @param context The context
  /// @param input The input vector (of type From)
  /// @param resultFlatVector The output vector (of type To)
  template <typename To, typename From>
  void applyCastWithTry(
      const SelectivityVector& rows,
      exec::EvalCtx& context,
      const DecodedVector& input,
      FlatVector<To>* resultFlatVector);

  /// @tparam To The target template
  /// @param fromType The source type pointer
  /// @param rows The list of rows
  /// @param context The context
  /// @param input The input vector (of type From)
  /// @param result The output vector (of type To)
  template <TypeKind To>
  void applyCast(
      const TypeKind fromType,
      const SelectivityVector& rows,
      exec::EvalCtx& context,
      const DecodedVector& input,
      VectorPtr& result);

  /// Apply the cast after generating the input vectors
  /// @param rows The list of rows being processed
  /// @param input The input vector to be casted
  /// @param context The context
  /// @param fromType the input type
  /// @param toType the target type
  /// @param result The result vector
  void apply(
      const SelectivityVector& rows,
      VectorPtr& input,
      exec::EvalCtx& context,
      const TypePtr& fromType,
      const TypePtr& toType,
      VectorPtr& result);

  VectorPtr applyMap(
      const SelectivityVector& rows,
      const MapVector* input,
      exec::EvalCtx& context,
      const MapType& fromType,
      const MapType& toType);

  VectorPtr applyArray(
      const SelectivityVector& rows,
      const ArrayVector* input,
      exec::EvalCtx& context,
      const ArrayType& fromType,
      const ArrayType& toType);

  VectorPtr applyRow(
      const SelectivityVector& rows,
      const RowVector* input,
      exec::EvalCtx& context,
      const RowType& fromType,
      const RowType& toType);

  // When enabled the error in casting leads to null being returned.
  const bool nullOnFailure_;

  // Custom cast operator for the from-type. Nullptr if the type is native or
  // doesn't support cast-from.
  CastOperatorPtr castFromOperator_;

  // Custom cast operator for the to-type. Nullptr if the type is native or
  // doesn't support cast-to.
  CastOperatorPtr castToOperator_;
};

} // namespace facebook::velox::exec
