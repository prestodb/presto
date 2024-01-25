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

#include "velox/expression/CastHooks.h"
#include "velox/expression/FunctionCallToSpecialForm.h"
#include "velox/expression/SpecialForm.h"

namespace facebook::velox::exec {

constexpr folly::StringPiece kCast = "cast";
constexpr folly::StringPiece kTryCast = "try_cast";

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

  /// Casts an input vector to the custom type. This function should not throw
  /// when processing input rows, but report errors via context.setError().
  /// @param input The flat or constant input vector
  /// @param context The context
  /// @param rows Non-null rows of input
  /// @param resultType The result type.
  /// @param result The result vector of the custom type
  virtual void castTo(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const TypePtr& resultType,
      VectorPtr& result) const = 0;

  /// Casts a vector of the custom type to another type. This function should
  /// not throw when processing input rows, but report errors via
  /// context.setError().
  /// @param input The flat or constant input vector
  /// @param context The context
  /// @param rows Non-null rows of input
  /// @param resultType The result type
  /// @param result The result vector of the destination type
  virtual void castFrom(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const TypePtr& resultType,
      VectorPtr& result) const = 0;
};

class CastExpr : public SpecialForm {
 public:
  /// @param type The target type of the cast expression
  /// @param expr The expression to cast
  /// @param trackCpuUsage Whether to track CPU usage
  CastExpr(
      TypePtr type,
      ExprPtr&& expr,
      bool trackCpuUsage,
      bool nullOnFailure,
      std::shared_ptr<CastHooks> hooks)
      : SpecialForm(
            type,
            std::vector<ExprPtr>({expr}),
            nullOnFailure ? kTryCast.data() : kCast.data(),
            false /* supportsFlatNoNullsFastPath */,
            trackCpuUsage),
        nullOnFailure_(nullOnFailure),
        hooks_(std::move(hooks)) {}

  void evalSpecialForm(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override;

  std::string toString(bool recursive = true) const override;

  std::string toSql(std::vector<VectorPtr>*) const override;

 private:
  /// Apply the cast after generating the input vectors
  /// @param rows The list of rows being processed
  /// @param input The input vector to be casted
  /// @param context The context
  /// @param fromType the input type
  /// @param toType the target type
  /// @param result The result vector
  void apply(
      const SelectivityVector& rows,
      const VectorPtr& input,
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
      const TypePtr& toType);

  /// Apply the cast between decimal vectors.
  /// @param rows Non-null rows of the input vector.
  /// @param input The input decimal vector. It is guaranteed to be flat or
  /// constant.
  template <typename ToDecimalType>
  VectorPtr applyDecimal(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& fromType,
      const TypePtr& toType);

  // Apply the cast to a vector after vector encodings being peeled off. The
  // input vector is guaranteed to be flat or constant.
  void applyPeeled(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& fromType,
      const TypePtr& toType,
      VectorPtr& result);

  template <typename Func>
  void applyToSelectedNoThrowLocal(
      EvalCtx& context,
      const SelectivityVector& rows,
      VectorPtr& result,
      Func&& func);

  /// The per-row level Kernel
  /// @tparam ToKind The cast target type
  /// @tparam FromKind The expression type
  /// @tparam TPolicy The policy used by the cast
  /// @param row The index of the current row
  /// @param input The input vector (of type FromKind)
  /// @param result The output vector (of type ToKind)
  template <TypeKind ToKind, TypeKind FromKind, typename TPolicy>
  void applyCastKernel(
      vector_size_t row,
      EvalCtx& context,
      const SimpleVector<typename TypeTraits<FromKind>::NativeType>* input,
      FlatVector<typename TypeTraits<ToKind>::NativeType>* result);

  VectorPtr castFromDate(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& toType);

  VectorPtr castToDate(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& fromType);

  template <typename TInput, typename TOutput>
  void applyDecimalCastKernel(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& fromType,
      const TypePtr& toType,
      VectorPtr& castResult);

  template <typename TInput, typename TOutput>
  void applyIntToDecimalCastKernel(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& toType,
      VectorPtr& castResult);

  template <typename TOutput>
  void applyDoubleToDecimalCastKernel(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& toType,
      VectorPtr& castResult);

  template <typename T>
  void applyVarcharToDecimalCastKernel(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& toType,
      VectorPtr& castResult);

  template <typename FromNativeType, TypeKind ToKind>
  VectorPtr applyDecimalToFloatCast(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& fromType,
      const TypePtr& toType);

  template <typename FromNativeType, TypeKind ToKind>
  VectorPtr applyDecimalToIntegralCast(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& fromType,
      const TypePtr& toType);

  template <typename FromNativeType>
  VectorPtr applyDecimalToBooleanCast(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context);

  template <typename FromNativeType>
  VectorPtr applyDecimalToPrimitiveCast(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& fromType,
      const TypePtr& toType);

  template <TypeKind ToKind, TypeKind FromKind>
  void applyCastPrimitives(
      const SelectivityVector& rows,
      exec::EvalCtx& context,
      const BaseVector& input,
      VectorPtr& result);

  template <typename FromNativeType>
  VectorPtr applyDecimalToVarcharCast(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& fromType);

  template <TypeKind ToKind>
  void applyCastPrimitivesDispatch(
      const TypePtr& fromType,
      const TypePtr& toType,
      const SelectivityVector& rows,
      exec::EvalCtx& context,
      const BaseVector& input,
      VectorPtr& result);

  template <bool adjustForTimeZone>
  void castTimestampToDate(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      VectorPtr& result,
      const date::time_zone* timeZone = nullptr);

  bool nullOnFailure() const {
    return nullOnFailure_;
  }

  bool setNullInResultAtError() const {
    return nullOnFailure() && inTopLevel;
  }

  CastOperatorPtr getCastOperator(const TypePtr& type);

  // Custom cast operators for to and from top-level as well as nested types.
  folly::F14FastMap<std::string, CastOperatorPtr> castOperators_;

  bool nullOnFailure_;
  std::shared_ptr<CastHooks> hooks_;

  bool inTopLevel = false;
};

class CastCallToSpecialForm : public FunctionCallToSpecialForm {
 public:
  TypePtr resolveType(const std::vector<TypePtr>& argTypes) override;

  ExprPtr constructSpecialForm(
      const TypePtr& type,
      std::vector<ExprPtr>&& compiledChildren,
      bool trackCpuUsage,
      const core::QueryConfig& config) override;
};

class TryCastCallToSpecialForm : public FunctionCallToSpecialForm {
 public:
  TypePtr resolveType(const std::vector<TypePtr>& argTypes) override;

  ExprPtr constructSpecialForm(
      const TypePtr& type,
      std::vector<ExprPtr>&& compiledChildren,
      bool trackCpuUsage,
      const core::QueryConfig& config) override;
};
} // namespace facebook::velox::exec

#include "velox/expression/CastExpr-inl.h"
