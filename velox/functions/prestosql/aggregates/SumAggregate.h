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

#include "velox/expression/FunctionSignature.h"
#include "velox/functions/prestosql/CheckedArithmeticImpl.h"
#include "velox/functions/prestosql/aggregates/SimpleNumericAggregate.h"

namespace facebook::velox::aggregate {

template <typename TInput, typename TAccumulator, typename ResultType>
class SumAggregate
    : public SimpleNumericAggregate<TInput, TAccumulator, ResultType> {
  using BaseAggregate =
      SimpleNumericAggregate<TInput, TAccumulator, ResultType>;

 public:
  explicit SumAggregate(TypePtr resultType) : BaseAggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(TAccumulator);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      *exec::Aggregate::value<ResultType>(groups[i]) = 0;
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    BaseAggregate::template doExtractValues<ResultType>(
        groups, numGroups, result, [&](char* group) {
          return *BaseAggregate::Aggregate::template value<ResultType>(group);
        });
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    BaseAggregate::template doExtractValues<TAccumulator>(
        groups, numGroups, result, [&](char* group) {
          return *BaseAggregate::Aggregate::template value<TAccumulator>(group);
        });
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    updateInternal<TAccumulator>(groups, rows, args, mayPushdown);
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    updateInternal<ResultType>(groups, rows, args, mayPushdown);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    BaseAggregate::template updateOneGroup<TAccumulator>(
        group,
        rows,
        args[0],
        &updateSingleValue<TAccumulator>,
        &updateDuplicateValues<TAccumulator>,
        mayPushdown,
        0);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    BaseAggregate::template updateOneGroup<ResultType>(
        group,
        rows,
        args[0],
        &updateSingleValue<ResultType>,
        &updateDuplicateValues<ResultType>,
        mayPushdown,
        0);
  }

 protected:
  // TData is either TAccumulator or TResult, which in most cases are the same,
  // but for sum(real) can differ.
  template <typename TData>
  void updateInternal(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) {
    const auto& arg = args[0];

    if (mayPushdown && arg->isLazy()) {
      BaseAggregate::template pushdown<SumHook<TInput, TData>>(
          groups, rows, arg);
      return;
    }

    if (exec::Aggregate::numNulls_) {
      BaseAggregate::template updateGroups<true, TData>(
          groups, rows, arg, &updateSingleValue<TData>, false);
    } else {
      BaseAggregate::template updateGroups<false, TData>(
          groups, rows, arg, &updateSingleValue<TData>, false);
    }
  }

 private:
  /// Update functions that check for overflows for integer types.
  /// For floating points, an overflow results in +/- infinity which is a
  /// valid output.
  template <typename TOutput>
  static void updateSingleValue(TOutput& result, TInput value) {
    if constexpr (
        std::is_same<TOutput, double>::value ||
        std::is_same<TOutput, float>::value) {
      result += value;
    } else {
      result = functions::checkedPlus<TOutput>(result, value);
    }
  }

  template <typename TOutput>
  static void updateDuplicateValues(TOutput& result, TInput value, int n) {
    if constexpr (
        std::is_same<TOutput, double>::value ||
        std::is_same<TOutput, float>::value) {
      result += n * value;
    } else {
      result = functions::checkedPlus<TOutput>(
          result, functions::checkedMultiply<TOutput>(n, value));
    }
  }
};

/// Override 'initializeNewGroups' for float values. Make sure to use 'double'
/// to store the intermediate results in accumulator.
template <>
inline void SumAggregate<float, double, float>::initializeNewGroups(
    char** groups,
    folly::Range<const vector_size_t*> indices) {
  exec::Aggregate::setAllNulls(groups, indices);
  for (auto i : indices) {
    *exec::Aggregate::value<double>(groups[i]) = 0;
  }
}

/// Override 'extractValues' for single aggregation over float values.
/// Make sure to correctly read 'float' value from 'double' accumulator.
template <>
inline void SumAggregate<float, double, float>::extractValues(
    char** groups,
    int32_t numGroups,
    VectorPtr* result) {
  BaseAggregate::template doExtractValues<float>(
      groups, numGroups, result, [&](char* group) {
        return (
            float)(*BaseAggregate::Aggregate::template value<double>(group));
      });
}

template <template <typename U, typename V, typename W> class T>
bool registerSumAggregate(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .returnType("real")
          .intermediateType("double")
          .argumentType("real")
          .build(),
      exec::AggregateFunctionSignatureBuilder()
          .returnType("double")
          .intermediateType("double")
          .argumentType("double")
          .build(),
  };

  for (const auto& inputType : {"tinyint", "smallint", "integer", "bigint"}) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType("bigint")
                             .intermediateType("bigint")
                             .argumentType(inputType)
                             .build());
  }

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType) -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(argTypes.size(), 1, "{} takes only one argument", name);
        auto inputType = argTypes[0];
        switch (inputType->kind()) {
          case TypeKind::TINYINT:
            return std::make_unique<T<int8_t, int64_t, int64_t>>(BIGINT());
          case TypeKind::SMALLINT:
            return std::make_unique<T<int16_t, int64_t, int64_t>>(BIGINT());
          case TypeKind::INTEGER:
            return std::make_unique<T<int32_t, int64_t, int64_t>>(BIGINT());
          case TypeKind::BIGINT:
            return std::make_unique<T<int64_t, int64_t, int64_t>>(BIGINT());
          case TypeKind::REAL:
            if (resultType->kind() == TypeKind::REAL) {
              return std::make_unique<T<float, double, float>>(resultType);
            }
            return std::make_unique<T<float, double, double>>(DOUBLE());
          case TypeKind::DOUBLE:
            if (resultType->kind() == TypeKind::REAL) {
              return std::make_unique<T<double, double, float>>(resultType);
            }
            return std::make_unique<T<double, double, double>>(DOUBLE());
          default:
            VELOX_CHECK(
                false,
                "Unknown input type for {} aggregation {}",
                name,
                inputType->kindName());
        }
      });
}

} // namespace facebook::velox::aggregate
