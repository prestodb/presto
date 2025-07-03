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

#include "folly/CPortability.h"

#include "velox/expression/FunctionSignature.h"
#include "velox/functions/lib/CheckedArithmeticImpl.h"
#include "velox/functions/lib/aggregates/DecimalAggregate.h"
#include "velox/functions/lib/aggregates/SimpleNumericAggregate.h"

namespace facebook::velox::functions::aggregate {

template <
    typename TInput,
    typename TAccumulator,
    typename ResultType,
    bool Overflow>
class SumAggregateBase
    : public SimpleNumericAggregate<TInput, TAccumulator, ResultType> {
  using BaseAggregate =
      SimpleNumericAggregate<TInput, TAccumulator, ResultType>;

 public:
  explicit SumAggregateBase(TypePtr resultType) : BaseAggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(TAccumulator);
  }

  int32_t accumulatorAlignmentSize() const override {
    return 1;
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    BaseAggregate::template doExtractValues<ResultType>(
        groups, numGroups, result, [&](char* group) {
          // 'ResultType' and 'TAccumulator' might not be same such as sum(real)
          // and we do an explicit type conversion here.
          return (ResultType)(*BaseAggregate::Aggregate::template value<
                              TAccumulator>(group));
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
    updateInternal<TAccumulator, TAccumulator>(groups, rows, args, mayPushdown);
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
        TAccumulator(0));
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    BaseAggregate::template updateOneGroup<TAccumulator, TAccumulator>(
        group,
        rows,
        args[0],
        &updateSingleValue<TAccumulator>,
        &updateDuplicateValues<TAccumulator>,
        mayPushdown,
        TAccumulator(0));
  }

 protected:
  // TData is used to store the updated sum state. It can be either
  // TAccumulator or TResult, which in most cases are the same, but for
  // sum(real) can differ. TValue is used to decode the sum input 'args'.
  // It can be either TAccumulator or TInput, which is most cases are the same
  // but for sum(real) can differ.
  template <typename TData, typename TValue = TInput>
  void updateInternal(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) {
    const auto& arg = args[0];

    if (mayPushdown && arg->isLazy() &&
        arg->asChecked<const LazyVector>()->supportsHook()) {
      BaseAggregate::template pushdown<
          facebook::velox::aggregate::SumHook<TData, Overflow>>(
          groups, rows, arg);
      return;
    }

    if (exec::Aggregate::numNulls_) {
      BaseAggregate::template updateGroups<true, TData, TValue>(
          groups, rows, arg, &updateSingleValue<TData>, false);
    } else {
      BaseAggregate::template updateGroups<false, TData, TValue>(
          groups, rows, arg, &updateSingleValue<TData>, false);
    }
  }

  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      *exec::Aggregate::value<TAccumulator>(groups[i]) = 0;
    }
  }

 private:
  template <typename TData>
  static void updateSingleValue(TData& result, TData value) {
    velox::aggregate::SumHook<TData, Overflow>::add(result, value);
  }

  // Disable undefined behavior sanitizer to not fail on signed integer
  // overflow.
  template <typename TData>
#if defined(FOLLY_DISABLE_UNDEFINED_BEHAVIOR_SANITIZER)
  FOLLY_DISABLE_UNDEFINED_BEHAVIOR_SANITIZER("signed-integer-overflow")
#endif
  static void updateDuplicateValues(TData& result, TData value, int n) {
    if constexpr (
        (std::is_same_v<TData, int64_t> && Overflow) ||
        std::is_same_v<TData, double> || std::is_same_v<TData, float>) {
      result += n * value;
    } else {
      result = functions::checkedPlus<TData>(
          result, functions::checkedMultiply<TData>(TData(n), value));
    }
  }
};

template <typename TInputType>
class DecimalSumAggregate
    : public functions::aggregate::DecimalAggregate<int128_t, TInputType> {
 public:
  explicit DecimalSumAggregate(TypePtr resultType)
      : functions::aggregate::DecimalAggregate<int128_t, TInputType>(
            resultType) {}

  virtual int128_t computeFinalValue(
      functions::aggregate::LongDecimalWithOverflowState* accumulator) final {
    auto sum = DecimalUtil::adjustSumForOverflow(
        accumulator->sum, accumulator->overflow);
    VELOX_USER_CHECK(sum.has_value(), "Decimal overflow");
    DecimalUtil::valueInRange(sum.value());
    return sum.value();
  }
};

} // namespace facebook::velox::functions::aggregate
