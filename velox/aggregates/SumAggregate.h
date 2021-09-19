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

#include "velox/aggregates/SimpleNumericAggregate.h"

namespace facebook::velox::aggregate {

template <typename TInput, typename TAccumulator, typename ResultType>
class SumAggregate
    : public SimpleNumericAggregate<TInput, TAccumulator, ResultType> {
  using BaseAggregate =
      SimpleNumericAggregate<TInput, TAccumulator, ResultType>;

 public:
  explicit SumAggregate(core::AggregationNode::Step step, TypePtr resultType)
      : BaseAggregate(step, resultType) {}

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

  void initializeNewGroups(
      char** /*groups*/,
      folly::Range<const vector_size_t*> /*indices*/,
      const VectorPtr& /*initialState*/) override {
    VELOX_NYI();
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

  void updatePartial(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    updateInternal<TAccumulator>(groups, rows, args, mayPushdown);
  }

  void updateFinal(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    updateInternal<ResultType>(groups, rows, args, mayPushdown);
  }

  void updateSingleGroupPartial(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    BaseAggregate::template updateOneGroup<TAccumulator>(
        group,
        rows,
        args[0],
        [](TAccumulator& result, TInput value) { result += value; },
        [](TAccumulator& result, TInput value, int n) { result += n * value; },
        mayPushdown,
        0);
  }

  void updateSingleGroupFinal(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    BaseAggregate::template updateOneGroup<ResultType>(
        group,
        rows,
        args[0],
        [](ResultType& result, TInput value) { result += value; },
        [](ResultType& result, TInput value, int n) { result += n * value; },
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

    if (mayPushdown) {
      BaseAggregate::template pushdown<SumHook<TInput, TData>>(
          groups, rows, arg);
      return;
    }

    if (exec::Aggregate::numNulls_) {
      BaseAggregate::template updateGroups<true, TData>(
          groups,
          rows,
          arg,
          [](TData& result, TInput value) { result += value; },
          false);
    } else {
      BaseAggregate::template updateGroups<false, TData>(
          groups,
          rows,
          arg,
          [](TData& result, TInput value) { result += value; },
          false);
    }
  }
};

template <template <typename U, typename V, typename W> class T>
bool registerSumAggregate(const std::string& name) {
  exec::AggregateFunctions().Register(
      name,
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType) -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(argTypes.size(), 1, "{} takes only one argument", name);
        auto inputType = argTypes[0];
        switch (inputType->kind()) {
          case TypeKind::TINYINT:
            return std::make_unique<T<int8_t, int64_t, int64_t>>(
                step, BIGINT());
          case TypeKind::SMALLINT:
            return std::make_unique<T<int16_t, int64_t, int64_t>>(
                step, BIGINT());
          case TypeKind::INTEGER:
            return std::make_unique<T<int32_t, int64_t, int64_t>>(
                step, BIGINT());
          case TypeKind::BIGINT:
            return std::make_unique<T<int64_t, int64_t, int64_t>>(
                step, BIGINT());
          case TypeKind::REAL:
            if (resultType->kind() == TypeKind::REAL) {
              return std::make_unique<T<float, double, float>>(
                  step, resultType);
            }
            return std::make_unique<T<float, double, double>>(step, DOUBLE());
          case TypeKind::DOUBLE:
            if (resultType->kind() == TypeKind::REAL) {
              return std::make_unique<T<double, double, float>>(
                  step, resultType);
            }
            return std::make_unique<T<double, double, double>>(step, DOUBLE());
          default:
            VELOX_CHECK(
                false,
                "Unknown input type for {} aggregation {}",
                name,
                inputType->kindName());
            return nullptr;
        }
      });
  return true;
}

} // namespace facebook::velox::aggregate
