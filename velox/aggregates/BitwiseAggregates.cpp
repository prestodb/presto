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

#include "velox/aggregates/AggregateNames.h"
#include "velox/aggregates/SimpleNumerics.h"
#include "velox/exec/Aggregate.h"

namespace facebook::velox::aggregate {

namespace {

template <typename T>
class BitwiseAndOrAggregate : public SimpleNumericAggregate<T, T, T> {
  using BaseAggregate = SimpleNumericAggregate<T, T, T>;

 public:
  BitwiseAndOrAggregate(
      core::AggregationNode::Step step,
      TypePtr resultType,
      T initialValue)
      : BaseAggregate(step, resultType), initialValue_(initialValue) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(T);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      *exec::Aggregate::value<T>(groups[i]) = initialValue_;
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    BaseAggregate::doExtractValues(groups, numGroups, result, [&](char* group) {
      return *BaseAggregate::Aggregate::template value<T>(group);
    });
  }

  void updateFinal(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    this->updatePartial(groups, rows, args, mayPushdown);
  }

  void updateSingleGroupFinal(
      char* group,
      const SelectivityVector& allRows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    this->updateSingleGroupPartial(group, allRows, args, mayPushdown);
  }

 protected:
  const T initialValue_;
};

template <typename T>
class BitwiseOrAggregate : public BitwiseAndOrAggregate<T> {
 public:
  explicit BitwiseOrAggregate(
      core::AggregationNode::Step step,
      TypePtr resultType)
      : BitwiseAndOrAggregate<T>(
            step,
            resultType,
            /* initialValue = */ 0) {}

  void updatePartial(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    SimpleNumericAggregate<T, T, T>::template updateGroups<true>(
        groups,
        rows,
        args[0],
        [](T& result, T value) { result |= value; },
        mayPushdown);
  }

  void updateSingleGroupPartial(
      char* group,
      const SelectivityVector& allRows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    SimpleNumericAggregate<T, T, T>::updateOneGroup(
        group,
        allRows,
        args[0],
        [](T& result, T value) { result |= value; },
        [](T& result, T value, int /* unused */
        ) { result |= value; },
        mayPushdown,
        this->initialValue_);
  }
};

template <typename T>
class BitwiseAndAggregate : public BitwiseAndOrAggregate<T> {
 public:
  explicit BitwiseAndAggregate(
      core::AggregationNode::Step step,
      TypePtr resultType)
      : BitwiseAndOrAggregate<T>(
            step,
            resultType,
            /* initialValue = */ -1) {}

  void updatePartial(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    SimpleNumericAggregate<T, T, T>::template updateGroups<true>(
        groups,
        rows,
        args[0],
        [](T& result, T value) { result &= value; },
        mayPushdown);
  }

  void updateSingleGroupPartial(
      char* group,
      const SelectivityVector& allRows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    SimpleNumericAggregate<T, T, T>::template updateOneGroup(
        group,
        allRows,
        args[0],
        [](T& result, T value) { result &= value; },
        [](T& result, T value, int /* unused */
        ) { result &= value; },
        mayPushdown,
        this->initialValue_);
  }
};

template <template <typename U> class T>
bool registerBitwiseAggregate(const std::string& name) {
  exec::AggregateFunctions().Register(
      name,
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr&
          /*resultType*/) -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_LE(argTypes.size(), 1, "{} takes only one argument", name);
        auto inputType = argTypes[0];
        switch (inputType->kind()) {
          case TypeKind::TINYINT:
            return std::make_unique<T<int8_t>>(step, inputType);
          case TypeKind::SMALLINT:
            return std::make_unique<T<int16_t>>(step, inputType);
          case TypeKind::INTEGER:
            return std::make_unique<T<int32_t>>(step, inputType);
          case TypeKind::BIGINT:
            return std::make_unique<T<int64_t>>(step, inputType);
          default:
            VELOX_CHECK(
                false,
                "Unknown input type for {} aggregation {}",
                name,
                inputType->kindName());
        }
      });
  return true;
}

static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) =
    registerBitwiseAggregate<BitwiseOrAggregate>(kBitwiseOr);
static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) =
    registerBitwiseAggregate<BitwiseAndAggregate>(kBitwiseAnd);

} // namespace
} // namespace facebook::velox::aggregate
