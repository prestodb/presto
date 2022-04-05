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
#include "velox/common/base/Exceptions.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/functions/prestosql/aggregates/SumAggregate.h"

namespace facebook::velox::aggregate {

namespace {

class CountAggregate : public SimpleNumericAggregate<bool, int64_t, int64_t> {
  using BaseAggregate = SimpleNumericAggregate<bool, int64_t, int64_t>;

 public:
  explicit CountAggregate() : BaseAggregate(BIGINT()) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(int64_t);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    for (auto i : indices) {
      // result of count is never null
      *value<int64_t>(groups[i]) = (int64_t)0;
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    BaseAggregate::doExtractValues(groups, numGroups, result, [&](char* group) {
      return *value<int64_t>(group);
    });
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    if (args.empty()) {
      rows.applyToSelected([&](vector_size_t i) { addToGroup(groups[i], 1); });
      return;
    }

    DecodedVector decoded(*args[0], rows);
    if (decoded.isConstantMapping()) {
      if (!decoded.isNullAt(0)) {
        rows.applyToSelected(
            [&](vector_size_t i) { addToGroup(groups[i], 1); });
      }
    } else if (decoded.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (decoded.isNullAt(i)) {
          return;
        }
        addToGroup(groups[i], 1);
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) { addToGroup(groups[i], 1); });
    }
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedIntermediate_.decode(*args[0], rows);
    rows.applyToSelected([&](vector_size_t i) {
      addToGroup(groups[i], decodedIntermediate_.valueAt<int64_t>(i));
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    if (args.empty()) {
      addToGroup(group, rows.size());
      return;
    }

    DecodedVector decoded(*args[0], rows);
    if (decoded.isConstantMapping()) {
      if (!decoded.isNullAt(0)) {
        addToGroup(group, rows.size());
      }
    } else if (decoded.mayHaveNulls()) {
      int64_t nonNullCount = 0;
      rows.applyToSelected([&](vector_size_t i) {
        if (!decoded.isNullAt(i)) {
          ++nonNullCount;
        }
      });
      addToGroup(group, nonNullCount);
    } else {
      addToGroup(group, rows.size());
    }
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    VELOX_CHECK_EQ(args[0]->encoding(), VectorEncoding::Simple::FLAT);

    auto vector = args[0]->asUnchecked<FlatVector<int64_t>>();
    auto rawValues = vector->rawValues();
    int64_t count = 0;
    if (vector->mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (!vector->isNullAt(i)) {
          count += rawValues[i];
        }
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) { count += rawValues[i]; });
    }

    addToGroup(group, count);
  }

 private:
  inline void addToGroup(char* group, int64_t count) {
    *value<int64_t>(group) += count;
  }

  DecodedVector decodedIntermediate_;
};

bool registerCountAggregate(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .returnType("bigint")
          .intermediateType("bigint")
          .build(),
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("bigint")
          .intermediateType("bigint")
          .argumentType("T")
          .build(),
  };

  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr&
          /*resultType*/) -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_LE(
            argTypes.size(), 1, "{} takes at most one argument", name);
        return std::make_unique<CountAggregate>();
      });
  return true;
}

static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) =
    registerCountAggregate(kCount);

} // namespace
} // namespace facebook::velox::aggregate
