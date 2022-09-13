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

#include "velox/exec/tests/utils/SumNonPODAggregate.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/HashAggregation.h"
#include "velox/expression/FunctionSignature.h"

namespace facebook::velox::exec::test {

int NonPODInt64::constructed = 0;
int NonPODInt64::destructed = 0;

namespace {

// SumNonPODAggregate uses NonPODInt64 as accumulator which has external memory
// NonPODInt64::constructed and NonPODInt64::destructed. By asserting their
// equality, we make sure Velox calls constructor/destructor properly.
class SumNonPODAggregate : public Aggregate {
 public:
  explicit SumNonPODAggregate(velox::TypePtr resultType)
      : Aggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(NonPODInt64);
  }

  bool accumulatorUsesExternalMemory() const override {
    return true;
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const velox::vector_size_t*> indices) override {
    for (auto i : indices) {
      new (groups[i] + offset_) NonPODInt64(0);
    }
  }

  void destroy(folly::Range<char**> groups) override {
    for (auto group : groups) {
      value<NonPODInt64>(group)->~NonPODInt64();
    }
  }

  void extractAccumulators(
      char** groups,
      int32_t numGroups,
      velox::VectorPtr* result) override {
    auto vector = (*result)->as<FlatVector<int64_t>>();
    vector->resize(numGroups);
    int64_t* rawValues = vector->mutableRawValues();
    uint64_t* rawNulls = getRawNulls(vector);
    for (int32_t i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        vector->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        rawValues[i] = value<NonPODInt64>(group)->value;
      }
    }
  }

  void extractValues(char** groups, int32_t numGroups, velox::VectorPtr* result)
      override {
    extractAccumulators(groups, numGroups, result);
  }

  void addIntermediateResults(
      char** groups,
      const velox::SelectivityVector& rows,
      const std::vector<velox::VectorPtr>& args,
      bool /*mayPushdown*/) override {
    DecodedVector decoded(*args[0], rows);

    rows.applyToSelected([&](vector_size_t i) {
      if (decoded.isNullAt(i)) {
        return;
      }
      clearNull(groups[i]);
      value<NonPODInt64>(groups[i])->value += decoded.valueAt<int64_t>(i);
    });
  }

  void addRawInput(
      char** groups,
      const velox::SelectivityVector& rows,
      const std::vector<velox::VectorPtr>& args,
      bool mayPushdown) override {
    addIntermediateResults(groups, rows, args, mayPushdown);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const velox::SelectivityVector& rows,
      const std::vector<velox::VectorPtr>& args,
      bool /*mayPushdown*/) override {
    DecodedVector decoded(*args[0], rows);

    rows.applyToSelected([&](vector_size_t i) {
      if (decoded.isNullAt(i)) {
        return;
      }
      clearNull(group);
      value<NonPODInt64>(group)->value += decoded.valueAt<int64_t>(i);
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const velox::SelectivityVector& rows,
      const std::vector<velox::VectorPtr>& args,
      bool mayPushdown) override {
    addSingleGroupIntermediateResults(group, rows, args, mayPushdown);
  }

  void finalize(char** /*groups*/, int32_t /*numGroups*/) override {}
};

bool registerSumNonPODAggregate(const std::string& name) {
  std::vector<std::shared_ptr<velox::exec::AggregateFunctionSignature>>
      signatures{
          velox::exec::AggregateFunctionSignatureBuilder()
              .returnType("bigint")
              .intermediateType("bigint")
              .argumentType("bigint")
              .build(),
      };

  velox::exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          velox::core::AggregationNode::Step /*step*/,
          const std::vector<velox::TypePtr>& /*argTypes*/,
          const velox::TypePtr& /*resultType*/)
          -> std::unique_ptr<velox::exec::Aggregate> {
        return std::make_unique<SumNonPODAggregate>(velox::BIGINT());
      });
  return true;
}

static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) =
    registerSumNonPODAggregate("sumnonpod");
} // namespace
} // namespace facebook::velox::exec::test
