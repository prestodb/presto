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

#include <gtest/gtest.h>

#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregateFunctionRegistry.h"
#include "velox/functions/Registerer.h"
#include "velox/type/Type.h"

namespace facebook::velox::exec::test {

namespace {

class AggregateFunc : public Aggregate {
 public:
  explicit AggregateFunc(TypePtr resultType) : Aggregate(resultType) {}

  void finalize(char** /*groups*/, int32_t /*numGroups*/) override {}

  int32_t accumulatorFixedWidthSize() const override {
    return 0;
  }

  void initializeNewGroups(
      char** /*groups*/,
      folly::Range<const vector_size_t*> /*indices*/) override {}

  void addRawInput(
      char** /*groups*/,
      const SelectivityVector& /*rows*/,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushdown*/) override {}

  void addIntermediateResults(
      char** /*groups*/,
      const SelectivityVector& /*rows*/,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushdown*/) override {}

  void addSingleGroupRawInput(
      char* /*group*/,
      const SelectivityVector& /*rows*/,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushdown*/) override {}

  void addSingleGroupIntermediateResults(
      char* /*group*/,
      const SelectivityVector& /*rows*/,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushdown*/) override {}

  void extractValues(
      char** /*groups*/,
      int32_t /*numGroups*/,
      VectorPtr* /*result*/) override {}

  void extractAccumulators(
      char** /*groups*/,
      int32_t /*numGroups*/,
      VectorPtr* /*result*/) override {}
};

bool registerAggregateFunc(const std::string& name) {
  std::vector<std::shared_ptr<AggregateFunctionSignature>> signatures{
      AggregateFunctionSignatureBuilder()
          .returnType("bigint")
          .intermediateType("array(bigint)")
          .argumentType("bigint")
          .argumentType("double")
          .build(),
      AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("T")
          .intermediateType("array(T)")
          .argumentType("T")
          .argumentType("T")
          .build(),
      AggregateFunctionSignatureBuilder()
          .returnType("date")
          .intermediateType("date")
          .build(),
  };

  registerAggregateFunction(
      name,
      std::move(signatures),
      [&](core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType) -> std::unique_ptr<exec::Aggregate> {
        if (isPartialOutput(step)) {
          if (argTypes.empty()) {
            return std::make_unique<AggregateFunc>(resultType);
          }
          return std::make_unique<AggregateFunc>(ARRAY(resultType));
        }
        return std::make_unique<AggregateFunc>(resultType);
      });

  return true;
}

} // namespace

class FunctionRegistryTest : public testing::Test {
 public:
  FunctionRegistryTest() {
    registerAggregateFunc("aggregate_func");
    registerAggregateFunc("Aggregate_Func_Alias");
  }

  void checkEqual(const TypePtr& actual, const TypePtr& expected) {
    if (expected) {
      EXPECT_EQ(*actual, *expected);
    } else {
      EXPECT_EQ(actual, nullptr);
    }
  }

  void testResolveAggregateFunction(
      const std::string& functionName,
      const std::vector<TypePtr>& argTypes,
      const TypePtr& expectedReturn,
      const TypePtr& expectedIntermediate) {
    auto result = resolveAggregateFunction(functionName, argTypes);
    checkEqual(result.first, expectedReturn);
    checkEqual(result.second, expectedIntermediate);
  }
};

TEST_F(FunctionRegistryTest, hasAggregateFunctionSignature) {
  testResolveAggregateFunction(
      "aggregate_func", {BIGINT(), DOUBLE()}, BIGINT(), ARRAY(BIGINT()));
  testResolveAggregateFunction(
      "aggregate_func", {DOUBLE(), DOUBLE()}, DOUBLE(), ARRAY(DOUBLE()));
  testResolveAggregateFunction(
      "aggregate_func",
      {ARRAY(BOOLEAN()), ARRAY(BOOLEAN())},
      ARRAY(BOOLEAN()),
      ARRAY(ARRAY(BOOLEAN())));
  testResolveAggregateFunction("aggregate_func", {}, DATE(), DATE());
}

TEST_F(FunctionRegistryTest, hasAggregateFunctionSignatureWrongFunctionName) {
  testResolveAggregateFunction(
      "aggregate_func_nonexist", {BIGINT(), BIGINT()}, nullptr, nullptr);
  testResolveAggregateFunction("aggregate_func_nonexist", {}, nullptr, nullptr);
}

TEST_F(FunctionRegistryTest, hasAggregateFunctionSignatureWrongArgType) {
  testResolveAggregateFunction(
      "aggregate_func", {DOUBLE(), BIGINT()}, nullptr, nullptr);
  testResolveAggregateFunction("aggregate_func", {BIGINT()}, nullptr, nullptr);
  testResolveAggregateFunction(
      "aggregate_func", {BIGINT(), BIGINT(), BIGINT()}, nullptr, nullptr);
}

TEST_F(FunctionRegistryTest, functionNameInMixedCase) {
  testResolveAggregateFunction(
      "aggregatE_funC", {BIGINT(), DOUBLE()}, BIGINT(), ARRAY(BIGINT()));
  testResolveAggregateFunction(
      "aggregatE_funC_aliaS", {DOUBLE(), DOUBLE()}, DOUBLE(), ARRAY(DOUBLE()));
}

} // namespace facebook::velox::exec::test
