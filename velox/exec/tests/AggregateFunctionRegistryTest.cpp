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

#include "velox/exec/AggregateFunctionRegistry.h"
#include <gtest/gtest.h>
#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregateUtil.h"
#include "velox/exec/WindowFunction.h"
#include "velox/functions/Registerer.h"
#include "velox/type/Type.h"

namespace facebook::velox::exec::test {

namespace {

class AggregateFunc : public Aggregate {
 public:
  explicit AggregateFunc(TypePtr resultType) : Aggregate(resultType) {}

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
  static std::vector<std::shared_ptr<AggregateFunctionSignature>> signatures() {
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
    return signatures;
  }
};

bool registerAggregateFunc(const std::string& name, bool overwrite = false) {
  auto signatures = AggregateFunc::signatures();

  return registerAggregateFunction(
             name,
             std::move(signatures),
             [&](core::AggregationNode::Step step,
                 const std::vector<TypePtr>& argTypes,
                 const TypePtr& resultType,
                 const core::QueryConfig& /*config*/)
                 -> std::unique_ptr<exec::Aggregate> {
               if (isPartialOutput(step)) {
                 if (argTypes.empty()) {
                   return std::make_unique<AggregateFunc>(resultType);
                 }
                 return std::make_unique<AggregateFunc>(ARRAY(resultType));
               }
               return std::make_unique<AggregateFunc>(resultType);
             },
             /*registerCompanionFunctions*/ false,
             overwrite)
      .mainFunction;
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

TEST_F(FunctionRegistryTest, getAggregateFunctionSignatures) {
  auto functionSignatures = getAggregateFunctionSignatures();
  auto aggregateFuncSignatures = functionSignatures["aggregate_func"];
  std::vector<std::string> aggregateFuncSignaturesStr;
  std::transform(
      aggregateFuncSignatures.begin(),
      aggregateFuncSignatures.end(),
      std::back_inserter(aggregateFuncSignaturesStr),
      [](auto& signature) { return signature->toString(); });

  auto expectedSignatures = AggregateFunc::signatures();
  std::vector<std::string> expectedSignaturesStr;
  std::transform(
      expectedSignatures.begin(),
      expectedSignatures.end(),
      std::back_inserter(expectedSignaturesStr),
      [](auto& signature) { return signature->toString(); });

  ASSERT_EQ(aggregateFuncSignaturesStr, expectedSignaturesStr);
}

TEST_F(FunctionRegistryTest, aggregateWindowFunctionSignature) {
  auto windowFunctionSignatures = getWindowFunctionSignatures("aggregate_func");
  ASSERT_EQ(windowFunctionSignatures->size(), 3);

  std::set<std::string> functionSignatures;
  for (const auto& signature : windowFunctionSignatures.value()) {
    functionSignatures.insert(signature->toString());
  }
  ASSERT_EQ(
      functionSignatures.count("(bigint,double) -> array(bigint) -> bigint"),
      1);
  ASSERT_EQ(functionSignatures.count("() -> date -> date"), 1);
  ASSERT_EQ(functionSignatures.count("(T,T) -> array(T) -> T"), 1);
}

TEST_F(FunctionRegistryTest, duplicateRegistration) {
  EXPECT_FALSE(registerAggregateFunc("aggregate_func"));
  EXPECT_TRUE(registerAggregateFunc("aggregate_func", true));
}

TEST_F(FunctionRegistryTest, multipleNames) {
  auto signatures = AggregateFunc::signatures();
  auto factory = [&](core::AggregationNode::Step step,
                     const std::vector<TypePtr>& argTypes,
                     const TypePtr& resultType,
                     const core::QueryConfig& /*config*/) {
    if (isPartialOutput(step)) {
      if (argTypes.empty()) {
        return std::make_unique<AggregateFunc>(resultType);
      }
      return std::make_unique<AggregateFunc>(ARRAY(resultType));
    }
    return std::make_unique<AggregateFunc>(resultType);
  };

  auto registrationResult = registerAggregateFunction(
      "aggregate_func1",
      signatures,
      factory,
      /*registerCompanionFunctions*/ true,
      /*overwrite*/ false);
  exec::AggregateRegistrationResult allSuccess{true, true, true, true, true};
  EXPECT_EQ(registrationResult, allSuccess);
  testResolveAggregateFunction(
      "aggregate_func1", {BIGINT(), DOUBLE()}, BIGINT(), ARRAY(BIGINT()));
  testResolveAggregateFunction(
      "aggregate_func1_partial",
      {BIGINT(), DOUBLE()},
      ARRAY(BIGINT()),
      ARRAY(BIGINT()));

  registrationResult = registerAggregateFunction(
      {std::string("aggregate_func2")},
      signatures,
      factory,
      /*registerCompanionFunctions*/ false,
      /*overwrite*/ false);
  exec::AggregateRegistrationResult onlyMainSuccess{
      true, false, false, false, false};
  EXPECT_EQ(registrationResult, onlyMainSuccess);
  testResolveAggregateFunction(
      "aggregate_func2", {BIGINT(), DOUBLE()}, BIGINT(), ARRAY(BIGINT()));

  auto registrationResults = registerAggregateFunction(
      std::vector<std::string>{"aggregate_func2", "aggregate_func3"},
      signatures,
      factory,
      /*registerCompanionFunctions*/ true,
      /*overwrite*/ false);
  exec::AggregateRegistrationResult allSuccessExceptMain{
      false, true, true, true, true};
  EXPECT_EQ(registrationResults[0], allSuccessExceptMain);
  EXPECT_EQ(registrationResults[1], allSuccess);
  testResolveAggregateFunction(
      "aggregate_func2", {BIGINT(), DOUBLE()}, BIGINT(), ARRAY(BIGINT()));
  testResolveAggregateFunction(
      "aggregate_func2_partial",
      {BIGINT(), DOUBLE()},
      ARRAY(BIGINT()),
      ARRAY(BIGINT()));
  testResolveAggregateFunction(
      "aggregate_func3", {BIGINT(), DOUBLE()}, BIGINT(), ARRAY(BIGINT()));
  testResolveAggregateFunction(
      "aggregate_func3_partial",
      {BIGINT(), DOUBLE()},
      ARRAY(BIGINT()),
      ARRAY(BIGINT()));
}

} // namespace facebook::velox::exec::test
