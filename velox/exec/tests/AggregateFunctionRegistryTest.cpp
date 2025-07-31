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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregateUtil.h"
#include "velox/exec/WindowFunction.h"
#include "velox/exec/tests/AggregateRegistryTestUtil.h"
#include "velox/type/Type.h"

namespace facebook::velox::exec::test {

class AggregateFunctionRegistryTest : public testing::Test {
 protected:
  AggregateFunctionRegistryTest() {
    registerAggregateFunc("aggregate_func");
    registerAggregateFunc("Aggregate_Func_Alias");
  }

  void testResolve(
      const std::string& name,
      const std::vector<TypePtr>& argTypes,
      const TypePtr& expectedFinalType,
      const TypePtr& expectedIntermediateType) {
    auto [finalType, intermediateType] =
        resolveAggregateFunction(name, argTypes);
    EXPECT_EQ(*finalType, *expectedFinalType);
    EXPECT_EQ(*intermediateType, *expectedIntermediateType);
  }

  void clearRegistry() {
    aggregateFunctions().withWLock(
        [](auto& aggregationFunctionMap) { aggregationFunctionMap.clear(); });
  }
};

TEST_F(AggregateFunctionRegistryTest, basic) {
  testResolve(
      "aggregate_func", {BIGINT(), DOUBLE()}, BIGINT(), ARRAY(BIGINT()));
  testResolve(
      "aggregate_func", {DOUBLE(), DOUBLE()}, DOUBLE(), ARRAY(DOUBLE()));
  testResolve(
      "aggregate_func",
      {ARRAY(BOOLEAN()), ARRAY(BOOLEAN())},
      ARRAY(BOOLEAN()),
      ARRAY(ARRAY(BOOLEAN())));
  testResolve("aggregate_func", {}, DATE(), DATE());
}

TEST_F(AggregateFunctionRegistryTest, wrongFunctionName) {
  VELOX_ASSERT_THROW(
      resolveAggregateFunction("aggregate_func_nonexist", {BIGINT(), BIGINT()}),
      "Aggregate function not registered: aggregate_func_nonexist");
  VELOX_ASSERT_THROW(
      resolveAggregateFunction("aggregate_func_nonexist", {}),
      "Aggregate function not registered: aggregate_func_nonexist");
}

TEST_F(AggregateFunctionRegistryTest, wrongArgType) {
  VELOX_ASSERT_THROW(
      resolveAggregateFunction("aggregate_func", {DOUBLE(), BIGINT()}),
      "Aggregate function signature is not supported");
  VELOX_ASSERT_THROW(
      resolveAggregateFunction("aggregate_func", {BIGINT()}),
      "Aggregate function signature is not supported");
  VELOX_ASSERT_THROW(
      resolveAggregateFunction(
          "aggregate_func", {BIGINT(), BIGINT(), BIGINT()}),
      "Aggregate function signature is not supported");
}

TEST_F(AggregateFunctionRegistryTest, functionNameInMixedCase) {
  testResolve(
      "aggregatE_funC", {BIGINT(), DOUBLE()}, BIGINT(), ARRAY(BIGINT()));
  testResolve(
      "aggregatE_funC_aliaS", {DOUBLE(), DOUBLE()}, DOUBLE(), ARRAY(DOUBLE()));
}

TEST_F(AggregateFunctionRegistryTest, getSignatures) {
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

TEST_F(AggregateFunctionRegistryTest, windowFunction) {
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

TEST_F(AggregateFunctionRegistryTest, duplicateRegistration) {
  EXPECT_FALSE(registerAggregateFunc("aggregate_func"));
  EXPECT_TRUE(registerAggregateFunc("aggregate_func", true));
}

TEST_F(AggregateFunctionRegistryTest, multipleNames) {
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
  testResolve(
      "aggregate_func1", {BIGINT(), DOUBLE()}, BIGINT(), ARRAY(BIGINT()));
  testResolve(
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
  testResolve(
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
  testResolve(
      "aggregate_func2", {BIGINT(), DOUBLE()}, BIGINT(), ARRAY(BIGINT()));
  testResolve(
      "aggregate_func2_partial",
      {BIGINT(), DOUBLE()},
      ARRAY(BIGINT()),
      ARRAY(BIGINT()));
  testResolve(
      "aggregate_func3", {BIGINT(), DOUBLE()}, BIGINT(), ARRAY(BIGINT()));
  testResolve(
      "aggregate_func3_partial",
      {BIGINT(), DOUBLE()},
      ARRAY(BIGINT()),
      ARRAY(BIGINT()));
}

TEST_F(AggregateFunctionRegistryTest, getAggregateFunctionNames) {
  clearRegistry();
  registerAggregateFunc("aggregate_func");
  registerAggregateFunc("Aggregate_Func_Alias");

  auto functions = getAggregateFunctionNames();
  EXPECT_EQ(functions.size(), 2);
  EXPECT_THAT(
      functions,
      testing::UnorderedElementsAre("aggregate_func", "aggregate_func_alias"));
}

} // namespace facebook::velox::exec::test
