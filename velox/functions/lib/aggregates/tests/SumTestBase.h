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

#include "folly/CPortability.h"

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/AggregationHook.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"

namespace facebook::velox::functions::aggregate::test {

template <typename Type>
struct SumRow {
  char nulls;
  Type sum;
};

template <typename InputType, typename ResultType, bool Overflow = false>
#if defined(FOLLY_DISABLE_UNDEFINED_BEHAVIOR_SANITIZER)
FOLLY_DISABLE_UNDEFINED_BEHAVIOR_SANITIZER("signed-integer-overflow")
#endif
void testHookLimits(bool expectOverflow = false) {
  // Pair of <limit, value to overflow>.
  std::vector<std::pair<InputType, InputType>> limits = {
      {std::numeric_limits<InputType>::min(), -1},
      {std::numeric_limits<InputType>::max(), 1}};

  for (const auto& [limit, overflow] : limits) {
    SumRow<ResultType> sumRow;
    sumRow.sum = 0;
    ResultType expected = 0;
    char* row = reinterpret_cast<char*>(&sumRow);
    uint64_t numNulls = 0;
    facebook::velox::aggregate::SumHook<ResultType, Overflow> hook(
        offsetof(SumRow<ResultType>, sum),
        offsetof(SumRow<ResultType>, nulls),
        0,
        &row,
        &numNulls);

    // Adding limit should not overflow.
    ASSERT_NO_THROW(hook.addValueTyped(0, limit));
    expected += limit;
    EXPECT_EQ(expected, sumRow.sum);
    // Adding overflow based on the ResultType should throw.
    if (expectOverflow) {
      VELOX_ASSERT_THROW(hook.addValueTyped(0, overflow), "overflow");
    } else {
      ASSERT_NO_THROW(hook.addValueTyped(0, overflow));
      expected += overflow;
      EXPECT_EQ(expected, sumRow.sum);
    }
  }
}

class SumTestBase : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
  }

  template <
      typename InputType,
      typename ResultType,
      typename IntermediateType = ResultType>
  void testAggregateOverflow(
      const std::string& function,
      bool expectOverflow = false,
      const TypePtr& type = CppToType<InputType>::create());
};

template <typename ResultType>
void verifyAggregates(
    const std::vector<std::pair<core::PlanNodePtr, ResultType>>& aggsToTest,
    bool expectOverflow) {
  for (const auto& [agg, expectedResult] : aggsToTest) {
    if (expectOverflow) {
      VELOX_ASSERT_THROW(
          facebook::velox::exec::test::readSingleValue(agg), "overflow");
    } else {
      auto result = facebook::velox::exec::test::readSingleValue(agg);
      if constexpr (std::is_same_v<ResultType, float>) {
        ASSERT_FLOAT_EQ(
            result.template value<TypeKind::REAL>(), expectedResult);
      } else if constexpr (std::is_same_v<ResultType, double>) {
        ASSERT_FLOAT_EQ(
            result.template value<TypeKind::DOUBLE>(), expectedResult);
      } else {
        ASSERT_EQ(result, expectedResult);
      }
    }
  }
}

template <typename InputType, typename ResultType, typename IntermediateType>
#if defined(FOLLY_DISABLE_UNDEFINED_BEHAVIOR_SANITIZER)
FOLLY_DISABLE_UNDEFINED_BEHAVIOR_SANITIZER("signed-integer-overflow")
#endif
void SumTestBase::testAggregateOverflow(
    const std::string& function,
    bool expectOverflow,
    const TypePtr& type) {
  const InputType maxLimit = std::numeric_limits<InputType>::max();
  const InputType overflow = InputType(1);
  const InputType zero = InputType(0);

  // Intermediate type size is always >= result type size. Hence, use
  // intermediate type to calculate the expected output.
  IntermediateType limitResult = IntermediateType(maxLimit);
  IntermediateType overflowResult = IntermediateType(overflow);

  // Single max limit value. 0's to induce dummy calculations.
  auto limitVector =
      makeRowVector({makeFlatVector<InputType>({maxLimit, zero, zero}, type)});

  // Test code path for single values with possible overflow hit in add.
  auto overflowFlatVector =
      makeRowVector({makeFlatVector<InputType>({maxLimit, overflow}, type)});
  IntermediateType expectedFlatSum = limitResult + overflowResult;

  // Test code path for duplicate values with possible overflow hit in
  // multiply.
  auto overflowConstantVector =
      makeRowVector({makeConstant<InputType>(maxLimit / 3, 4, type)});
  IntermediateType expectedConstantSum = (limitResult / 3) * 4;

  // Test code path for duplicate values with possible overflow hit in add.
  auto overflowHybridVector = {limitVector, overflowConstantVector};
  IntermediateType expectedHybridSum = limitResult + expectedConstantSum;

  // Vector with element pairs of a partial aggregate node, expected result.
  std::vector<std::pair<core::PlanNodePtr, IntermediateType>> partialAggsToTest;
  // Partial Aggregation (raw input in - partial result out).
  partialAggsToTest.push_back(
      {facebook::velox::exec::test::PlanBuilder()
           .values({overflowFlatVector})
           .partialAggregation({}, {fmt::format("{}(c0)", function)})
           .planNode(),
       expectedFlatSum});
  partialAggsToTest.push_back(
      {facebook::velox::exec::test::PlanBuilder()
           .values({overflowConstantVector})
           .partialAggregation({}, {fmt::format("{}(c0)", function)})
           .planNode(),
       expectedConstantSum});
  partialAggsToTest.push_back(
      {facebook::velox::exec::test::PlanBuilder()
           .values(overflowHybridVector)
           .partialAggregation({}, {fmt::format("{}(c0)", function)})
           .planNode(),
       expectedHybridSum});

  // Vector with element pairs of a full aggregate node, expected result.
  std::vector<std::pair<core::PlanNodePtr, ResultType>> aggsToTest;
  // Single Aggregation (raw input in - final result out).
  aggsToTest.push_back(
      {facebook::velox::exec::test::PlanBuilder()
           .values({overflowFlatVector})
           .singleAggregation({}, {fmt::format("{}(c0)", function)})
           .planNode(),
       expectedFlatSum});
  aggsToTest.push_back(
      {facebook::velox::exec::test::PlanBuilder()
           .values({overflowConstantVector})
           .singleAggregation({}, {fmt::format("{}(c0)", function)})
           .planNode(),
       expectedConstantSum});
  aggsToTest.push_back(
      {facebook::velox::exec::test::PlanBuilder()
           .values(overflowHybridVector)
           .singleAggregation({}, {fmt::format("{}(c0)", function)})
           .planNode(),
       expectedHybridSum});
  // Final Aggregation (partial result in - final result out):
  // To make sure that the overflow occurs in the final aggregation step, we
  // create 2 plan fragments and plugging their partially aggregated
  // output into a final aggregate plan node. Each of those input fragments
  // only have a single input value under the max limit which when added in
  // the final step causes a potential overflow.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  aggsToTest.push_back(
      {facebook::velox::exec::test::PlanBuilder(planNodeIdGenerator)
           .localPartition(
               {},
               {facebook::velox::exec::test::PlanBuilder(planNodeIdGenerator)
                    .values({limitVector})
                    .partialAggregation({}, {fmt::format("{}(c0)", function)})
                    .planNode(),
                facebook::velox::exec::test::PlanBuilder(planNodeIdGenerator)
                    .values({limitVector})
                    .partialAggregation({}, {fmt::format("{}(c0)", function)})
                    .planNode()})
           .finalAggregation()
           .planNode(),
       limitResult + limitResult});

  // Verify all partial aggregates.
  verifyAggregates<IntermediateType>(partialAggsToTest, expectOverflow);
  // Verify all aggregates.
  verifyAggregates<ResultType>(aggsToTest, expectOverflow);
}

} // namespace facebook::velox::functions::aggregate::test
