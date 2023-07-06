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

#include "velox/exec/Aggregate.h"
#include "velox/exec/tests/SimpleAggregateFunctionsRegistration.h"
#include "velox/functions/lib/aggregates/tests/AggregationTestBase.h"

using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using facebook::velox::functions::aggregate::test::AggregationTestBase;

namespace facebook::velox::aggregate::test {
namespace {

const char* const kSimpleAvg = "simple_avg";

class SimpleAverageAggregationTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    allowInputShuffle();

    registerSimpleAverageAggregate(kSimpleAvg);
  }
};

TEST_F(SimpleAverageAggregationTest, averageAggregate) {
  auto inputVectors = makeRowVector(
      {makeFlatVector<bool>(
           {true,
            false,
            true,
            false,
            true,
            false,
            true,
            false,
            true,
            false,
            true,
            false}),
       makeFlatVector<bool>(
           {true,
            true,
            true,
            true,
            true,
            true,
            true,
            true,
            true,
            true,
            false,
            false}),
       makeNullableFlatVector<int64_t>(
           {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, std::nullopt, std::nullopt}),
       makeNullableFlatVector<double>(
           {1.1,
            2.2,
            3.3,
            4.4,
            5.5,
            6.6,
            7.7,
            8.8,
            9.9,
            11,
            std::nullopt,
            std::nullopt})});

  auto expected = makeRowVector(
      {makeFlatVector<bool>({true, false}),
       makeFlatVector<double>({5, 6}),
       makeFlatVector<double>({5.5, 6.6})});
  testAggregations(
      {inputVectors}, {"c0"}, {"simple_avg(c2)", "simple_avg(c3)"}, {expected});

  expected = makeRowVector(
      {makeFlatVector<bool>({true, false}),
       makeNullableFlatVector<double>({5.5, std::nullopt}),
       makeNullableFlatVector<double>({6.05, std::nullopt})});
  testAggregations(
      {inputVectors}, {"c1"}, {"simple_avg(c2)", "simple_avg(c3)"}, {expected});

  expected = makeRowVector(
      {makeFlatVector<double>(std::vector<double>{5.5}),
       makeFlatVector<double>(std::vector<double>{6.05})});
  testAggregations(
      {inputVectors}, {}, {"simple_avg(c2)", "simple_avg(c3)"}, {expected});

  inputVectors = makeRowVector({makeNullableFlatVector<int64_t>(
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt})});
  expected = makeRowVector({makeNullableFlatVector<double>({std::nullopt})});
  testAggregations({inputVectors}, {}, {"simple_avg(c0)"}, {expected});
}

} // namespace
} // namespace facebook::velox::aggregate::test
