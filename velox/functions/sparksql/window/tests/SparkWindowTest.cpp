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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/lib/window/tests/WindowTestBase.h"
#include "velox/functions/sparksql/window/WindowFunctionsRegistration.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::window::test {

namespace {

static const std::vector<std::string> kSparkWindowFunctions = {
    std::string("nth_value(c0, 1)"),
    std::string("nth_value(c0, c3)"),
    std::string("row_number()"),
    std::string("rank()"),
    std::string("dense_rank()"),
    std::string("ntile(c3)"),
    std::string("ntile(1)"),
    std::string("ntile(7)"),
    std::string("ntile(10)"),
    std::string("ntile(16)")};

struct SparkWindowTestParam {
  const std::string function;
  const std::string overClause;
};

class SparkWindowTestBase : public WindowTestBase {
 protected:
  explicit SparkWindowTestBase(const SparkWindowTestParam& testParam)
      : function_(testParam.function), overClause_(testParam.overClause) {}

  void testWindowFunction(const std::vector<RowVectorPtr>& vectors) {
    WindowTestBase::testWindowFunction(vectors, function_, {overClause_});
  }

  void SetUp() override {
    WindowTestBase::SetUp();
    WindowTestBase::options_.parseIntegerAsBigint = false;
    velox::functions::window::sparksql::registerWindowFunctions("");
  }

  const std::string function_;
  const std::string overClause_;
};

std::vector<SparkWindowTestParam> getSparkWindowTestParams() {
  std::vector<SparkWindowTestParam> params;

  for (auto function : kSparkWindowFunctions) {
    for (auto overClause : kOverClauses) {
      params.push_back({function, overClause});
    }
  }

  return params;
}

class SparkWindowTest
    : public SparkWindowTestBase,
      public testing::WithParamInterface<SparkWindowTestParam> {
 public:
  SparkWindowTest() : SparkWindowTestBase(GetParam()) {}
};

// Tests all functions with a dataset with uniform distribution of partitions.
TEST_P(SparkWindowTest, basic) {
  testWindowFunction({makeSimpleVector(40)});
}

// Tests all functions with a dataset with all rows in a single partition,
// but in 2 input vectors.
TEST_P(SparkWindowTest, singlePartition) {
  testWindowFunction(
      {makeSinglePartitionVector(40), makeSinglePartitionVector(50)});
}

// Tests all functions with a dataset in which all partitions have a single row.
TEST_P(SparkWindowTest, singleRowPartitions) {
  testWindowFunction({makeSingleRowPartitionsVector(40)});
}

// Tests all functions with a dataset with randomly generated data.
TEST_P(SparkWindowTest, randomInput) {
  testWindowFunction({makeRandomInputVector(30)});
}

// Run above tests for all combinations of rank function and over clauses.
VELOX_INSTANTIATE_TEST_SUITE_P(
    SparkWindowTestInstantiation,
    SparkWindowTest,
    testing::ValuesIn(getSparkWindowTestParams()));

}; // namespace
}; // namespace facebook::velox::window::test
