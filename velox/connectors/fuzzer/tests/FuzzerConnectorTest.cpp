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

#include "velox/connectors/fuzzer/FuzzerConnector.h"
#include <folly/init/Init.h>
#include "gtest/gtest.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace {

using namespace facebook::velox;
using namespace facebook::velox::connector::fuzzer;

using facebook::velox::exec::test::PlanBuilder;

class FuzzerConnectorTest : public exec::test::OperatorTestBase {
 public:
  const std::string kFuzzerConnectorId = "test-fuzzer";

  void SetUp() override {
    OperatorTestBase::SetUp();
    auto fuzzerConnector =
        connector::getConnectorFactory(
            connector::fuzzer::FuzzerConnectorFactory::kFuzzerConnectorName)
            ->newConnector(kFuzzerConnectorId, nullptr);
    connector::registerConnector(fuzzerConnector);
  }

  void TearDown() override {
    connector::unregisterConnector(kFuzzerConnectorId);
    OperatorTestBase::TearDown();
  }

  exec::Split makeFuzzerSplit(size_t numRows) const {
    return exec::Split(
        std::make_shared<FuzzerConnectorSplit>(kFuzzerConnectorId, numRows));
  }

  std::vector<exec::Split> makeFuzzerSplits(
      size_t rowsPerSplit,
      size_t numSplits) const {
    std::vector<exec::Split> splits;
    splits.reserve(numSplits);

    for (size_t i = 0; i < numSplits; ++i) {
      splits.emplace_back(makeFuzzerSplit(rowsPerSplit));
    }
    return splits;
  }

  std::shared_ptr<FuzzerTableHandle> makeFuzzerTableHandle(
      size_t fuzzerSeed = 0) const {
    return std::make_shared<FuzzerTableHandle>(
        kFuzzerConnectorId, fuzzerOptions_, fuzzerSeed);
  }

 private:
  VectorFuzzer::Options fuzzerOptions_;
};

TEST_F(FuzzerConnectorTest, singleSplit) {
  const size_t numRows = 100;
  auto type = ROW({BIGINT(), DOUBLE(), VARCHAR()});

  auto plan =
      PlanBuilder().tableScan(type, makeFuzzerTableHandle(), {}).planNode();

  exec::test::AssertQueryBuilder(plan)
      .split(makeFuzzerSplit(numRows))
      .assertTypeAndNumRows(type, numRows);
}

TEST_F(FuzzerConnectorTest, floatingPoints) {
  const size_t numRows = 1000;
  auto type = ROW({REAL(), DOUBLE()});

  auto plan =
      PlanBuilder().tableScan(type, makeFuzzerTableHandle(), {}).planNode();

  exec::test::AssertQueryBuilder(plan)
      .split(makeFuzzerSplit(numRows))
      .assertTypeAndNumRows(type, numRows);
}

TEST_F(FuzzerConnectorTest, complexTypes) {
  const size_t numRows = 100;
  auto type = ROW({
      ARRAY(BIGINT()),
      ROW({VARCHAR(), MAP(INTEGER(), ARRAY(DOUBLE())), VARBINARY()}),
      REAL(),
  });

  auto plan =
      PlanBuilder().tableScan(type, makeFuzzerTableHandle(), {}).planNode();

  exec::test::AssertQueryBuilder(plan)
      .split(makeFuzzerSplit(numRows))
      .assertTypeAndNumRows(type, numRows);
}

TEST_F(FuzzerConnectorTest, multipleSplits) {
  const size_t rowsPerSplit = 100;
  const size_t numSplits = 10;
  auto type = ROW({BIGINT(), DOUBLE(), VARCHAR()});

  auto plan =
      PlanBuilder().tableScan(type, makeFuzzerTableHandle(), {}).planNode();

  exec::test::AssertQueryBuilder(plan)
      .splits(makeFuzzerSplits(rowsPerSplit, numSplits))
      .assertTypeAndNumRows(type, rowsPerSplit * numSplits);
}

TEST_F(FuzzerConnectorTest, randomTypes) {
  const size_t rowsPerSplit = 100;
  const size_t numSplits = 10;

  const size_t iterations = 20;

  for (size_t i = 0; i < iterations; ++i) {
    auto type = VectorFuzzer({}, pool()).randRowType();

    auto plan =
        PlanBuilder().tableScan(type, makeFuzzerTableHandle(), {}).planNode();
    exec::test::AssertQueryBuilder(plan)
        .splits(makeFuzzerSplits(rowsPerSplit, numSplits))
        .assertTypeAndNumRows(type, rowsPerSplit * numSplits);
  }
}

TEST_F(FuzzerConnectorTest, reproducible) {
  const size_t numRows = 100;
  auto type = ROW({BIGINT(), ARRAY(INTEGER()), VARCHAR()});

  auto plan1 =
      PlanBuilder()
          .tableScan(type, makeFuzzerTableHandle(/*fuzerSeed=*/1234), {})
          .planNode();
  auto plan2 =
      PlanBuilder()
          .tableScan(type, makeFuzzerTableHandle(/*fuzerSeed=*/1234), {})
          .planNode();

  auto results1 = exec::test::AssertQueryBuilder(plan1)
                      .split(makeFuzzerSplit(numRows))
                      .copyResults(pool());
  auto results2 = exec::test::AssertQueryBuilder(plan2)
                      .split(makeFuzzerSplit(numRows))
                      .copyResults(pool());

  exec::test::assertEqualResults({results1}, {results2});
}

} // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
