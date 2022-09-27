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

#include "velox/connectors/tpch/TpchConnector.h"
#include <folly/init/Init.h>
#include "gtest/gtest.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace {

using namespace facebook::velox;
using namespace facebook::velox::connector::tpch;

using facebook::velox::exec::test::PlanBuilder;
using facebook::velox::tpch::Table;

class TpchConnectorTest : public exec::test::OperatorTestBase {
 public:
  const std::string kTpchConnectorId = "test-tpch";

  void SetUp() override {
    OperatorTestBase::SetUp();
    auto tpchConnector =
        connector::getConnectorFactory(
            connector::tpch::TpchConnectorFactory::kTpchConnectorName)
            ->newConnector(kTpchConnectorId, nullptr);
    connector::registerConnector(tpchConnector);
  }

  void TearDown() override {
    connector::unregisterConnector(kTpchConnectorId);
    OperatorTestBase::TearDown();
  }

  exec::Split makeTpchSplit(size_t totalParts = 1, size_t partNumber = 0)
      const {
    return exec::Split(std::make_shared<TpchConnectorSplit>(
        kTpchConnectorId, totalParts, partNumber));
  }

  RowVectorPtr getResults(
      const core::PlanNodePtr& planNode,
      std::vector<exec::Split>&& splits) {
    return exec::test::AssertQueryBuilder(planNode)
        .splits(std::move(splits))
        .copyResults(pool());
  }

  void runScaleFactorTest(double scaleFactor);
};

// Simple scan of first 5 rows of "nation".
TEST_F(TpchConnectorTest, simple) {
  auto plan = PlanBuilder()
                  .tableScan(
                      Table::TBL_NATION,
                      {"n_nationkey", "n_name", "n_regionkey", "n_comment"})
                  .limit(0, 5, false)
                  .planNode();

  auto output = getResults(plan, {makeTpchSplit()});
  auto expected = makeRowVector({
      // n_nationkey
      makeFlatVector<int64_t>({0, 1, 2, 3, 4}),
      // n_name
      makeFlatVector<StringView>({
          "ALGERIA",
          "ARGENTINA",
          "BRAZIL",
          "CANADA",
          "EGYPT",
      }),
      // n_regionkey
      makeFlatVector<int64_t>({0, 1, 1, 1, 4}),
      // n_comment
      makeFlatVector<StringView>({
          "furiously regular requests. platelets affix furious",
          "instructions wake quickly. final deposits haggle. final, silent theodolites ",
          "asymptotes use fluffily quickly bold instructions. slyly bold dependencies sleep carefully pending accounts",
          "ss deposits wake across the pending foxes. packages after the carefully bold requests integrate caref",
          "usly ironic, pending foxes. even, special instructions nag. sly, final foxes detect slyly fluffily ",
      }),
  });
  test::assertEqualVectors(expected, output);
}

// Extract single column from "nation".
TEST_F(TpchConnectorTest, singleColumn) {
  auto plan = PlanBuilder().tableScan(Table::TBL_NATION, {"n_name"}).planNode();

  auto output = getResults(plan, {makeTpchSplit()});
  auto expected = makeRowVector({makeFlatVector<StringView>({
      "ALGERIA",       "ARGENTINA", "BRAZIL", "CANADA",
      "EGYPT",         "ETHIOPIA",  "FRANCE", "GERMANY",
      "INDIA",         "INDONESIA", "IRAN",   "IRAQ",
      "JAPAN",         "JORDAN",    "KENYA",  "MOROCCO",
      "MOZAMBIQUE",    "PERU",      "CHINA",  "ROMANIA",
      "SAUDI ARABIA",  "VIETNAM",   "RUSSIA", "UNITED KINGDOM",
      "UNITED STATES",
  })});
  test::assertEqualVectors(expected, output);
  EXPECT_EQ("n_name", output->type()->asRow().nameOf(0));
}

// Check that aliases are correctly resolved.
TEST_F(TpchConnectorTest, singleColumnWithAlias) {
  const std::string aliasedName = "my_aliased_column_name";

  auto outputType = ROW({aliasedName}, {VARCHAR()});
  auto plan =
      PlanBuilder()
          .tableScan(
              outputType,
              std::make_shared<TpchTableHandle>(
                  kTpchConnectorId, Table::TBL_NATION),
              {
                  {aliasedName, std::make_shared<TpchColumnHandle>("n_name")},
                  {"other_name", std::make_shared<TpchColumnHandle>("n_name")},
                  {"third_column",
                   std::make_shared<TpchColumnHandle>("n_regionkey")},
              })
          .limit(0, 1, false)
          .planNode();

  auto output = getResults(plan, {makeTpchSplit()});
  auto expected = makeRowVector({makeFlatVector<StringView>({
      "ALGERIA",
  })});
  test::assertEqualVectors(expected, output);

  EXPECT_EQ(aliasedName, output->type()->asRow().nameOf(0));
  EXPECT_EQ(1, output->childrenSize());
}

void TpchConnectorTest::runScaleFactorTest(double scaleFactor) {
  auto plan = PlanBuilder()
                  .tableScan(
                      ROW({}, {}),
                      std::make_shared<TpchTableHandle>(
                          kTpchConnectorId, Table::TBL_SUPPLIER, scaleFactor),
                      {})
                  .singleAggregation({}, {"count(1)"})
                  .planNode();

  auto output = getResults(plan, {makeTpchSplit()});
  int64_t expectedRows = tpch::getRowCount(Table::TBL_SUPPLIER, scaleFactor);
  auto expected = makeRowVector(
      {makeFlatVector<int64_t>(std::vector<int64_t>{expectedRows})});
  test::assertEqualVectors(expected, output);
}

// Aggregation over a larger table.
TEST_F(TpchConnectorTest, simpleAggregation) {
  VELOX_ASSERT_THROW(
      runScaleFactorTest(-1), "Tpch scale factor must be non-negative");
  runScaleFactorTest(0.01);
  runScaleFactorTest(1.0);
  runScaleFactorTest(5.0);
  runScaleFactorTest(13.0);
}

TEST_F(TpchConnectorTest, lineitemTinyRowCount) {
  // Lineitem row count depends on the orders.
  // Verify against Java tiny result.
  auto plan = PlanBuilder()
                  .tableScan(
                      ROW({}, {}),
                      std::make_shared<TpchTableHandle>(
                          kTpchConnectorId, Table::TBL_LINEITEM, 0.01),
                      {})
                  .singleAggregation({}, {"count(1)"})
                  .planNode();

  auto output = getResults(plan, {makeTpchSplit()});
  EXPECT_EQ(60'175, output->childAt(0)->asFlatVector<int64_t>()->valueAt(0));
}

TEST_F(TpchConnectorTest, unknownColumn) {
  EXPECT_THROW(
      {
        PlanBuilder()
            .tableScan(Table::TBL_NATION, {"does_not_exist"})
            .planNode();
      },
      VeloxUserError);
}

// Ensures that splits broken down using different configurations return the
// same dataset in the end.
TEST_F(TpchConnectorTest, multipleSplits) {
  auto plan = PlanBuilder()
                  .tableScan(
                      Table::TBL_NATION,
                      {"n_nationkey", "n_name", "n_regionkey", "n_comment"})
                  .planNode();

  // Use a full read from a single split to use as the source of truth.
  auto fullResult = getResults(plan, {makeTpchSplit()});
  size_t nationRowCount = tpch::getRowCount(tpch::Table::TBL_NATION, 1);
  EXPECT_EQ(nationRowCount, fullResult->size());

  // Run query with different number of parts, up until `totalParts >
  // nationRowCount` in which cases each split will return one or zero records.
  for (size_t totalParts = 1; totalParts < (nationRowCount + 5); ++totalParts) {
    std::vector<exec::Split> splits;
    splits.reserve(totalParts);

    for (size_t i = 0; i < totalParts; ++i) {
      splits.emplace_back(makeTpchSplit(totalParts, i));
    }

    auto output = getResults(plan, std::move(splits));
    test::assertEqualVectors(fullResult, output);
  }
}

// Join nation and region.
TEST_F(TpchConnectorTest, join) {
  auto planNodeIdGenerator =
      std::make_shared<exec::test::PlanNodeIdGenerator>();
  core::PlanNodeId nationScanId;
  core::PlanNodeId regionScanId;
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(
              tpch::Table::TBL_NATION, {"n_regionkey"}, 1.0 /*scaleFactor*/)
          .capturePlanNodeId(nationScanId)
          .hashJoin(
              {"n_regionkey"},
              {"r_regionkey"},
              PlanBuilder(planNodeIdGenerator)
                  .tableScan(
                      tpch::Table::TBL_REGION,
                      {"r_regionkey", "r_name"},
                      1.0 /*scaleFactor*/)
                  .capturePlanNodeId(regionScanId)
                  .planNode(),
              "", // extra filter
              {"r_name"})
          .singleAggregation({"r_name"}, {"count(1) as nation_cnt"})
          .orderBy({"r_name"}, false)
          .planNode();

  auto output = exec::test::AssertQueryBuilder(plan)
                    .split(nationScanId, makeTpchSplit())
                    .split(regionScanId, makeTpchSplit())
                    .copyResults(pool());

  auto expected = makeRowVector({
      makeFlatVector<StringView>(
          {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"}),
      makeConstant<int64_t>(5, 5),
  });
  test::assertEqualVectors(expected, output);
}

TEST_F(TpchConnectorTest, orderDateCount) {
  auto plan = PlanBuilder()
                  .tableScan(Table::TBL_ORDERS, {"o_orderdate"}, 0.01)
                  .filter("o_orderdate = '1992-01-01'::DATE")
                  .limit(0, 10, false)
                  .planNode();

  auto output = getResults(plan, {makeTpchSplit()});
  auto orderDate = output->childAt(0)->asFlatVector<Date>();
  EXPECT_EQ("1992-01-01", orderDate->valueAt(0).toString());
  // Match with count obtained from Java.
  EXPECT_EQ(9, orderDate->size());
}

} // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
