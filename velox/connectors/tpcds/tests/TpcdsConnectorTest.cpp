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

#include "velox/connectors/tpcds/TpcdsConnector.h"
#include <folly/init/Init.h>
#include "gtest/gtest.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::velox::connector::tpcds::test {

namespace {
class TpcdsConnectorTest : public exec::test::OperatorTestBase {
 public:
  const std::string kTpcdsConnectorId = "test-tpcds";

  void SetUp() override {
    OperatorTestBase::SetUp();
    connector::registerConnectorFactory(
        std::make_shared<connector::tpcds::TpcdsConnectorFactory>());
    auto tpcdsConnector =
        connector::getConnectorFactory(
            connector::tpcds::TpcdsConnectorFactory::kTpcdsConnectorName)
            ->newConnector(
                kTpcdsConnectorId,
                std::make_shared<config::ConfigBase>(
                    std::unordered_map<std::string, std::string>()));
    connector::registerConnector(tpcdsConnector);
  }

  void TearDown() override {
    connector::unregisterConnector(kTpcdsConnectorId);
    connector::unregisterConnectorFactory(
        connector::tpcds::TpcdsConnectorFactory::kTpcdsConnectorName);
    OperatorTestBase::TearDown();
  }

  exec::Split makeTpcdsSplit(size_t totalParts = 1, size_t partNumber = 0)
      const {
    return exec::Split(std::make_shared<TpcdsConnectorSplit>(
        kTpcdsConnectorId, /*cacheable=*/true, totalParts, partNumber));
  }

  RowVectorPtr getResults(
      const core::PlanNodePtr& planNode,
      std::vector<exec::Split>&& splits) {
    return exec::test::AssertQueryBuilder(planNode)
        .splits(std::move(splits))
        .copyResults(pool());
  }
};

// Simple scan of first 5 rows of table 'warehouse'.
TEST_F(TpcdsConnectorTest, simple) {
  auto plan =
      exec::test::PlanBuilder()
          .tpcdsTableScan(
              velox::tpcds::Table::TBL_WAREHOUSE,
              {"w_warehouse_sk", "w_warehouse_name", "w_city", "w_state"},
              1)
          .limit(0, 5, false)
          .planNode();

  auto output = getResults(plan, {makeTpcdsSplit()});
  auto expected = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
      makeNullableFlatVector<StringView>(
          {"Conventional childr",
           "Important issues liv",
           "Doors canno",
           "Bad cards must make.",
           std::nullopt}),
      makeConstant<StringView>("Fairview", 5),
      makeConstant<StringView>("TN", 5),
  });
  velox::test::assertEqualVectors(expected, output);
}

// Extract single column from table 'store'.
TEST_F(TpcdsConnectorTest, singleColumn) {
  auto plan =
      exec::test::PlanBuilder()
          .tpcdsTableScan(velox::tpcds::Table::TBL_STORE, {"s_store_name"}, 1)
          .planNode();

  auto output = getResults(plan, {makeTpcdsSplit()});
  auto expected = makeRowVector({makeFlatVector<StringView>({
      "ought",
      "able",
      "able",
      "ese",
      "anti",
      "cally",
      "ation",
      "eing",
      "eing",
      "bar",
      "ought",
      "ought",
  })});
  velox::test::assertEqualVectors(expected, output);
  EXPECT_EQ("s_store_name", output->type()->asRow().nameOf(0));
}

// Check that aliases are correctly resolved.
TEST_F(TpcdsConnectorTest, singleColumnWithAlias) {
  const std::string aliasedName = "my_aliased_column_name";

  auto outputType = ROW({aliasedName}, {VARCHAR()});
  auto plan = exec::test::PlanBuilder()
                  .startTableScan()
                  .outputType(outputType)
                  .tableHandle(std::make_shared<TpcdsTableHandle>(
                      kTpcdsConnectorId, velox::tpcds::Table::TBL_ITEM))
                  .assignments({
                      {aliasedName,
                       std::make_shared<TpcdsColumnHandle>("i_product_name")},
                      {"other_name",
                       std::make_shared<TpcdsColumnHandle>("i_product_name")},
                      {"third_column",
                       std::make_shared<TpcdsColumnHandle>("i_manager_id")},
                  })
                  .endTableScan()
                  .limit(0, 1, false)
                  .planNode();

  auto output = getResults(plan, {makeTpcdsSplit()});
  auto expected = makeRowVector({makeFlatVector<StringView>({
      "ought",
  })});
  velox::test::assertEqualVectors(expected, output);

  EXPECT_EQ(aliasedName, output->type()->asRow().nameOf(0));
  EXPECT_EQ(1, output->childrenSize());
}

TEST_F(TpcdsConnectorTest, unknownColumn) {
  EXPECT_THROW(
      {
        exec::test::PlanBuilder()
            .tpcdsTableScan(velox::tpcds::Table::TBL_ITEM, {"invalid_column"})
            .planNode();
      },
      VeloxUserError);
}

// Join warehouse and store on state.
TEST_F(TpcdsConnectorTest, join) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId itemScanId;
  core::PlanNodeId inventoryScanId;
  auto plan = exec::test::PlanBuilder(planNodeIdGenerator)
                  .tpcdsTableScan(
                      velox::tpcds::Table::TBL_WAREHOUSE,
                      {"w_warehouse_id", "w_state"},
                      1)
                  .capturePlanNodeId(itemScanId)
                  .hashJoin(
                      {"w_state"},
                      {"s_state"},
                      exec::test::PlanBuilder(planNodeIdGenerator)
                          .tpcdsTableScan(
                              velox::tpcds::Table::TBL_STORE, {"s_state"}, 1)
                          .capturePlanNodeId(inventoryScanId)
                          .planNode(),
                      "",
                      {"w_warehouse_id"})
                  // Get distinct values of warehouse_id
                  .partialAggregation({"w_warehouse_id"}, {})
                  .finalAggregation()
                  .orderBy({"w_warehouse_id"}, false)
                  .planNode();

  auto output = exec::test::AssertQueryBuilder(plan)
                    .split(itemScanId, makeTpcdsSplit())
                    .split(inventoryScanId, makeTpcdsSplit())
                    .copyResults(pool());

  auto expected = makeRowVector({
      makeFlatVector<StringView>(
          {"AAAAAAAABAAAAAAA",
           "AAAAAAAACAAAAAAA",
           "AAAAAAAADAAAAAAA",
           "AAAAAAAAEAAAAAAA",
           "AAAAAAAAFAAAAAAA"}),
  });
  velox::test::assertEqualVectors(expected, output);
}

TEST_F(TpcdsConnectorTest, inventoryDateCount) {
  auto plan =
      exec::test::PlanBuilder()
          .tpcdsTableScan(
              velox::tpcds::Table::TBL_WEB_SITE, {"web_rec_start_date"}, 1)
          .filter("web_rec_start_date = '1997-08-16'::DATE")
          .limit(0, 10, false)
          .planNode();

  auto output = getResults(plan, {makeTpcdsSplit()});
  auto inventoryDate = output->childAt(0)->asFlatVector<int32_t>();
  EXPECT_EQ("1997-08-16", DATE()->toString(inventoryDate->valueAt(0)));
  EXPECT_EQ(10, inventoryDate->size());
}
} // namespace
} // namespace facebook::velox::connector::tpcds::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
