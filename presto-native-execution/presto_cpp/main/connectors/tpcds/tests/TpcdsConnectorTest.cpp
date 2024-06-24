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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace {

using namespace facebook::velox;
using namespace facebook::velox::connector::tpcds;

using facebook::velox::exec::test::PlanBuilder;
using facebook::velox::tpcds::Table;

class TpcdsConnectorTest : public exec::test::OperatorTestBase {
 public:
  const std::string kTpcdsConnectorId = "test-tpcds";

  void SetUp() override {
    OperatorTestBase::SetUp();
    auto tpcdsConnector =
        connector::getConnectorFactory(
            connector::tpcds::TpcdsConnectorFactory::kTpcdsConnectorName)
            ->newConnector(
                kTpcdsConnectorId, std::make_shared<core::MemConfig>());
    connector::registerConnector(tpcdsConnector);
  }

  void TearDown() override {
    connector::unregisterConnector(kTpcdsConnectorId);
    OperatorTestBase::TearDown();
  }

  exec::Split makeTpcdsSplit(size_t totalParts = 1, size_t partNumber = 0)
      const {
    return exec::Split(std::make_shared<TpcdsConnectorSplit>(
        kTpcdsConnectorId, totalParts, partNumber));
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

// // Simple scan of first 5 rows of "nation".
// TEST_F(TpcdsConnectorTest, simple) {
//   auto plan = PlanBuilder()
//                   .tpcdsTableScan(
//                       Table::TBL_CALL_CENTER,
//                       {"n_nationkey", "n_name", "n_regionkey", "n_comment"})
//                   .limit(0, 5, false)
//                   .planNode();

//   auto output = getResults(plan, {makeTpcdsSplit()});
//   auto expected = makeRowVector({
//       // n_nationkey
//       makeFlatVector<int64_t>({0, 1, 2, 3, 4}),
//       // n_name
//       makeFlatVector<StringView>({
//           "ALGERIA",
//           "ARGENTINA",
//           "BRAZIL",
//           "CANADA",
//           "EGYPT",
//       }),
//       // n_regionkey
//       makeFlatVector<int64_t>({0, 1, 1, 1, 4}),
//       // n_comment
//       makeFlatVector<StringView>({
//           "furiously regular requests. platelets affix furious",
//           "instructions wake quickly. final deposits haggle. final, silent
//           theodolites ", "asymptotes use fluffily quickly bold instructions.
//           slyly bold dependencies sleep carefully pending accounts", "ss
//           deposits wake across the pending foxes. packages after the
//           carefully bold requests integrate caref", "usly ironic, pending
//           foxes. even, special instructions nag. sly, final foxes detect
//           slyly fluffily ",
//       }),
//   });
//   test::assertEqualVectors(expected, output);
// }

// // Extract single column from "nation".
// TEST_F(TpcdsConnectorTest, singleColumn) {
//   auto plan =
//       PlanBuilder().tpcdsTableScan(Table::TBL_NATION, {"n_name"}).planNode();

//   auto output = getResults(plan, {makeTpcdsSplit()});
//   auto expected = makeRowVector({makeFlatVector<StringView>({
//       "ALGERIA",       "ARGENTINA", "BRAZIL", "CANADA",
//       "EGYPT",         "ETHIOPIA",  "FRANCE", "GERMANY",
//       "INDIA",         "INDONESIA", "IRAN",   "IRAQ",
//       "JAPAN",         "JORDAN",    "KENYA",  "MOROCCO",
//       "MOZAMBIQUE",    "PERU",      "CHINA",  "ROMANIA",
//       "SAUDI ARABIA",  "VIETNAM",   "RUSSIA", "UNITED KINGDOM",
//       "UNITED STATES",
//   })});
//   test::assertEqualVectors(expected, output);
//   EXPECT_EQ("n_name", output->type()->asRow().nameOf(0));
// }

// // Check that aliases are correctly resolved.
// TEST_F(TpcdsConnectorTest, singleColumnWithAlias) {
//   const std::string aliasedName = "my_aliased_column_name";

//   auto outputType = ROW({aliasedName}, {VARCHAR()});
//   auto plan =
//       PlanBuilder()
//           .startTableScan()
//           .outputType(outputType)
//           .tableHandle(std::make_shared<TpcdsTableHandle>(
//               kTpcdsConnectorId, Table::TBL_NATION))
//           .assignments({
//               {aliasedName, std::make_shared<TpcdsColumnHandle>("n_name")},
//               {"other_name", std::make_shared<TpcdsColumnHandle>("n_name")},
//               {"third_column",
//                std::make_shared<TpcdsColumnHandle>("n_regionkey")},
//           })
//           .endTableScan()
//           .limit(0, 1, false)
//           .planNode();

//   auto output = getResults(plan, {makeTpcdsSplit()});
//   auto expected = makeRowVector({makeFlatVector<StringView>({
//       "ALGERIA",
//   })});
//   test::assertEqualVectors(expected, output);

//   EXPECT_EQ(aliasedName, output->type()->asRow().nameOf(0));
//   EXPECT_EQ(1, output->childrenSize());
// }

// void TpcdsConnectorTest::runScaleFactorTest(double scaleFactor) {
//   auto plan = PlanBuilder()
//                   .startTableScan()
//                   .outputType(ROW({}, {}))
//                   .tableHandle(std::make_shared<TpcdsTableHandle>(
//                       kTpcdsConnectorId, Table::TBL_SUPPLIER, scaleFactor))
//                   .endTableScan()
//                   .singleAggregation({}, {"count(1)"})
//                   .planNode();

//   auto output = getResults(plan, {makeTpcdsSplit()});
//   int64_t expectedRows = tpcds::getRowCount(Table::TBL_SUPPLIER,
//   scaleFactor); auto expected = makeRowVector(
//       {makeFlatVector<int64_t>(std::vector<int64_t>{expectedRows})});
//   test::assertEqualVectors(expected, output);
// }

// // Aggregation over a larger table.
// TEST_F(TpcdsConnectorTest, simpleAggregation) {
//   VELOX_ASSERT_THROW(
//       runScaleFactorTest(-1), "Tpcds scale factor must be non-negative");
//   runScaleFactorTest(0.01);
//   runScaleFactorTest(1.0);
//   runScaleFactorTest(5.0);
//   runScaleFactorTest(13.0);
// }

// TEST_F(TpcdsConnectorTest, lineitemTinyRowCount) {
//   // Lineitem row count depends on the orders.
//   // Verify against Java tiny result.
//   auto plan = PlanBuilder()
//                   .startTableScan()
//                   .outputType(ROW({}, {}))
//                   .tableHandle(std::make_shared<TpcdsTableHandle>(
//                       kTpcdsConnectorId, Table::TBL_LINEITEM, 0.01))
//                   .endTableScan()
//                   .singleAggregation({}, {"count(1)"})
//                   .planNode();

//   auto output = getResults(plan, {makeTpcdsSplit()});
//   EXPECT_EQ(60'175, output->childAt(0)->asFlatVector<int64_t>()->valueAt(0));
// }

// TEST_F(TpcdsConnectorTest, unknownColumn) {
//   EXPECT_THROW(
//       {
//         PlanBuilder()
//             .tpcdsTableScan(Table::TBL_NATION, {"does_not_exist"})
//             .planNode();
//       },
//       VeloxUserError);
// }

// // Ensures that splits broken down using different configurations return the
// // same dataset in the end.
// TEST_F(TpcdsConnectorTest, multipleSplits) {
//   auto plan = PlanBuilder()
//                   .tpcdsTableScan(
//                       Table::TBL_NATION,
//                       {"n_nationkey", "n_name", "n_regionkey", "n_comment"})
//                   .planNode();

//   // Use a full read from a single split to use as the source of truth.
//   auto fullResult = getResults(plan, {makeTpcdsSplit()});
//   size_t nationRowCount = tpcds::getRowCount(tpcds::Table::TBL_NATION, 1);
//   EXPECT_EQ(nationRowCount, fullResult->size());

//   // Run query with different number of parts, up until `totalParts >
//   // nationRowCount` in which cases each split will return one or zero
//   records. for (size_t totalParts = 1; totalParts < (nationRowCount + 5);
//   ++totalParts) {
//     std::vector<exec::Split> splits;
//     splits.reserve(totalParts);

//     for (size_t i = 0; i < totalParts; ++i) {
//       splits.emplace_back(makeTpcdsSplit(totalParts, i));
//     }

//     auto output = getResults(plan, std::move(splits));
//     test::assertEqualVectors(fullResult, output);
//   }
// }

} // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}