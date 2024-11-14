/*
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
#include "presto_cpp/main/connectors/arrow_flight/ArrowFlightConnector.h"
#include <arrow/testing/gtest_util.h>
#include <folly/init/Init.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include "presto_cpp/main/connectors/arrow_flight/Macros.h"
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/ArrowFlightConnectorTestBase.h"
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/ArrowFlightPlanBuilder.h"
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/Utils.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/config/Config.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PortUtil.h"

using namespace arrow;
using namespace facebook::velox;
using namespace facebook::velox::exec::test;

namespace facebook::presto::test {

class ArrowFlightConnectorTest : public ArrowFlightConnectorTestBase {};

TEST_F(ArrowFlightConnectorTest, invalidSplit) {
  auto plan = ArrowFlightPlanBuilder()
                  .flightTableScan(velox::ROW({{"id", velox::BIGINT()}}))
                  .planNode();

  VELOX_ASSERT_THROW(
      velox::exec::test::AssertQueryBuilder(plan)
          .splits(makeSplits({"unknown"}))
          .copyResults(pool()),
      "table does not exist");
}

TEST_F(ArrowFlightConnectorTest, dataSourceCreation) {
  // missing columnHandle test
  auto plan =
      ArrowFlightPlanBuilder()
          .flightTableScan(
              velox::ROW({"id", "value"}, {velox::BIGINT(), velox::INTEGER()}),
              {{"id", std::make_shared<ArrowFlightColumnHandle>("id")}},
              false /*createDefaultColumnHandles*/)
          .planNode();

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan)
          .splits(makeSplits({"sample-data"}))
          .copyResults(pool()),
      "missing columnHandle for column 'value'");
}

TEST_F(ArrowFlightConnectorTest, dataSource) {
  std::vector<int64_t> idData = {1, 12, 2, std::numeric_limits<int64_t>::max()};
  std::vector<int32_t> valueData = {
      41, 42, 43, std::numeric_limits<int32_t>::min()};
  std::vector<uint64_t> unsignedData = {
      41, 42, 43, std::numeric_limits<uint64_t>::min()};

  updateTable(
      "sample-data",
      makeArrowTable(
          {"id", "value", "unsigned"},
          {makeNumericArray<arrow::Int64Type>(idData),
           makeNumericArray<arrow::Int32Type>(valueData),
           // note that velox doesn't support unsigned types
           // connector should still be able to query such tables
           // as long as this specific column isn't requested.
           makeNumericArray<arrow::UInt64Type>(unsignedData)}));

  auto idColumn = std::make_shared<ArrowFlightColumnHandle>("id");
  auto idVec = makeFlatVector<int64_t>(idData);

  auto valueColumn = std::make_shared<ArrowFlightColumnHandle>("value");
  auto valueVec = makeFlatVector<int32_t>(valueData);

  core::PlanNodePtr plan;

  // direct test
  plan = ArrowFlightPlanBuilder()
             .flightTableScan(velox::ROW(
                 {"id", "value"}, {velox::BIGINT(), velox::INTEGER()}))
             .planNode();

  AssertQueryBuilder(plan)
      .splits(makeSplits({"sample-data"}))
      .assertResults(makeRowVector({idVec, valueVec}));

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan)
          .splits(makeSplits({"sample-data"}, std::vector<flight::Location>{}))
          .assertResults(makeRowVector({idVec, valueVec})),
      "default host or port is missing");

  // column alias test
  plan =
      ArrowFlightPlanBuilder()
          .flightTableScan(
              velox::ROW({"ducks", "id"}, {velox::BIGINT(), velox::BIGINT()}),
              {{"ducks", idColumn}})
          .planNode();

  AssertQueryBuilder(plan)
      .splits(makeSplits({"sample-data"}))
      .assertResults(makeRowVector({idVec, idVec}));

  // invalid columnHandle test
  plan = ArrowFlightPlanBuilder()
             .flightTableScan(velox::ROW(
                 {"ducks", "value"}, {velox::BIGINT(), velox::INTEGER()}))
             .planNode();

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan)
          .splits(makeSplits({"sample-data"}))
          .copyResults(pool()),
      "column with name 'ducks' not found");
}

TEST_F(ArrowFlightConnectorTest, multipleBatches) {
  vector_size_t numValues = 100;
  int64_t batchSize = 30;
  std::string letters = "abcdefghijklmnopqrstuvwxyz";
  std::vector<int32_t> intData(numValues);
  std::vector<std::string> varcharData(numValues);
  std::vector<double> doubleData(numValues);

  for (vector_size_t i = 0; i < numValues; i++) {
    intData[i] = i;
    size_t pos = i % letters.size();
    size_t len = std::min<size_t>(i % 5, letters.size() - pos);
    varcharData[i] = letters.substr(pos, len);
    doubleData[i] = i * 1.1;
  }

  updateTable(
      "sample-data",
      makeArrowTable(
          {"int_col", "varchar_col", "double_col"},
          {makeNumericArray<arrow::Int32Type>(intData),
           makeStringArray(varcharData),
           makeNumericArray<arrow::DoubleType>(doubleData)}));
  setBatchSize(batchSize);

  core::PlanNodePtr plan;
  plan = ArrowFlightPlanBuilder()
             .flightTableScan(velox::ROW(
                 {"int_col", "varchar_col", "double_col"},
                 {velox::INTEGER(), velox::VARCHAR(), velox::DOUBLE()}))
             .planNode();

  auto intVec = makeFlatVector(intData);
  auto varcharVec = makeFlatVector(varcharData);
  auto doubleVec = makeFlatVector(doubleData);

  AssertQueryBuilder(plan)
      .splits(makeSplits({"sample-data"}))
      .assertResults(makeRowVector({intVec, varcharVec, doubleVec}));
}

TEST_F(ArrowFlightConnectorTest, multipleSplits) {
  vector_size_t numValues = 100;
  int64_t batchSize = 30;
  std::string letters = "abcdefghijklmnopqrstuvwxyz";
  std::vector<int32_t> intData(numValues);
  std::vector<std::string> varcharData(numValues);
  std::vector<double> doubleData(numValues);

  for (vector_size_t i = 0; i < numValues; i++) {
    intData[i] = i;
    size_t pos = i % letters.size();
    size_t len = std::min<size_t>(i % 5, letters.size() - pos);
    varcharData[i] = letters.substr(pos, len);
    doubleData[i] = i * 1.1;
  }

  updateTable(
      "sample-data-1",
      makeArrowTable(
          {"int_col", "varchar_col", "double_col"},
          {makeNumericArray<arrow::Int32Type>(intData),
           makeStringArray(varcharData),
           makeNumericArray<arrow::DoubleType>(doubleData)}));
  updateTable(
      "sample-data-2",
      makeArrowTable(
          {"int_col", "varchar_col", "double_col"},
          {makeNumericArray<arrow::Int32Type>(intData),
           makeStringArray(varcharData),
           makeNumericArray<arrow::DoubleType>(doubleData)}));
  setBatchSize(batchSize);

  core::PlanNodePtr plan;
  plan = ArrowFlightPlanBuilder()
             .flightTableScan(velox::ROW(
                 {"int_col", "varchar_col", "double_col"},
                 {velox::INTEGER(), velox::VARCHAR(), velox::DOUBLE()}))
             .planNode();

  std::vector<int32_t> intDataAll(intData);
  intDataAll.insert(intDataAll.begin(), intData.begin(), intData.end());
  std::vector<std::string> varcharDataAll(varcharData);
  varcharDataAll.insert(
      varcharDataAll.end(), varcharData.begin(), varcharData.end());
  std::vector<double> doubleDataAll(doubleData);
  doubleDataAll.insert(
      doubleDataAll.end(), doubleData.begin(), doubleData.end());
  auto intVec = makeFlatVector(intDataAll);
  auto varcharVec = makeFlatVector(varcharDataAll);
  auto doubleVec = makeFlatVector(doubleDataAll);

  AssertQueryBuilder(plan)
      .splits(makeSplits({"sample-data-1", "sample-data-2"}))
      .assertResults(makeRowVector({intVec, varcharVec, doubleVec}));
}

class ArrowFlightConnectorTestDefaultServer
    : public ArrowFlightConnectorTestBase {
 public:
  ArrowFlightConnectorTestDefaultServer()
      : ArrowFlightConnectorTestBase(
            std::make_shared<velox::config::ConfigBase>(
                std::unordered_map<std::string, std::string>{
                    {ArrowFlightConfig::kDefaultServerHost, kConnectHost},
                    {ArrowFlightConfig::kDefaultServerPort,
                     std::to_string(getFreePort())}})) {}
};

TEST_F(ArrowFlightConnectorTestDefaultServer, dataSource) {
  std::vector<int64_t> idData = {1, 12, 2, std::numeric_limits<int64_t>::max()};
  std::vector<int32_t> valueData = {
      41, 42, 43, std::numeric_limits<int32_t>::min()};

  updateTable(
      "sample-data",
      makeArrowTable(
          {"id", "value"},
          {makeNumericArray<arrow::Int64Type>(idData),
           makeNumericArray<arrow::Int32Type>(valueData)}));

  auto idColumn = std::make_shared<ArrowFlightColumnHandle>("id");
  auto idVec = makeFlatVector<int64_t>(idData);

  auto valueColumn = std::make_shared<ArrowFlightColumnHandle>("value");
  auto valueVec = makeFlatVector<int32_t>(valueData);

  core::PlanNodePtr plan;

  // direct test
  plan = ArrowFlightPlanBuilder()
             .flightTableScan(velox::ROW(
                 {"id", "value"}, {velox::BIGINT(), velox::INTEGER()}))
             .planNode();

  AssertQueryBuilder(plan)
      .splits(makeSplits({"sample-data"}))
      .assertResults(makeRowVector({idVec, valueVec}));

  AssertQueryBuilder(plan)
      .splits(makeSplits(
          {"sample-data"},
          std::vector<flight::Location>{})) // Using default connector
      .assertResults(makeRowVector({idVec, valueVec}));
}

} // namespace facebook::presto::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
