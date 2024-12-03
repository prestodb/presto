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
#include "arrow/testing/gtest_util.h"
#include "folly/init/Init.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "presto_cpp/main/connectors/arrow_flight/Macros.h"
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/FlightConnectorTestBase.h"
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/FlightPlanBuilder.h"
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/StaticFlightServer.h"
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/Utils.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/config/Config.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"

namespace facebook::presto::connector::arrow_flight::test {

namespace {

namespace velox = facebook::velox;
namespace core = facebook::velox::core;
namespace exec = facebook::velox::exec;
namespace connector = facebook::velox::connector;
namespace flight = arrow::flight;

using namespace facebook::presto::connector::arrow_flight;
using namespace facebook::presto::connector::arrow_flight::test;
using exec::test::AssertQueryBuilder;
using exec::test::OperatorTestBase;

static const std::string kFlightConnectorId = "test-flight";

class FlightConnectorTest : public FlightWithServerTestBase {};

TEST_F(FlightConnectorTest, invalidSplitTest) {
  auto plan = FlightPlanBuilder()
                  .flightTableScan(velox::ROW({{"id", velox::BIGINT()}}))
                  .planNode();

  VELOX_ASSERT_THROW(
      velox::exec::test::AssertQueryBuilder(plan)
          .splits(makeSplits({"unknown"}))
          .copyResults(pool()),
      "Server replied with error");
}

TEST_F(FlightConnectorTest, dataSourceCreationTest) {
  // missing columnHandle test
  auto plan =
      FlightPlanBuilder()
          .flightTableScan(
              velox::ROW({"id", "value"}, {velox::BIGINT(), velox::INTEGER()}),
              {{"id", std::make_shared<FlightColumnHandle>("id")}},
              false /*createDefaultColumnHandles*/)
          .planNode();

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan)
          .splits(makeSplits({"sample-data"}))
          .copyResults(pool()),
      "missing columnHandle for column 'value'");
}

TEST_F(FlightConnectorTest, dataSourceTest) {
  updateTable(
      "sample-data",
      makeArrowTable(
          {"id", "value", "unsigned"},
          {makeNumericArray<arrow::Int64Type>(
               {1, 12, 2, std::numeric_limits<int64_t>::max()}),
           makeNumericArray<arrow::Int32Type>(
               {41, 42, 43, std::numeric_limits<int32_t>::min()}),
           // note that velox doesn't support unsigned types
           // connector should still be able to query such tables
           // as long as this specifc column isn't requested.
           makeNumericArray<arrow::UInt64Type>(
               {101, 102, 12, std::numeric_limits<uint64_t>::max()})}));

  auto idColumn = std::make_shared<FlightColumnHandle>("id");
  auto idVec =
      makeFlatVector<int64_t>({1, 12, 2, std::numeric_limits<int64_t>::max()});

  auto valueColumn = std::make_shared<FlightColumnHandle>("value");
  auto valueVec = makeFlatVector<int32_t>(
      {41, 42, 43, std::numeric_limits<int32_t>::min()});

  core::PlanNodePtr plan;

  // direct test
  plan = FlightPlanBuilder()
             .flightTableScan(velox::ROW(
                 {"id", "value"}, {velox::BIGINT(), velox::INTEGER()}))
             .planNode();

  AssertQueryBuilder(plan)
      .splits(makeSplits({"sample-data"}))
      .assertResults(makeRowVector({idVec, valueVec}));

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan)
          .splits(makeSplits({"sample-data"}, std::vector<std::string>{""}))
          .assertResults(makeRowVector({idVec, valueVec})),
      "URI has empty scheme");

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan)
          .splits(makeSplits({"sample-data"}, std::vector<std::string>{}))
          .assertResults(makeRowVector({idVec, valueVec})),
      "Server Hostname not given");

  // column alias test
  plan =
      FlightPlanBuilder()
          .flightTableScan(
              velox::ROW({"ducks", "id"}, {velox::BIGINT(), velox::BIGINT()}),
              {{"ducks", idColumn}})
          .planNode();

  AssertQueryBuilder(plan)
      .splits(makeSplits({"sample-data"}))
      .assertResults(makeRowVector({idVec, idVec}));

  // invalid columnHandle test
  plan = FlightPlanBuilder()
             .flightTableScan(velox::ROW(
                 {"ducks", "value"}, {velox::BIGINT(), velox::INTEGER()}))
             .planNode();

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan)
          .splits(makeSplits({"sample-data"}))
          .copyResults(pool()),
      "column with name 'ducks' not found");
}

class FlightConnectorTestDefaultServer : public FlightWithServerTestBase {
 public:
  FlightConnectorTestDefaultServer()
      : FlightWithServerTestBase(std::make_shared<velox::config::ConfigBase>(
            std::unordered_map<std::string, std::string>{
                {FlightConfig::kDefaultServerHost, CONNECT_HOST},
                {FlightConfig::kDefaultServerPort,
                 std::to_string(LISTEN_PORT)}})) {}
};

TEST_F(FlightConnectorTestDefaultServer, dataSourceTest) {
  updateTable(
      "sample-data",
      makeArrowTable(
          {"id", "value"},
          {makeNumericArray<arrow::Int64Type>(
               {1, 12, 2, std::numeric_limits<int64_t>::max()}),
           makeNumericArray<arrow::Int32Type>(
               {41, 42, 43, std::numeric_limits<int32_t>::min()})}));

  auto idColumn = std::make_shared<FlightColumnHandle>("id");
  auto idVec =
      makeFlatVector<int64_t>({1, 12, 2, std::numeric_limits<int64_t>::max()});

  auto valueColumn = std::make_shared<FlightColumnHandle>("value");
  auto valueVec = makeFlatVector<int32_t>(
      {41, 42, 43, std::numeric_limits<int32_t>::min()});

  core::PlanNodePtr plan;

  // direct test
  plan = FlightPlanBuilder()
             .flightTableScan(velox::ROW(
                 {"id", "value"}, {velox::BIGINT(), velox::INTEGER()}))
             .planNode();

  AssertQueryBuilder(plan)
      .splits(makeSplits({"sample-data"}))
      .assertResults(makeRowVector({idVec, valueVec}));

  AssertQueryBuilder(plan)
      .splits(makeSplits(
          {"sample-data"},
          std::vector<std::string>{})) // Using default connector
      .assertResults(makeRowVector({idVec, valueVec}));
}

} // namespace

} // namespace facebook::presto::connector::arrow_flight::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
