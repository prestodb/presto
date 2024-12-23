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
#include "arrow/testing/gtest_util.h"
#include "folly/init/Init.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "presto_cpp/main/connectors/arrow_flight/ArrowFlightConnector.h"
#include "presto_cpp/main/connectors/arrow_flight/Macros.h"
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/FlightConnectorTestBase.h"
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/FlightPlanBuilder.h"
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/TestFlightServer.h"
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
using exec::test::PlanBuilder;

static const std::string kFlightConnectorId = "test-flight";

class FlightConnectorDataTypeTest : public FlightWithServerTestBase {};

TEST_F(FlightConnectorDataTypeTest, booleanType) {
  updateTable(
      "sample-data",
      makeArrowTable(
          {"bool_col"}, {makeBooleanArray({true, false, true, false})}));

  auto boolVec = makeFlatVector<bool>({true, false, true, false});

  core::PlanNodePtr plan;
  plan = FlightPlanBuilder()
             .flightTableScan(velox::ROW({"bool_col"}, {velox::BOOLEAN()}))
             .planNode();

  AssertQueryBuilder(plan)
      .splits(makeSplits({"sample-data"}))
      .assertResults(makeRowVector({boolVec}));
}

TEST_F(FlightConnectorDataTypeTest, integerTypes) {
  updateTable(
      "sample-data",
      makeArrowTable(
          {"tinyint_col", "smallint_col", "integer_col", "bigint_col"},
          {makeNumericArray<arrow::Int8Type>(
               {-128, 0, 127, std::numeric_limits<int8_t>::max()}),
           makeNumericArray<arrow::Int16Type>(
               {-32768, 0, 32767, std::numeric_limits<int16_t>::max()}),
           makeNumericArray<arrow::Int32Type>(
               {-2147483648,
                0,
                2147483647,
                std::numeric_limits<int32_t>::max()}),
           makeNumericArray<arrow::Int64Type>(
               {-3435678987654321234LL,
                0,
                4527897896541234567LL,
                std::numeric_limits<int64_t>::max()})}));

  auto tinyintVec = makeFlatVector<int8_t>(
      {-128, 0, 127, std::numeric_limits<int8_t>::max()});

  auto smallintVec = makeFlatVector<int16_t>(
      {-32768, 0, 32767, std::numeric_limits<int16_t>::max()});

  auto integerVec = makeFlatVector<int32_t>(
      {-2147483648, 0, 2147483647, std::numeric_limits<int32_t>::max()});

  auto bigintVec = makeFlatVector<int64_t>(
      {-3435678987654321234LL,
       0,
       4527897896541234567LL,
       std::numeric_limits<int64_t>::max()});

  core::PlanNodePtr plan;
  plan = FlightPlanBuilder()
             .flightTableScan(velox::ROW(
                 {"tinyint_col", "smallint_col", "integer_col", "bigint_col"},
                 {velox::TINYINT(),
                  velox::SMALLINT(),
                  velox::INTEGER(),
                  velox::BIGINT()}))
             .planNode();

  AssertQueryBuilder(plan)
      .splits(makeSplits({"sample-data"}))
      .assertResults(
          makeRowVector({tinyintVec, smallintVec, integerVec, bigintVec}));
}

TEST_F(FlightConnectorDataTypeTest, realType) {
  updateTable(
      "sample-data",
      makeArrowTable(
          {"real_col", "double_col"},
          {makeNumericArray<arrow::FloatType>(
               {std::numeric_limits<float>::min(),
                0.0f,
                3.14f,
                std::numeric_limits<float>::max()}),
           makeNumericArray<arrow::DoubleType>(
               {std::numeric_limits<double>::min(),
                0.0,
                3.14159,
                std::numeric_limits<double>::max()})}));

  auto realVec = makeFlatVector<float>(
      {std::numeric_limits<float>::min(),
       0.0f,
       3.14f,
       std::numeric_limits<float>::max()});
  auto doubleVec = makeFlatVector<double>(
      {std::numeric_limits<double>::min(),
       0.0,
       3.14159,
       std::numeric_limits<double>::max()});

  core::PlanNodePtr plan;
  plan = FlightPlanBuilder()
             .flightTableScan(velox::ROW(
                 {"real_col", "double_col"}, {velox::REAL(), velox::DOUBLE()}))
             .planNode();

  AssertQueryBuilder(plan)
      .splits(makeSplits({"sample-data"}))
      .assertResults(makeRowVector({realVec, doubleVec}));
}

TEST_F(FlightConnectorDataTypeTest, varcharType) {
  updateTable(
      "sample-data",
      makeArrowTable(
          {"varchar_col"}, {makeStringArray({"Hello", "World", "India"})}));

  auto vec = makeFlatVector<facebook::velox::StringView>(
      {facebook::velox::StringView("Hello"),
       facebook::velox::StringView("World"),
       facebook::velox::StringView("India")});

  core::PlanNodePtr plan;
  plan = FlightPlanBuilder()
             .flightTableScan(velox::ROW({"varchar_col"}, {velox::VARCHAR()}))
             .planNode();

  AssertQueryBuilder(plan)
      .splits(makeSplits({"sample-data"}))
      .assertResults(makeRowVector({vec}));
}

TEST_F(FlightConnectorDataTypeTest, timestampType) {
  auto timestampValues =
      std::vector<int64_t>{1622538000, 1622541600, 1622545200};

  updateTable(
      "sample-data",
      makeArrowTable(
          {"timestampsec_col", "timestampmilli_col", "timestampmicro_col"},
          {makeTimestampArray(timestampValues, arrow::TimeUnit::SECOND),
           makeTimestampArray(timestampValues, arrow::TimeUnit::MILLI),
           makeTimestampArray(timestampValues, arrow::TimeUnit::MICRO)}));

  std::vector<facebook::velox::Timestamp> veloxTimestampSec;
  for (const auto& ts : timestampValues) {
    veloxTimestampSec.emplace_back(ts, 0); // Assuming 0 microseconds part
  }

  auto timestampSecCol =
      makeFlatVector<facebook::velox::Timestamp>(veloxTimestampSec);

  std::vector<facebook::velox::Timestamp> veloxTimestampMilli;
  for (const auto& ts : timestampValues) {
    veloxTimestampMilli.emplace_back(
        ts / 1000, (ts % 1000) * 1000000); // Convert to seconds and nanoseconds
  }

  auto timestampMilliCol =
      makeFlatVector<facebook::velox::Timestamp>(veloxTimestampMilli);

  std::vector<facebook::velox::Timestamp> veloxTimestampMicro;
  for (const auto& ts : timestampValues) {
    veloxTimestampMicro.emplace_back(
        ts / 1000000,
        (ts % 1000000) * 1000); // Convert to seconds and nanoseconds
  }

  auto timestampMicroCol =
      makeFlatVector<facebook::velox::Timestamp>(veloxTimestampMicro);

  core::PlanNodePtr plan;
  plan =
      FlightPlanBuilder()
          .flightTableScan(velox::ROW(
              {"timestampsec_col", "timestampmilli_col", "timestampmicro_col"},
              {velox::TIMESTAMP(), velox::TIMESTAMP(), velox::TIMESTAMP()}))
          .planNode();

  AssertQueryBuilder(plan)
      .splits(makeSplits({"sample-data"}))
      .assertResults(makeRowVector(
          {timestampSecCol, timestampMilliCol, timestampMicroCol}));
}

TEST_F(FlightConnectorDataTypeTest, dateDayType) {
  std::vector<int32_t> datesDay = {18748, 18749, 18750}; // Days since epoch
  std::vector<int64_t> datesMilli = {
      1622538000000, 1622541600000, 1622545200000}; // Milliseconds since epoch

  updateTable(
      "sample-data",
      makeArrowTable(
          {"daydate_col", "daymilli_col"},
          {makeNumericArray<arrow::Date32Type>(datesDay),
           makeNumericArray<arrow::Date64Type>(datesMilli)}));

  auto dateVec = makeFlatVector<int32_t>(datesDay);
  auto milliVec = makeFlatVector<int64_t>(datesMilli);

  core::PlanNodePtr plan;
  plan = FlightPlanBuilder()
             .flightTableScan(velox::ROW({"daydate_col"}, {velox::DATE()}))
             .planNode();

  AssertQueryBuilder(plan)
      .splits(makeSplits({"sample-data"}))
      .assertResults(makeRowVector({dateVec}));

  plan = FlightPlanBuilder()
             .flightTableScan(velox::ROW({"daymilli_col"}, {velox::DATE()}))
             .planNode();

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan)
          .splits(makeSplits({"sample-data"}))
          .assertResults(makeRowVector({milliVec})),
      "Unable to convert 'tdm' ArrowSchema format type to Velox");
}

TEST_F(FlightConnectorDataTypeTest, decimalType) {
  std::vector<int64_t> decimalValuesBigInt = {
      123456789012345678,
      -123456789012345678,
      std::numeric_limits<int64_t>::max()};
  std::vector<std::shared_ptr<arrow::Array>> decimalArrayVec;
  decimalArrayVec.push_back(makeDecimalArray(decimalValuesBigInt, 18, 2));
  updateTable(
      "sample-data", makeArrowTable({"decimal_col_bigint"}, decimalArrayVec));
  auto decimalVecBigInt = makeFlatVector<int64_t>(decimalValuesBigInt);

  core::PlanNodePtr plan;
  plan = FlightPlanBuilder()
             .flightTableScan(velox::ROW(
                 {"decimal_col_bigint"},
                 {velox::DECIMAL(18, 2)})) // precision can't be 0 and < scale
             .planNode();

  // Execute the query and assert the results
  AssertQueryBuilder(plan)
      .splits(makeSplits({"sample-data"}))
      .assertResults(makeRowVector({decimalVecBigInt}));
}

TEST_F(FlightConnectorDataTypeTest, allTypes) {
  auto timestampValues =
      std::vector<int64_t>{1622550000, 1622553600, 1622557200};

  auto sampleTable = makeArrowTable(
      {"id",
       "daydate_col",
       "timestamp_col",
       "varchar_col",
       "real_col",
       "int_col",
       "bool_col"},
      {makeNumericArray<arrow::UInt32Type>({1, 2, 3}),
       makeNumericArray<arrow::Date32Type>({18748, 18749, 18750}),
       makeTimestampArray(timestampValues, arrow::TimeUnit::SECOND),
       makeStringArray({"apple", "banana", "cherry"}),
       makeNumericArray<arrow::DoubleType>({3.14, 2.718, 1.618}),
       makeNumericArray<arrow::Int32Type>(
           {-32768, 32767, std::numeric_limits<int32_t>::max()}),
       makeBooleanArray({true, false, true})});

  updateTable("gen-data", sampleTable);

  auto dateVec = makeFlatVector<int32_t>({18748, 18749, 18750});

  std::vector<facebook::velox::Timestamp> veloxTimestampSec;
  for (const auto& ts : timestampValues) {
    veloxTimestampSec.emplace_back(ts, 0); // Assuming 0 microseconds part
  }
  auto timestampSecVec =
      makeFlatVector<facebook::velox::Timestamp>(veloxTimestampSec);

  auto stringVec = makeFlatVector<facebook::velox::StringView>(
      {facebook::velox::StringView("apple"),
       facebook::velox::StringView("banana"),
       facebook::velox::StringView("cherry")});
  auto realVec = makeFlatVector<double>({3.14, 2.718, 1.618});
  auto intVec = makeFlatVector<int32_t>(
      {-32768, 32767, std::numeric_limits<int32_t>::max()});
  auto boolVec = makeFlatVector<bool>({true, false, true});

  core::PlanNodePtr plan;
  plan = FlightPlanBuilder()
             .flightTableScan(velox::ROW(
                 {"daydate_col",
                  "timestamp_col",
                  "varchar_col",
                  "real_col",
                  "int_col",
                  "bool_col"},
                 {velox::DATE(),
                  velox::TIMESTAMP(),
                  velox::VARCHAR(),
                  velox::DOUBLE(),
                  velox::INTEGER(),
                  velox::BOOLEAN()}))
             .planNode();

  AssertQueryBuilder(plan)
      .splits(makeSplits({"gen-data"}))
      .assertResults(makeRowVector(
          {dateVec, timestampSecVec, stringVec, realVec, intVec, boolVec}));
}

} // namespace

} // namespace facebook::presto::connector::arrow_flight::test
