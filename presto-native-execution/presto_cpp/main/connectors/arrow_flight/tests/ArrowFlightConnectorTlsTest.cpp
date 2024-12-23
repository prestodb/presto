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
#include "arrow/flight/types.h"
#include "arrow/testing/gtest_util.h"
#include "folly/init/Init.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "presto_cpp/main/connectors/arrow_flight/ArrowFlightConnector.h"
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/FlightConnectorTestBase.h"
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/FlightPlanBuilder.h"
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

class FlightConnectorTlsTestBase : public FlightWithServerTestBase {
 protected:
  explicit FlightConnectorTlsTestBase(
      std::shared_ptr<velox::config::ConfigBase> config)
      : FlightWithServerTestBase(
            std::move(config),
            createFlightServerOptions(
                true, /* isSecure */
                "./data/tls_certs/server.crt",
                "./data/tls_certs/server.key")) {}

  void executeTest(bool isPositiveTest = true) {
    updateTable(
        "sample-data",
        makeArrowTable(
            {"id"},
            {makeNumericArray<arrow::Int64Type>(
                {1, 12, 2, std::numeric_limits<int64_t>::max()})}));

    auto idVec = makeFlatVector<int64_t>(
        {1, 12, 2, std::numeric_limits<int64_t>::max()});

    auto plan = FlightPlanBuilder()
                    .flightTableScan(velox::ROW({"id"}, {velox::BIGINT()}))
                    .planNode();

    auto loc = std::vector<std::string>{
        fmt::format("grpc+tls://{}:{}", CONNECT_HOST, LISTEN_PORT)};
    if (isPositiveTest) {
      AssertQueryBuilder(plan)
          .splits(makeSplits({"sample-data"}, loc))
          .assertResults(makeRowVector({idVec}));
    } else {
      VELOX_ASSERT_THROW(
          AssertQueryBuilder(plan)
              .splits(makeSplits({"sample-data"}, loc))
              .assertResults(makeRowVector({idVec})),
          "Server replied with error");
    }
  }
};

class FlightConnectorTlsTest : public FlightConnectorTlsTestBase {
 protected:
  explicit FlightConnectorTlsTest()
      : FlightConnectorTlsTestBase(std::make_shared<velox::config::ConfigBase>(
            std::unordered_map<std::string, std::string>{
                {FlightConfig::kServerVerify, "true"},
                {FlightConfig::kServerSslCertificate,
                 "./data/tls_certs/ca.crt"}})) {}
};

TEST_F(FlightConnectorTlsTest, tlsTest) {
  executeTest();
}

class FlightConnectorTlsNoCertValidationTest
    : public FlightConnectorTlsTestBase {
 protected:
  explicit FlightConnectorTlsNoCertValidationTest()
      : FlightConnectorTlsTestBase(std::make_shared<velox::config::ConfigBase>(
            std::unordered_map<std::string, std::string>{
                {FlightConfig::kServerVerify, "false"}})) {}
};

TEST_F(FlightConnectorTlsNoCertValidationTest, tlsNoCertValidationTest) {
  executeTest();
}

class FlightConnectorTlsNoCertTest : public FlightConnectorTlsTestBase {
 protected:
  FlightConnectorTlsNoCertTest()
      : FlightConnectorTlsTestBase(std::make_shared<velox::config::ConfigBase>(
            std::unordered_map<std::string, std::string>{
                {FlightConfig::kServerVerify, "true"}})) {}
};

TEST_F(FlightConnectorTlsNoCertTest, tlsNoCertTest) {
  executeTest(false /* isPositiveTest */);
}

} // namespace

} // namespace facebook::presto::connector::arrow_flight::test
