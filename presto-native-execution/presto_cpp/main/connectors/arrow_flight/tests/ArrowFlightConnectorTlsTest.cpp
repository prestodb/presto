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
#include <arrow/flight/types.h>
#include <arrow/testing/gtest_util.h>
#include <folly/init/Init.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include "presto_cpp/main/connectors/arrow_flight/ArrowFlightConnector.h"
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/ArrowFlightConnectorTestBase.h"
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/ArrowFlightPlanBuilder.h"
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/Utils.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/config/Config.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"

using namespace arrow;
using namespace facebook::velox;
using namespace facebook::velox::exec::test;

namespace facebook::presto::test {

class ArrowFlightConnectorTlsTestBase : public ArrowFlightConnectorTestBase {
 protected:
  explicit ArrowFlightConnectorTlsTestBase(
      std::shared_ptr<velox::config::ConfigBase> config)
      : ArrowFlightConnectorTestBase(std::move(config)) {}

  void setFlightServerOptions(
      flight::FlightServerOptions* serverOptions) override {
    flight::CertKeyPair tlsCertificate{
        .pem_cert = readFile("./data/certs/server.crt"),
        .pem_key = readFile("./data/certs/server.key")};
    serverOptions->tls_certificates.push_back(tlsCertificate);
  }

  void executeTest(
      bool isPositiveTest = true,
      const std::string& expectedError = "") {
    std::vector<int64_t> idData = {
        1, 12, 2, std::numeric_limits<int64_t>::max()};

    updateTable(
        "sample-data",
        makeArrowTable({"id"}, {makeNumericArray<arrow::Int64Type>(idData)}));

    auto idVec = makeFlatVector<int64_t>(idData);

    auto plan = ArrowFlightPlanBuilder()
                    .flightTableScan(velox::ROW({"id"}, {velox::BIGINT()}))
                    .planNode();

    auto runQuery = [&]() {
      AssertQueryBuilder(plan)
          .splits(makeSplits({"sample-data"}))
          .assertResults(makeRowVector({idVec}));
    };

    if (isPositiveTest) {
      runQuery();
    } else {
      VELOX_ASSERT_THROW(runQuery(), expectedError);
    }
  }
};

class ArrowFlightConnectorTlsTest : public ArrowFlightConnectorTlsTestBase {
 protected:
  explicit ArrowFlightConnectorTlsTest()
      : ArrowFlightConnectorTlsTestBase(
            std::make_shared<velox::config::ConfigBase>(
                std::unordered_map<std::string, std::string>{
                    {ArrowFlightConfig::kDefaultServerSslEnabled, "true"},
                    {ArrowFlightConfig::kServerVerify, "true"},
                    {ArrowFlightConfig::kServerSslCertificate,
                     "./data/certs/ca.crt"}})) {}
};

TEST_F(ArrowFlightConnectorTlsTest, tlsEnabled) {
  executeTest();
}

class ArrowFlightTlsNoCertValidationTest
    : public ArrowFlightConnectorTlsTestBase {
 protected:
  explicit ArrowFlightTlsNoCertValidationTest()
      : ArrowFlightConnectorTlsTestBase(
            std::make_shared<velox::config::ConfigBase>(
                std::unordered_map<std::string, std::string>{
                    {ArrowFlightConfig::kDefaultServerSslEnabled, "true"},
                    {ArrowFlightConfig::kServerVerify, "false"}})) {}
};

TEST_F(ArrowFlightTlsNoCertValidationTest, tlsNoCertValidation) {
  executeTest();
}

class ArrowFlightTlsNoCertTest : public ArrowFlightConnectorTlsTestBase {
 protected:
  ArrowFlightTlsNoCertTest()
      : ArrowFlightConnectorTlsTestBase(
            std::make_shared<velox::config::ConfigBase>(
                std::unordered_map<std::string, std::string>{
                    {ArrowFlightConfig::kDefaultServerSslEnabled, "true"},
                    {ArrowFlightConfig::kServerVerify, "true"}})) {}
};

TEST_F(ArrowFlightTlsNoCertTest, tlsNoCert) {
  executeTest(false, "failed to connect");
}

} // namespace facebook::presto::test
