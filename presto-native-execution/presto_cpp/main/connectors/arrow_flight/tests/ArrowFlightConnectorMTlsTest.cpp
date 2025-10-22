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

class ArrowFlightConnectorMtlsTestBase : public ArrowFlightConnectorTestBase {
 protected:
  explicit ArrowFlightConnectorMtlsTestBase(
      std::shared_ptr<velox::config::ConfigBase> config)
      : ArrowFlightConnectorTestBase(std::move(config)) {}

  void setFlightServerOptions(
      flight::FlightServerOptions* serverOptions) override {
    flight::CertKeyPair tlsCertificate{
        .pem_cert = readFile("./data/certs/server.crt"),
        .pem_key = readFile("./data/certs/server.key")};
    serverOptions->tls_certificates.push_back(tlsCertificate);
    serverOptions->verify_client = true;
    serverOptions->root_certificates = readFile("./data/certs/ca.crt");
  }

  void executeSuccessfulQuery() {
    std::vector<int64_t> idData = {
        1, 12, 2, std::numeric_limits<int64_t>::max()};

    updateTable(
        "sample-data",
        makeArrowTable({"id"}, {makeNumericArray<arrow::Int64Type>(idData)}));

    auto idVec = makeFlatVector<int64_t>(idData);

    auto plan = ArrowFlightPlanBuilder()
                    .flightTableScan(ROW({"id"}, {BIGINT()}))
                    .planNode();

    AssertQueryBuilder(plan)
        .splits(makeSplits({"sample-data"}))
        .assertResults(makeRowVector({idVec}));
  }

  std::function<void()> createQueryFunction() {
    std::vector<int64_t> idData = {
        1, 12, 2, std::numeric_limits<int64_t>::max()};

    updateTable(
        "sample-data",
        makeArrowTable({"id"}, {makeNumericArray<arrow::Int64Type>(idData)}));

    auto idVec = makeFlatVector<int64_t>(idData);

    auto plan = ArrowFlightPlanBuilder()
                    .flightTableScan(ROW({"id"}, {BIGINT()}))
                    .planNode();

    return [this, plan, idVec]() {
      AssertQueryBuilder(plan)
          .splits(makeSplits({"sample-data"}))
          .assertResults(makeRowVector({idVec}));
    };
  }
};

class ArrowFlightConnectorMtlsTest : public ArrowFlightConnectorMtlsTestBase {
 protected:
  explicit ArrowFlightConnectorMtlsTest()
      : ArrowFlightConnectorMtlsTestBase(
            std::make_shared<velox::config::ConfigBase>(
                std::unordered_map<std::string, std::string>{
                    {ArrowFlightConfig::kDefaultServerSslEnabled, "true"},
                    {ArrowFlightConfig::kServerVerify, "true"},
                    {ArrowFlightConfig::kServerSslCertificate,
                         "./data/certs/ca.crt"},
                    {ArrowFlightConfig::kClientSslCertificate, "./data/certs/client.crt"},
                    {ArrowFlightConfig::kClientSslKey, "./data/certs/client.key"}})) {}
};

TEST_F(ArrowFlightConnectorMtlsTest, successfulMtlsConnection) {
  executeSuccessfulQuery();
}

class ArrowFlightMtlsNoClientCertTest : public ArrowFlightConnectorMtlsTestBase {
 protected:
  ArrowFlightMtlsNoClientCertTest()
      : ArrowFlightConnectorMtlsTestBase(
            std::make_shared<velox::config::ConfigBase>(
                std::unordered_map<std::string, std::string>{
                    {ArrowFlightConfig::kDefaultServerSslEnabled, "true"},
                    {ArrowFlightConfig::kServerVerify, "true"},
                    {ArrowFlightConfig::kServerSslCertificate, "./data/certs/ca.crt"}})) {}
};

TEST_F(ArrowFlightMtlsNoClientCertTest, mtlsFailsWithoutClientCert) {
  auto queryFunction = createQueryFunction();
  VELOX_ASSERT_THROW(queryFunction(), "failed to connect");
}

class ArrowFlightConnectorImplicitSslTest : public ArrowFlightConnectorMtlsTestBase {
 protected:
  ArrowFlightConnectorImplicitSslTest()
      : ArrowFlightConnectorMtlsTestBase(
            std::make_shared<velox::config::ConfigBase>(
                std::unordered_map<std::string, std::string>{
                    {ArrowFlightConfig::kServerVerify, "true"},
                    {ArrowFlightConfig::kServerSslCertificate,
                         "./data/certs/ca.crt"},
                    {ArrowFlightConfig::kClientSslCertificate, "./data/certs/client.crt"},
                    {ArrowFlightConfig::kClientSslKey, "./data/certs/client.key"}})) {}
};

TEST_F(ArrowFlightConnectorImplicitSslTest, successfulImplicitSslConnection) {
  executeSuccessfulQuery();
}

class ArrowFlightImplicitSslNoClientCertTest : public ArrowFlightConnectorMtlsTestBase {
 protected:
  ArrowFlightImplicitSslNoClientCertTest()
      : ArrowFlightConnectorMtlsTestBase(
            std::make_shared<velox::config::ConfigBase>(
                std::unordered_map<std::string, std::string>{
                    {ArrowFlightConfig::kDefaultServerSslEnabled, "true"},
                    {ArrowFlightConfig::kServerVerify, "true"},
                    {ArrowFlightConfig::kServerSslCertificate, "./data/certs/ca.crt"}
                    })) {}
};

TEST_F(ArrowFlightImplicitSslNoClientCertTest, mtlsFailsWithoutClientCertOnImplicitSsl) {
  auto queryFunction = createQueryFunction();
  VELOX_ASSERT_THROW(queryFunction(), "failed to connect");
}

} // namespace facebook::presto::test
