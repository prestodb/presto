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
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/FlightConnectorTestBase.h"
#include "arrow/testing/gtest_util.h"
#include "presto_cpp/main/connectors/arrow_flight/ArrowFlightConnector.h"
#include "presto_cpp/main/connectors/arrow_flight/Macros.h"
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/Utils.h"

namespace facebook::presto::connector::arrow_flight::test {

namespace connector = velox::connector;
namespace core = velox::core;

using namespace arrow::flight;
using velox::exec::test::OperatorTestBase;

void FlightConnectorTestBase::SetUp() {
  OperatorTestBase::SetUp();

  if (!velox::connector::hasConnectorFactory(
          presto::connector::arrow_flight::ArrowFlightConnectorFactory::
              kArrowFlightConnectorName)) {
    connector::registerConnectorFactory(
        std::make_shared<
            presto::connector::arrow_flight::ArrowFlightConnectorFactory>());
  }
  connector::registerConnector(
      connector::getConnectorFactory(
          ArrowFlightConnectorFactory::kArrowFlightConnectorName)
          ->newConnector(kFlightConnectorId, config_));
}

void FlightConnectorTestBase::TearDown() {
  connector::unregisterConnector(kFlightConnectorId);
  OperatorTestBase::TearDown();
}

void FlightWithServerTestBase::SetUp() {
  FlightConnectorTestBase::SetUp();

  server_ = std::make_unique<StaticFlightServer>();
  ASSERT_OK(server_->Init(*options_));
}

void FlightWithServerTestBase::TearDown() {
  ASSERT_OK(server_->Shutdown());
  FlightConnectorTestBase::TearDown();
}

std::vector<std::shared_ptr<connector::ConnectorSplit>>
FlightWithServerTestBase::makeSplits(
    const std::initializer_list<std::string>& tickets,
    const std::vector<std::string>& location) {
  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
  splits.reserve(tickets.size());
  for (auto& ticket : tickets) {
    splits.push_back(
        std::make_shared<FlightSplit>(kFlightConnectorId, ticket, location));
  }
  return splits;
}

std::shared_ptr<FlightServerOptions>
FlightWithServerTestBase::createFlightServerOptions(
    bool isSecure,
    const std::string& certPath,
    const std::string& keyPath) {
  AFC_ASSIGN_OR_RAISE(
      auto loc,
      isSecure ? Location::ForGrpcTls(BIND_HOST, LISTEN_PORT)
               : Location::ForGrpcTcp(BIND_HOST, LISTEN_PORT));
  auto options = std::make_shared<FlightServerOptions>(loc);
  if (!isSecure)
    return options;

  CertKeyPair tlsCertificate{
      .pem_cert = readFile(certPath), .pem_key = readFile(keyPath)};
  options->tls_certificates.push_back(tlsCertificate);
  return options;
}

} // namespace facebook::presto::connector::arrow_flight::test
