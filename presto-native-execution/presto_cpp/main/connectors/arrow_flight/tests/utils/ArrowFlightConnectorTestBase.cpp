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
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/ArrowFlightConnectorTestBase.h"
#include <arrow/testing/gtest_util.h>
#include <folly/base64.h>
#include "presto_cpp/main/connectors/arrow_flight/ArrowFlightConnector.h"
#include "presto_cpp/main/connectors/arrow_flight/Macros.h"
#include "velox/exec/tests/utils/PortUtil.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

namespace facebook::presto::test {

void ArrowFlightConnectorTestBase::SetUp() {
  OperatorTestBase::SetUp();
  presto::ArrowFlightConnectorFactory factory;
  velox::connector::registerConnector(
      factory.newConnector(kFlightConnectorId, config_));

  ArrowFlightConfig config(config_);
  if (config.defaultServerPort().has_value()) {
    port_ = config.defaultServerPort().value();
  } else {
    port_ = getFreePort();
  }
  AFC_ASSIGN_OR_RAISE(
      auto serverLocation,
      config.defaultServerSslEnabled()
          ? arrow::flight::Location::ForGrpcTls(kBindHost, port_)
          : arrow::flight::Location::ForGrpcTcp(kBindHost, port_));

  arrow::flight::FlightServerOptions serverOptions(serverLocation);
  server_ = std::make_unique<TestingArrowFlightServer>();
  setFlightServerOptions(&serverOptions);
  ASSERT_OK(server_->Init(serverOptions));
}

void ArrowFlightConnectorTestBase::TearDown() {
  ASSERT_OK(server_->Shutdown());
  velox::connector::unregisterConnector(kFlightConnectorId);
  OperatorTestBase::TearDown();
}

std::vector<std::shared_ptr<velox::connector::ConnectorSplit>>
ArrowFlightConnectorTestBase::makeSplits(
    const std::initializer_list<std::string>& tickets) {
  ArrowFlightConfig config(config_);
  AFC_ASSIGN_OR_RAISE(
      auto loc,
      config.defaultServerSslEnabled()
          ? arrow::flight::Location::ForGrpcTls(kConnectHost, port_)
          : arrow::flight::Location::ForGrpcTcp(kConnectHost, port_));
  return makeSplits(tickets, {loc});
}

std::vector<std::shared_ptr<velox::connector::ConnectorSplit>>
ArrowFlightConnectorTestBase::makeSplits(
    const std::initializer_list<std::string>& tickets,
    const std::vector<arrow::flight::Location>& locations) {
  std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> splits;
  splits.reserve(tickets.size());
  for (auto& ticket : tickets) {
    arrow::flight::FlightEndpoint flightEndpoint;
    flightEndpoint.ticket.ticket = ticket;
    flightEndpoint.locations = locations;
    AFC_ASSIGN_OR_RAISE(
        auto flightEndpointStr, flightEndpoint.SerializeToString());
    auto flightEndpointBytes = folly::base64Encode(flightEndpointStr);
    splits.push_back(std::make_shared<ArrowFlightSplit>(
        kFlightConnectorId, flightEndpointBytes));
  }
  return splits;
}

} // namespace facebook::presto::test
