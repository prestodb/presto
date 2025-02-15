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

using namespace arrow::flight;
using namespace facebook::velox;
using namespace facebook::velox::exec::test;

namespace facebook::presto::test {

void ArrowFlightConnectorTestBase::SetUp() {
  OperatorTestBase::SetUp();

  if (!velox::connector::hasConnectorFactory(
          presto::ArrowFlightConnectorFactory::kArrowFlightConnectorName)) {
    velox::connector::registerConnectorFactory(
        std::make_shared<presto::ArrowFlightConnectorFactory>());
  }
  velox::connector::registerConnector(
      velox::connector::getConnectorFactory(
          ArrowFlightConnectorFactory::kArrowFlightConnectorName)
          ->newConnector(kFlightConnectorId, config_));
}

void ArrowFlightConnectorTestBase::TearDown() {
  velox::connector::unregisterConnector(kFlightConnectorId);
  OperatorTestBase::TearDown();
}

void FlightWithServerTestBase::SetUp() {
  ArrowFlightConnectorTestBase::SetUp();

  FlightServerOptions serverOptions(getServerLocation());
  server_ = std::make_unique<TestingArrowFlightServer>();
  setFlightServerOptions(&serverOptions);
  ASSERT_OK(server_->Init(serverOptions));
}

void FlightWithServerTestBase::TearDown() {
  ASSERT_OK(server_->Shutdown());
  ArrowFlightConnectorTestBase::TearDown();
}

Location FlightWithServerTestBase::getServerLocation() {
  AFC_ASSIGN_OR_RAISE(auto loc, Location::ForGrpcTcp(BIND_HOST, LISTEN_PORT));
  return loc;
}

std::vector<std::shared_ptr<velox::connector::ConnectorSplit>>
FlightWithServerTestBase::makeSplits(
    const std::initializer_list<std::string>& tickets,
    const std::vector<Location>& locations) {
  std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> splits;
  splits.reserve(tickets.size());
  for (auto& ticket : tickets) {
    FlightEndpoint flightEndpoint;
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
