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
#include "presto_cpp/main/connectors/arrow_flight/arrow_federation/ArrowFederationConnector.h"
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/flight/api.h>
#include <folly/base64.h>
#include <utility>
#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "presto_cpp/main/common/ConfigReader.h"
#include "presto_cpp/main/connectors/arrow_flight/Macros.h"
#include "velox/vector/arrow/Bridge.h"

using namespace facebook::velox::connector;

namespace facebook::presto {
ArrowFederationDataSource::ArrowFederationDataSource(
    const velox::RowTypePtr& outputType,
    const velox::connector::ColumnHandleMap& columnHandles,
    std::shared_ptr<Authenticator> authenticator,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<ArrowFlightConfig>& flightConfig,
    const std::shared_ptr<arrow::flight::FlightClientOptions>& clientOpts)
    : ArrowFlightDataSource(
          outputType,
          columnHandles,
          authenticator,
          connectorQueryCtx,
          flightConfig,
          clientOpts) {}

void ArrowFederationDataSource::addSplit(
    std::shared_ptr<ConnectorSplit> split) {
  auto federationSplit = std::dynamic_pointer_cast<ArrowFederationSplit>(split);
  VELOX_CHECK(
      federationSplit,
      "ArrowFederationDataSource received wrong type of split");

  nlohmann::json request;
  request["split"] = federationSplit->splitBytes_;
  request["columns"] = columnMapping_;

  arrow::flight::FlightEndpoint flightEndpoint{request.dump()};

  std::string flightEndpointBytes;
  AFC_ASSIGN_OR_RAISE(flightEndpointBytes, flightEndpoint.SerializeToString());

  auto flightSplit = std::make_shared<ArrowFlightSplit>(
      federationSplit->connectorId, flightEndpointBytes);

  ArrowFlightDataSource::addSplit(flightSplit);
}

std::optional<velox::RowVectorPtr> ArrowFederationDataSource::next(
    uint64_t size,
    velox::ContinueFuture& future) {
  return ArrowFlightDataSource::next(size, future);
}

std::unique_ptr<velox::connector::DataSource>
ArrowFederationConnector::createDataSource(
    const velox::RowTypePtr& outputType,
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    const velox::connector::ColumnHandleMap& columnHandles,
    velox::connector::ConnectorQueryCtx* connectorQueryCtx) {
  return std::make_unique<ArrowFederationDataSource>(
      outputType,
      columnHandles,
      authenticator_,
      connectorQueryCtx,
      flightConfig_,
      clientOpts_);
}

} // namespace facebook::presto
