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

namespace {
velox::connector::ColumnHandleMap toArrowFlightColumnHandleMap(
    const velox::connector::ColumnHandleMap& columnHandles,
    const velox::RowTypePtr& outputType) {
  velox::connector::ColumnHandleMap arrowFlightColumnHandles;

  for (const auto& columnName : outputType->names()) {
    auto it = columnHandles.find(columnName);
    VELOX_CHECK(
        it != columnHandles.end(),
        "missing columnHandle for column '{}'",
        columnName);
    arrowFlightColumnHandles[columnName] =
        std::make_shared<facebook::presto::ArrowFlightColumnHandle>(
            it->second->name());
  }
  return arrowFlightColumnHandles;
}

const std::vector<std::string> toArrowFederationColumnHandleList(
    const velox::RowTypePtr& outputType,
    const velox::connector::ColumnHandleMap& columnHandles,
    std::vector<protocol::Field>& fields) {
  std::vector<std::string> arrowFederationColumnHandles;
  // Iterate over outputType to preserve order and extract column info
  for (size_t i = 0; i < outputType->size(); ++i) {
    const auto& name = outputType->nameOf(i);
    auto it = columnHandles.find(name);
    VELOX_CHECK(
        it != columnHandles.end(),
        "Column handle not found for variable: {}",
        name);
    auto handle = std::dynamic_pointer_cast<const ArrowFederationColumnHandle>(
        it->second);
    VELOX_CHECK(
        handle,
        "ArrowFederationDataSource received wrong type of column handle");
    arrowFederationColumnHandles.push_back(handle->columnHandleBytes());
    protocol::Field field;
    field.name = std::make_shared<std::string>(handle->name());
    field.type = outputType->childAt(i)->toString();
    fields.push_back(std::move(field));
  }
  return arrowFederationColumnHandles;
}

nlohmann::json buildFederationRequest(
    const std::shared_ptr<const ArrowFederationSplit>& federationSplit,
    const std::shared_ptr<const ArrowFederationTableHandle>&
        federationTableHandle,
    const velox::RowTypePtr& outputType,
    const velox::connector::ColumnHandleMap& columnHandles) {
  nlohmann::json request;
  request["connectorId"] = federationSplit->connectorId;
  request["splitBytes"] = federationSplit->splitBytes_;

  request["tableHandleBytes"] = federationTableHandle->tableHandleBytes();
  request["tableLayoutHandleBytes"] =
      federationTableHandle->tableLayoutHandleBytes();
  request["transactionHandleBytes"] =
      federationTableHandle->transactionHandleBytes();

  std::vector<protocol::Field> fields;
  request["columnHandlesBytes"] =
      toArrowFederationColumnHandleList(outputType, columnHandles, fields);
  request["fields"] = fields;
  return request;
}
} // namespace

ArrowFederationDataSource::ArrowFederationDataSource(
    const velox::RowTypePtr& outputType,
    const velox::connector::ColumnHandleMap& columnHandles,
    std::shared_ptr<Authenticator> authenticator,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<ArrowFlightConfig>& flightConfig,
    const std::shared_ptr<arrow::flight::FlightClientOptions>& clientOpts,
    const velox::connector::ConnectorTableHandlePtr& tableHandle)
    : ArrowFlightDataSource(
          outputType,
          toArrowFlightColumnHandleMap(columnHandles, outputType),
          authenticator,
          connectorQueryCtx,
          flightConfig,
          clientOpts),
      columnHandles_(columnHandles),
      tableHandle_(tableHandle) {}

void ArrowFederationDataSource::addSplit(
    std::shared_ptr<ConnectorSplit> split) {
  auto federationSplit = std::dynamic_pointer_cast<ArrowFederationSplit>(split);
  VELOX_CHECK(
      federationSplit,
      "ArrowFederationDataSource received wrong type of split");

  auto federationTableHandle =
      std::dynamic_pointer_cast<const ArrowFederationTableHandle>(tableHandle_);
  VELOX_CHECK(
      federationTableHandle,
      "ArrowFederationDataSource received wrong type of table handle");

  nlohmann::json request = buildFederationRequest(
      federationSplit, federationTableHandle, outputType_, columnHandles_);

  arrow::flight::FlightEndpoint flightEndpoint{
      request.dump(), {}, std::nullopt, ""};

  std::string flightEndpointBytes;
  AFC_ASSIGN_OR_RAISE(flightEndpointBytes, flightEndpoint.SerializeToString());

  auto flightSplit = std::make_shared<ArrowFlightSplit>(
      federationSplit->connectorId, folly::base64Encode(flightEndpointBytes));

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
      clientOpts_,
      tableHandle);
}

} // namespace facebook::presto
