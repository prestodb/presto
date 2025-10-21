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
#include "presto_cpp/main/connectors/arrow_flight/arrow_federation/ArrowFederationPrestoToVeloxConnector.h"
#include <folly/base64.h>
#include "presto_cpp/main/connectors/arrow_flight/arrow_federation/ArrowFederationConnector.h"
#include "presto_cpp/presto_protocol/connector/arrow_flight/ArrowFlightConnectorProtocol.h"

namespace facebook::presto {

std::unique_ptr<velox::connector::ConnectorSplit>
ArrowFederationPrestoToVeloxConnector::toVeloxSplit(
    const protocol::ConnectorId& catalogId,
    const protocol::ConnectorSplit* const connectorSplit,
    const protocol::SplitContext* /*splitContext*/) const {
  auto arrowSplit =
      dynamic_cast<const protocol::arrow_flight::ArrowSplit*>(connectorSplit);
  VELOX_CHECK_NOT_NULL(
      arrowSplit, "Unexpected split type {}", connectorSplit->_type);
  return std::make_unique<presto::ArrowFederationSplit>(
      catalogId, arrowSplit->flightEndpointBytes);
}

std::unique_ptr<velox::connector::ColumnHandle>
ArrowFederationPrestoToVeloxConnector::toVeloxColumnHandle(
    const protocol::ColumnHandle* column,
    const TypeParser& /*typeParser*/) const {
  auto arrowColumn =
      dynamic_cast<const protocol::arrow_flight::ArrowColumnHandle*>(column);
  VELOX_CHECK_NOT_NULL(
      arrowColumn, "Unexpected column handle type {}", column->_type);
  return std::make_unique<presto::ArrowFederationColumnHandle>(
      arrowColumn->columnName);
}

std::unique_ptr<velox::connector::ConnectorTableHandle>
ArrowFederationPrestoToVeloxConnector::toVeloxTableHandle(
    const protocol::TableHandle& tableHandle,
    const VeloxExprConverter& /*exprConverter*/,
    const TypeParser& /*typeParser*/,
    velox::connector::ColumnHandleMap& assignments) const {
  return std::make_unique<presto::ArrowFederationTableHandle>(
      tableHandle.connectorId);
}

std::unique_ptr<protocol::ConnectorProtocol>
ArrowFederationPrestoToVeloxConnector::createConnectorProtocol() const {
  return std::make_unique<protocol::arrow_flight::ArrowConnectorProtocol>();
}

} // namespace facebook::presto
