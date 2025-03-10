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
#include "presto_cpp/main/connectors/arrow_flight/ArrowPrestoToVeloxConnector.h"
#include <folly/base64.h>
#include "presto_cpp/main/connectors/arrow_flight/ArrowFlightConnector.h"
#include "presto_cpp/presto_protocol/connector/arrow_flight/ArrowFlightConnectorProtocol.h"

namespace facebook::presto {

std::unique_ptr<velox::connector::ConnectorSplit>
ArrowPrestoToVeloxConnector::toVeloxSplit(
    const protocol::ConnectorId& catalogId,
    const protocol::ConnectorSplit* const connectorSplit,
    const protocol::SplitContext* /*splitContext*/) const {
  auto arrowSplit =
      dynamic_cast<const protocol::arrow_flight::ArrowSplit*>(connectorSplit);
  VELOX_CHECK_NOT_NULL(
      arrowSplit, "Unexpected split type {}", connectorSplit->_type);
  return std::make_unique<presto::ArrowFlightSplit>(
      catalogId, arrowSplit->flightEndpointBytes);
}

std::unique_ptr<velox::connector::ColumnHandle>
ArrowPrestoToVeloxConnector::toVeloxColumnHandle(
    const protocol::ColumnHandle* column,
    const TypeParser& /*typeParser*/) const {
  auto arrowColumn =
      dynamic_cast<const protocol::arrow_flight::ArrowColumnHandle*>(column);
  VELOX_CHECK_NOT_NULL(
      arrowColumn, "Unexpected column handle type {}", column->_type);
  return std::make_unique<presto::ArrowFlightColumnHandle>(
      arrowColumn->columnName);
}

std::unique_ptr<velox::connector::ConnectorTableHandle>
ArrowPrestoToVeloxConnector::toVeloxTableHandle(
    const protocol::TableHandle& tableHandle,
    const VeloxExprConverter& /*exprConverter*/,
    const TypeParser& /*typeParser*/,
    std::unordered_map<
        std::string,
        std::shared_ptr<velox::connector::ColumnHandle>>& assignments) const {
  return std::make_unique<presto::ArrowFlightTableHandle>(
      tableHandle.connectorId);
}

std::unique_ptr<protocol::ConnectorProtocol>
ArrowPrestoToVeloxConnector::createConnectorProtocol() const {
  return std::make_unique<protocol::arrow_flight::ArrowConnectorProtocol>();
}

} // namespace facebook::presto
