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
#include "presto_cpp/main/types/TpchPrestoToVeloxConnector.h"
#include "presto_cpp/presto_protocol/connector/tpch/TpchConnectorProtocol.h"
#include "velox/velox/connectors/tpch/TpchConnector.h"
#include "velox/velox/connectors/tpch/TpchConnectorSplit.h"

namespace facebook::presto {

using namespace velox;

std::unique_ptr<velox::connector::ConnectorSplit>
TpchPrestoToVeloxConnector::toVeloxSplit(
    const protocol::ConnectorId& catalogId,
    const protocol::ConnectorSplit* const connectorSplit) const {
  auto tpchSplit =
      dynamic_cast<const protocol::tpch::TpchSplit*>(connectorSplit);
  VELOX_CHECK_NOT_NULL(
      tpchSplit, "Unexpected split type {}", connectorSplit->_type);
  return std::make_unique<velox::connector::tpch::TpchConnectorSplit>(
      catalogId, tpchSplit->totalParts, tpchSplit->partNumber);
}

std::unique_ptr<velox::connector::ColumnHandle>
TpchPrestoToVeloxConnector::toVeloxColumnHandle(
    const protocol::ColumnHandle* column,
    const TypeParser& typeParser) const {
  auto tpchColumn =
      dynamic_cast<const protocol::tpch::TpchColumnHandle*>(column);
  VELOX_CHECK_NOT_NULL(
      tpchColumn, "Unexpected column handle type {}", column->_type);
  return std::make_unique<velox::connector::tpch::TpchColumnHandle>(
      tpchColumn->columnName);
}

std::unique_ptr<velox::connector::ConnectorTableHandle>
TpchPrestoToVeloxConnector::toVeloxTableHandle(
    const protocol::TableHandle& tableHandle,
    const VeloxExprConverter& exprConverter,
    const TypeParser& typeParser,
    std::unordered_map<
        std::string,
        std::shared_ptr<velox::connector::ColumnHandle>>& assignments) const {
  auto tpchLayout =
      std::dynamic_pointer_cast<const protocol::tpch::TpchTableLayoutHandle>(
          tableHandle.connectorTableLayout);
  VELOX_CHECK_NOT_NULL(
      tpchLayout,
      "Unexpected layout type {}",
      tableHandle.connectorTableLayout->_type);
  return std::make_unique<velox::connector::tpch::TpchTableHandle>(
      tableHandle.connectorId,
      tpch::fromTableName(tpchLayout->table.tableName),
      tpchLayout->table.scaleFactor);
}

std::unique_ptr<protocol::ConnectorProtocol>
TpchPrestoToVeloxConnector::createConnectorProtocol() const {
  return std::make_unique<protocol::tpch::TpchConnectorProtocol>();
}

} // namespace facebook::presto
