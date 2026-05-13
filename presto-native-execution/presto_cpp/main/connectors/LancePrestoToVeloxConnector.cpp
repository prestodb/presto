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

#include "presto_cpp/main/connectors/LancePrestoToVeloxConnector.h"

#include "presto_cpp/presto_protocol/connector/lance/LanceConnectorProtocol.h"
#include "presto_cpp/presto_protocol/connector/lance/presto_protocol_lance.h"
#include "velox/connectors/lance/LanceColumnHandle.h"
#include "velox/connectors/lance/LanceConnectorSplit.h"
#include "velox/connectors/lance/LanceTableHandle.h"

namespace facebook::presto {

std::unique_ptr<velox::connector::ConnectorSplit>
LancePrestoToVeloxConnector::toVeloxSplit(
    const protocol::ConnectorId& catalogId,
    const protocol::ConnectorSplit* connectorSplit,
    const protocol::SplitContext* /*splitContext*/) const {
  auto lanceSplit =
      dynamic_cast<const protocol::lance::LanceSplit*>(connectorSplit);
  VELOX_CHECK_NOT_NULL(
      lanceSplit, "Unexpected split type {}", connectorSplit->_type);

  // Convert fragment IDs from int to uint64_t.
  std::vector<uint64_t> fragmentIds;
  fragmentIds.reserve(lanceSplit->fragments.size());
  for (const auto& id : lanceSplit->fragments) {
    VELOX_CHECK_GE(
        id, 0, "Lance fragment ID must be non-negative, got: {}", id);
    fragmentIds.push_back(static_cast<uint64_t>(id));
  }

  if (fragmentIds.empty()) {
    return std::make_unique<velox::connector::lance::LanceConnectorSplit>(
        catalogId, lanceSplit->datasetPath);
  }
  return std::make_unique<velox::connector::lance::LanceConnectorSplit>(
      catalogId, lanceSplit->datasetPath, std::move(fragmentIds));
}

std::unique_ptr<velox::connector::ColumnHandle>
LancePrestoToVeloxConnector::toVeloxColumnHandle(
    const protocol::ColumnHandle* column,
    const TypeParser& /*typeParser*/) const {
  auto lanceColumn =
      dynamic_cast<const protocol::lance::LanceColumnHandle*>(column);
  VELOX_CHECK_NOT_NULL(
      lanceColumn, "Unexpected column handle type {}", column->_type);

  return std::make_unique<velox::connector::lance::LanceColumnHandle>(
      lanceColumn->columnName);
}

std::unique_ptr<velox::connector::ConnectorTableHandle>
LancePrestoToVeloxConnector::toVeloxTableHandle(
    const protocol::TableHandle& tableHandle,
    const VeloxExprConverter& /*exprConverter*/,
    const TypeParser& /*typeParser*/) const {
  auto lanceLayout =
      std::dynamic_pointer_cast<const protocol::lance::LanceTableLayoutHandle>(
          tableHandle.connectorTableLayout);
  VELOX_CHECK_NOT_NULL(
      lanceLayout, "Unexpected table layout type for Lance connector");

  auto lanceTable = lanceLayout->table;
  VELOX_CHECK_NOT_NULL(lanceTable, "LanceTableHandle is null in layout");

  // Pass the logical table name to LanceTableHandle. The physical dataset path
  // is carried by the split and used by LanceDataSource to open the dataset;
  // LanceTableHandle.datasetPath() / name() is not read during scans.
  return std::make_unique<velox::connector::lance::LanceTableHandle>(
      tableHandle.connectorId, lanceTable->tableName);
}

std::unique_ptr<protocol::ConnectorProtocol>
LancePrestoToVeloxConnector::createConnectorProtocol() const {
  return std::make_unique<protocol::lance::LanceConnectorProtocol>();
}

} // namespace facebook::presto
