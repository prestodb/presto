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

#include <unordered_map>

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

  // Pin the snapshot the coordinator planned against; 0 means latest.
  const uint64_t datasetVersion = lanceTable->datasetVersion
      ? static_cast<uint64_t>(*lanceTable->datasetVersion)
      : 0;

  // Translate the pushed-down vector search, narrowing the query vector
  // from the JSON-portable doubles to the float32 Lance searches with
  // (same narrowing the Java page source performs). requiresGlobalTopK is
  // coordinator-planning state and is deliberately not forwarded.
  std::optional<velox::connector::lance::LanceVectorSearchParams> vectorSearch;
  if (lanceTable->vectorSearch) {
    const auto& search = *lanceTable->vectorSearch;
    velox::connector::lance::LanceVectorSearchParams params;
    params.column = search.column;
    params.queryVector.reserve(search.queryVector.size());
    for (const double value : search.queryVector) {
      params.queryVector.push_back(static_cast<float>(value));
    }
    params.k = static_cast<uint32_t>(search.k);
    params.metric = search.metric;
    if (search.nprobes) {
      params.nprobes = static_cast<uint32_t>(*search.nprobes);
    }
    if (search.ef) {
      params.ef = static_cast<uint32_t>(*search.ef);
    }
    if (search.refineFactor) {
      params.refineFactor = static_cast<uint32_t>(*search.refineFactor);
    }
    vectorSearch = std::move(params);
  }

  // Storage options are resolved on the coordinator (from the Lance namespace,
  // or from a locally provisioned key file) and forwarded here, so the worker
  // never has to read a credential off local disk.
  std::unordered_map<std::string, std::string> storageOptions(
      lanceTable->storageOptions.begin(), lanceTable->storageOptions.end());

  // The physical dataset path is carried by the split and used by
  // LanceDataSource to open the dataset; the table name here is not read
  // during scans.
  return std::make_unique<velox::connector::lance::LanceTableHandle>(
      tableHandle.connectorId,
      lanceTable->tableName,
      datasetVersion,
      std::move(storageOptions),
      /*filter=*/"",
      std::move(vectorSearch));
}

std::unique_ptr<protocol::ConnectorProtocol>
LancePrestoToVeloxConnector::createConnectorProtocol() const {
  return std::make_unique<protocol::lance::LanceConnectorProtocol>();
}

} // namespace facebook::presto
