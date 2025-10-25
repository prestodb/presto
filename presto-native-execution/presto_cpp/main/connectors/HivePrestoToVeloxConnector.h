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
#pragma once

#include "presto_cpp/main/connectors/PrestoToVeloxConnector.h"
#include "presto_cpp/main/types/PrestoToVeloxExpr.h"
#include "presto_cpp/presto_protocol/connector/hive/presto_protocol_hive.h"
#include "presto_cpp/presto_protocol/core/ConnectorProtocol.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/core/PlanNode.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::presto {

class HivePrestoToVeloxConnector final : public PrestoToVeloxConnector {
 public:
  explicit HivePrestoToVeloxConnector(std::string connectorName)
      : PrestoToVeloxConnector(std::move(connectorName)) {}

  std::unique_ptr<velox::connector::ConnectorSplit> toVeloxSplit(
      const protocol::ConnectorId& catalogId,
      const protocol::ConnectorSplit* connectorSplit,
      const protocol::SplitContext* splitContext) const final;

  std::unique_ptr<velox::connector::ColumnHandle> toVeloxColumnHandle(
      const protocol::ColumnHandle* column,
      const TypeParser& typeParser) const final;

  std::unique_ptr<velox::connector::ConnectorTableHandle> toVeloxTableHandle(
      const protocol::TableHandle& tableHandle,
      const VeloxExprConverter& exprConverter,
      const TypeParser& typeParser,
      velox::connector::ColumnHandleMap& assignments) const final;

  std::unique_ptr<velox::connector::ConnectorInsertTableHandle>
  toVeloxInsertTableHandle(
      const protocol::CreateHandle* createHandle,
      const TypeParser& typeParser) const final;

  std::unique_ptr<velox::connector::ConnectorInsertTableHandle>
  toVeloxInsertTableHandle(
      const protocol::InsertHandle* insertHandle,
      const TypeParser& typeParser) const final;

  std::unique_ptr<velox::core::PartitionFunctionSpec>
  createVeloxPartitionFunctionSpec(
      const protocol::ConnectorPartitioningHandle* partitioningHandle,
      const std::vector<int>& bucketToPartition,
      const std::vector<velox::column_index_t>& channels,
      const std::vector<velox::VectorPtr>& constValues,
      bool& effectivelyGather) const final;

  std::unique_ptr<protocol::ConnectorProtocol> createConnectorProtocol()
      const final;

 private:
  std::vector<std::shared_ptr<const velox::connector::hive::HiveColumnHandle>>
  toHiveColumns(
      const protocol::List<protocol::hive::HiveColumnHandle>& inputColumns,
      const TypeParser& typeParser,
      bool& hasPartitionColumn) const;
};

} // namespace facebook::presto
