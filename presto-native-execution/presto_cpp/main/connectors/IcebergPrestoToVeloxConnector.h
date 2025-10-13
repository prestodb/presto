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
#include "presto_cpp/presto_protocol/connector/iceberg/presto_protocol_iceberg.h"
#include "velox/connectors/hive/iceberg/IcebergColumnHandle.h"
#include "velox/connectors/hive/iceberg/IcebergDataSink.h"
#include "velox/connectors/hive/iceberg/PartitionSpec.h"

namespace facebook::presto {

class IcebergPrestoToVeloxConnector final : public PrestoToVeloxConnector {
 public:
  explicit IcebergPrestoToVeloxConnector(std::string connectorName)
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

  std::unique_ptr<protocol::ConnectorProtocol> createConnectorProtocol()
      const final;

  std::unique_ptr<velox::connector::ConnectorInsertTableHandle>
  toVeloxInsertTableHandle(
      const protocol::CreateHandle* createHandle,
      const TypeParser& typeParser,
      velox::memory::MemoryPool* pool) const final;

  std::unique_ptr<velox::connector::ConnectorInsertTableHandle>
  toVeloxInsertTableHandle(
      const protocol::InsertHandle* insertHandle,
      const TypeParser& typeParser,
      velox::memory::MemoryPool* pool) const final;

 private:
  std::vector<std::shared_ptr<
      const velox::connector::hive::iceberg::IcebergColumnHandle>>
  toIcebergColumns(
      const protocol::List<protocol::iceberg::IcebergColumnHandle>&
          inputColumns,
      const TypeParser& typeParser) const;

  std::vector<velox::connector::hive::iceberg::IcebergSortingColumn>
  toIcebergSortingColumns(
      protocol::List<protocol::iceberg::SortField>,
      const protocol::iceberg::PrestoIcebergSchema& schema) const;

  velox::connector::hive::iceberg::IcebergPartitionSpec::Field
  toVeloxIcebergPartitionField(
      const protocol::iceberg::IcebergPartitionField& filed,
      const facebook::presto::TypeParser& typeParser,
      const protocol::iceberg::PrestoIcebergSchema& schema) const;

  std::unique_ptr<velox::connector::hive::iceberg::IcebergPartitionSpec>
  toVeloxIcebergPartitionSpec(
      const protocol::iceberg::PrestoIcebergPartitionSpec& spec,
      const TypeParser& typeParser) const;
};

} // namespace facebook::presto
