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
#include "presto_cpp/presto_protocol/connector/delta/presto_protocol_delta.h"

namespace facebook::presto {

/// Connector for Delta Lake tables that bridges Presto protocol and Velox
/// execution. Converts Presto Delta protocol objects to Velox connector objects
/// for native execution.
class DeltaPrestoToVeloxConnector final : public PrestoToVeloxConnector {
 public:
  explicit DeltaPrestoToVeloxConnector(std::string connectorName)
      : PrestoToVeloxConnector(std::move(connectorName)) {}

  /// Convert Presto Delta split to Velox split for execution
  /// @param catalogId Catalog identifier
  /// @param connectorSplit Presto connector split (must be DeltaSplit)
  /// @param splitContext Split execution context
  /// @return Velox connector split for execution
  std::unique_ptr<velox::connector::ConnectorSplit> toVeloxSplit(
      const protocol::ConnectorId& catalogId,
      const protocol::ConnectorSplit* connectorSplit,
      const protocol::SplitContext* splitContext) const final;

  /// Convert Presto Delta column handle to Velox column handle
  /// @param column Presto column handle (must be DeltaColumnHandle)
  /// @param typeParser Type parser for converting type strings
  /// @return Velox column handle
  std::unique_ptr<velox::connector::ColumnHandle> toVeloxColumnHandle(
      const protocol::ColumnHandle* column,
      const TypeParser& typeParser) const final;

  /// Convert Presto Delta table handle to Velox table handle
  /// @param tableHandle Presto table handle containing DeltaTableHandle
  /// @param exprConverter Expression converter for predicates
  /// @param typeParser Type parser for converting type strings
  /// @return Velox table handle for execution
  std::unique_ptr<velox::connector::ConnectorTableHandle> toVeloxTableHandle(
      const protocol::TableHandle& tableHandle,
      const VeloxExprConverter& exprConverter,
      const TypeParser& typeParser) const final;

  /// Create Delta connector protocol instance
  /// @return Delta connector protocol for serialization/deserialization
  std::unique_ptr<protocol::ConnectorProtocol> createConnectorProtocol()
      const final;

 private:
  /// Convert list of Delta column handles to Hive column handles
  /// Delta uses Hive's column handle infrastructure for execution
  /// @param inputColumns List of Delta column handles
  /// @param typeParser Type parser for converting type strings
  /// @return Vector of Hive column handles
  std::vector<velox::connector::hive::HiveColumnHandlePtr> toHiveColumns(
      const protocol::List<protocol::delta::DeltaColumnHandle>& inputColumns,
      const TypeParser& typeParser) const;
};

} // namespace facebook::presto
