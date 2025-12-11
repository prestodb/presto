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

#include <memory>
#include <string>
#include <vector>
#include "presto_cpp/main/connectors/PrestoToVeloxConnector.h"
#include "presto_cpp/presto_protocol/core/ConnectorProtocol.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"

namespace velox::connector {
class ConnectorSplit;
class ColumnHandle;
class ConnectorTableHandle;
} // namespace velox::connector

namespace facebook::presto {
class TypeParser;
class VeloxExprConverter;
} // namespace facebook::presto

namespace facebook::presto::protocol::tpch {

struct TpchTableHandle : public ConnectorTableHandle {
  std::string tableName;
  double scaleFactor;

  TpchTableHandle() {
    _type = "tpch";
  }
};

struct TpchTableLayoutHandle : public ConnectorTableLayoutHandle {
  TpchTableHandle table;
  TupleDomain<std::shared_ptr<ColumnHandle>> predicate;

  TpchTableLayoutHandle() {
    _type = "tpch";
    // Initialize predicate as "all" (empty domains means no filtering)
    predicate.domains = nullptr;
  }
};

struct TpchColumnHandle : public ColumnHandle {
  std::string columnName;
  std::string type;
  std::vector<std::string> requiredSubfields;

  TpchColumnHandle() {
    _type = "tpch";
  }
};

struct TpchSplit : public ConnectorSplit {
  TpchTableHandle tableHandle;
  int partNumber;
  int totalParts;
  List<HostAddress> addresses;
  TupleDomain<std::shared_ptr<ColumnHandle>> predicate;

  TpchSplit() {
    _type = "tpch";
    // Initialize predicate as "all" (empty domains means no filtering)
    predicate.domains = nullptr;
  }
};

struct TpchTransactionHandle : public ConnectorTransactionHandle {
  std::string instance;

  TpchTransactionHandle() {
    _type = "tpch";
  }
};

struct TpchPartitioningHandle : public ConnectorPartitioningHandle {
  std::string table;
  int64_t totalRows;

  TpchPartitioningHandle() {
    _type = "tpch";
  }
};

class TpchConnectorProtocol final : public ConnectorProtocol {
 public:
  void deserialize(
      const std::string& binaryData,
      std::shared_ptr<ConnectorTableHandle>& proto) const override;

  void deserialize(
      const std::string& binaryData,
      std::shared_ptr<ConnectorTableLayoutHandle>& proto) const override;

  void deserialize(
      const std::string& binaryData,
      std::shared_ptr<ColumnHandle>& proto) const override;

  void deserialize(
      const std::string& binaryData,
      std::shared_ptr<ConnectorInsertTableHandle>& proto) const override;

  void deserialize(
      const std::string& binaryData,
      std::shared_ptr<ConnectorOutputTableHandle>& proto) const override;

  void deserialize(
      const std::string& binaryData,
      std::shared_ptr<ConnectorSplit>& proto) const override;

  void deserialize(
      const std::string& binaryData,
      std::shared_ptr<ConnectorPartitioningHandle>& proto) const override;

  void deserialize(
      const std::string& binaryData,
      std::shared_ptr<ConnectorTransactionHandle>& proto) const override;

  void deserialize(
      const std::string& binaryData,
      std::shared_ptr<ConnectorDeleteTableHandle>& proto) const override;

  void deserialize(
      const std::string& binaryData,
      std::shared_ptr<ConnectorIndexHandle>& proto) const override;

  void to_json(json& j, const std::shared_ptr<ConnectorTableHandle>& p)
      const override {
    VELOX_NYI("JSON not supported with binary serialization");
  }

  void from_json(const json& j, std::shared_ptr<ConnectorTableHandle>& p)
      const override {
    VELOX_NYI("JSON not supported with binary serialization");
  }

  void serialize(
      const std::shared_ptr<ConnectorTableHandle>& proto,
      std::string& thrift) const override {
    VELOX_NYI("Serialize not implemented");
  }

  void to_json(json& j, const std::shared_ptr<ConnectorTableLayoutHandle>& p)
      const override {
    VELOX_NYI("JSON not supported with binary serialization");
  }

  void from_json(const json& j, std::shared_ptr<ConnectorTableLayoutHandle>& p)
      const override {
    VELOX_NYI("JSON not supported with binary serialization");
  }

  void serialize(
      const std::shared_ptr<ConnectorTableLayoutHandle>& proto,
      std::string& thrift) const override {
    VELOX_NYI("Serialize not implemented");
  }

  void to_json(json& j, const std::shared_ptr<ColumnHandle>& p) const override {
    VELOX_NYI("JSON not supported with binary serialization");
  }

  void from_json(const json& j, std::shared_ptr<ColumnHandle>& p)
      const override {
    VELOX_NYI("JSON not supported with binary serialization");
  }

  void serialize(
      const std::shared_ptr<ColumnHandle>& proto,
      std::string& thrift) const override {
    VELOX_NYI("Serialize not implemented");
  }

  void to_json(json& j, const std::shared_ptr<ConnectorSplit>& p)
      const override {
    VELOX_NYI("JSON not supported with binary serialization");
  }

  void from_json(const json& j, std::shared_ptr<ConnectorSplit>& p)
      const override {
    VELOX_NYI("JSON not supported with binary serialization");
  }

  void serialize(
      const std::shared_ptr<ConnectorSplit>& proto,
      std::string& thrift) const override {
    VELOX_NYI("Serialize not implemented");
  }

  void to_json(json& j, const std::shared_ptr<ConnectorPartitioningHandle>& p)
      const override {
    VELOX_NYI("JSON not supported with binary serialization");
  }

  void from_json(const json& j, std::shared_ptr<ConnectorPartitioningHandle>& p)
      const override {
    VELOX_NYI("JSON not supported with binary serialization");
  }

  void serialize(
      const std::shared_ptr<ConnectorPartitioningHandle>& proto,
      std::string& thrift) const override {
    VELOX_NYI("Serialize not implemented");
  }

  void to_json(json& j, const std::shared_ptr<ConnectorTransactionHandle>& p)
      const override {
    VELOX_NYI("JSON not supported with binary serialization");
  }

  void from_json(const json& j, std::shared_ptr<ConnectorTransactionHandle>& p)
      const override {
    VELOX_NYI("JSON not supported with binary serialization");
  }

  void serialize(
      const std::shared_ptr<ConnectorTransactionHandle>& proto,
      std::string& thrift) const override {
    VELOX_NYI("Serialize not implemented");
  }

  // These handle types are not used by TPCH, but need dummy implementations
  void to_json(json& j, const std::shared_ptr<ConnectorInsertTableHandle>& p)
      const override {
    VELOX_NYI("JSON not supported with binary serialization");
  }

  void from_json(const json& j, std::shared_ptr<ConnectorInsertTableHandle>& p)
      const override {
    VELOX_NYI("JSON not supported with binary serialization");
  }

  void serialize(
      const std::shared_ptr<ConnectorInsertTableHandle>& proto,
      std::string& thrift) const override {
    VELOX_NYI("Serialize not implemented");
  }

  void to_json(json& j, const std::shared_ptr<ConnectorOutputTableHandle>& p)
      const override {
    VELOX_NYI("JSON not supported with binary serialization");
  }

  void from_json(const json& j, std::shared_ptr<ConnectorOutputTableHandle>& p)
      const override {
    VELOX_NYI("JSON not supported with binary serialization");
  }

  void serialize(
      const std::shared_ptr<ConnectorOutputTableHandle>& proto,
      std::string& thrift) const override {
    VELOX_NYI("Serialize not implemented");
  }

  void to_json(json& j, const std::shared_ptr<ConnectorDeleteTableHandle>& p)
      const override {
    VELOX_NYI("JSON not supported with binary serialization");
  }

  void from_json(const json& j, std::shared_ptr<ConnectorDeleteTableHandle>& p)
      const override {
    VELOX_NYI("JSON not supported with binary serialization");
  }

  void serialize(
      const std::shared_ptr<ConnectorDeleteTableHandle>& proto,
      std::string& thrift) const override {
    VELOX_NYI("Serialize not implemented");
  }

  void to_json(json& j, const std::shared_ptr<ConnectorIndexHandle>& p)
      const override {
    VELOX_NYI("JSON not supported with binary serialization");
  }

  void from_json(const json& j, std::shared_ptr<ConnectorIndexHandle>& p)
      const override {
    VELOX_NYI("JSON not supported with binary serialization");
  }

  void serialize(
      const std::shared_ptr<ConnectorIndexHandle>& proto,
      std::string& thrift) const override {
    VELOX_NYI("Serialize not implemented");
  }

  void to_json(
      json& j,
      const std::shared_ptr<ConnectorDistributedProcedureHandle>& p)
      const override {
    VELOX_NYI("JSON not supported with binary serialization");
  }

  void from_json(
      const json& j,
      std::shared_ptr<ConnectorDistributedProcedureHandle>& p) const override {
    VELOX_NYI("JSON not supported with binary serialization");
  }
};

} // namespace facebook::presto::protocol::tpch

namespace facebook::presto {

class TpchPrestoToVeloxConnector final : public PrestoToVeloxConnector {
 public:
  explicit TpchPrestoToVeloxConnector(std::string connectorName)
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
};

} // namespace facebook::presto
