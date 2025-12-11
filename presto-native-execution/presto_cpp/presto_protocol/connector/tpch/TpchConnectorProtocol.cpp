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

#include "presto_cpp/presto_protocol/connector/tpch/TpchConnectorProtocol.h"
#include <folly/lang/Bits.h>
#include <glog/logging.h>
#include <sstream>
#include "presto_cpp/main/types/PrestoToVeloxExpr.h"
#include "presto_cpp/main/types/TypeParser.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/connectors/tpch/TpchConnectorSplit.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/RoundRobinPartitionFunction.h"
#include "velox/tpch/gen/TpchGen.h"

namespace facebook::presto::protocol::tpch {

namespace {

std::string readUTF(std::istringstream& in) {
  // Java's modified UTF-8 format: 2-byte length followed by UTF-8 bytes
  uint16_t length;
  in.read(reinterpret_cast<char*>(&length), sizeof(length));
  length = folly::Endian::big(length);

  std::string result(length, '\0');
  in.read(&result[0], length);
  return result;
}

double readDouble(std::istringstream& in) {
  // Java writes doubles as 8 bytes in big-endian
  uint64_t value;
  in.read(reinterpret_cast<char*>(&value), sizeof(value));
  value = folly::Endian::big(value);

  double result;
  memcpy(&result, &value, sizeof(double));
  return result;
}

int32_t readInt(std::istringstream& in) {
  // Java writes ints as 4 bytes in big-endian
  uint32_t value;
  in.read(reinterpret_cast<char*>(&value), sizeof(value));
  value = folly::Endian::big(value);
  return static_cast<int32_t>(value);
}

int64_t readLong(std::istringstream& in) {
  // Java writes longs as 8 bytes in big-endian
  uint64_t value;
  in.read(reinterpret_cast<char*>(&value), sizeof(value));
  value = folly::Endian::big(value);
  return static_cast<int64_t>(value);
}

} // namespace

void TpchConnectorProtocol::deserialize(
    const std::string& binaryData,
    std::shared_ptr<ConnectorTableHandle>& proto) const {
  std::istringstream in(binaryData);
  auto handle = std::make_shared<TpchTableHandle>();

  handle->tableName = readUTF(in);
  handle->scaleFactor = readDouble(in);
  handle->_type = "tpch";

  proto = handle;
}

void TpchConnectorProtocol::deserialize(
    const std::string& binaryData,
    std::shared_ptr<ConnectorTableLayoutHandle>& proto) const {
  std::istringstream in(binaryData);
  auto handle = std::make_shared<TpchTableLayoutHandle>();

  handle->table.tableName = readUTF(in);
  handle->table.scaleFactor = readDouble(in);
  handle->table._type = "tpch";

  std::string predicateJson = readUTF(in);
  json j = json::parse(predicateJson);
  handle->predicate =
      j.get<protocol::TupleDomain<std::shared_ptr<protocol::ColumnHandle>>>();

  handle->_type = "tpch";

  proto = handle;
}

void TpchConnectorProtocol::deserialize(
    const std::string& binaryData,
    std::shared_ptr<ColumnHandle>& proto) const {
  std::istringstream in(binaryData);
  auto handle = std::make_shared<TpchColumnHandle>();

  handle->columnName = readUTF(in);
  handle->type = readUTF(in);

  int32_t subfieldCount = readInt(in);

  handle->requiredSubfields.reserve(subfieldCount);
  for (int32_t i = 0; i < subfieldCount; i++) {
    std::string subfield = readUTF(in);
    handle->requiredSubfields.push_back(subfield);
  }

  handle->_type = "tpch";

  proto = handle;
}

void TpchConnectorProtocol::deserialize(
    const std::string& binaryData,
    std::shared_ptr<ConnectorSplit>& proto) const {
  std::istringstream in(binaryData);
  auto split = std::make_shared<TpchSplit>();

  split->tableHandle.tableName = readUTF(in);
  split->tableHandle.scaleFactor = readDouble(in);
  split->tableHandle._type = "tpch";

  split->partNumber = readInt(in);
  split->totalParts = readInt(in);

  int32_t addressCount = readInt(in);
  split->addresses.reserve(addressCount);
  for (int32_t i = 0; i < addressCount; i++) {
    std::string host = readUTF(in);
    int32_t port = readInt(in);
    split->addresses.push_back(host + ":" + std::to_string(port));
  }

  std::string predicateJson = readUTF(in);

  json j = json::parse(predicateJson);
  split->predicate =
      j.get<protocol::TupleDomain<std::shared_ptr<protocol::ColumnHandle>>>();

  split->_type = "tpch";

  proto = split;
}

void TpchConnectorProtocol::deserialize(
    const std::string& binaryData,
    std::shared_ptr<ConnectorPartitioningHandle>& proto) const {
  std::istringstream in(binaryData);
  auto handle = std::make_shared<TpchPartitioningHandle>();

  handle->table = readUTF(in);
  handle->totalRows = readLong(in);
  handle->_type = "tpch";

  proto = handle;
}

void TpchConnectorProtocol::deserialize(
    const std::string& binaryData,
    std::shared_ptr<ConnectorTransactionHandle>& proto) const {
  // TpchTransactionHandle in Java is essentially a singleton with no data
  // The binary data is empty (size 0)
  auto handle = std::make_shared<TpchTransactionHandle>();
  handle->instance = "INSTANCE";
  handle->_type = "tpch";

  proto = handle;
}

void TpchConnectorProtocol::deserialize(
    const std::string& binaryData,
    std::shared_ptr<ConnectorInsertTableHandle>& proto) const {
  VELOX_NYI("TpchInsertTableHandle not supported");
}

void TpchConnectorProtocol::deserialize(
    const std::string& binaryData,
    std::shared_ptr<ConnectorOutputTableHandle>& proto) const {
  VELOX_NYI("TpchOutputTableHandle not supported");
}

void TpchConnectorProtocol::deserialize(
    const std::string& binaryData,
    std::shared_ptr<ConnectorDeleteTableHandle>& proto) const {
  VELOX_NYI("TpchDeleteTableHandle not supported");
}

void TpchConnectorProtocol::deserialize(
    const std::string& binaryData,
    std::shared_ptr<ConnectorIndexHandle>& proto) const {
  VELOX_NYI("TpchIndexHandle not supported");
}

} // namespace facebook::presto::protocol::tpch

namespace facebook::presto {

using namespace protocol;
using namespace protocol::tpch;

std::unique_ptr<velox::connector::ConnectorSplit>
TpchPrestoToVeloxConnector::toVeloxSplit(
    const protocol::ConnectorId& catalogId,
    const protocol::ConnectorSplit* connectorSplit,
    const protocol::SplitContext* splitContext) const {
  auto* tpchSplit = dynamic_cast<const tpch::TpchSplit*>(connectorSplit);
  if (!tpchSplit) {
    VELOX_FAIL("Expected TpchSplit but got {}", connectorSplit->_type);
  }

  return std::make_unique<velox::connector::tpch::TpchConnectorSplit>(
      catalogId,
      splitContext->cacheable,
      tpchSplit->totalParts,
      tpchSplit->partNumber);
}

std::unique_ptr<velox::connector::ColumnHandle>
TpchPrestoToVeloxConnector::toVeloxColumnHandle(
    const protocol::ColumnHandle* column,
    const TypeParser& typeParser) const {
  auto* tpchColumn = dynamic_cast<const tpch::TpchColumnHandle*>(column);
  if (!tpchColumn) {
    VELOX_FAIL("Expected TpchColumnHandle but got {}", column->_type);
  }

  return std::make_unique<velox::connector::tpch::TpchColumnHandle>(
      tpchColumn->columnName);
}

std::unique_ptr<velox::connector::ConnectorTableHandle>
TpchPrestoToVeloxConnector::toVeloxTableHandle(
    const protocol::TableHandle& tableHandle,
    const VeloxExprConverter& exprConverter,
    const TypeParser& typeParser) const {
  auto tpchLayout =
      std::dynamic_pointer_cast<const tpch::TpchTableLayoutHandle>(
          tableHandle.connectorTableLayout);
  VELOX_CHECK_NOT_NULL(
      tpchLayout,
      "Unexpected layout type {}",
      tableHandle.connectorTableLayout->_type);

  return std::make_unique<velox::connector::tpch::TpchTableHandle>(
      tableHandle.connectorId,
      velox::tpch::fromTableName(tpchLayout->table.tableName),
      tpchLayout->table.scaleFactor);
}

std::unique_ptr<velox::core::PartitionFunctionSpec>
TpchPrestoToVeloxConnector::createVeloxPartitionFunctionSpec(
    const protocol::ConnectorPartitioningHandle* partitioningHandle,
    const std::vector<int>& bucketToPartition,
    const std::vector<velox::column_index_t>& channels,
    const std::vector<velox::VectorPtr>& constValues,
    bool& effectivelyGather) const {
  auto* tpchPartitioningHandle =
      dynamic_cast<const tpch::TpchPartitioningHandle*>(partitioningHandle);
  if (!tpchPartitioningHandle) {
    VELOX_FAIL(
        "Expected TpchPartitioningHandle but got {}",
        partitioningHandle->_type);
  }

  effectivelyGather = false;
  return std::make_unique<velox::exec::RoundRobinPartitionFunctionSpec>();
}

std::unique_ptr<protocol::ConnectorProtocol>
TpchPrestoToVeloxConnector::createConnectorProtocol() const {
  return std::make_unique<protocol::tpch::TpchConnectorProtocol>();
}

} // namespace facebook::presto
