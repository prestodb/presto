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

#include "presto_cpp/main/connectors/PrestoToVeloxConnector.h"
#include "presto_cpp/main/connectors/PrestoToVeloxConnectorUtils.h"
#include "presto_cpp/main/types/PrestoToVeloxExpr.h"
#include "presto_cpp/main/types/TypeParser.h"
#include "presto_cpp/presto_protocol/connector/hive/HiveConnectorProtocol.h"
#include "presto_cpp/presto_protocol/connector/tpcds/TpcdsConnectorProtocol.h"
#include "presto_cpp/presto_protocol/connector/tpch/TpchConnectorProtocol.h"

#include <velox/type/fbhive/HiveTypeParser.h>
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/HiveDataSink.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/connectors/tpcds/TpcdsConnector.h"
#include "velox/connectors/tpcds/TpcdsConnectorSplit.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/connectors/tpch/TpchConnectorSplit.h"
#include "velox/type/Filter.h"

namespace facebook::presto {
using namespace velox;

namespace {
std::unordered_map<std::string, std::unique_ptr<const PrestoToVeloxConnector>>&
connectors() {
  static std::
      unordered_map<std::string, std::unique_ptr<const PrestoToVeloxConnector>>
          connectors;
  return connectors;
}
} // namespace

void registerPrestoToVeloxConnector(
    std::unique_ptr<const PrestoToVeloxConnector> connector) {
  auto connectorName = connector->connectorName();
  auto connectorProtocol = connector->createConnectorProtocol();
  VELOX_CHECK(
      connectors().insert({connectorName, std::move(connector)}).second,
      "Connector {} is already registered",
      connectorName);
  protocol::registerConnectorProtocol(
      connectorName, std::move(connectorProtocol));
}

void unregisterPrestoToVeloxConnector(const std::string& connectorName) {
  connectors().erase(connectorName);
  protocol::unregisterConnectorProtocol(connectorName);
}

const PrestoToVeloxConnector& getPrestoToVeloxConnector(
    const std::string& connectorName) {
  auto it = connectors().find(connectorName);
  VELOX_CHECK(
      it != connectors().end(), "Connector {} not registered", connectorName);
  return *(it->second);
}

std::unique_ptr<velox::connector::ConnectorSplit>
TpchPrestoToVeloxConnector::toVeloxSplit(
    const protocol::ConnectorId& catalogId,
    const protocol::ConnectorSplit* connectorSplit,
    const protocol::SplitContext* splitContext) const {
  auto tpchSplit =
      dynamic_cast<const protocol::tpch::TpchSplit*>(connectorSplit);
  VELOX_CHECK_NOT_NULL(
      tpchSplit, "Unexpected split type {}", connectorSplit->_type);
  return std::make_unique<connector::tpch::TpchConnectorSplit>(
      catalogId,
      splitContext->cacheable,
      tpchSplit->totalParts,
      tpchSplit->partNumber);
}

std::unique_ptr<velox::connector::ColumnHandle>
TpchPrestoToVeloxConnector::toVeloxColumnHandle(
    const protocol::ColumnHandle* column,
    const TypeParser& typeParser) const {
  auto tpchColumn =
      dynamic_cast<const protocol::tpch::TpchColumnHandle*>(column);
  VELOX_CHECK_NOT_NULL(
      tpchColumn, "Unexpected column handle type {}", column->_type);
  return std::make_unique<connector::tpch::TpchColumnHandle>(
      tpchColumn->columnName);
}

std::unique_ptr<velox::connector::ConnectorTableHandle>
TpchPrestoToVeloxConnector::toVeloxTableHandle(
    const protocol::TableHandle& tableHandle,
    const VeloxExprConverter& exprConverter,
    const TypeParser& typeParser) const {
  auto tpchLayout =
      std::dynamic_pointer_cast<const protocol::tpch::TpchTableLayoutHandle>(
          tableHandle.connectorTableLayout);
  VELOX_CHECK_NOT_NULL(
      tpchLayout,
      "Unexpected layout type {}",
      tableHandle.connectorTableLayout->_type);
  return std::make_unique<connector::tpch::TpchTableHandle>(
      tableHandle.connectorId,
      tpch::fromTableName(tpchLayout->table.tableName),
      tpchLayout->table.scaleFactor);
}

std::unique_ptr<protocol::ConnectorProtocol>
TpchPrestoToVeloxConnector::createConnectorProtocol() const {
  return std::make_unique<protocol::tpch::TpchConnectorProtocol>();
}

std::unique_ptr<velox::connector::ConnectorSplit>
TpcdsPrestoToVeloxConnector::toVeloxSplit(
    const protocol::ConnectorId& catalogId,
    const protocol::ConnectorSplit* connectorSplit,
    const protocol::SplitContext* splitContext) const {
  auto tpcdsSplit =
      dynamic_cast<const protocol::tpcds::TpcdsSplit*>(connectorSplit);
  VELOX_CHECK_NOT_NULL(
      tpcdsSplit, "Unexpected split type {}", connectorSplit->_type);
  return std::make_unique<connector::tpcds::TpcdsConnectorSplit>(
      catalogId,
      splitContext->cacheable,
      tpcdsSplit->totalParts,
      tpcdsSplit->partNumber);
}

std::unique_ptr<velox::connector::ColumnHandle>
TpcdsPrestoToVeloxConnector::toVeloxColumnHandle(
    const protocol::ColumnHandle* column,
    const TypeParser& typeParser) const {
  auto tpcdsColumn =
      dynamic_cast<const protocol::tpcds::TpcdsColumnHandle*>(column);
  VELOX_CHECK_NOT_NULL(
      tpcdsColumn, "Unexpected column handle type {}", column->_type);
  return std::make_unique<connector::tpcds::TpcdsColumnHandle>(
      tpcdsColumn->columnName);
}

std::unique_ptr<velox::connector::ConnectorTableHandle>
TpcdsPrestoToVeloxConnector::toVeloxTableHandle(
    const protocol::TableHandle& tableHandle,
    const VeloxExprConverter& exprConverter,
    const TypeParser& typeParser) const {
  auto tpcdsLayout =
      std::dynamic_pointer_cast<const protocol::tpcds::TpcdsTableLayoutHandle>(
          tableHandle.connectorTableLayout);
  VELOX_CHECK_NOT_NULL(
      tpcdsLayout,
      "Unexpected layout type {}",
      tableHandle.connectorTableLayout->_type);
  return std::make_unique<connector::tpcds::TpcdsTableHandle>(
      tableHandle.connectorId,
      tpcds::fromTableName(tpcdsLayout->table.tableName),
      tpcdsLayout->table.scaleFactor);
}

std::unique_ptr<protocol::ConnectorProtocol>
TpcdsPrestoToVeloxConnector::createConnectorProtocol() const {
  return std::make_unique<protocol::tpcds::TpcdsConnectorProtocol>();
}
} // namespace facebook::presto
