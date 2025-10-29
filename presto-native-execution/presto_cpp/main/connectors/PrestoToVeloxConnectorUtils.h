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

#include "presto_cpp/main/types/PrestoToVeloxExpr.h"
#include "presto_cpp/presto_protocol/connector/hive/presto_protocol_hive.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/dwio/common/Options.h"
#include "velox/type/Filter.h"
#include "velox/type/Type.h"

namespace facebook::presto {

velox::TypePtr stringToType(
    const std::string& typeString,
    const TypeParser& typeParser);

template <velox::TypeKind KIND>
velox::TypePtr fieldNamesToLowerCase(const velox::TypePtr& type);

std::unique_ptr<velox::common::Filter> toFilter(
    const protocol::Domain& domain,
    const VeloxExprConverter& exprConverter,
    const TypeParser& typeParser);

template <typename T>
std::string toJsonString(const T& value) {
  return ((json)value).dump();
}

std::vector<velox::common::Subfield> toRequiredSubfields(
    const protocol::List<protocol::Subfield>& subfields);

velox::common::CompressionKind toFileCompressionKind(
    const protocol::hive::HiveCompressionCodec& hiveCompressionCodec);

velox::connector::hive::HiveColumnHandle::ColumnType toHiveColumnType(
    protocol::hive::ColumnType type);

std::unique_ptr<velox::connector::ConnectorTableHandle> toHiveTableHandle(
    const protocol::TupleDomain<protocol::Subfield>& domainPredicate,
    const std::shared_ptr<protocol::RowExpression>& remainingPredicate,
    bool isPushdownFilterEnabled,
    const std::string& tableName,
    const protocol::List<protocol::Column>& dataColumns,
    const protocol::TableHandle& tableHandle,
    const protocol::Map<protocol::String, protocol::String>& tableParameters,
    const VeloxExprConverter& exprConverter,
    const TypeParser& typeParser);

} // namespace facebook::presto
