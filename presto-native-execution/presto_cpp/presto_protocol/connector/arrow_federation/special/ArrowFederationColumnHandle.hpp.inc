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
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
namespace facebook::presto::protocol::arrow_federation {

struct ArrowFederationColumnHandle : public ColumnHandle {
  String connectorId = {};
  String columnName = {};
  String columnType = {};
  boolean nullable = {};
  JdbcTypeHandle jdbcTypeHandle = {};
};

void to_json(json& j, const ArrowFederationColumnHandle& p) {
  j = json::object();
  j["@type"] = "arrow-federation";
  to_json_key(
      j,
      "connectorId",
      p.connectorId,
      "ArrowFederationColumnHandle",
      "ConnectorId",
      "connectorId");
  to_json_key(
      j,
      "columnName",
      p.columnName,
      "ArrowFederationColumnHandle",
      "ColumnName",
      "columnName");
  to_json_key(
      j,
      "columnType",
      p.columnType,
      "ArrowFederationColumnHandle",
      "ColumnType",
      "columnType");
  to_json_key(
      j,
      "nullable",
      p.nullable,
      "ArrowFederationColumnHandle",
      "Nullable",
      "nullable");
  to_json_key(
      j,
      "jdbcTypeHandle",
      p.jdbcTypeHandle,
      "ArrowFederationColumnHandle",
      "JdbcTypeHandle",
      "jdbcTypeHandle");
}

void from_json(const json& j, ArrowFederationColumnHandle& p) {
  p._type = j["@type"];
  from_json_key(
      j,
      "connectorId",
      p.connectorId,
      "ArrowFederationColumnHandle",
      "ConnectorId",
      "connectorId");
  from_json_key(
      j,
      "columnName",
      p.columnName,
      "ArrowFederationColumnHandle",
      "ColumnName",
      "columnName");
  from_json_key(
      j,
      "columnType",
      p.columnType,
      "ArrowFederationColumnHandle",
      "ColumnType",
      "columnType");
  from_json_key(
      j,
      "nullable",
      p.nullable,
      "ArrowFederationColumnHandle",
      "Nullable",
      "nullable");
  from_json_key(
      j,
      "jdbcTypeHandle",
      p.jdbcTypeHandle,
      "ArrowFederationColumnHandle",
      "JdbcTypeHandle",
      "jdbcTypeHandle");
}
} // namespace facebook::presto::protocol::arrow_federation
