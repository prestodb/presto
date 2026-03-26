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

#include "presto_cpp/presto_protocol/connector/lance/presto_protocol_lance.h"

namespace facebook::presto::protocol::lance {

// LanceColumnHandle

LanceColumnHandle::LanceColumnHandle() noexcept {
  _type = "lance";
}

void to_json(json& j, const LanceColumnHandle& p) {
  j = json::object();
  j["@type"] = "lance";
  to_json_key(
      j,
      "columnName",
      p.columnName,
      "LanceColumnHandle",
      "String",
      "columnName");
  to_json_key(
      j, "columnType", p.columnType, "LanceColumnHandle", "Type", "columnType");
  to_json_key(
      j, "nullable", p.nullable, "LanceColumnHandle", "boolean", "nullable");
}

void from_json(const json& j, LanceColumnHandle& p) {
  from_json_key(
      j,
      "columnName",
      p.columnName,
      "LanceColumnHandle",
      "String",
      "columnName");
  from_json_key(
      j, "columnType", p.columnType, "LanceColumnHandle", "Type", "columnType");
  from_json_key(
      j, "nullable", p.nullable, "LanceColumnHandle", "boolean", "nullable");
}

// LanceTableHandle

LanceTableHandle::LanceTableHandle() noexcept {
  _type = "lance";
}

void to_json(json& j, const LanceTableHandle& p) {
  j = json::object();
  j["@type"] = "lance";
  to_json_key(
      j,
      "schemaName",
      p.schemaName,
      "LanceTableHandle",
      "String",
      "schemaName");
  to_json_key(
      j, "tableName", p.tableName, "LanceTableHandle", "String", "tableName");
}

void from_json(const json& j, LanceTableHandle& p) {
  from_json_key(
      j,
      "schemaName",
      p.schemaName,
      "LanceTableHandle",
      "String",
      "schemaName");
  from_json_key(
      j, "tableName", p.tableName, "LanceTableHandle", "String", "tableName");
}

// LanceTableLayoutHandle

LanceTableLayoutHandle::LanceTableLayoutHandle() noexcept {
  _type = "lance";
}

void to_json(json& j, const LanceTableLayoutHandle& p) {
  j = json::object();
  j["@type"] = "lance";
  to_json_key(
      j,
      "table",
      p.table,
      "LanceTableLayoutHandle",
      "LanceTableHandle",
      "table");
  to_json_key(
      j,
      "tupleDomain",
      p.tupleDomain,
      "LanceTableLayoutHandle",
      "TupleDomain<ColumnHandle>",
      "tupleDomain");
}

void from_json(const json& j, LanceTableLayoutHandle& p) {
  from_json_key(
      j,
      "table",
      p.table,
      "LanceTableLayoutHandle",
      "LanceTableHandle",
      "table");
  from_json_key(
      j,
      "tupleDomain",
      p.tupleDomain,
      "LanceTableLayoutHandle",
      "TupleDomain<ColumnHandle>",
      "tupleDomain");
}

// LanceSplit

LanceSplit::LanceSplit() noexcept {
  _type = "lance";
}

void to_json(json& j, const LanceSplit& p) {
  j = json::object();
  j["@type"] = "lance";
  to_json_key(
      j, "datasetPath", p.datasetPath, "LanceSplit", "String", "datasetPath");
  to_json_key(
      j, "fragments", p.fragments, "LanceSplit", "List<int>", "fragments");
}

void from_json(const json& j, LanceSplit& p) {
  from_json_key(
      j, "datasetPath", p.datasetPath, "LanceSplit", "String", "datasetPath");
  from_json_key(
      j, "fragments", p.fragments, "LanceSplit", "List<int>", "fragments");
}

// LanceTransactionHandle

LanceTransactionHandle::LanceTransactionHandle() noexcept {
  _type = "lance";
}

void to_json(json& j, const LanceTransactionHandle& p) {
  j = json::object();
  j["@type"] = "lance";
}

void from_json(const json& j, LanceTransactionHandle& p) {
  // No fields to deserialize
}

} // namespace facebook::presto::protocol::lance
