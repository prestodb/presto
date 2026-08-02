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

namespace facebook::presto::protocol::delta {
DeltaSplit::DeltaSplit() noexcept {
  _type = "hive-delta";
}

void to_json(json& j, const DeltaSplit& p) {
  j = json::object();
  j["@type"] = "hive-delta";
  to_json_key(
      j, "connectorId", p.connectorId, "DeltaSplit", "String", "connectorId");
  to_json_key(
      j, "schemaName", p.schemaName, "DeltaSplit", "String", "schemaName");
  to_json_key(j, "tableName", p.tableName, "DeltaSplit", "String", "tableName");
  to_json_key(j, "tableLocation", p.tableLocation, "DeltaSplit", "String", "tableLocation");
  to_json_key(j, "filePath", p.filePath, "DeltaSplit", "String", "filePath");
  to_json_key(j, "start", p.start, "DeltaSplit", "int64_t", "start");
  to_json_key(j, "length", p.length, "DeltaSplit", "int64_t", "length");
  to_json_key(j, "fileSize", p.fileSize, "DeltaSplit", "int64_t", "fileSize");
  to_json_key(
      j,
      "partitionValues",
      p.partitionValues,
      "DeltaSplit",
      "Map<String, String>",
      "partitionValues");
}

void from_json(const json& j, DeltaSplit& p) {
  p._type = j["@type"];
  from_json_key(
      j, "connectorId", p.connectorId, "DeltaSplit", "String", "connectorId");
  from_json_key(
      j, "schemaName", p.schemaName, "DeltaSplit", "String", "schemaName");
  from_json_key(
      j, "tableName", p.tableName, "DeltaSplit", "String", "tableName");
  from_json_key(j, "tableLocation", p.tableLocation, "DeltaSplit", "String", "tableLocation");
  from_json_key(j, "filePath", p.filePath, "DeltaSplit", "String", "filePath");
  from_json_key(j, "start", p.start, "DeltaSplit", "int64_t", "start");
  from_json_key(j, "length", p.length, "DeltaSplit", "int64_t", "length");
  from_json_key(j, "fileSize", p.fileSize, "DeltaSplit", "int64_t", "fileSize");
  from_json_key(
      j,
      "partitionValues",
      p.partitionValues,
      "DeltaSplit",
      "Map<String, String>",
      "partitionValues");
}
} // namespace facebook::presto::protocol::delta
