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
namespace facebook::presto::protocol {

void to_json(json& j, const HivePageSinkMetadata& p) {
  j = json::object();
  to_json_key(
      j,
      "schemaTableName",
      p.schemaTableName,
      "HivePageSinkMetadata",
      "SchemaTableName",
      "schemaTableName");
  to_json_key(j, "table", p.table, "HivePageSinkMetadata", "Table", "table");
}

void from_json(const json& j, HivePageSinkMetadata& p) {
  from_json_key(
      j,
      "schemaTableName",
      p.schemaTableName,
      "HivePageSinkMetadata",
      "SchemaTableName",
      "schemaTableName");
  from_json_key(j, "table", p.table, "HivePageSinkMetadata", "Table", "table");
}
} // namespace facebook::presto::protocol
