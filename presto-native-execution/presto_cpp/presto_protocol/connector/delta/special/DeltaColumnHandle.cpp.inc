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

// DeltaColumnHandle is special since it needs to use "columnName" as the JSON
// key instead of "name" to match the Java @JsonProperty annotation.

namespace facebook::presto::protocol::delta {

DeltaColumnHandle::DeltaColumnHandle() noexcept {
  _type = "hive-delta";
}

void to_json(json& j, const DeltaColumnHandle& p) {
  j = json::object();
  j["@type"] = "hive-delta";
  to_json_key(j, "columnName", p.name, "DeltaColumnHandle", "String", "name");
  to_json_key(
      j,
      "dataType",
      p.dataType,
      "DeltaColumnHandle",
      "TypeSignature",
      "dataType");
  to_json_key(
      j,
      "columnType",
      p.columnType,
      "DeltaColumnHandle",
      "ColumnType",
      "columnType");
  to_json_key(
      j, "subfield", p.subfield, "DeltaColumnHandle", "Subfield", "subfield");
}

void from_json(const json& j, DeltaColumnHandle& p) {
  p._type = j["@type"];
  from_json_key(j, "columnName", p.name, "DeltaColumnHandle", "String", "name");
  from_json_key(
      j,
      "dataType",
      p.dataType,
      "DeltaColumnHandle",
      "TypeSignature",
      "dataType");
  from_json_key(
      j,
      "columnType",
      p.columnType,
      "DeltaColumnHandle",
      "ColumnType",
      "columnType");
  from_json_key(
      j, "subfield", p.subfield, "DeltaColumnHandle", "Subfield", "subfield");
}

} // namespace facebook::presto::protocol::delta
