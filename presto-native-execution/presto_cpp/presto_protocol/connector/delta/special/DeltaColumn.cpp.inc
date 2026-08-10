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

// DeltaColumn is special since only a subset of the Java fields is used and
// the optional ones are carried as pointers.

namespace facebook::presto::protocol::delta {

void to_json(json& j, const DeltaColumn& p) {
  j = json::object();
  if (p.id) {
    to_json_key(j, "id", *p.id, "DeltaColumn", "int64_t", "id");
  }
  if (p.physicalName) {
    to_json_key(
        j,
        "physicalName",
        *p.physicalName,
        "DeltaColumn",
        "std::string",
        "physicalName");
  }
  to_json_key(
      j,
      "logicalName",
      p.logicalName,
      "DeltaColumn",
      "std::string",
      "logicalName");
  to_json_key(j, "type", p.type, "DeltaColumn", "std::string", "type");
  to_json_key(j, "nullable", p.nullable, "DeltaColumn", "bool", "nullable");
  to_json_key(j, "partition", p.partition, "DeltaColumn", "bool", "partition");
}

void from_json(const json& j, DeltaColumn& p) {
  // Optional fields - only deserialize if present
  if (j.contains("id")) {
    p.id = std::make_shared<int64_t>();
    from_json_key(j, "id", *p.id, "DeltaColumn", "int64_t", "id");
  }
  if (j.contains("physicalName")) {
    p.physicalName = std::make_shared<std::string>();
    from_json_key(
        j,
        "physicalName",
        *p.physicalName,
        "DeltaColumn",
        "std::string",
        "physicalName");
  }
  // Required fields
  from_json_key(
      j,
      "logicalName",
      p.logicalName,
      "DeltaColumn",
      "std::string",
      "logicalName");
  from_json_key(j, "type", p.type, "DeltaColumn", "std::string", "type");
  from_json_key(j, "nullable", p.nullable, "DeltaColumn", "bool", "nullable");
  from_json_key(
      j, "partition", p.partition, "DeltaColumn", "bool", "partition");
}

} // namespace facebook::presto::protocol::delta
