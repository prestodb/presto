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

// dependency String
// ArrowFederationColumnHandle is special since it needs an implementation of
// operator<().

namespace facebook::presto::protocol::arrow_federation {

struct ArrowFederationColumnHandle : public ColumnHandle {
  String columnHandleBytes = {};
  String columnName = {};
};

void to_json(json& j, const ArrowFederationColumnHandle& p) {
  j["@type"] = "arrow-federation";
  to_json_key(
      j,
      "columnName",
      p.columnName,
      "ArrowFederationTableHandle",
      "ColumnName",
      "columnName");
  auto columnHandle = folly::base64Decode(p.columnHandleBytes);
  j = json::parse(columnHandle);
}

void from_json(const json& j, ArrowFederationColumnHandle& p) {
  p._type = j["@type"];
  if (j.contains("columnName")) {
    from_json_key(
        j,
        "columnName",
        p.columnName,
        "ArrowFederationTableHandle",
        "ColumnName",
        "columnName");
  }
  else {
  from_json_key(
      j,
      "name",
      p.columnName,
      "ArrowFederationTableHandle",
      "Name",
      "name");
  }
  p.columnHandleBytes = folly::base64Encode(j.dump());
}

} // namespace facebook::presto::protocol::arrow_federation
