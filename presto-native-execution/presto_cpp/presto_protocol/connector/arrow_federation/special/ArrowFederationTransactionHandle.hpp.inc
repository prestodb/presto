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

namespace facebook::presto::protocol::arrow_federation {

struct ArrowFederationTransactionHandle : public ConnectorTransactionHandle {
  String uuid = {};
};
void to_json(json& j, const ArrowFederationTransactionHandle& p) {
  j = json::object();
  j["@type"] = "arrow-federation";
  to_json_key(
      j, "uuid", p.uuid, "ArrowFederationTransactionHandle", "UUID", "uuid");
};
void from_json(const json& j, ArrowFederationTransactionHandle& p) {
  if (j.is_array()) {
    // enum is serialized as an array: ["type","instance"]
    p._type = j[0];
  } else {
    p._type = j["@type"];
    from_json_key(
      j, "uuid", p.uuid, "ArrowFederationTransactionHandle", "UUID", "uuid");
  }
};

} // namespace facebook::presto::protocol::arrow_federation
