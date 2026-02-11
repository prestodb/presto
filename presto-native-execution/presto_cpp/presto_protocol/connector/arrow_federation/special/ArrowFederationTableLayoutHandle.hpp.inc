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

struct ArrowFederationTableLayoutHandle : public ConnectorTableLayoutHandle {
  String tableLayoutHandleBytes = {};
};
void to_json(json& j, const ArrowFederationTableLayoutHandle& p) {
  j = json::parse(folly::base64Decode(p.tableLayoutHandleBytes));
  j["@type"] = "arrow-federation";
}

void from_json(const json& j, ArrowFederationTableLayoutHandle& p) {
  p._type = j["@type"];
  p.tableLayoutHandleBytes = folly::base64Encode(j.dump());;
}

} // namespace facebook::presto::protocol::arrow_federation
