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

namespace facebook::presto::protocol::tpch {
TpchSplit::TpchSplit() noexcept {
  _type = "tpch";
}

void to_json(json& j, const TpchSplit& p) {
  j = json::object();
  j["@type"] = "tpch";
  to_json_key(
      j,
      "tableHandle",
      p.tableHandle,
      "TpchSplit",
      "TpchTableHandle",
      "tableHandle");
  to_json_key(j, "partNumber", p.partNumber, "TpchSplit", "int", "partNumber");
  to_json_key(j, "totalParts", p.totalParts, "TpchSplit", "int", "totalParts");
  to_json_key(
      j,
      "addresses",
      p.addresses,
      "TpchSplit",
      "List<HostAddress>",
      "addresses");
  to_json_key(
      j,
      "predicate",
      p.predicate,
      "TpchSplit",
      "TupleDomain<std::shared_ptr<ColumnHandle>>",
      "predicate");
}

void from_json(const json& j, TpchSplit& p) {
  p._type = j["@type"];

  json modifiedJson = j;

  modifiedJson["tableHandle"]["@type"] = p._type;

  // Set the connectorId in the columnDomains in predicate
  for (auto& [exp, colDomain] :
       modifiedJson["predicate"]["columnDomains"].items()) {
    colDomain["column"]["@type"] = p._type;
  }

  const json& json = modifiedJson;

  from_json_key(
      json,
      "tableHandle",
      p.tableHandle,
      "TpchSplit",
      "TpchTableHandle",
      "tableHandle");
  from_json_key(
      j, "partNumber", p.partNumber, "TpchSplit", "int", "partNumber");
  from_json_key(
      j, "totalParts", p.totalParts, "TpchSplit", "int", "totalParts");
  from_json_key(
      j,
      "addresses",
      p.addresses,
      "TpchSplit",
      "List<HostAddress>",
      "addresses");
  from_json_key(
      json,
      "predicate",
      p.predicate,
      "TpchSplit",
      "TupleDomain<std::shared_ptr<ColumnHandle>>",
      "predicate");
}
} // namespace facebook::presto::protocol::tpch
                                               