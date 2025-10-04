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
#include <string>
namespace facebook::presto::protocol {

void to_json(json& j, const TableHandle& p) {
  j = json::object();
  to_json_key(
      j,
      "connectorId",
      p.connectorId,
      "TableHandle",
      "ConnectorId",
      "connectorId");
  to_json_key(
      j,
      "connectorHandle",
      p.connectorHandle,
      "TableHandle",
      "ConnectorTableHandle",
      "connectorHandle");
  to_json_key(
      j,
      "transaction",
      p.transaction,
      "TableHandle",
      "ConnectorTransactionHandle",
      "transaction");
  to_json_key(
      j,
      "connectorTableLayout",
      p.connectorTableLayout,
      "TableHandle",
      "ConnectorTableLayoutHandle",
      "connectorTableLayout");
}

void from_json(const json& j, TableHandle& p) {
    if (!j.contains("connectorId")) {
        throw ParseError("Missing required field: connectorID");
    }

    json modifiedJson = j;
    if (j.contains("transaction") && j["transaction"].is_array()) {
        std::string instance = j["transaction"][1].get<std::string>();
        modifiedJson["transaction"] =
            json::array({j["connectorId"], instance});
    }

    modifiedJson["connectorHandle"]["@type"] = j["connectorId"];
    modifiedJson["connectorTableLayout"]["@type"] = j["connectorId"];

    const json& json = modifiedJson;  
    from_json_key(
      j,
      "connectorId",
      p.connectorId,
      "TableHandle",
      "ConnectorId",
      "connectorId");
  from_json_key(
      json,
      "connectorHandle",
      p.connectorHandle,
      "TableHandle",
      "ConnectorTableHandle",
      "connectorHandle");
  from_json_key(
      json,
      "transaction",
      p.transaction,
      "TableHandle",
      "ConnectorTransactionHandle",
      "transaction");
  from_json_key(
      json,
      "connectorTableLayout",
      p.connectorTableLayout,
      "TableHandle",
      "ConnectorTableLayoutHandle",
      "connectorTableLayout");
}
} // namespace facebook::presto::protocol