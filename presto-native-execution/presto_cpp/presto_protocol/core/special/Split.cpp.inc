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

void to_json(json& j, const Split& p) {
  j = json::object();
  to_json_key(
      j, "connectorId", p.connectorId, "Split", "ConnectorId", "connectorId");
  to_json_key(
      j,
      "transactionHandle",
      p.transactionHandle,
      "Split",
      "ConnectorTransactionHandle",
      "transactionHandle");
  to_json_key(
      j,
      "connectorSplit",
      p.connectorSplit,
      "Split",
      "ConnectorSplit",
      "connectorSplit");
  to_json_key(j, "lifespan", p.lifespan, "Split", "Lifespan", "lifespan");
  to_json_key(
      j,
      "splitContext",
      p.splitContext,
      "Split",
      "SplitContext",
      "splitContext");
}

void from_json(const json& j, Split& p) {
  if (!j.contains("connectorId")) {
    throw ParseError("Missing required field: connectorID");
  }

  json modifiedJson = j;
  if (j.contains("transactionHandle") && j["transactionHandle"].is_array()) {
    std::string instance = j["transactionHandle"][1].get<std::string>();
    modifiedJson["transactionHandle"] =
        json::array({j["connectorId"], instance});
  }

  modifiedJson["connectorSplit"]["@type"] = j["connectorId"];

  const json& json = modifiedJson;

  from_json_key(
      json,
      "connectorId",
      p.connectorId,
      "Split",
      "ConnectorId",
      "connectorId");
  from_json_key(
      json,
      "transactionHandle",
      p.transactionHandle,
      "Split",
      "ConnectorTransactionHandle",
      "transactionHandle");
  from_json_key(
      json,
      "connectorSplit",
      p.connectorSplit,
      "Split",
      "ConnectorSplit",
      "connectorSplit");
  from_json_key(j, "lifespan", p.lifespan, "Split", "Lifespan", "lifespan");
  from_json_key(
      j,
      "splitContext",
      p.splitContext,
      "Split",
      "SplitContext",
      "splitContext");
}
} // namespace facebook::presto::protocol
                                         