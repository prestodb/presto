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

void to_json(json& j, const PartitioningHandle& p) {
  j = json::object();
  to_json_key(
      j,
      "connectorId",
      p.connectorId,
      "PartitioningHandle",
      "ConnectorId",
      "connectorId");
  to_json_key(
      j,
      "transactionHandle",
      p.transactionHandle,
      "PartitioningHandle",
      "ConnectorTransactionHandle",
      "transactionHandle");
  to_json_key(
      j,
      "connectorHandle",
      p.connectorHandle,
      "PartitioningHandle",
      "ConnectorPartitioningHandle",
      "connectorHandle");
}

void from_json(const json& j, PartitioningHandle& p) {
  if (j["connectorHandle"]["@type"] == "$remote") {
    from_json_key(
        j,
        "connectorId",
        p.connectorId,
        "PartitioningHandle",
        "ConnectorId",
        "connectorId");

    from_json_key(
        j,
        "transactionHandle",
        p.transactionHandle,
        "PartitioningHandle",
        "ConnectorTransactionHandle",
        "transactionHandle");
    from_json_key(
        j,
        "connectorHandle",
        p.connectorHandle,
        "PartitioningHandle",
        "ConnectorPartitioningHandle",
        "connectorHandle");
  }

  else {
    if (!j.contains("connectorId")) {
      throw ParseError("Missing required field: connectorID");
    }

    json modifiedJson = j;
    if (j.contains("transactionHandle") && j["transactionHandle"].is_array()) {
      std::string instance = j["transactionHandle"][1].get<std::string>();
      modifiedJson["transactionHandle"] =
          json::array({j["connectorId"], instance});
    }

    modifiedJson["connectorHandle"]["@type"] = j["connectorId"];

    const json& json = modifiedJson;

    from_json_key(
        j,
        "connectorId",
        p.connectorId,
        "PartitioningHandle",
        "ConnectorId",
        "connectorId");

    from_json_key(
        json,
        "transactionHandle",
        p.transactionHandle,
        "PartitioningHandle",
        "ConnectorTransactionHandle",
        "transactionHandle");
    from_json_key(
        json,
        "connectorHandle",
        p.connectorHandle,
        "PartitioningHandle",
        "ConnectorPartitioningHandle",
        "connectorHandle");
  }
}
} // namespace facebook::presto::protocol
                                         