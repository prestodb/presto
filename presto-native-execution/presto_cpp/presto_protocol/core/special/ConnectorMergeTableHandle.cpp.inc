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
// Route ConnectorMergeTableHandle through the registered connector protocol
// (mirrors ConnectorDeleteTableHandle). Without this the default codegen
// hard-throws "no abstract type ConnectorMergeTableHandle", which breaks
// UPDATE/MERGE deserialization on the worker (MergeHandle.from_json ->
// from_json_key(connectorMergeTableHandle) reaches here). Unlike the Delete/
// Insert handles there is no deserialize(ConnectorMergeTableHandle) overload
// on ConnectorProtocol, so the customSerializedValue branch is omitted.
void to_json(json& j, const std::shared_ptr<ConnectorMergeTableHandle>& p) {
  if (p == nullptr) {
    return;
  }
  String type = p->_type;
  getConnectorProtocol(type).to_json(j, p);
}

void from_json(const json& j, std::shared_ptr<ConnectorMergeTableHandle>& p) {
  String type;
  try {
    type = p->getSubclassKey(j);
  } catch (json::parse_error& e) {
    throw ParseError(
        std::string(e.what()) +
        " ConnectorMergeTableHandle  ConnectorMergeTableHandle");
  }

  getConnectorProtocol(type).from_json(j, p);
}
} // namespace facebook::presto::protocol
