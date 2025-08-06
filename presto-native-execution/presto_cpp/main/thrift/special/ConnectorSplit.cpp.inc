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

void toThrift(const std::shared_ptr<facebook::presto::protocol::ConnectorSplit>& proto, ConnectorSplit& thrift) {
}
void fromThrift(const ConnectorSplit& thrift, std::shared_ptr<facebook::presto::protocol::ConnectorSplit>& proto) {
  if (thrift.connectorId().has_value() && thrift.customSerializedValue().has_value()) {
    if (thrift.connectorId() == "$remote") {
      auto protoRemoteSplit = std::make_shared<facebook::presto::protocol::RemoteSplit>();
      auto thriftRemoteSplit = std::make_shared<RemoteSplit>();
      thriftRead(thrift.customSerializedValue().value(), thriftRemoteSplit);
      facebook::presto::thrift::fromThrift(thriftRemoteSplit, protoRemoteSplit);
      proto = protoRemoteSplit;
      return;
    }
    facebook::presto::protocol::getConnectorProtocol(thrift.connectorId().value())
      .deserialize(thrift.customSerializedValue().value(), proto);
  } else if (thrift.jsonValue().has_value()) {
    json j = json::parse(thrift.jsonValue().value());
    from_json(j, proto);
  }
}
