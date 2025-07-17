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
struct ConnectorSplit : public JsonEncodedSubclass {
  static std::string serialize(ConnectorSplit& p) {
    VELOX_NYI("Serialization not implemented for ConnectorSplit");
  }
  static std::shared_ptr<ConnectorSplit> deserialize(const std::string& data, std::shared_ptr<ConnectorSplit> p) {
    VELOX_NYI("Deserialization not implemented for ConnectorSplit");
  }
};
void to_json(json& j, const std::shared_ptr<ConnectorSplit>& p);
void from_json(const json& j, std::shared_ptr<ConnectorSplit>& p);
} // namespace facebook::presto::protocol
