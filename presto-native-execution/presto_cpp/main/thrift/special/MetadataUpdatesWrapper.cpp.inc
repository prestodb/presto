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

void toThrift(const facebook::presto::protocol::MetadataUpdates& metadataUpdates, MetadataUpdatesWrapper& thriftMetadataUpdatesWrapper) {
  toThrift(metadataUpdates, *thriftMetadataUpdatesWrapper.metadataUpdates_ref());
}
void toThrift(const facebook::presto::protocol::MetadataUpdates& metadataUpdates, std::string& thriftMetadataUpdates) {
  json jsonMetadataUpdates = metadataUpdates;
  std::string str = jsonMetadataUpdates.dump();
  toThrift(str, thriftMetadataUpdates);
}
void fromThrift(const MetadataUpdatesWrapper& thriftMetadataUpdatesWrapper, facebook::presto::protocol::MetadataUpdates& metadataUpdates) {
  fromThrift(*thriftMetadataUpdatesWrapper.metadataUpdates_ref(), metadataUpdates);
}
void fromThrift(const std::string& thriftMetadataUpdates, facebook::presto::protocol::MetadataUpdates& metadataUpdates) {
  json j = json::parse(thriftMetadataUpdates);
  metadataUpdates = j;
}
