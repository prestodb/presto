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

// IcebergSplit is special since it needs an usage of
// hive::.

namespace facebook::presto::protocol::iceberg {
struct IcebergSplit : public ConnectorSplit {
  String path = {};
  int64_t start = {};
  int64_t length = {};
  FileFormat fileFormat = {};
  List<HostAddress> addresses = {};
  Map<Integer, hive::HivePartitionKey> partitionKeys = {};
  String partitionSpecAsJson = {};
  std::shared_ptr<String> partitionDataJson = {};
  NodeSelectionStrategy nodeSelectionStrategy = {};
  SplitWeight splitWeight = {};
  List<DeleteFile> deletes = {};
  std::shared_ptr<ChangelogSplitInfo> changelogSplitInfo = {};
  int64_t dataSequenceNumber = {};
  int64_t affinitySchedulingSectionSize = {};

  IcebergSplit() noexcept;
};
void to_json(json& j, const IcebergSplit& p);
void from_json(const json& j, IcebergSplit& p);
} // namespace facebook::presto::protocol::iceberg
