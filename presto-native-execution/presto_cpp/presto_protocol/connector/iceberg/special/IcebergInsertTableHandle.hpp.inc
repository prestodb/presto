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

// IcebergInsertTableHandle is special since it needs an usage of
// hive::.

namespace facebook::presto::protocol::iceberg {
struct IcebergInsertTableHandle : public ConnectorInsertTableHandle {
  String schemaName = {};
  IcebergTableName tableName = {};
  PrestoIcebergSchema schema = {};
  PrestoIcebergPartitionSpec partitionSpec = {};
  List<IcebergColumnHandle> inputColumns = {};
  String outputPath = {};
  FileFormat fileFormat = {};
  hive::HiveCompressionCodec compressionCodec = {};
  Map<String, String> storageProperties = {};
  List<SortField> sortOrder = {};

  IcebergInsertTableHandle() noexcept;
};
void to_json(json& j, const IcebergInsertTableHandle& p);
void from_json(const json& j, IcebergInsertTableHandle& p);
} // namespace facebook::presto::protocol::iceberg
