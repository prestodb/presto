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

// IcebergDistributedProcedureHandle is special since it needs an usage of
// hive::.

namespace facebook::presto::protocol::iceberg {
struct IcebergDistributedProcedureHandle
  : public ConnectorDistributedProcedureHandle {
  String schemaName = {};
  IcebergTableName tableName = {};
  PrestoIcebergSchema schema = {};
  PrestoIcebergPartitionSpec partitionSpec = {};
  List<IcebergColumnHandle> inputColumns = {};
  String outputPath = {};
  FileFormat fileFormat = {};
  hive::HiveCompressionCodec compressionCodec = {};
  Map<String, String> storageProperties = {};
  IcebergTableLayoutHandle tableLayoutHandle = {};
  Map<String, String> relevantData = {};

  IcebergDistributedProcedureHandle() noexcept;
};
void to_json(json& j, const IcebergDistributedProcedureHandle& p);
void from_json(const json& j, IcebergDistributedProcedureHandle& p);
} // namespace facebook::presto::protocol::iceberg
