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
namespace facebook::presto::protocol::iceberg {

IcebergDeleteTableHandle::IcebergDeleteTableHandle() noexcept {
  _type = "hive-iceberg";
}

void to_json(json& j, const IcebergDeleteTableHandle& p) {
  j = json::object();
  j["@type"] = "hive-iceberg";
  to_json_key(
      j,
      "schemaName",
      p.schemaName,
      "IcebergDeleteTableHandle",
      "String",
      "schemaName");
  to_json_key(
      j,
      "tableName",
      p.tableName,
      "IcebergDeleteTableHandle",
      "IcebergTableName",
      "tableName");
  to_json_key(
      j,
      "schema",
      p.schema,
      "IcebergDeleteTableHandle",
      "PrestoIcebergSchema",
      "schema");
  to_json_key(
      j,
      "partitionSpec",
      p.partitionSpec,
      "IcebergDeleteTableHandle",
      "PrestoIcebergPartitionSpec",
      "partitionSpec");
  to_json_key(
      j,
      "inputColumns",
      p.inputColumns,
      "IcebergDeleteTableHandle",
      "List<IcebergColumnHandle>",
      "inputColumns");
  to_json_key(
      j,
      "outputPath",
      p.outputPath,
      "IcebergDeleteTableHandle",
      "String",
      "outputPath");
  to_json_key(
      j,
      "fileFormat",
      p.fileFormat,
      "IcebergDeleteTableHandle",
      "FileFormat",
      "fileFormat");
  to_json_key(
      j,
      "compressionCodec",
      p.compressionCodec,
      "IcebergDeleteTableHandle",
      "HiveCompressionCodec",
      "compressionCodec");
  to_json_key(
      j,
      "storageProperties",
      p.storageProperties,
      "IcebergDeleteTableHandle",
      "Map<String, String>",
      "storageProperties");
  to_json_key(
      j,
      "sortOrder",
      p.sortOrder,
      "IcebergDeleteTableHandle",
      "List<SortField>",
      "sortOrder");
  to_json_key(
      j,
      "materializedViewName",
      p.materializedViewName,
      "IcebergDeleteTableHandle",
      "SchemaTableName",
      "materializedViewName");
  to_json_key(
      j,
      "fileContent",
      p.fileContent,
      "IcebergDeleteTableHandle",
      "FileContent",
      "fileContent");
  to_json_key(
      j,
      "existingDeletionVectors",
      p.existingDeletionVectors,
      "IcebergDeleteTableHandle",
      "Map<String, DeleteFile>",
      "existingDeletionVectors");
}

void from_json(const json& j, IcebergDeleteTableHandle& p) {
  p._type = j["@type"];
  from_json_key(
      j,
      "schemaName",
      p.schemaName,
      "IcebergDeleteTableHandle",
      "String",
      "schemaName");
  from_json_key(
      j,
      "tableName",
      p.tableName,
      "IcebergDeleteTableHandle",
      "IcebergTableName",
      "tableName");
  from_json_key(
      j,
      "schema",
      p.schema,
      "IcebergDeleteTableHandle",
      "PrestoIcebergSchema",
      "schema");
  from_json_key(
      j,
      "partitionSpec",
      p.partitionSpec,
      "IcebergDeleteTableHandle",
      "PrestoIcebergPartitionSpec",
      "partitionSpec");
  from_json_key(
      j,
      "inputColumns",
      p.inputColumns,
      "IcebergDeleteTableHandle",
      "List<IcebergColumnHandle>",
      "inputColumns");
  from_json_key(
      j,
      "outputPath",
      p.outputPath,
      "IcebergDeleteTableHandle",
      "String",
      "outputPath");
  from_json_key(
      j,
      "fileFormat",
      p.fileFormat,
      "IcebergDeleteTableHandle",
      "FileFormat",
      "fileFormat");
  from_json_key(
      j,
      "compressionCodec",
      p.compressionCodec,
      "IcebergDeleteTableHandle",
      "HiveCompressionCodec",
      "compressionCodec");
  from_json_key(
      j,
      "storageProperties",
      p.storageProperties,
      "IcebergDeleteTableHandle",
      "Map<String, String>",
      "storageProperties");
  from_json_key(
      j,
      "sortOrder",
      p.sortOrder,
      "IcebergDeleteTableHandle",
      "List<SortField>",
      "sortOrder");
  from_json_key(
      j,
      "materializedViewName",
      p.materializedViewName,
      "IcebergDeleteTableHandle",
      "SchemaTableName",
      "materializedViewName");
  from_json_key(
      j,
      "fileContent",
      p.fileContent,
      "IcebergDeleteTableHandle",
      "FileContent",
      "fileContent");
  from_json_key(
      j,
      "existingDeletionVectors",
      p.existingDeletionVectors,
      "IcebergDeleteTableHandle",
      "Map<String, DeleteFile>",
      "existingDeletionVectors");
}

} // namespace facebook::presto::protocol::iceberg
