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
IcebergSplit::IcebergSplit() noexcept {
  _type = "hive-iceberg";
}

void to_json(json& j, const IcebergSplit& p) {
  j = json::object();
  j["@type"] = "hive-iceberg";
  to_json_key(j, "path", p.path, "IcebergSplit", "String", "path");
  to_json_key(j, "start", p.start, "IcebergSplit", "int64_t", "start");
  to_json_key(j, "length", p.length, "IcebergSplit", "int64_t", "length");
  to_json_key(
      j, "fileFormat", p.fileFormat, "IcebergSplit", "FileFormat", "fileFormat");
  to_json_key(
      j,
      "addresses",
      p.addresses,
      "IcebergSplit",
      "List<HostAddress>",
      "addresses");
  to_json_key(
      j,
      "partitionKeys",
      p.partitionKeys,
      "IcebergSplit",
      "Map<Integer, HivePartitionKey>",
      "partitionKeys");
  to_json_key(
      j,
      "partitionSpecAsJson",
      p.partitionSpecAsJson,
      "IcebergSplit",
      "String",
      "partitionSpecAsJson");
  to_json_key(
      j,
      "partitionDataJson",
      p.partitionDataJson,
      "IcebergSplit",
      "String",
      "partitionDataJson");
  to_json_key(
      j,
      "nodeSelectionStrategy",
      p.nodeSelectionStrategy,
      "IcebergSplit",
      "NodeSelectionStrategy",
      "nodeSelectionStrategy");
  to_json_key(
      j,
      "splitWeight",
      p.splitWeight,
      "IcebergSplit",
      "SplitWeight",
      "splitWeight");
  to_json_key(
      j, "deletes", p.deletes, "IcebergSplit", "List<DeleteFile>", "deletes");
  to_json_key(
      j,
      "changelogSplitInfo",
      p.changelogSplitInfo,
      "IcebergSplit",
      "ChangelogSplitInfo",
      "changelogSplitInfo");
  to_json_key(
      j,
      "dataSequenceNumber",
      p.dataSequenceNumber,
      "IcebergSplit",
      "int64_t",
      "dataSequenceNumber");
  to_json_key(
      j, "firstRowId", p.firstRowId, "IcebergSplit", "int64_t", "firstRowId");
  to_json_key(
      j,
      "affinitySchedulingSectionSize",
      p.affinitySchedulingSectionSize,
      "IcebergSplit",
      "int64_t",
      "affinitySchedulingSectionSize");
}

void from_json(const json& j, IcebergSplit& p) {
  p._type = j["@type"];
  from_json_key(j, "path", p.path, "IcebergSplit", "String", "path");
  from_json_key(j, "start", p.start, "IcebergSplit", "int64_t", "start");
  from_json_key(j, "length", p.length, "IcebergSplit", "int64_t", "length");
  from_json_key(
      j,
      "fileFormat",
      p.fileFormat,
      "IcebergSplit",
      "FileFormat",
      "fileFormat");
  from_json_key(
      j,
      "addresses",
      p.addresses,
      "IcebergSplit",
      "List<HostAddress>",
      "addresses");
  from_json_key(
      j,
      "partitionKeys",
      p.partitionKeys,
      "IcebergSplit",
      "Map<Integer, HivePartitionKey>",
      "partitionKeys");
  from_json_key(
      j,
      "partitionSpecAsJson",
      p.partitionSpecAsJson,
      "IcebergSplit",
      "String",
      "partitionSpecAsJson");
  from_json_key(
      j,
      "partitionDataJson",
      p.partitionDataJson,
      "IcebergSplit",
      "String",
      "partitionDataJson");
  from_json_key(
      j,
      "nodeSelectionStrategy",
      p.nodeSelectionStrategy,
      "IcebergSplit",
      "NodeSelectionStrategy",
      "nodeSelectionStrategy");
  from_json_key(
      j,
      "splitWeight",
      p.splitWeight,
      "IcebergSplit",
      "SplitWeight",
      "splitWeight");
  from_json_key(
      j, "deletes", p.deletes, "IcebergSplit", "List<DeleteFile>", "deletes");
  from_json_key(
      j,
      "changelogSplitInfo",
      p.changelogSplitInfo,
      "IcebergSplit",
      "ChangelogSplitInfo",
      "changelogSplitInfo");
  // V3 fields: optional for backward compatibility with coordinators that
  // do not yet send them. Default values from the struct definition apply.
  if (j.count("dataSequenceNumber")) {
    from_json_key(
        j,
        "dataSequenceNumber",
        p.dataSequenceNumber,
        "IcebergSplit",
        "int64_t",
        "dataSequenceNumber");
  }
  if (j.count("firstRowId")) {
    from_json_key(
        j,
        "firstRowId",
        p.firstRowId,
        "IcebergSplit",
        "int64_t",
        "firstRowId");
  }
  if (j.count("affinitySchedulingSectionSize")) {
    from_json_key(
        j,
        "affinitySchedulingSectionSize",
        p.affinitySchedulingSectionSize,
        "IcebergSplit",
        "int64_t",
        "affinitySchedulingSectionSize");
  }
}
} // namespace facebook::presto::protocol::iceberg
