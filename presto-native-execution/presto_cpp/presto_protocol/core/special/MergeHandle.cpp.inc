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
MergeHandle::MergeHandle() noexcept {
  _type = "MergeHandle";
}

void to_json(json& j, const MergeHandle& p) {
  j = json::object();
  j["@type"] = "MergeHandle";
  to_json_key(
      j,
      "tableHandle",
      p.tableHandle,
      "MergeHandle",
      "TableHandle",
      "tableHandle");
  to_json_key(
      j,
      "connectorMergeTableHandle",
      p.connectorMergeTableHandle,
      "MergeHandle",
      "ConnectorMergeTableHandle",
      "connectorMergeTableHandle");
}

void from_json(const json& j, MergeHandle& p) {
  // Protocol codegen keys its class table by simple name, so the two Java
  // classes both called "MergeHandle" collapse into this one struct, and it is
  // reached with two different JSON shapes:
  //
  //   com.facebook.presto.spi.MergeHandle  (MergeTarget.mergeHandle)
  //     {"tableHandle": ..., "connectorMergeTableHandle": ...}   -- no "@type"
  //
  //   ExecutionWriterTarget.MergeHandle    (TableWriteInfo.writerTarget)
  //     {"@type": "MergeHandle", "handle": {<the spi.MergeHandle above>}}
  //
  // Tolerate the missing "@type" on the first: the constructor already defaults
  // _type, and without this an UPDATE/MERGE fails with json type_error.302.
  if (j.contains("@type")) {
    p._type = j["@type"];
  }
  // Unwrap the second. ExecutionWriterTarget.MergeHandle holds exactly one
  // field, so folding it into this struct loses nothing: consumers
  // (PrestoToVeloxQueryPlan, IcebergPrestoToVeloxConnector) only ever read
  // tableHandle / connectorMergeTableHandle. Without this, the wrapped form
  // fails with json out_of_range.403 "key 'tableHandle' not found".
  const json& fields = j.contains("handle") ? j.at("handle") : j;
  from_json_key(
      fields,
      "tableHandle",
      p.tableHandle,
      "MergeHandle",
      "TableHandle",
      "tableHandle");
  from_json_key(
      fields,
      "connectorMergeTableHandle",
      p.connectorMergeTableHandle,
      "MergeHandle",
      "ConnectorMergeTableHandle",
      "connectorMergeTableHandle");
}
} // namespace facebook::presto::protocol
