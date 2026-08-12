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
  // The concrete com.facebook.presto.spi.MergeHandle (held by MergeTarget) is a
  // non-polymorphic data class serialized without an "@type" key, whereas the
  // polymorphic ExecutionWriterTarget.MergeHandle carries one. Protocol codegen
  // conflates both into this single struct, so tolerate a missing "@type": the
  // constructor already defaults _type to "MergeHandle". Without this, an
  // UPDATE/MERGE fails on the worker with json type_error.302 (the missing-key
  // read surfaces as "type must be string, but is number").
  if (j.contains("@type")) {
    p._type = j["@type"];
  }
  from_json_key(
      j,
      "tableHandle",
      p.tableHandle,
      "MergeHandle",
      "TableHandle",
      "tableHandle");
  from_json_key(
      j,
      "connectorMergeTableHandle",
      p.connectorMergeTableHandle,
      "MergeHandle",
      "ConnectorMergeTableHandle",
      "connectorMergeTableHandle");
}
} // namespace facebook::presto::protocol
