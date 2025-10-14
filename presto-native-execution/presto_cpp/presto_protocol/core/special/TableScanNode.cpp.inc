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
#include <string>
namespace facebook::presto::protocol {

TableScanNode::TableScanNode() noexcept {
  _type = ".TableScanNode";
}

void to_json(json& j, const TableScanNode& p) {
  j = json::object();
  j["@type"] = ".TableScanNode";
  to_json_key(j, "id", p.id, "TableScanNode", "PlanNodeId", "id");
  to_json_key(j, "table", p.table, "TableScanNode", "TableHandle", "table");
  to_json_key(
      j,
      "outputVariables",
      p.outputVariables,
      "TableScanNode",
      "List<VariableReferenceExpression>",
      "outputVariables");
  to_json_key(
      j,
      "assignments",
      p.assignments,
      "TableScanNode",
      "Map<VariableReferenceExpression, std::shared_ptr<ColumnHandle>>",
      "assignments");
}

void from_json(const json& j, TableScanNode& p) {
  p._type = j["@type"];
  from_json_key(j, "id", p.id, "TableScanNode", "PlanNodeId", "id");

  // Initialize a TableHandle to retrieve the connectorId
  auto k = std::make_shared<TableHandle>();
  j["table"].get_to(*k);
  p.table = *k;
  String connectorId = k->connectorId;

  json modifiedJson = j;
  // Set the connectorId in the ColumnHandles in assignments
  for (auto& [exp, columnHandleJson] : modifiedJson["assignments"].items()) {
    columnHandleJson["@type"] = connectorId;
  }

  const json& json = modifiedJson;

  from_json_key(
      j,
      "outputVariables",
      p.outputVariables,
      "TableScanNode",
      "List<VariableReferenceExpression>",
      "outputVariables");
  from_json_key(
      json,
      "assignments",
      p.assignments,
      "TableScanNode",
      "Map<VariableReferenceExpression, std::shared_ptr<ColumnHandle>>",
      "assignments");
}
} // namespace facebook::presto::protocol
                                         