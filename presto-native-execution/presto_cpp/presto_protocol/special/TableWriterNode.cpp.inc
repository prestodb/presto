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
TableWriterNode::TableWriterNode() noexcept {
  _type = "com.facebook.presto.sql.planner.plan.TableWriterNode";
}

void to_json(json& j, const TableWriterNode& p) {
  j = json::object();
  j["@type"] = "com.facebook.presto.sql.planner.plan.TableWriterNode";
  to_json_key(j, "id", p.id, "TableWriterNode", "PlanNodeId", "id");
  to_json_key(j, "source", p.source, "TableWriterNode", "PlanNode", "source");
  to_json_key(
      j,
      "rowCountVariable",
      p.rowCountVariable,
      "TableWriterNode",
      "VariableReferenceExpression",
      "rowCountVariable");
  to_json_key(
      j,
      "fragmentVariable",
      p.fragmentVariable,
      "TableWriterNode",
      "VariableReferenceExpression",
      "fragmentVariable");
  to_json_key(
      j,
      "tableCommitContextVariable",
      p.tableCommitContextVariable,
      "TableWriterNode",
      "VariableReferenceExpression",
      "tableCommitContextVariable");
  to_json_key(
      j,
      "columns",
      p.columns,
      "TableWriterNode",
      "List<VariableReferenceExpression>",
      "columns");
  to_json_key(
      j,
      "columnNames",
      p.columnNames,
      "TableWriterNode",
      "List<String>",
      "columnNames");
  to_json_key(
      j,
      "notNullColumnVariables",
      p.notNullColumnVariables,
      "TableWriterNode",
      "List<VariableReferenceExpression>",
      "notNullColumnVariables");
  to_json_key(
      j,
      "partitioningScheme",
      p.partitioningScheme,
      "TableWriterNode",
      "PartitioningScheme",
      "partitioningScheme");
  to_json_key(
      j,
      "preferredShufflePartitioningScheme",
      p.preferredShufflePartitioningScheme,
      "TableWriterNode",
      "PartitioningScheme",
      "preferredShufflePartitioningScheme");
}

void from_json(const json& j, TableWriterNode& p) {
  p._type = j["@type"];
  from_json_key(j, "id", p.id, "TableWriterNode", "PlanNodeId", "id");
  from_json_key(j, "source", p.source, "TableWriterNode", "PlanNode", "source");
  from_json_key(
      j,
      "rowCountVariable",
      p.rowCountVariable,
      "TableWriterNode",
      "VariableReferenceExpression",
      "rowCountVariable");
  from_json_key(
      j,
      "fragmentVariable",
      p.fragmentVariable,
      "TableWriterNode",
      "VariableReferenceExpression",
      "fragmentVariable");
  from_json_key(
      j,
      "tableCommitContextVariable",
      p.tableCommitContextVariable,
      "TableWriterNode",
      "VariableReferenceExpression",
      "tableCommitContextVariable");
  from_json_key(
      j,
      "columns",
      p.columns,
      "TableWriterNode",
      "List<VariableReferenceExpression>",
      "columns");
  from_json_key(
      j,
      "columnNames",
      p.columnNames,
      "TableWriterNode",
      "List<String>",
      "columnNames");
  from_json_key(
      j,
      "notNullColumnVariables",
      p.notNullColumnVariables,
      "TableWriterNode",
      "List<VariableReferenceExpression>",
      "notNullColumnVariables");
  from_json_key(
      j,
      "partitioningScheme",
      p.partitioningScheme,
      "TableWriterNode",
      "PartitioningScheme",
      "partitioningScheme");
  from_json_key(
      j,
      "preferredShufflePartitioningScheme",
      p.preferredShufflePartitioningScheme,
      "TableWriterNode",
      "PartitioningScheme",
      "preferredShufflePartitioningScheme");
}
} // namespace facebook::presto::protocol
