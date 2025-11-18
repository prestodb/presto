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
CallDistributedProcedureNode::CallDistributedProcedureNode() noexcept {
  _type = "com.facebook.presto.sql.planner.plan.CallDistributedProcedureNode";
}

void to_json(json& j, const CallDistributedProcedureNode& p) {
  j = json::object();
  j["@type"] =
      "com.facebook.presto.sql.planner.plan.CallDistributedProcedureNode";
  to_json_key(
      j, "id", p.id, "CallDistributedProcedureNode", "PlanNodeId", "id");
  to_json_key(
      j,
      "source",
      p.source,
      "CallDistributedProcedureNode",
      "PlanNode",
      "source");
  to_json_key(
      j,
      "rowCountVariable",
      p.rowCountVariable,
      "CallDistributedProcedureNode",
      "VariableReferenceExpression",
      "rowCountVariable");
  to_json_key(
      j,
      "fragmentVariable",
      p.fragmentVariable,
      "CallDistributedProcedureNode",
      "VariableReferenceExpression",
      "fragmentVariable");
  to_json_key(
      j,
      "tableCommitContextVariable",
      p.tableCommitContextVariable,
      "CallDistributedProcedureNode",
      "VariableReferenceExpression",
      "tableCommitContextVariable");
  to_json_key(
      j,
      "columns",
      p.columns,
      "CallDistributedProcedureNode",
      "List<VariableReferenceExpression>",
      "columns");
  to_json_key(
      j,
      "columnNames",
      p.columnNames,
      "CallDistributedProcedureNode",
      "List<String>",
      "columnNames");
  to_json_key(
      j,
      "notNullColumnVariables",
      p.notNullColumnVariables,
      "CallDistributedProcedureNode",
      "List<VariableReferenceExpression>",
      "notNullColumnVariables");
  to_json_key(
      j,
      "partitioningScheme",
      p.partitioningScheme,
      "CallDistributedProcedureNode",
      "PartitioningScheme",
      "partitioningScheme");
}

void from_json(const json& j, CallDistributedProcedureNode& p) {
  p._type = j["@type"];
  from_json_key(
      j, "id", p.id, "CallDistributedProcedureNode", "PlanNodeId", "id");
  from_json_key(
      j,
      "source",
      p.source,
      "CallDistributedProcedureNode",
      "PlanNode",
      "source");
  from_json_key(
      j,
      "rowCountVariable",
      p.rowCountVariable,
      "CallDistributedProcedureNode",
      "VariableReferenceExpression",
      "rowCountVariable");
  from_json_key(
      j,
      "fragmentVariable",
      p.fragmentVariable,
      "CallDistributedProcedureNode",
      "VariableReferenceExpression",
      "fragmentVariable");
  from_json_key(
      j,
      "tableCommitContextVariable",
      p.tableCommitContextVariable,
      "CallDistributedProcedureNode",
      "VariableReferenceExpression",
      "tableCommitContextVariable");
  from_json_key(
      j,
      "columns",
      p.columns,
      "CallDistributedProcedureNode",
      "List<VariableReferenceExpression>",
      "columns");
  from_json_key(
      j,
      "columnNames",
      p.columnNames,
      "CallDistributedProcedureNode",
      "List<String>",
      "columnNames");
  from_json_key(
      j,
      "notNullColumnVariables",
      p.notNullColumnVariables,
      "CallDistributedProcedureNode",
      "List<VariableReferenceExpression>",
      "notNullColumnVariables");
  from_json_key(
      j,
      "partitioningScheme",
      p.partitioningScheme,
      "CallDistributedProcedureNode",
      "PartitioningScheme",
      "partitioningScheme");
}
} // namespace facebook::presto::protocol
