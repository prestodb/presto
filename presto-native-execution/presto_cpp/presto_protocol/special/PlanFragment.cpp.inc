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

void to_json(json& j, const PlanFragment& p) {
  j = json::object();
  to_json_key(j, "id", p.id, "PlanFragment", "PlanFragmentId", "id");
  to_json_key(j, "root", p.root, "PlanFragment", "PlanNode", "root");
  to_json_key(
      j,
      "variables",
      p.variables,
      "PlanFragment",
      "List<VariableReferenceExpression>",
      "variables");
  to_json_key(
      j,
      "partitioning",
      p.partitioning,
      "PlanFragment",
      "PartitioningHandle",
      "partitioning");
  to_json_key(
      j,
      "tableScanSchedulingOrder",
      p.tableScanSchedulingOrder,
      "PlanFragment",
      "List<PlanNodeId>",
      "tableScanSchedulingOrder");
  to_json_key(
      j,
      "partitioningScheme",
      p.partitioningScheme,
      "PlanFragment",
      "PartitioningScheme",
      "partitioningScheme");
  to_json_key(
      j,
      "stageExecutionDescriptor",
      p.stageExecutionDescriptor,
      "PlanFragment",
      "StageExecutionDescriptor",
      "stageExecutionDescriptor");
  to_json_key(
      j,
      "outputTableWriterFragment",
      p.outputTableWriterFragment,
      "PlanFragment",
      "bool",
      "outputTableWriterFragment");
  to_json_key(
      j,
      "jsonRepresentation",
      p.jsonRepresentation,
      "PlanFragment",
      "String",
      "jsonRepresentation");
}

void from_json(const json& j, PlanFragment& p) {
  from_json_key(j, "id", p.id, "PlanFragment", "PlanFragmentId", "id");
  from_json_key(j, "root", p.root, "PlanFragment", "PlanNode", "root");
  from_json_key(
      j,
      "variables",
      p.variables,
      "PlanFragment",
      "List<VariableReferenceExpression>",
      "variables");
  from_json_key(
      j,
      "partitioning",
      p.partitioning,
      "PlanFragment",
      "PartitioningHandle",
      "partitioning");
  from_json_key(
      j,
      "tableScanSchedulingOrder",
      p.tableScanSchedulingOrder,
      "PlanFragment",
      "List<PlanNodeId>",
      "tableScanSchedulingOrder");
  from_json_key(
      j,
      "partitioningScheme",
      p.partitioningScheme,
      "PlanFragment",
      "PartitioningScheme",
      "partitioningScheme");
  from_json_key(
      j,
      "stageExecutionDescriptor",
      p.stageExecutionDescriptor,
      "PlanFragment",
      "StageExecutionDescriptor",
      "stageExecutionDescriptor");
  from_json_key(
      j,
      "outputTableWriterFragment",
      p.outputTableWriterFragment,
      "PlanFragment",
      "bool",
      "outputTableWriterFragment");
  from_json_key(
      j,
      "jsonRepresentation",
      p.jsonRepresentation,
      "PlanFragment",
      "String",
      "jsonRepresentation");
}
} // namespace facebook::presto::protocol