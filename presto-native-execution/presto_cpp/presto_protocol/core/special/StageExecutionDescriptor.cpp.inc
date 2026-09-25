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

void to_json(json& j, const StageExecutionDescriptor& p) {
  j = json::object();
  to_json_key(
      j,
      "stageExecutionStrategy",
      p.stageExecutionStrategy,
      "StageExecutionDescriptor",
      "StageExecutionStrategy",
      "stageExecutionStrategy");
  to_json_key(
      j,
      "groupedExecutionScanNodes",
      p.groupedExecutionScanNodes,
      "StageExecutionDescriptor",
      "List<PlanNodeId>",
      "groupedExecutionScanNodes");
  to_json_key(
      j,
      "totalLifespans",
      p.totalLifespans,
      "StageExecutionDescriptor",
      "int",
      "totalLifespans");
  to_json_key(
      j,
      "groupedExecutionPartitionValues",
      p.groupedExecutionPartitionValues,
      "StageExecutionDescriptor",
      "List<Map<String, String>>",
      "groupedExecutionPartitionValues");
  to_json_key(
      j,
      "partitionColumnMappings",
      p.partitionColumnMappings,
      "StageExecutionDescriptor",
      "Map<PlanNodeId, Map<String, String>>",
      "partitionColumnMappings");
}

void from_json(const json& j, StageExecutionDescriptor& p) {
  from_json_key(
      j,
      "stageExecutionStrategy",
      p.stageExecutionStrategy,
      "StageExecutionDescriptor",
      "StageExecutionStrategy",
      "stageExecutionStrategy");
  from_json_key(
      j,
      "groupedExecutionScanNodes",
      p.groupedExecutionScanNodes,
      "StageExecutionDescriptor",
      "List<PlanNodeId>",
      "groupedExecutionScanNodes");
  from_json_key(
      j,
      "totalLifespans",
      p.totalLifespans,
      "StageExecutionDescriptor",
      "int",
      "totalLifespans");
  // from_json_key requires the key for non-pointer fields; older coordinators
  // and serialized fixtures predate these fields, so read them only when
  // present and otherwise leave them empty.
  if (j.count("groupedExecutionPartitionValues")) {
    from_json_key(
        j,
        "groupedExecutionPartitionValues",
        p.groupedExecutionPartitionValues,
        "StageExecutionDescriptor",
        "List<Map<String, String>>",
        "groupedExecutionPartitionValues");
  }
  if (j.count("partitionColumnMappings")) {
    from_json_key(
        j,
        "partitionColumnMappings",
        p.partitionColumnMappings,
        "StageExecutionDescriptor",
        "Map<PlanNodeId, Map<String, String>>",
        "partitionColumnMappings");
  }
}

} // namespace facebook::presto::protocol
