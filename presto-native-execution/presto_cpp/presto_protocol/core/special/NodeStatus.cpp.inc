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

void to_json(json& j, const NodeStatus& p) {
  j = json::object();
  to_json_key(j, "nodeId", p.nodeId, "NodeStatus", "String", "nodeId");
  to_json_key(
      j,
      "nodeVersion",
      p.nodeVersion,
      "NodeStatus",
      "NodeVersion",
      "nodeVersion");
  to_json_key(
      j, "environment", p.environment, "NodeStatus", "String", "environment");
  to_json_key(
      j, "coordinator", p.coordinator, "NodeStatus", "bool", "coordinator");
  to_json_key(j, "uptime", p.uptime, "NodeStatus", "Duration", "uptime");
  to_json_key(
      j,
      "externalAddress",
      p.externalAddress,
      "NodeStatus",
      "String",
      "externalAddress");
  to_json_key(
      j,
      "internalAddress",
      p.internalAddress,
      "NodeStatus",
      "String",
      "internalAddress");
  to_json_key(
      j, "memoryInfo", p.memoryInfo, "NodeStatus", "MemoryInfo", "memoryInfo");
  to_json_key(j, "processors", p.processors, "NodeStatus", "int", "processors");
  to_json_key(
      j,
      "processCpuLoad",
      p.processCpuLoad,
      "NodeStatus",
      "double",
      "processCpuLoad");
  to_json_key(
      j,
      "systemCpuLoad",
      p.systemCpuLoad,
      "NodeStatus",
      "double",
      "systemCpuLoad");
  to_json_key(j, "heapUsed", p.heapUsed, "NodeStatus", "int64_t", "heapUsed");
  to_json_key(
      j,
      "heapAvailable",
      p.heapAvailable,
      "NodeStatus",
      "int64_t",
      "heapAvailable");
  to_json_key(
      j, "nonHeapUsed", p.nonHeapUsed, "NodeStatus", "int64_t", "nonHeapUsed");
  to_json_key(
      j,
      "asyncDataCacheBytes",
      p.asyncDataCacheBytes,
      "NodeStatus",
      "int64_t",
      "asyncDataCacheBytes");
  to_json_key(
      j,
      "queryMemoryBytes",
      p.queryMemoryBytes,
      "NodeStatus",
      "int64_t",
      "queryMemoryBytes");
}

void from_json(const json& j, NodeStatus& p) {
  from_json_key(j, "nodeId", p.nodeId, "NodeStatus", "String", "nodeId");
  from_json_key(
      j,
      "nodeVersion",
      p.nodeVersion,
      "NodeStatus",
      "NodeVersion",
      "nodeVersion");
  from_json_key(
      j, "environment", p.environment, "NodeStatus", "String", "environment");
  from_json_key(
      j, "coordinator", p.coordinator, "NodeStatus", "bool", "coordinator");
  from_json_key(j, "uptime", p.uptime, "NodeStatus", "Duration", "uptime");
  from_json_key(
      j,
      "externalAddress",
      p.externalAddress,
      "NodeStatus",
      "String",
      "externalAddress");
  from_json_key(
      j,
      "internalAddress",
      p.internalAddress,
      "NodeStatus",
      "String",
      "internalAddress");
  from_json_key(
      j, "memoryInfo", p.memoryInfo, "NodeStatus", "MemoryInfo", "memoryInfo");
  from_json_key(
      j, "processors", p.processors, "NodeStatus", "int", "processors");
  from_json_key(
      j,
      "processCpuLoad",
      p.processCpuLoad,
      "NodeStatus",
      "double",
      "processCpuLoad");
  from_json_key(
      j,
      "systemCpuLoad",
      p.systemCpuLoad,
      "NodeStatus",
      "double",
      "systemCpuLoad");
  from_json_key(j, "heapUsed", p.heapUsed, "NodeStatus", "int64_t", "heapUsed");
  from_json_key(
      j,
      "heapAvailable",
      p.heapAvailable,
      "NodeStatus",
      "int64_t",
      "heapAvailable");
  from_json_key(
      j, "nonHeapUsed", p.nonHeapUsed, "NodeStatus", "int64_t", "nonHeapUsed");
  // Backward-compat guards: pre-upgrade coordinators (or in-flight messages
  // from them) will not include these fields; default to 0 so new native
  // workers can still parse them. See MemoryInfo.cpp.inc for the rationale on
  // why these overrides live in .inc files rather than as hand-edits on the
  // generated .cpp.
  if (j.count("asyncDataCacheBytes")) {
    from_json_key(
        j,
        "asyncDataCacheBytes",
        p.asyncDataCacheBytes,
        "NodeStatus",
        "int64_t",
        "asyncDataCacheBytes");
  }
  if (j.count("queryMemoryBytes")) {
    from_json_key(
        j,
        "queryMemoryBytes",
        p.queryMemoryBytes,
        "NodeStatus",
        "int64_t",
        "queryMemoryBytes");
  }
}

} // namespace facebook::presto::protocol
