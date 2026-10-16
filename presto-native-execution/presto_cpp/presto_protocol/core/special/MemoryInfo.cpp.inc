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

void to_json(json& j, const MemoryInfo& p) {
  j = json::object();
  to_json_key(
      j,
      "totalNodeMemory",
      p.totalNodeMemory,
      "MemoryInfo",
      "DataSize",
      "totalNodeMemory");
  to_json_key(
      j,
      "systemMemoryBytes",
      p.systemMemoryBytes,
      "MemoryInfo",
      "int64_t",
      "systemMemoryBytes");
  to_json_key(
      j,
      "pools",
      p.pools,
      "MemoryInfo",
      "Map<MemoryPoolId, MemoryPoolInfo>",
      "pools");
}

void from_json(const json& j, MemoryInfo& p) {
  from_json_key(
      j,
      "totalNodeMemory",
      p.totalNodeMemory,
      "MemoryInfo",
      "DataSize",
      "totalNodeMemory");
  // Backward-compat guard: a pre-upgrade coordinator (or an in-flight message
  // from one) will not include this field; default to 0 so new native workers
  // can still parse it. This handling lives in the .inc override (rather than
  // as a hand-edit on the generated .cpp) so that `make presto_protocol`
  // preserves it across regenerations.
  if (j.count("systemMemoryBytes")) {
    from_json_key(
        j,
        "systemMemoryBytes",
        p.systemMemoryBytes,
        "MemoryInfo",
        "int64_t",
        "systemMemoryBytes");
  }
  from_json_key(
      j,
      "pools",
      p.pools,
      "MemoryInfo",
      "Map<MemoryPoolId, MemoryPoolInfo>",
      "pools");
}

} // namespace facebook::presto::protocol
