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

void to_json(json& j, const ServerInfo& p) {
  j = json::object();
  to_json_key(
      j,
      "nodeVersion",
      p.nodeVersion,
      "ServerInfo",
      "NodeVersion",
      "nodeVersion");
  to_json_key(
      j, "environment", p.environment, "ServerInfo", "String", "environment");
  to_json_key(
      j, "coordinator", p.coordinator, "ServerInfo", "bool", "coordinator");
  to_json_key(j, "starting", p.starting, "ServerInfo", "bool", "starting");
  to_json_key(j, "uptime", p.uptime, "ServerInfo", "Duration", "uptime");
  to_json_key(j, "executionType", p.executionType, "ServerInfo", "ExecutionType", "executionType");
}

void from_json(const json& j, ServerInfo& p) {
  from_json_key(
      j,
      "nodeVersion",
      p.nodeVersion,
      "ServerInfo",
      "NodeVersion",
      "nodeVersion");
  from_json_key(
      j, "environment", p.environment, "ServerInfo", "String", "environment");
  from_json_key(
      j, "coordinator", p.coordinator, "ServerInfo", "bool", "coordinator");
  from_json_key(j, "starting", p.starting, "ServerInfo", "bool", "starting");
  from_json_key(j, "uptime", p.uptime, "ServerInfo", "Duration", "uptime");
  from_json_key(j, "executionType", p.executionType, "ServerInfo", "ExecutionType", "executionType");
}

} // namespace facebook::presto::protocol
