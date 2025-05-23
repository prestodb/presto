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

void toThrift(const facebook::presto::protocol::TaskId& proto, TaskId& thrift) {
  std::vector<std::string> parts;
  folly::split(".", proto, parts);
  if (parts.size() == 5) {
    thrift.stageExecutionId_ref()->stageId_ref()->queryId_ref() = parts[0];
    thrift.stageExecutionId_ref()->stageId_ref()->id_ref() = std::stoi(parts[1]);
    thrift.stageExecutionId_ref()->id_ref() = std::stoi(parts[2]);
    thrift.id_ref() = std::stoi(parts[3]);
    thrift.attemptNumber_ref() = std::stoi(parts[4]);
  }
}
void fromThrift(const TaskId& thrift, facebook::presto::protocol::TaskId& proto) {
  std::string taskIdStr =
    *thrift.stageExecutionId_ref()->stageId_ref()->queryId_ref() + "." +
    std::to_string(*thrift.stageExecutionId_ref()->stageId_ref()->id_ref()) + "." +
    std::to_string(*thrift.stageExecutionId_ref()->id_ref()) + "." +
    std::to_string(*thrift.id_ref()) + "." +
    std::to_string(*thrift.attemptNumber_ref());
  fromThrift(taskIdStr, proto);
}
