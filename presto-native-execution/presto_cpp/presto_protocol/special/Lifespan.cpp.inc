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

void to_json(json& j, const Lifespan& p) {
  if (p.isgroup) {
    j = "Group" + std::to_string(p.groupid);
  } else {
    j = "TaskWide";
  }
}

void from_json(const json& j, Lifespan& p) {
  String lifespan = j;

  if (lifespan == "TaskWide") {
    p.isgroup = false;
    p.groupid = 0;
  } else {
    if (lifespan != "Group") {
      // fail...
    }
    p.isgroup = true;
    p.groupid = std::stoi(lifespan.substr(strlen("Group")));
  }
}

} // namespace facebook::presto::protocol
