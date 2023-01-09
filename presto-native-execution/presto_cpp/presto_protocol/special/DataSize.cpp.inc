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

void to_json(nlohmann::json& j, const DataSize& p) {
  j = p.toString();
}

void from_json(const nlohmann::json& j, DataSize& p) {
  p = DataSize(std::string(j));
}

std::ostream& operator<<(std::ostream& os, const DataSize& d) {
  return os << d.toString();
}

} // namespace facebook::presto::protocol