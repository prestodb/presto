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

struct VariableReferenceExpression : RowExpression {
  String name;
  Type type; // dependency

  VariableReferenceExpression() noexcept;
  explicit VariableReferenceExpression(const String& str) {
    _type = "variable";

    std::vector<std::string, std::allocator<std::string>> parts;

    folly::split("<", str, parts);
    name = parts[0];
    type = parts[1].substr(0, parts[1].length() - 1);
  }

  bool operator<(const VariableReferenceExpression& o) const {
    if (name == o.name) {
      return type < o.type;
    }

    return name < o.name;
  }
};

void to_json(json& j, const VariableReferenceExpression& p);
void from_json(const json& j, VariableReferenceExpression& p);

std::string json_map_key(
    const facebook::presto::protocol::VariableReferenceExpression& p);

} // namespace facebook::presto::protocol
