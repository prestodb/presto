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
ScalarArgumentSpecification::ScalarArgumentSpecification() noexcept {
  _type = "scalar";
}

void to_json(json& j, const ScalarArgumentSpecification& p) {
  j = json::object();
  j["@type"] = "scalar";
  to_json_key(
      j, "name", p.name, "ScalarArgumentSpecification", "String", "name");
  to_json_key(j, "type", p.type, "ScalarArgumentSpecification", "Type", "type");
  to_json_key(
      j,
      "required",
      p.required,
      "ScalarArgumentSpecification",
      "bool",
      "required");
  if (!p.defaultValue.empty()) {
    j["defaultValue"] = p.defaultValue;
  }
}

void from_json(const json& j, ScalarArgumentSpecification& p) {
  p._type = j["@type"];
  from_json_key(
      j, "name", p.name, "ScalarArgumentSpecification", "String", "name");
  from_json_key(
      j, "type", p.type, "ScalarArgumentSpecification", "Type", "type");
  from_json_key(
      j,
      "required",
      p.required,
      "ScalarArgumentSpecification",
      "bool",
      "required");
  from_json_key(
      j,
      "defaultValue",
      p.defaultValue,
      "ScalarArgumentSpecification",
      "String",
      "defaultValue");
}
} // namespace facebook::presto::protocol
