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

static const std::pair<ExecutionType, json>
    ExecutionType_enum_table[] = { // NOLINT: cert-err58-cpp
        {ExecutionType::JAVA, "java"},
        {ExecutionType::NATIVE, "native"},
        {ExecutionType::NATIVE_CUDF, "native_cudf"}};

void to_json(json& j, const ExecutionType& e) {
  static_assert(
      std::is_enum<ExecutionType>::value,
      "ExecutionType must be an enum!");
  const auto* it = std::find_if(
      std::begin(ExecutionType_enum_table),
      std::end(ExecutionType_enum_table),
      [e](const std::pair<ExecutionType, json>& ej_pair) -> bool {
        return ej_pair.first == e;
      });
  j = ((it != std::end(ExecutionType_enum_table))
           ? it
           : std::begin(ExecutionType_enum_table))
          ->second;
}

void from_json(const json& j, ExecutionType& e) {
  static_assert(
      std::is_enum<ExecutionType>::value,
      "ExecutionType must be an enum!");
  const auto* it = std::find_if(
      std::begin(ExecutionType_enum_table),
      std::end(ExecutionType_enum_table),
      [&j](const std::pair<ExecutionType, json>& ej_pair) -> bool {
        return ej_pair.second == j;
      });
  e = ((it != std::end(ExecutionType_enum_table))
           ? it
           : std::begin(ExecutionType_enum_table))
          ->first;
}

} // namespace facebook::presto::protocol
