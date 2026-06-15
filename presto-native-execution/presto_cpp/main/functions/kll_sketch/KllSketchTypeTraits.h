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
#pragma once

#include <string>
#include "velox/type/SimpleFunctionApi.h"
#include "velox/type/StringView.h"

namespace facebook::presto::functions::kll_sketch {

template <typename T>
struct SketchTypeMapper {
  using type = T;

  static T toSketchType(const T& value) {
    return value;
  }
};

namespace detail {
// Both string specializations (StringView for aggregates, Varchar for scalars).
inline std::string stringViewToString(const velox::StringView& value) {
  return std::string(value.data(), value.size());
}
} // namespace detail

// Specialization for StringView: used by aggregate functions where T is
// velox::StringView directly.
template <>
struct SketchTypeMapper<velox::StringView> {
  using type = std::string;

  static std::string toSketchType(const velox::StringView& value) {
    return detail::stringViewToString(value);
  }
};

// Specialization for Varchar: used by scalar functions where T is
// velox::Varchar (the Velox simple-function type tag). arg_type<Varchar>
// resolves to StringView at runtime, so toSketchType takes a StringView.
template <>
struct SketchTypeMapper<velox::Varchar> {
  using type = std::string;

  static std::string toSketchType(const velox::StringView& value) {
    return detail::stringViewToString(value);
  }
};

} // namespace facebook::presto::functions::kll_sketch
