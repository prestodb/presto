/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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

#include <fmt/format.h>
#include <string>

namespace facebook::velox {

struct CompileTimeEmptyString {
  CompileTimeEmptyString() = default;
  constexpr operator const char*() const {
    return "";
  }
  constexpr operator std::string_view() const {
    return {};
  }
  operator std::string() const {
    return {};
  }
};

// When there is no message passed, we can statically detect this case
// and avoid passing even a single unnecessary argument pointer,
// minimizing size and thus maximizing eligibility for inlining.
inline CompileTimeEmptyString errorMessage() {
  return {};
}

inline const char* errorMessage(const char* s) {
  return s;
}

inline std::string errorMessage(const std::string& str) {
  return str;
}

template <typename... Args>
std::string errorMessage(fmt::string_view fmt, const Args&... args) {
  return fmt::vformat(fmt, fmt::make_format_args(args...));
}
} // namespace facebook::velox
