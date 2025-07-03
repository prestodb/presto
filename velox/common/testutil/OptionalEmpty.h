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
#include <optional>
#include <vector>

namespace facebook::velox::common::testutil {

// Workaround for GCC to avoid matching optional(std::in_place_t, ...)
struct OptionalEmptyT {
  template <typename T>
  operator std::optional<std::vector<T>>() const {
    return std::make_optional<std::vector<T>>({});
  }
};

inline constexpr OptionalEmptyT optionalEmpty;

struct EmptyT {
  template <typename T>
  operator std::vector<T>() const {
    return {};
  }
};

inline constexpr EmptyT Empty;

} // namespace facebook::velox::common::testutil
