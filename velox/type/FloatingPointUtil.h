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

#include <array>
#include <cmath>
#include <vector>

namespace facebook::velox {

/// Custom comparator and hash functors for floating point types. These are
/// designed to ensure consistent NaN handling according to the following rules:
/// -> NaN == NaN returns true, even for NaNs with differing binary
///   representations.
/// -> NaN is considered greater than infinity. There is no ordering between
///    NaNs with differing binary representations.
/// These can be passed to standard containers and functions like std::map and
/// std::sort, etc.
namespace util::floating_point {
template <
    typename FLOAT,
    std::enable_if_t<std::is_floating_point<FLOAT>::value, bool> = true>
struct NaNAwareEquals {
  bool operator()(const FLOAT& lhs, const FLOAT& rhs) const {
    if (std::isnan(lhs) && std::isnan(rhs)) {
      return true;
    }
    return lhs == rhs;
  }
};

template <
    typename FLOAT,
    std::enable_if_t<std::is_floating_point<FLOAT>::value, bool> = true>
struct NaNAwareLessThan {
  bool operator()(const FLOAT& lhs, const FLOAT& rhs) const {
    if (!std::isnan(lhs) && std::isnan(rhs)) {
      return true;
    }
    return lhs < rhs;
  }
};

template <
    typename FLOAT,
    std::enable_if_t<std::is_floating_point<FLOAT>::value, bool> = true>
struct NaNAwareGreaterThan {
  bool operator()(const FLOAT& lhs, const FLOAT& rhs) const {
    if (std::isnan(lhs) && !std::isnan(rhs)) {
      return true;
    }
    return lhs > rhs;
  }
};

template <
    typename FLOAT,
    std::enable_if_t<std::is_floating_point<FLOAT>::value, bool> = true>
struct NaNAwareHash {
  std::size_t operator()(const FLOAT& val) const noexcept {
    static const std::size_t kNanHash =
        std::hash<FLOAT>{}(std::numeric_limits<FLOAT>::quiet_NaN());
    if (std::isnan(val)) {
      return kNanHash;
    }
    return std::hash<FLOAT>{}(val);
  }
};
} // namespace util::floating_point

/// A static class that holds helper functions for DOUBLE type.
class DoubleUtil {
 public:
  static const std::array<double, 309> kPowersOfTen;

}; // DoubleUtil
} // namespace facebook::velox
