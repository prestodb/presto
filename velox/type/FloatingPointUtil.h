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

#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>

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
        folly::hasher<FLOAT>{}(std::numeric_limits<FLOAT>::quiet_NaN());
    if (std::isnan(val)) {
      return kNanHash;
    }
    return folly::hasher<FLOAT>{}(val);
  }
};

// Utility struct to provide a clean way of defining a hash set and map type
// using folly::F14FastSet and folly::F14FastMap respectively with overrides for
// floating point types.
template <typename Key>
class HashSetNaNAware : public folly::F14FastSet<Key> {};

template <>
class HashSetNaNAware<float>
    : public folly::
          F14FastSet<float, NaNAwareHash<float>, NaNAwareEquals<float>> {};

template <>
class HashSetNaNAware<double>
    : public folly::
          F14FastSet<double, NaNAwareHash<double>, NaNAwareEquals<double>> {};

template <
    typename Key,
    typename Mapped,
    typename Alloc = folly::f14::DefaultAlloc<std::pair<Key const, Mapped>>>
struct HashMapNaNAwareTypeTraits {
  using Type = folly::F14FastMap<
      Key,
      Mapped,
      folly::f14::DefaultHasher<Key>,
      folly::f14::DefaultKeyEqual<Key>,
      Alloc>;
};

template <typename Mapped, typename Alloc>
struct HashMapNaNAwareTypeTraits<float, Mapped, Alloc> {
  using Type = folly::F14FastMap<
      float,
      Mapped,
      NaNAwareHash<float>,
      NaNAwareEquals<float>,
      Alloc>;
};

template <typename Mapped, typename Alloc>
struct HashMapNaNAwareTypeTraits<double, Mapped, Alloc> {
  using Type = folly::F14FastMap<
      double,
      Mapped,
      NaNAwareHash<double>,
      NaNAwareEquals<double>,
      Alloc>;
};
} // namespace util::floating_point

/// A static class that holds helper functions for DOUBLE type.
class DoubleUtil {
 public:
  static const std::array<double, 309> kPowersOfTen;

}; // DoubleUtil
} // namespace facebook::velox
