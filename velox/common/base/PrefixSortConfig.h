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

#include <stdint.h>

namespace facebook::velox::common {

/// Specifies the config for prefix-sort.
struct PrefixSortConfig {
  PrefixSortConfig() = default;

  PrefixSortConfig(
      uint32_t _maxNormalizedKeyBytes,
      uint32_t _minNumRows,
      uint32_t _maxStringPrefixLength)
      : maxNormalizedKeyBytes(_maxNormalizedKeyBytes),
        minNumRows(_minNumRows),
        maxStringPrefixLength(_maxStringPrefixLength) {}

  /// Maximum bytes that can be used to store normalized keys in prefix-sort
  /// buffer per entry. Same with QueryConfig kPrefixSortNormalizedKeyMaxBytes.
  uint32_t maxNormalizedKeyBytes{128};

  /// Minimum number of rows to apply prefix sort. Prefix sort does not perform
  /// with small datasets.
  uint32_t minNumRows{128};

  /// Maximum number of bytes to be stored in prefix-sort buffer for a string
  /// column.
  uint32_t maxStringPrefixLength{16};
};
} // namespace facebook::velox::common
