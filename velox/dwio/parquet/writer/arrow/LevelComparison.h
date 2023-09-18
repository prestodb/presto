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

// Adapted from Apache Arrow.

#pragma once

#include <algorithm>
#include <cstdint>

#include "velox/dwio/parquet/writer/arrow/Platform.h"

namespace facebook::velox::parquet::arrow::internal {

/// Builds a  bitmap where each set bit indicates the corresponding level is
/// greater than rhs.
uint64_t PARQUET_EXPORT
GreaterThanBitmap(const int16_t* levels, int64_t num_levels, int16_t rhs);

struct MinMax {
  int16_t min;
  int16_t max;
};

MinMax FindMinMax(const int16_t* levels, int64_t num_levels);

} // namespace facebook::velox::parquet::arrow::internal
