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

#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::velox::exec {

class AggregationMasks {
 public:
  /// @param maskChannel Index of the 'mask' column for each aggregation.
  /// Aggregations without masks use std::nullopt.
  explicit AggregationMasks(
      std::vector<std::optional<column_index_t>> maskChannels);

  /// Process the input batch and prepare selectivity vectors for each mask by
  /// removing masked rows.
  void addInput(const RowVectorPtr& input, const SelectivityVector& rows);

  // Return prepared selectivity vector for a given aggregation. Must be called
  // after calling addInput().
  const SelectivityVector* FOLLY_NULLABLE
  activeRows(int32_t aggregationIndex) const;

 private:
  std::vector<std::optional<column_index_t>> maskChannels_;
  std::unordered_map<column_index_t, SelectivityVector> maskedRows_;
  DecodedVector decodedMask_;
};
} // namespace facebook::velox::exec
