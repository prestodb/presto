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

#include "velox/core/PlanNode.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::exec {

class Aggregate;

/// Information needed to evaluate an aggregate function.
struct AggregateInfo {
  /// Instance of the Aggregate class.
  std::unique_ptr<Aggregate> function;

  /// Indices of the input columns in the input RowVector.
  std::vector<column_index_t> inputs;

  /// Optional constant inputs. The size of this vector matches the size of
  /// 'inputs'. Non-constant inputs have null entries.
  std::vector<VectorPtr> constantInputs;

  /// Optional index of an input boolean column that should be used as a mask.
  std::optional<column_index_t> mask;

  /// Optional list of input columns that should be used to sort input rows
  /// before aggregating. Thes column may or may not overlap with 'inputs'.
  std::vector<column_index_t> sortingKeys;

  /// Optional list of sorting orders that goes with 'sortingKeys'.
  std::vector<core::SortOrder> sortingOrders;

  /// Index of the result column in the output RowVector.
  column_index_t output;

  /// Type of intermediate results. Used for spilling.
  TypePtr intermediateType;
};
} // namespace facebook::velox::exec
