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

#include "velox/core/QueryConfig.h"
#include "velox/functions/Macros.h"

namespace facebook::velox::functions::sparksql {

template <typename T>
struct SparkPartitionIdFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config) {
    partitionId_ = config.sparkPartitionId();
  }

  FOLLY_ALWAYS_INLINE void call(int32_t& result) {
    result = partitionId_;
  }

 private:
  int32_t partitionId_;
};
} // namespace facebook::velox::functions::sparksql
