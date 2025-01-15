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

namespace facebook::velox::exec {

class OrderByBenchmarkUtil {
 public:
  /// Add the benchmarks with the parameter.
  /// @param benchmarkFunc benchmark generator.
  static void addBenchmarks(const std::function<void(
                                const std::string& benchmarkName,
                                vector_size_t numRows,
                                const RowTypePtr& rowType,
                                int iterations,
                                int numKeys)>& benchmarkFunc);

  /// Generate RowVector by VectorFuzzer according to rowType. Use
  /// FLAGS_data_null_ratio to specify the columns null ratio
  static RowVectorPtr fuzzRows(
      const RowTypePtr& rowType,
      vector_size_t numRows,
      memory::MemoryPool* pool);
};
} // namespace facebook::velox::exec
