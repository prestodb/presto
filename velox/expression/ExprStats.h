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

#include "velox/common/time/CpuWallTimer.h"

namespace facebook::velox::exec {

struct ExprStats {
  /// Requires QueryConfig.exprTrackCpuUsage() to be 'true'.
  CpuWallTiming timing;

  /// Number of processed rows.
  uint64_t numProcessedRows{0};

  /// Number of processed vectors / batches. Allows to compute average batch
  /// size.
  uint64_t numProcessedVectors{0};

  /// Whether default-null behavior of an expression resulted in skipping
  /// evaluation of rows.
  bool defaultNullRowsSkipped{false};

  void add(const ExprStats& other) {
    timing.add(other.timing);
    numProcessedRows += other.numProcessedRows;
    numProcessedVectors += other.numProcessedVectors;
    defaultNullRowsSkipped |= other.defaultNullRowsSkipped;
  }

  std::string toString() const {
    return fmt::format(
        "timing: {}, numProcessedRows: {}, numProcessedVectors: {}, defaultNullRowsSkipped: {}",
        timing.toString(),
        numProcessedRows,
        numProcessedVectors,
        defaultNullRowsSkipped ? "true" : "false");
  }
};
} // namespace facebook::velox::exec
