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

#include "velox/exec/Trace.h"

#include <fmt/core.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/SuccinctPrinter.h"

namespace facebook::velox::exec::trace {

std::string OperatorTraceSummary::toString() const {
  if (numSplits.has_value()) {
    VELOX_CHECK_EQ(opType, "TableScan");
    return fmt::format(
        "opType {}, numSplits {}, inputRows {}, inputBytes {}, rawInputRows {}, rawInputBytes {}, peakMemory {}",
        opType,
        numSplits.value(),
        inputRows,
        succinctBytes(inputBytes),
        rawInputRows,
        succinctBytes(rawInputBytes),
        succinctBytes(peakMemory));
  } else {
    VELOX_CHECK_NE(opType, "TableScan");
    return fmt::format(
        "opType {}, inputRows {},  inputBytes {}, rawInputRows {}, rawInputBytes {}, peakMemory {}",
        opType,
        inputRows,
        succinctBytes(inputBytes),
        rawInputRows,
        succinctBytes(rawInputBytes),
        succinctBytes(peakMemory));
  }
}
} // namespace facebook::velox::exec::trace
