/*
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

#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "presto_cpp/presto_protocol/presto_protocol.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/core/QueryCtx.h"

using RowExpressionPtr =
    std::shared_ptr<facebook::presto::protocol::RowExpression>;

namespace facebook::presto::expression {

/// TODO: Constants for ExpressionOptimizerLevel, these should be obtained from
///  Presto protocol after refactoring the enum in Presto SPI.
static constexpr char const* kOptimized = "OPTIMIZED";
static constexpr char const* kEvaluated = "EVALUATED";

/// Optimizes the input list of RowExpressions. For each input RowExpression,
/// the result is an optimized expression on success or failure info.
/// @param input List of RowExpressions to be optimized.
/// @param timezone Session timezone, received from Presto coordinator.
/// @param optimizerLevel Optimizer level, received from Presto coordinator.
/// The optimizerLevel can either be OPTIMIZED or EVALUATED. OPTIMIZED removes
/// all redundancy in a RowExpression by leveraging the ExpressionOptimizer in
/// Velox, and EVALUATED attempts to evaluate the RowExpression into a constant
/// even when the expression is non-deterministic.
/// @param queryCtx Query context to be used during optimization.
/// @param pool Memory pool, required for expression evaluation.
std::vector<protocol::RowExpressionOptimizationResult> optimizeExpressions(
    const std::vector<RowExpressionPtr>& input,
    const std::string& timezone,
    const std::string& optimizerLevel,
    velox::core::QueryCtx* queryCtx,
    velox::memory::MemoryPool* pool);
} // namespace facebook::presto::expression
