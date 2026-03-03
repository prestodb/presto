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
#include "presto_cpp/main/sidecar/properties/CudfSessionProperties.h"
#include "presto_cpp/main/common/Utils.h"
#include "velox/experimental/cudf/CudfConfig.h"

namespace facebook::presto::cudf {

using facebook::presto::util::boolToString;
using facebook::velox::BOOLEAN;
using facebook::velox::VARCHAR;
using facebook::velox::INTEGER;

CudfSessionProperties* CudfSessionProperties::instance() {
  static std::unique_ptr<CudfSessionProperties> instance =
      std::make_unique<CudfSessionProperties>();
  return instance.get();
}

// Initialize GPU session properties from cuDF configuration
CudfSessionProperties::CudfSessionProperties() {
  using facebook::velox::cudf_velox::CudfConfig;
  const auto& config = CudfConfig::getInstance();

  // Enable cuDF GPU acceleration
  addSessionProperty(
      kCudfEnabled,
      "Enable cuDF GPU acceleration for query execution",
      BOOLEAN(),
      false,
      CudfConfig::kCudfEnabled,
      boolToString(config.enabled));

  // Enable debug mode for cuDF operations
  addSessionProperty(
      kCudfDebugEnabled,
      "Enable debug printing for cuDF operations",
      BOOLEAN(),
      false,
      CudfConfig::kCudfDebugEnabled,
      boolToString(config.debugEnabled));

  // GPU memory resource type
  addSessionProperty(
      kCudfMemoryResource,
      "GPU memory resource type (cuda, pool, async, arena, managed, managed_pool)",
      VARCHAR(),
      false,
      CudfConfig::kCudfMemoryResource,
      config.memoryResource);

  // GPU memory allocation percentage
  addSessionProperty(
      kCudfMemoryPercent,
      "Initial percent of GPU memory to allocate for pool or arena memory resources",
      INTEGER(),
      false,
      CudfConfig::kCudfMemoryPercent,
      std::to_string(config.memoryPercent));

  // Function name prefix for cuDF functions
  addSessionProperty(
      kCudfFunctionNamePrefix,
      "Register all cuDF functions with this name prefix",
      VARCHAR(),
      false,
      CudfConfig::kCudfFunctionNamePrefix,
      config.functionNamePrefix);

  // Enable AST expression evaluation on GPU
  addSessionProperty(
      kCudfAstExpressionEnabled,
      "Enable AST expression evaluation on GPU",
      BOOLEAN(),
      false,
      CudfConfig::kCudfAstExpressionEnabled,
      boolToString(config.astExpressionEnabled));

  // Priority of AST expression evaluation
  addSessionProperty(
      kCudfAstExpressionPriority,
      "Priority of AST expression evaluation (higher priority is chosen)",
      INTEGER(),
      false,
      CudfConfig::kCudfAstExpressionPriority,
      std::to_string(config.astExpressionPriority));

  // Enable JIT expression evaluation on GPU
  addSessionProperty(
      kCudfJitExpressionEnabled,
      "Enable JIT expression evaluation on GPU",
      BOOLEAN(),
      false,
      CudfConfig::kCudfJitExpressionEnabled,
      boolToString(config.jitExpressionEnabled));

  // Priority of JIT expression evaluation
  addSessionProperty(
      kCudfJitExpressionPriority,
      "Priority of JIT expression evaluation (higher priority is chosen)",
      INTEGER(),
      false,
      CudfConfig::kCudfJitExpressionPriority,
      std::to_string(config.jitExpressionPriority));

  // Allow fallback to CPU execution if GPU operation fails
  addSessionProperty(
      kCudfAllowCpuFallback,
      "Allow fallback to CPU execution if GPU operation fails",
      BOOLEAN(),
      false,
      CudfConfig::kCudfAllowCpuFallback,
      boolToString(config.allowCpuFallback));

  // Log reasons for CPU fallback
  addSessionProperty(
      kCudfLogFallback,
      "Log reasons for falling back to CPU execution",
      BOOLEAN(),
      false,
      CudfConfig::kCudfLogFallback,
      boolToString(config.logFallback));

  // Maximum number of TopN batches to buffer before merging
  addSessionProperty(
      kCudfTopNBatchSize,
      "Maximum number of TopN batches to buffer before merging",
      INTEGER(),
      false,
      CudfConfig::kCudfTopNBatchSize,
      std::to_string(config.topNBatchSize));
}

} // namespace facebook::presto::cudf
