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
#pragma once

#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "presto_cpp/main/properties/session/SessionPropertiesProvider.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/type/Type.h"

using json = nlohmann::json;

namespace facebook::presto::cudf {

/// Defines all cuDF GPU-specific session properties
class CudfSessionProperties
    : public facebook::presto::SessionPropertiesProvider {
 public:
  /// Enable cuDF GPU acceleration
  static constexpr const char* kCudfEnabled = "cudf_enabled";

  /// Enable debug mode for cuDF operations
  static constexpr const char* kCudfDebugEnabled = "cudf_debug_enabled";

  /// GPU memory resource type
  static constexpr const char* kCudfMemoryResource = "cudf_memory_resource";

  /// GPU memory allocation percentage
  static constexpr const char* kCudfMemoryPercent = "cudf_memory_percent";

  /// Function name prefix for cuDF functions
  static constexpr const char* kCudfFunctionNamePrefix =
      "cudf_function_name_prefix";

  /// Enable AST expression evaluation on GPU
  static constexpr const char* kCudfAstExpressionEnabled =
      "cudf_ast_expression_enabled";

  /// Priority of AST expression evaluation
  static constexpr const char* kCudfAstExpressionPriority =
      "cudf_ast_expression_priority";

  /// Enable JIT expression evaluation on GPU
  static constexpr const char* kCudfJitExpressionEnabled =
      "cudf_jit_expression_enabled";

  /// Priority of JIT expression evaluation
  static constexpr const char* kCudfJitExpressionPriority =
      "cudf_jit_expression_priority";

  /// Allow fallback to CPU execution if GPU operation fails
  static constexpr const char* kCudfAllowCpuFallback =
      "cudf_allow_cpu_fallback";

  /// Log reasons for CPU fallback
  static constexpr const char* kCudfLogFallback = "cudf_log_fallback";

  /// Get singleton instance
  static CudfSessionProperties* instance();

  /// Constructor - initializes all GPU session properties from CudfConfig
  CudfSessionProperties();
};

} // namespace facebook::presto::cudf
