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

#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/SIMDJsonFunctions.h"

namespace facebook::velox::functions {
void registerJsonFunctions(const std::string& prefix) {
  registerJsonType();

  registerFunction<SIMDIsJsonScalarFunction, bool, Json>(
      {prefix + "is_json_scalar"});
  registerFunction<SIMDIsJsonScalarFunction, bool, Varchar>(
      {prefix + "is_json_scalar"});

  registerFunction<SIMDJsonExtractScalarFunction, Varchar, Json, Varchar>(
      {prefix + "json_extract_scalar"});
  registerFunction<SIMDJsonExtractScalarFunction, Varchar, Varchar, Varchar>(
      {prefix + "json_extract_scalar"});

  registerFunction<SIMDJsonExtractFunction, Json, Json, Varchar>(
      {prefix + "json_extract"});
  registerFunction<SIMDJsonExtractFunction, Json, Varchar, Varchar>(
      {prefix + "json_extract"});

  registerFunction<SIMDJsonArrayLengthFunction, int64_t, Json>(
      {prefix + "json_array_length"});
  registerFunction<SIMDJsonArrayLengthFunction, int64_t, Varchar>(
      {prefix + "json_array_length"});

  registerFunction<SIMDJsonArrayContainsFunction, bool, Json, bool>(
      {prefix + "json_array_contains"});
  registerFunction<SIMDJsonArrayContainsFunction, bool, Varchar, bool>(
      {prefix + "json_array_contains"});
  registerFunction<SIMDJsonArrayContainsFunction, bool, Json, int64_t>(
      {prefix + "json_array_contains"});
  registerFunction<SIMDJsonArrayContainsFunction, bool, Varchar, int64_t>(
      {prefix + "json_array_contains"});
  registerFunction<SIMDJsonArrayContainsFunction, bool, Json, double>(
      {prefix + "json_array_contains"});
  registerFunction<SIMDJsonArrayContainsFunction, bool, Varchar, double>(
      {prefix + "json_array_contains"});
  registerFunction<SIMDJsonArrayContainsFunction, bool, Json, Varchar>(
      {prefix + "json_array_contains"});
  registerFunction<SIMDJsonArrayContainsFunction, bool, Varchar, Varchar>(
      {prefix + "json_array_contains"});

  registerFunction<SIMDJsonSizeFunction, int64_t, Json, Varchar>(
      {prefix + "json_size"});
  registerFunction<SIMDJsonSizeFunction, int64_t, Varchar, Varchar>(
      {prefix + "json_size"});

  VELOX_REGISTER_VECTOR_FUNCTION(udf_json_format, prefix + "json_format");

  VELOX_REGISTER_VECTOR_FUNCTION(udf_json_parse, prefix + "json_parse");
}

} // namespace facebook::velox::functions
