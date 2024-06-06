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
#include "velox/functions/prestosql/JsonFunctions.h"

namespace facebook::velox::functions {
void registerJsonFunctions(const std::string& prefix) {
  registerJsonType();

  registerFunction<IsJsonScalarFunction, bool, Json>(
      {prefix + "is_json_scalar"});
  registerFunction<IsJsonScalarFunction, bool, Varchar>(
      {prefix + "is_json_scalar"});

  registerFunction<JsonExtractScalarFunction, Varchar, Json, Varchar>(
      {prefix + "json_extract_scalar"});
  registerFunction<JsonExtractScalarFunction, Varchar, Varchar, Varchar>(
      {prefix + "json_extract_scalar"});

  registerFunction<JsonExtractFunction, Json, Json, Varchar>(
      {prefix + "json_extract"});
  registerFunction<JsonExtractFunction, Json, Varchar, Varchar>(
      {prefix + "json_extract"});

  registerFunction<JsonArrayLengthFunction, int64_t, Json>(
      {prefix + "json_array_length"});
  registerFunction<JsonArrayLengthFunction, int64_t, Varchar>(
      {prefix + "json_array_length"});

  registerFunction<JsonArrayContainsFunction, bool, Json, bool>(
      {prefix + "json_array_contains"});
  registerFunction<JsonArrayContainsFunction, bool, Varchar, bool>(
      {prefix + "json_array_contains"});
  registerFunction<JsonArrayContainsFunction, bool, Json, int64_t>(
      {prefix + "json_array_contains"});
  registerFunction<JsonArrayContainsFunction, bool, Varchar, int64_t>(
      {prefix + "json_array_contains"});
  registerFunction<JsonArrayContainsFunction, bool, Json, double>(
      {prefix + "json_array_contains"});
  registerFunction<JsonArrayContainsFunction, bool, Varchar, double>(
      {prefix + "json_array_contains"});
  registerFunction<JsonArrayContainsFunction, bool, Json, Varchar>(
      {prefix + "json_array_contains"});
  registerFunction<JsonArrayContainsFunction, bool, Varchar, Varchar>(
      {prefix + "json_array_contains"});

  registerFunction<JsonArrayGetFunction, Json, Json, int64_t>(
      {prefix + "json_array_get"});
  registerFunction<JsonArrayGetFunction, Json, Varchar, int64_t>(
      {prefix + "json_array_get"});

  registerFunction<JsonSizeFunction, int64_t, Json, Varchar>(
      {prefix + "json_size"});
  registerFunction<JsonSizeFunction, int64_t, Varchar, Varchar>(
      {prefix + "json_size"});

  VELOX_REGISTER_VECTOR_FUNCTION(udf_json_format, prefix + "json_format");

  VELOX_REGISTER_VECTOR_FUNCTION(udf_json_parse, prefix + "json_parse");
}

} // namespace facebook::velox::functions
