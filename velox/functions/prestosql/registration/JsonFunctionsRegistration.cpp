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
void registerJsonFunctions() {
  registerFunction<IsJsonScalarFunction, bool, Varchar>({"is_json_scalar"});
  registerFunction<JsonExtractScalarFunction, Varchar, Varchar, Varchar>(
      {"json_extract_scalar"});
  registerFunction<JsonArrayLengthFunction, int64_t, Varchar>(
      {"json_array_length"});
}

} // namespace facebook::velox::functions
