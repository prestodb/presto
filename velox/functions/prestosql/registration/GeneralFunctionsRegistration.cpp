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
#include "velox/functions/prestosql/Cardinality.h"

namespace facebook::velox::functions {

void registerGeneralFunctions() {
  VELOX_REGISTER_VECTOR_FUNCTION(udf_element_at, "element_at");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_subscript, "subscript");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_transform, "transform");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_reduce, "reduce");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_coalesce, "coalesce");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_is_null, "is_null");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_in, "in");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_filter, "filter");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_concat_row, "row_constructor");

  VELOX_REGISTER_VECTOR_FUNCTION(udf_least, "least");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_greatest, "greatest");

  registerFunction<CardinalityFunction, int64_t, Array<Generic<>>>(
      {"cardinality"});
  registerFunction<CardinalityFunction, int64_t, Map<Generic<>, Generic<>>>(
      {"cardinality"});
}

} // namespace facebook::velox::functions
