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
#include "velox/functions/prestosql/ArrayFunctions.h"
#include "velox/functions/prestosql/WidthBucketArray.h"

namespace facebook::velox::functions {
template <typename T>
inline void registerArrayMinMaxFunctions() {
  registerFunction<ArrayMinFunction, T, Array<T>>({"array_min"});
  registerFunction<ArrayMaxFunction, T, Array<T>>({"array_max"});
}

template <typename T>
inline void registerArrayJoinFunctions() {
  registerFunction<
      ParameterBinder<ArrayJoinFunction, T>,
      Varchar,
      Array<T>,
      Varchar>({"array_join"});

  registerFunction<
      ParameterBinder<ArrayJoinFunction, T>,
      Varchar,
      Array<T>,
      Varchar,
      Varchar>({"array_join"});
}

void registerArrayFunctions() {
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_constructor, "array_constructor");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_distinct, "array_distinct");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_duplicates, "array_duplicates");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_intersect, "array_intersect");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_contains, "contains");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_except, "array_except");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_duplicates, "array_duplicates");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_arrays_overlap, "arrays_overlap");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_slice, "slice");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_zip, "zip");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_position, "array_position");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_sort, "array_sort");

  exec::registerStatefulVectorFunction(
      "width_bucket", widthBucketArraySignature(), makeWidthBucketArray);

  registerArrayMinMaxFunctions<int8_t>();
  registerArrayMinMaxFunctions<int16_t>();
  registerArrayMinMaxFunctions<int32_t>();
  registerArrayMinMaxFunctions<int64_t>();
  registerArrayMinMaxFunctions<float>();
  registerArrayMinMaxFunctions<double>();
  registerArrayMinMaxFunctions<bool>();
  registerArrayMinMaxFunctions<Varchar>();
  registerArrayMinMaxFunctions<Timestamp>();
  registerArrayMinMaxFunctions<Date>();

  registerArrayJoinFunctions<int8_t>();
  registerArrayJoinFunctions<int16_t>();
  registerArrayJoinFunctions<int32_t>();
  registerArrayJoinFunctions<int64_t>();
  registerArrayJoinFunctions<float>();
  registerArrayJoinFunctions<double>();
  registerArrayJoinFunctions<bool>();
  registerArrayJoinFunctions<Varchar>();
  registerArrayJoinFunctions<Timestamp>();
  registerArrayJoinFunctions<Date>();
}
}; // namespace facebook::velox::functions
