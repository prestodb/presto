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
#include "velox/functions/lib/ArrayShuffle.h"
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/lib/Repeat.h"
#include "velox/functions/lib/Slice.h"
#include "velox/functions/prestosql/ArrayFunctions.h"
#include "velox/functions/sparksql/ArrayFlattenFunction.h"
#include "velox/functions/sparksql/ArrayInsert.h"
#include "velox/functions/sparksql/ArrayMinMaxFunction.h"
#include "velox/functions/sparksql/ArraySort.h"

namespace facebook::velox::functions {

// VELOX_REGISTER_VECTOR_FUNCTION must be invoked in the same namespace as the
// vector function definition.
// Higher order functions.
void registerSparkArrayFunctions(const std::string& prefix) {
  VELOX_REGISTER_VECTOR_FUNCTION(udf_transform, prefix + "transform");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_reduce, prefix + "aggregate");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_constructor, prefix + "array");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_contains, prefix + "array_contains");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_distinct, prefix + "array_distinct");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_except, prefix + "array_except");
  VELOX_REGISTER_VECTOR_FUNCTION(
      udf_array_intersect, prefix + "array_intersect");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_position, prefix + "array_position");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_zip, prefix + "arrays_zip");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_any_match, prefix + "exists");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_filter, prefix + "filter");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_all_match, prefix + "forall");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_zip_with, prefix + "zip_with");
}

namespace sparksql {

template <typename T>
inline void registerArrayMinMaxFunctions(const std::string& prefix) {
  registerFunction<ArrayMinFunction, T, Array<T>>({prefix + "array_min"});
  registerFunction<ArrayMaxFunction, T, Array<T>>({prefix + "array_max"});
}

inline void registerArrayMinMaxFunctions(const std::string& prefix) {
  registerArrayMinMaxFunctions<int8_t>(prefix);
  registerArrayMinMaxFunctions<int16_t>(prefix);
  registerArrayMinMaxFunctions<int32_t>(prefix);
  registerArrayMinMaxFunctions<int64_t>(prefix);
  registerArrayMinMaxFunctions<int128_t>(prefix);
  registerArrayMinMaxFunctions<float>(prefix);
  registerArrayMinMaxFunctions<double>(prefix);
  registerArrayMinMaxFunctions<bool>(prefix);
  registerArrayMinMaxFunctions<Varchar>(prefix);
  registerArrayMinMaxFunctions<Timestamp>(prefix);
  registerArrayMinMaxFunctions<Date>(prefix);
}

template <typename T>
inline void registerArrayRemoveFunctions(const std::string& prefix) {
  registerFunction<ArrayRemoveFunction, Array<T>, Array<T>, T>(
      {prefix + "array_remove"});
}

inline void registerArrayRemoveFunctions(const std::string& prefix) {
  registerArrayRemoveFunctions<int8_t>(prefix);
  registerArrayRemoveFunctions<int16_t>(prefix);
  registerArrayRemoveFunctions<int32_t>(prefix);
  registerArrayRemoveFunctions<int64_t>(prefix);
  registerArrayRemoveFunctions<int128_t>(prefix);
  registerArrayRemoveFunctions<float>(prefix);
  registerArrayRemoveFunctions<double>(prefix);
  registerArrayRemoveFunctions<bool>(prefix);
  registerArrayRemoveFunctions<Timestamp>(prefix);
  registerArrayRemoveFunctions<Date>(prefix);
  registerArrayRemoveFunctions<Varbinary>(prefix);
  registerArrayRemoveFunctions<Generic<T1>>(prefix);
  registerFunction<
      ArrayRemoveFunctionString,
      Array<Varchar>,
      Array<Varchar>,
      Varchar>({prefix + "array_remove"});
}

void registerArrayFunctions(const std::string& prefix) {
  registerArrayMinMaxFunctions(prefix);
  registerArrayRemoveFunctions(prefix);
  registerSparkArrayFunctions(prefix);
  // Register array sort functions.
  exec::registerStatefulVectorFunction(
      prefix + "array_sort", arraySortSignatures(), makeArraySort);
  exec::registerStatefulVectorFunction(
      prefix + "sort_array", sortArraySignatures(), makeSortArray);
  exec::registerStatefulVectorFunction(
      prefix + "array_repeat",
      repeatSignatures(),
      makeRepeatAllowNegativeCount,
      repeatMetadata());
  registerFunction<
      ArrayFlattenFunction,
      Array<Generic<T1>>,
      Array<Array<Generic<T1>>>>({prefix + "flatten"});
  registerFunction<
      ArrayInsert,
      Array<Generic<T1>>,
      Array<Generic<T1>>,
      int32_t,
      Generic<T1>,
      bool>({prefix + "array_insert"});
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_get, prefix + "get");
  exec::registerStatefulVectorFunction(
      prefix + "shuffle",
      arrayShuffleWithCustomSeedSignatures(),
      makeArrayShuffleWithCustomSeed,
      getMetadataForArrayShuffle());
  registerIntegerSliceFunction(prefix);
}

} // namespace sparksql
} // namespace facebook::velox::functions
