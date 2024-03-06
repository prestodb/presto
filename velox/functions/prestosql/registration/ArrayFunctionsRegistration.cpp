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

#include <string>

#include "velox/functions/Registerer.h"
#include "velox/functions/lib/Repeat.h"
#include "velox/functions/prestosql/ArrayConstructor.h"
#include "velox/functions/prestosql/ArrayFunctions.h"
#include "velox/functions/prestosql/ArraySort.h"
#include "velox/functions/prestosql/WidthBucketArray.h"

namespace facebook::velox::functions {
extern void registerArrayConcatFunctions(const std::string& prefix);
extern void registerArrayNGramsFunctions(const std::string& prefix);

template <typename T>
inline void registerArrayMinMaxFunctions(const std::string& prefix) {
  registerFunction<ArrayMinFunction, T, Array<T>>({prefix + "array_min"});
  registerFunction<ArrayMaxFunction, T, Array<T>>({prefix + "array_max"});
}

template <typename T>
inline void registerArrayJoinFunctions(const std::string& prefix) {
  registerFunction<
      ParameterBinder<ArrayJoinFunction, T>,
      Varchar,
      Array<T>,
      Varchar>({prefix + "array_join"});

  registerFunction<
      ParameterBinder<ArrayJoinFunction, T>,
      Varchar,
      Array<T>,
      Varchar,
      Varchar>({prefix + "array_join"});
}

template <typename T>
inline void registerArrayCombinationsFunctions(const std::string& prefix) {
  registerFunction<
      ParameterBinder<CombinationsFunction, T>,
      Array<Array<T>>,
      Array<T>,
      int32_t>({prefix + "combinations"});
}

template <typename T>
inline void registerArrayHasDuplicatesFunctions(const std::string& prefix) {
  registerFunction<
      ParameterBinder<ArrayHasDuplicatesFunction, T>,
      bool,
      Array<T>>({prefix + "array_has_duplicates"});
}

template <typename T>
inline void registerArrayFrequencyFunctions(const std::string& prefix) {
  registerFunction<
      ParameterBinder<ArrayFrequencyFunction, T>,
      Map<T, int>,
      Array<T>>({prefix + "array_frequency"});
}

template <typename T>
inline void registerArrayNormalizeFunctions(const std::string& prefix) {
  registerFunction<
      ParameterBinder<ArrayNormalizeFunction, T>,
      Array<T>,
      Array<T>,
      T>({prefix + "array_normalize"});
}

template <typename T>
inline void registerArrayTrimFunctions(const std::string& prefix) {
  registerFunction<ArrayTrimFunction, Array<T>, Array<T>, int64_t>(
      {prefix + "trim_array"});
}

template <typename T>
inline void registerArrayRemoveNullFunctions(const std::string& prefix) {
  registerFunction<ArrayRemoveNullFunction, Array<T>, Array<T>>(
      {prefix + "remove_nulls"});
}

template <typename T>
inline void registerArrayUnionFunctions(const std::string& prefix) {
  registerFunction<ArrayUnionFunction, Array<T>, Array<T>, Array<T>>(
      {prefix + "array_union"});
}

template <typename T>
inline void registerArrayRemoveFunctions(const std::string& prefix) {
  registerFunction<ArrayRemoveFunction, Array<T>, Array<T>, T>(
      {prefix + "array_remove"});
}

void registerInternalArrayFunctions() {
  VELOX_REGISTER_VECTOR_FUNCTION(
      udf_$internal$canonicalize, "$internal$canonicalize");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_$internal$contains, "$internal$contains");
}

void registerArrayFunctions(const std::string& prefix) {
  registerArrayConstructor(prefix + "array_constructor");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_all_match, prefix + "all_match");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_any_match, prefix + "any_match");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_none_match, prefix + "none_match");

  VELOX_REGISTER_VECTOR_FUNCTION(udf_find_first, prefix + "find_first");
  VELOX_REGISTER_VECTOR_FUNCTION(
      udf_find_first_index, prefix + "find_first_index");

  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_distinct, prefix + "array_distinct");
  VELOX_REGISTER_VECTOR_FUNCTION(
      udf_array_duplicates, prefix + "array_duplicates");
  VELOX_REGISTER_VECTOR_FUNCTION(
      udf_array_intersect, prefix + "array_intersect");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_contains, prefix + "contains");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_except, prefix + "array_except");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_arrays_overlap, prefix + "arrays_overlap");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_slice, prefix + "slice");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_zip, prefix + "zip");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_zip_with, prefix + "zip_with");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_position, prefix + "array_position");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_shuffle, prefix + "shuffle");

  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_sort, prefix + "array_sort");
  VELOX_REGISTER_VECTOR_FUNCTION(
      udf_array_sort_desc, prefix + "array_sort_desc");

  exec::registerExpressionRewrite([prefix](const auto& expr) {
    return rewriteArraySortCall(prefix, expr);
  });

  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_sum, prefix + "array_sum");
  exec::registerStatefulVectorFunction(
      prefix + "repeat", repeatSignatures(), makeRepeat);
  VELOX_REGISTER_VECTOR_FUNCTION(udf_sequence, prefix + "sequence");

  exec::registerStatefulVectorFunction(
      prefix + "width_bucket",
      widthBucketArraySignature(),
      makeWidthBucketArray);

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
  registerArrayMinMaxFunctions<Orderable<T1>>(prefix);

  registerArrayJoinFunctions<int8_t>(prefix);
  registerArrayJoinFunctions<int16_t>(prefix);
  registerArrayJoinFunctions<int32_t>(prefix);
  registerArrayJoinFunctions<int64_t>(prefix);
  registerArrayJoinFunctions<int128_t>(prefix);
  registerArrayJoinFunctions<float>(prefix);
  registerArrayJoinFunctions<double>(prefix);
  registerArrayJoinFunctions<bool>(prefix);
  registerArrayJoinFunctions<Varchar>(prefix);
  registerArrayJoinFunctions<Timestamp>(prefix);
  registerArrayJoinFunctions<Date>(prefix);

  registerFunction<ArrayAverageFunction, double, Array<double>>(
      {prefix + "array_average"});

  registerArrayConcatFunctions(prefix);
  registerArrayNGramsFunctions(prefix);

  registerFunction<
      ArrayFlattenFunction,
      Array<Generic<T1>>,
      Array<Array<Generic<T1>>>>({prefix + "flatten"});

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

  registerArrayTrimFunctions<int8_t>(prefix);
  registerArrayTrimFunctions<int16_t>(prefix);
  registerArrayTrimFunctions<int32_t>(prefix);
  registerArrayTrimFunctions<int64_t>(prefix);
  registerArrayTrimFunctions<int128_t>(prefix);
  registerArrayTrimFunctions<float>(prefix);
  registerArrayTrimFunctions<double>(prefix);
  registerArrayTrimFunctions<bool>(prefix);
  registerArrayTrimFunctions<Timestamp>(prefix);
  registerArrayTrimFunctions<Date>(prefix);
  registerArrayTrimFunctions<Varbinary>(prefix);
  registerArrayTrimFunctions<Generic<T1>>(prefix);
  registerFunction<
      ArrayTrimFunctionString,
      Array<Varchar>,
      Array<Varchar>,
      int64_t>({prefix + "trim_array"});

  registerArrayRemoveNullFunctions<int8_t>(prefix);
  registerArrayRemoveNullFunctions<int16_t>(prefix);
  registerArrayRemoveNullFunctions<int32_t>(prefix);
  registerArrayRemoveNullFunctions<int64_t>(prefix);
  registerArrayRemoveNullFunctions<int128_t>(prefix);
  registerArrayRemoveNullFunctions<float>(prefix);
  registerArrayRemoveNullFunctions<double>(prefix);
  registerArrayRemoveNullFunctions<bool>(prefix);
  registerArrayRemoveNullFunctions<Timestamp>(prefix);
  registerArrayRemoveNullFunctions<Date>(prefix);
  registerArrayRemoveNullFunctions<Varbinary>(prefix);
  registerArrayRemoveNullFunctions<Generic<T1>>(prefix);
  registerFunction<
      ArrayRemoveNullFunctionString,
      Array<Varchar>,
      Array<Varchar>>({prefix + "remove_nulls"});

  registerArrayUnionFunctions<int8_t>(prefix);
  registerArrayUnionFunctions<int16_t>(prefix);
  registerArrayUnionFunctions<int32_t>(prefix);
  registerArrayUnionFunctions<int64_t>(prefix);
  registerArrayUnionFunctions<int128_t>(prefix);
  registerArrayUnionFunctions<float>(prefix);
  registerArrayUnionFunctions<double>(prefix);
  registerArrayUnionFunctions<bool>(prefix);
  registerArrayUnionFunctions<Timestamp>(prefix);
  registerArrayUnionFunctions<Date>(prefix);
  registerArrayUnionFunctions<Varbinary>(prefix);
  registerArrayUnionFunctions<Generic<T1>>(prefix);

  registerArrayCombinationsFunctions<int8_t>(prefix);
  registerArrayCombinationsFunctions<int16_t>(prefix);
  registerArrayCombinationsFunctions<int32_t>(prefix);
  registerArrayCombinationsFunctions<int64_t>(prefix);
  registerArrayCombinationsFunctions<int128_t>(prefix);
  registerArrayCombinationsFunctions<float>(prefix);
  registerArrayCombinationsFunctions<double>(prefix);
  registerArrayCombinationsFunctions<bool>(prefix);
  registerArrayCombinationsFunctions<Varchar>(prefix);
  registerArrayCombinationsFunctions<Timestamp>(prefix);
  registerArrayCombinationsFunctions<Date>(prefix);

  registerArrayHasDuplicatesFunctions<int8_t>(prefix);
  registerArrayHasDuplicatesFunctions<int16_t>(prefix);
  registerArrayHasDuplicatesFunctions<int32_t>(prefix);
  registerArrayHasDuplicatesFunctions<int64_t>(prefix);
  registerArrayHasDuplicatesFunctions<int128_t>(prefix);
  registerArrayHasDuplicatesFunctions<Varchar>(prefix);

  registerArrayFrequencyFunctions<bool>(prefix);
  registerArrayFrequencyFunctions<int8_t>(prefix);
  registerArrayFrequencyFunctions<int16_t>(prefix);
  registerArrayFrequencyFunctions<int32_t>(prefix);
  registerArrayFrequencyFunctions<int64_t>(prefix);
  registerArrayFrequencyFunctions<int128_t>(prefix);
  registerArrayFrequencyFunctions<float>(prefix);
  registerArrayFrequencyFunctions<double>(prefix);
  registerArrayFrequencyFunctions<Timestamp>(prefix);
  registerArrayFrequencyFunctions<Date>(prefix);
  registerArrayFrequencyFunctions<Varchar>(prefix);

  registerArrayNormalizeFunctions<float>(prefix);
  registerArrayNormalizeFunctions<double>(prefix);
}
} // namespace facebook::velox::functions
