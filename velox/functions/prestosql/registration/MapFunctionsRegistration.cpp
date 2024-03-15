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
#include "velox/expression/VectorFunction.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/MapConcat.h"
#include "velox/functions/prestosql/MapNormalize.h"
#include "velox/functions/prestosql/MapSubset.h"
#include "velox/functions/prestosql/MapTopN.h"
#include "velox/functions/prestosql/MultimapFromEntries.h"

namespace facebook::velox::functions {

namespace {

template <template <class> typename TFunc, typename T>
void registerMapSubset(const std::string& prefix) {
  registerFunction<TFunc, Map<T, Generic<T1>>, Map<T, Generic<T1>>, Array<T>>(
      {prefix + "map_subset"});
}

} // namespace

void registerMapFunctions(const std::string& prefix) {
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_filter, prefix + "map_filter");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_transform_keys, prefix + "transform_keys");
  VELOX_REGISTER_VECTOR_FUNCTION(
      udf_transform_values, prefix + "transform_values");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map, prefix + "map");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_entries, prefix + "map_entries");
  VELOX_REGISTER_VECTOR_FUNCTION(
      udf_map_from_entries, prefix + "map_from_entries");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_keys, prefix + "map_keys");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_values, prefix + "map_values");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_zip_with, prefix + "map_zip_with");

  VELOX_REGISTER_VECTOR_FUNCTION(udf_all_keys_match, prefix + "all_keys_match");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_any_keys_match, prefix + "any_keys_match");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_no_keys_match, prefix + "no_keys_match");

  VELOX_REGISTER_VECTOR_FUNCTION(
      udf_any_values_match, prefix + "any_values_match");
  VELOX_REGISTER_VECTOR_FUNCTION(
      udf_no_values_match, prefix + "no_values_match");

  registerMapConcatFunction(prefix + "map_concat");

  registerFunction<
      MultimapFromEntriesFunction,
      Map<Generic<T1>, Array<Generic<T2>>>,
      Array<Row<Generic<T1>, Generic<T2>>>>({prefix + "multimap_from_entries"});

  registerFunction<
      MapTopNFunction,
      Map<Generic<T1>, Orderable<T2>>,
      Map<Generic<T1>, Orderable<T2>>,
      int64_t>({prefix + "map_top_n"});

  registerMapSubset<MapSubsetIntegerFunction, int32_t>(prefix);
  registerMapSubset<MapSubsetBigintFunction, int64_t>(prefix);
  registerMapSubset<MapSubsetVarcharFunction, Varchar>(prefix);

  registerFunction<
      MapSubsetFunction,
      Map<Generic<T1>, Generic<T2>>,
      Map<Generic<T1>, Generic<T2>>,
      Array<Generic<T1>>>({prefix + "map_subset"});

  registerFunction<
      MapNormalizeFunction,
      Map<Varchar, double>,
      Map<Varchar, double>>({prefix + "map_normalize"});
}

void registerMapAllowingDuplicates(
    const std::string& name,
    const std::string& prefix) {
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_allow_duplicates, prefix + name);
}
} // namespace facebook::velox::functions
