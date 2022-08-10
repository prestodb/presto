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

namespace facebook::velox::functions {
void registerMapFunctions() {
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_filter, "map_filter");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_transform_keys, "transform_keys");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_transform_values, "transform_values");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map, "map");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_concat, "map_concat");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_entries, "map_entries");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_keys, "map_keys");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_values, "map_values");
  VELOX_REGISTER_VECTOR_FUNCTION(
      udf_map_concat_empty_null, "map_concat_empty_nulls");
}

void registerMapAllowingDuplicates(const std::string& name) {
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_allow_duplicates, name);
}
} // namespace facebook::velox::functions
