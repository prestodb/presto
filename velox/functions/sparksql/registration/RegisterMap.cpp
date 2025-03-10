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
#include "velox/functions/lib/MapConcat.h"
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/sparksql/Size.h"

namespace facebook::velox::functions {
extern void registerElementAtFunction(
    const std::string& name,
    bool enableCaching);

void registerSparkMapFunctions(const std::string& prefix) {
  VELOX_REGISTER_VECTOR_FUNCTION(
      udf_map_allow_duplicates, prefix + "map_from_arrays");
  // Spark and Presto map_filter function has the same definition:
  //   function expression corresponds to body, arguments to signature
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_filter, prefix + "map_filter");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_entries, prefix + "map_entries");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_keys, prefix + "map_keys");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_values, prefix + "map_values");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_zip_with, prefix + "map_zip_with");

  registerMapConcatAllowSingleArg(prefix + "map_concat");
}

namespace sparksql {
void registerMapFunctions(const std::string& prefix) {
  registerSparkMapFunctions(prefix);
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map, prefix + "map");
  // This is the semantics of spark.sql.ansi.enabled = false.
  registerElementAtFunction(prefix + "element_at", true);
  registerSize(prefix + "size");
}
} // namespace sparksql
} // namespace facebook::velox::functions
