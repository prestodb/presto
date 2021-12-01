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
#include "velox/functions/prestosql/VectorFunctions.h"
#include "velox/functions/lib/Re2Functions.h"
#include "velox/functions/prestosql/WidthBucketArray.h"
#include "velox/functions/prestosql/types/HyperLogLogType.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"

namespace facebook::velox::functions {

std::shared_ptr<exec::VectorFunction> makeRegexExtract(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  return makeRe2Extract(name, inputArgs, /*emptyNoMatch=*/false);
}

void registerVectorFunctions() {
  registerType("timestamp with time zone", [](auto /*childTypes*/) {
    return TIMESTAMP_WITH_TIME_ZONE();
  });

  registerType(
      "hyperloglog", [](auto /*childTypes*/) { return HYPERLOGLOG(); });

  VELOX_REGISTER_VECTOR_FUNCTION(udf_element_at, "element_at");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_subscript, "subscript");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_transform, "transform");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_reduce, "reduce");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_coalesce, "coalesce");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_is_null, "is_null");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_in, "in");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_not, "not");

  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_constructor, "array_constructor");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_distinct, "array_distinct");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_duplicates, "array_duplicates");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_intersect, "array_intersect");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_except, "array_except");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_cardinality, "cardinality");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_slice, "slice");

  VELOX_REGISTER_VECTOR_FUNCTION(udf_filter, "filter");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_filter, "map_filter");

  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_contains, "contains");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map, "map");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_concat, "map_concat");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_entries, "map_entries");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_keys, "map_keys");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_map_values, "map_values");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_zip, "zip");

  VELOX_REGISTER_VECTOR_FUNCTION(udf_lower, "lower");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_split, "split");

  VELOX_REGISTER_VECTOR_FUNCTION(udf_upper, "upper");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_concat, "concat");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_strpos, "strpos");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_replace, "replace");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_reverse, "reverse");

  exec::registerStatefulVectorFunction(
      "width_bucket", widthBucketArraySignature(), makeWidthBucketArray);

  exec::registerStatefulVectorFunction("like", likeSignatures(), makeLike);
  exec::registerStatefulVectorFunction(
      "regexp_extract", re2ExtractSignatures(), makeRegexExtract);
  exec::registerStatefulVectorFunction(
      "regexp_extract_all", re2ExtractAllSignatures(), makeRe2ExtractAll);
  exec::registerStatefulVectorFunction(
      "regexp_like", re2SearchSignatures(), makeRe2Search);

  VELOX_REGISTER_VECTOR_FUNCTION(udf_to_utf8, "to_utf8");

  VELOX_REGISTER_VECTOR_FUNCTION(udf_from_unixtime, "from_unixtime");

  // TODO Fix Koski parser and clean this up.
  VELOX_REGISTER_VECTOR_FUNCTION(udf_concat_row, "ROW");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_concat_row, "concatRow");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_concat_row, "concatrow");

  VELOX_REGISTER_VECTOR_FUNCTION(udf_least, "least");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_greatest, "greatest");
}

} // namespace facebook::velox::functions
