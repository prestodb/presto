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

#pragma once

#include "velox/functions/Macros.h"
#include "velox/functions/prestosql/json/JsonPathTokenizer.h"
#include "velox/functions/prestosql/json/SIMDJsonExtractor.h"
#include "velox/functions/prestosql/json/SIMDJsonWrapper.h"
#include "velox/functions/prestosql/types/JsonType.h"

namespace facebook::velox::functions {

/// json_array_length(jsonString) -> length
///
/// Returns the number of elements in the outermost JSON array from jsonString.
/// If jsonString is not a valid JSON array or NULL, the function returns null.
/// Presto:
/// https://prestodb.io/docs/current/functions/json.html#json_array_length-json-bigint
/// SparkSQL:
/// https://spark.apache.org/docs/latest/api/sql/index.html#json_array_length
template <typename T>
struct JsonArrayLengthFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TOutput>
  FOLLY_ALWAYS_INLINE bool call(TOutput& len, const arg_type<Json>& json) {
    simdjson::ondemand::document jsonDoc;

    simdjson::padded_string paddedJson(json.data(), json.size());
    if (simdjsonParse(paddedJson).get(jsonDoc)) {
      return false;
    }
    if (jsonDoc.type().error()) {
      return false;
    }

    if (jsonDoc.type() != simdjson::ondemand::json_type::array) {
      return false;
    }

    size_t numElements;
    if (jsonDoc.count_elements().get(numElements)) {
      return false;
    }

    VELOX_USER_CHECK_LE(
        numElements,
        std::numeric_limits<TOutput>::max(),
        "The json array length {} is bigger than the max value of output type {}.",
        numElements,
        std::numeric_limits<TOutput>::max());

    len = numElements;
    return true;
  }
};
} // namespace facebook::velox::functions
