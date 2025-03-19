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
#include "velox/functions/UDFOutputString.h"
#include "velox/functions/prestosql/json/JsonPathTokenizer.h"
#include "velox/functions/prestosql/json/SIMDJsonExtractor.h"
#include "velox/functions/prestosql/json/SIMDJsonWrapper.h"
#include "velox/functions/prestosql/types/JsonType.h"

namespace facebook::velox::functions {

template <typename T>
struct IsJsonScalarFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status call(bool& result, const arg_type<Json>& json) {
    simdjson::ondemand::document jsonDoc;

    simdjson::padded_string paddedJson(json.data(), json.size());
    if (auto errorCode = simdjsonParse(paddedJson).get(jsonDoc)) {
      if (threadSkipErrorDetails()) {
        return Status::UserError();
      }
      return Status::UserError(
          "Failed to parse JSON: {}", simdjson::error_message(errorCode));
    }

    result =
        (jsonDoc.type() == simdjson::ondemand::json_type::number ||
         jsonDoc.type() == simdjson::ondemand::json_type::string ||
         jsonDoc.type() == simdjson::ondemand::json_type::boolean ||
         jsonDoc.type() == simdjson::ondemand::json_type::null);

    return Status::OK();
  }
};

template <typename TExec>
struct JsonArrayContainsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  template <typename T>
  FOLLY_ALWAYS_INLINE bool
  call(bool& result, const arg_type<Json>& json, const T& value) {
    if constexpr (std::is_same_v<T, double>) {
      if (!std::isfinite(value)) {
        result = false;
        return true;
      }
    }

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

    result = false;
    for (auto&& v : jsonDoc) {
      if (v.error()) {
        return false;
      }
      if constexpr (std::is_same_v<T, bool>) {
        bool boolValue;
        if (v.type() == simdjson::ondemand::json_type::boolean &&
            !v.get_bool().get(boolValue) && boolValue == value) {
          result = true;
          break;
        }
      } else if constexpr (std::is_same_v<T, int64_t>) {
        int64_t intValue;
        if (v.type() == simdjson::ondemand::json_type::number &&
            v.get_number_type() ==
                simdjson::ondemand::number_type::signed_integer &&
            !v.get_int64().get(intValue) && intValue == value) {
          result = true;
          break;
        }
      } else if constexpr (std::is_same_v<T, double>) {
        double doubleValue;
        if (v.type() == simdjson::ondemand::json_type::number &&
            v.get_number_type() ==
                simdjson::ondemand::number_type::floating_point_number &&
            !v.get_double().get(doubleValue) && doubleValue == value) {
          result = true;
          break;
        }
      } else {
        std::string_view stringValue;
        if (v.type() == simdjson::ondemand::json_type::string &&
            !v.get_string().get(stringValue) &&
            ((std::string_view)value).compare(stringValue) == 0) {
          result = true;
          break;
        }
      }
    }

    return true;
  }
};

template <typename TExec>
struct JsonArrayGetFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE bool
  call(out_type<Json>& result, const arg_type<Json>& jsonArray, int64_t index) {
    simdjson::ondemand::document jsonDoc;

    simdjson::padded_string paddedJson(jsonArray.data(), jsonArray.size());
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

    if (index >= 0) {
      if (index >= numElements) {
        return false;
      }
    } else if ((int64_t)numElements + index < 0) {
      return false;
    } else {
      index += numElements;
    }

    std::string_view resultStr;
    if (simdjson::to_json_string(jsonDoc.at(index)).get(resultStr)) {
      return false;
    }

    result.copy_from(resultStr);
    return true;
  }
};

// json_extract_scalar(json, json_path) -> varchar
// Like json_extract(), but returns the result value as a string (as opposed
// to being encoded as JSON). The value referenced by json_path must be a scalar
// (boolean, number or string)
template <typename T>
struct JsonExtractScalarFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Json>& json,
      const arg_type<Varchar>& jsonPath) {
    return callImpl(result, json, jsonPath) == simdjson::SUCCESS;
  }

 private:
  FOLLY_ALWAYS_INLINE simdjson::error_code callImpl(
      out_type<Varchar>& result,
      const arg_type<Json>& json,
      const arg_type<Varchar>& jsonPath) {
    bool resultPopulated = false;
    std::optional<std::string> resultStr;

    auto consumer = [&resultStr, &resultPopulated](auto& v) {
      if (resultPopulated) {
        // We should just get a single value, if we see multiple, it's an error
        // and we should return null.
        resultStr = std::nullopt;
        return simdjson::SUCCESS;
      }

      resultPopulated = true;

      SIMDJSON_ASSIGN_OR_RAISE(auto vtype, v.type());
      switch (vtype) {
        case simdjson::ondemand::json_type::boolean: {
          SIMDJSON_ASSIGN_OR_RAISE(bool vbool, v.get_bool());
          resultStr = vbool ? "true" : "false";
          break;
        }
        case simdjson::ondemand::json_type::string: {
          SIMDJSON_ASSIGN_OR_RAISE(resultStr, v.get_string());
          break;
        }
        case simdjson::ondemand::json_type::object:
        case simdjson::ondemand::json_type::array:
        case simdjson::ondemand::json_type::null:
          // Do nothing.
          break;
        default: {
          SIMDJSON_ASSIGN_OR_RAISE(resultStr, simdjson::to_json_string(v));
        }
      }
      return simdjson::SUCCESS;
    };

    auto& extractor = SIMDJsonExtractor::getInstance(jsonPath);

    // Check for valid json
    simdjson::padded_string paddedJson(json.data(), json.size());
    {
      SIMDJSON_ASSIGN_OR_RAISE(auto jsonDoc, simdjsonParse(paddedJson));
      simdjson::ondemand::document parsedDoc;
      if (simdjsonParse(paddedJson).get(parsedDoc)) {
        return simdjson::TAPE_ERROR;
      }
    }

    bool isDefinitePath = true;
    SIMDJSON_TRY(extractor.extract(paddedJson, consumer, isDefinitePath));

    if (resultStr.has_value()) {
      result.copy_from(*resultStr);
      return simdjson::SUCCESS;
    } else {
      return simdjson::NO_SUCH_FIELD;
    }
  }
};

template <typename T>
struct JsonSizeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      int64_t& result,
      const arg_type<Json>& json,
      const arg_type<Varchar>& jsonPath) {
    return callImpl(result, json, jsonPath) == simdjson::SUCCESS;
  }

 private:
  FOLLY_ALWAYS_INLINE simdjson::error_code callImpl(
      int64_t& result,
      const arg_type<Json>& json,
      const arg_type<Varchar>& jsonPath) {
    size_t resultCount = 0;
    size_t singleResultSize = 0;
    auto consumer = [&resultCount, &singleResultSize](auto& v) {
      resultCount++;

      if (resultCount == 1) {
        // We only need the size of the actual object if there's only one
        // returned, if multiple are returned we use the number of objects
        // returned instead.
        SIMDJSON_ASSIGN_OR_RAISE(auto vtype, v.type());
        switch (vtype) {
          case simdjson::ondemand::json_type::object: {
            SIMDJSON_ASSIGN_OR_RAISE(singleResultSize, v.count_fields());
            break;
          }
          case simdjson::ondemand::json_type::array: {
            SIMDJSON_ASSIGN_OR_RAISE(singleResultSize, v.count_elements());
            break;
          }
          case simdjson::ondemand::json_type::string:
          case simdjson::ondemand::json_type::number:
          case simdjson::ondemand::json_type::boolean:
          case simdjson::ondemand::json_type::null:
            singleResultSize = 0;
            break;
        }
      }
      return simdjson::SUCCESS;
    };

    auto& extractor = SIMDJsonExtractor::getInstance(jsonPath);
    bool isDefinitePath = true;
    simdjson::padded_string paddedJson(json.data(), json.size());
    SIMDJSON_TRY(extractor.extract(paddedJson, consumer, isDefinitePath));

    if (resultCount == 0) {
      // If the path didn't map to anything in the JSON object, return null.
      return simdjson::NO_SUCH_FIELD;
    }

    result = resultCount == 1 ? singleResultSize : resultCount;

    return simdjson::SUCCESS;
  }
};

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
