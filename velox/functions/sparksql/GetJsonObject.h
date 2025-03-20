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

#include "velox/functions/Macros.h"
#include "velox/functions/prestosql/json/SIMDJsonUtil.h"

namespace facebook::velox::functions::sparksql {

/// Parses a JSON string and returns the value at the specified path.
/// Simdjson On-Demand API is used to parse JSON string.
/// get_json_object(jsonString, path) -> value
template <typename T>
struct GetJsonObjectFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* /*json*/,
      const arg_type<Varchar>* jsonPath) {
    if (jsonPath != nullptr && checkJsonPath(*jsonPath)) {
      jsonPath_ = removeSingleQuotes(*jsonPath);
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& json,
      const arg_type<Varchar>& jsonPath) {
    // Spark requires the first char in jsonPath is '$'.
    if (!checkJsonPath(jsonPath)) {
      return false;
    }
    // jsonPath is "$".
    if (jsonPath.size() == 1) {
      result.append(json);
      return true;
    }
    simdjson::ondemand::document jsonDoc;
    simdjson::padded_string paddedJson(json.data(), json.size());
    if (simdjsonParse(paddedJson).get(jsonDoc)) {
      return false;
    }
    const auto formattedJsonPath = jsonPath_.has_value()
        ? jsonPath_.value()
        : removeSingleQuotes(jsonPath);
    try {
      // Can return error result or throw exception possibly.
      auto rawResult = jsonDoc.at_path(formattedJsonPath);
      if (rawResult.error()) {
        return false;
      }

      if (!extractStringResult(rawResult, result)) {
        return false;
      }
    } catch (simdjson::simdjson_error&) {
      return false;
    }

    const char* currentPos;
    if (jsonDoc.current_location().get(currentPos)) {
      return false;
    }

    return isValidEndingCharacter(currentPos);
  }

 private:
  FOLLY_ALWAYS_INLINE bool checkJsonPath(StringView jsonPath) {
    // Spark requires the first char in jsonPath is '$'.
    if (jsonPath.empty() || jsonPath.data()[0] != '$') {
      return false;
    }
    return true;
  }

  // Spark's json path requires field name surrounded by single quotes if it is
  // specified in "[]". But simdjson lib requires not. This method just removes
  // such single quotes to adapt to simdjson lib, e.g., converts "['a']['b']" to
  // "[a][b]".
  std::string removeSingleQuotes(StringView jsonPath) {
    // Skip the initial "$".
    std::string result(jsonPath.data() + 1, jsonPath.size() - 1);
    size_t pairEnd = 0;
    while (true) {
      auto pairBegin = result.find("['", pairEnd);
      if (pairBegin == std::string::npos) {
        break;
      }
      pairEnd = result.find("]", pairBegin);
      // If expected pattern, like ['a'], is not found.
      if (pairEnd == std::string::npos || result[pairEnd - 1] != '\'') {
        return "-1";
      }
      result.erase(pairEnd - 1, 1);
      result.erase(pairBegin + 1, 1);
      pairEnd -= 2;
    }
    return result;
  }

  // Extracts a string representation from a simdjson result. Handles various
  // JSON types including numbers, booleans, strings, objects, and arrays.
  // Returns true if the conversion is successful. Otherwise, returns false.
  bool extractStringResult(
      simdjson::simdjson_result<simdjson::ondemand::value> rawResult,
      out_type<Varchar>& result) {
    std::stringstream ss;
    switch (rawResult.type()) {
      // For number and bool types, we need to explicitly get the value
      // for specific types instead of using `ss << rawResult`. Thus, we
      // can make simdjson's internal parsing position moved and then we
      // can check the validity of ending character.
      case simdjson::ondemand::json_type::number: {
        switch (rawResult.get_number_type()) {
          case simdjson::ondemand::number_type::unsigned_integer: {
            uint64_t numberResult;
            if (!rawResult.get_uint64().get(numberResult)) {
              ss << numberResult;
              result.append(ss.str());
              return true;
            }
            return false;
          }
          case simdjson::ondemand::number_type::signed_integer: {
            int64_t numberResult;
            if (!rawResult.get_int64().get(numberResult)) {
              ss << numberResult;
              result.append(ss.str());
              return true;
            }
            return false;
          }
          case simdjson::ondemand::number_type::floating_point_number: {
            double numberResult;
            if (!rawResult.get_double().get(numberResult)) {
              ss << rawResult;
              result.append(ss.str());
              return true;
            }
            return false;
          }
          default:
            VELOX_UNREACHABLE();
        }
      }
      case simdjson::ondemand::json_type::boolean: {
        bool boolResult;
        if (!rawResult.get_bool().get(boolResult)) {
          result.append(boolResult ? "true" : "false");
          return true;
        }
        return false;
      }
      case simdjson::ondemand::json_type::string: {
        std::string_view stringResult;
        if (!rawResult.get_string().get(stringResult)) {
          result.append(stringResult);
          return true;
        }
        return false;
      }
      case simdjson::ondemand::json_type::object: {
        // For nested case, e.g., for "{"my": {"hello": 10}}", "$.my" will
        // return an object type.
        ss << rawResult;
        result.append(ss.str());
        return true;
      }
      case simdjson::ondemand::json_type::array: {
        ss << rawResult;
        result.append(ss.str());
        return true;
      }
      default:
        return false;
    }
  }

  // Checks whether the obtained result is followed by valid char. Because
  // On-Demand API we are using ignores json format validation for characters
  // following the current parsing position. As json doc is padded with NULL
  // characters, it's safe to do recursively check.
  bool isValidEndingCharacter(const char* currentPos) {
    char endingChar = *currentPos;
    if (endingChar == ',' || endingChar == '}' || endingChar == ']') {
      return true;
    }
    // These chars can be prior to a valid ending char. See reference:
    // https://github.com/simdjson/simdjson/blob/v3.9.0/dependencies/jsoncppdist/jsoncpp.cpp
    if (endingChar == ' ' || endingChar == '\r' || endingChar == '\n' ||
        endingChar == '\t') {
      return isValidEndingCharacter(++currentPos);
    }
    return false;
  }

  // Used for constant json path.
  std::optional<std::string> jsonPath_;
};

} // namespace facebook::velox::functions::sparksql
