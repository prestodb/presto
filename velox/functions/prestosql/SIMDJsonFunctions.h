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

struct ParserContext {
 public:
  explicit ParserContext() noexcept;
  explicit ParserContext(const char* data, size_t length) noexcept
      : padded_json(data, length) {}
  void parseElement() {
    jsonEle = domParser.parse(padded_json);
  }
  void parseDocument() {
    jsonDoc = ondemandParser.iterate(padded_json);
  }
  simdjson::dom::element jsonEle;
  simdjson::ondemand::document jsonDoc;

 private:
  simdjson::padded_string padded_json;
  simdjson::dom::parser domParser;
  simdjson::ondemand::parser ondemandParser;
};

template <typename T>
struct SIMDIsJsonScalarFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(bool& result, const arg_type<Json>& json) {
    ParserContext ctx(json.data(), json.size());

    ctx.parseDocument();

    result =
        (ctx.jsonDoc.type() == simdjson::ondemand::json_type::number ||
         ctx.jsonDoc.type() == simdjson::ondemand::json_type::string ||
         ctx.jsonDoc.type() == simdjson::ondemand::json_type::boolean ||
         ctx.jsonDoc.type() == simdjson::ondemand::json_type::null);
  }
};

template <typename T>
struct SIMDJsonArrayContainsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool
  call(bool& result, const arg_type<Json>& json, const TInput& value) {
    ParserContext ctx(json.data(), json.size());
    result = false;

    try {
      ctx.parseDocument();
    } catch (const simdjson::simdjson_error&) {
      return false;
    }

    if (ctx.jsonDoc.type() != simdjson::ondemand::json_type::array) {
      return false;
    }

    for (auto&& v : ctx.jsonDoc) {
      try {
        if constexpr (std::is_same_v<TInput, bool>) {
          if (v.type() == simdjson::ondemand::json_type::boolean &&
              v.get_bool() == value) {
            result = true;
            break;
          }
        } else if constexpr (std::is_same_v<TInput, int64_t>) {
          if (v.type() == simdjson::ondemand::json_type::number &&
              v.get_number_type() ==
                  simdjson::ondemand::number_type::signed_integer &&
              v.get_int64() == value) {
            result = true;
            break;
          }
        } else if constexpr (std::is_same_v<TInput, double>) {
          if (v.type() == simdjson::ondemand::json_type::number &&
              v.get_number_type() ==
                  simdjson::ondemand::number_type::floating_point_number &&
              v.get_double() == value) {
            result = true;
            break;
          }
        } else {
          if (v.type() == simdjson::ondemand::json_type::string) {
            std::string_view rlt = v.get_string();
            std::string str_value{value.getString()};
            if (rlt.compare(str_value) == 0) {
              result = true;
              break;
            }
          }
        }
      } catch (const simdjson::simdjson_error& e) {
        // For bool/int64_t/double type, "get_bool()/get_int64()/get_double()"
        // may throw an exception if the conversion of json value to the
        // specified type failed, and the corresponding error code is
        // "INCORRECT_TYPE" or "NUMBER_ERROR".
        // If there are multiple json values in the json array, and some of them
        // cannot be converted to the type of input `value`, it should not
        // return null directly. It should continue to judge whether there is a
        // json value that matches the input value, e.g.
        // jsonArrayContains("[truet, false]", false) => true.
        if (e.error() != simdjson::INCORRECT_TYPE &&
            e.error() != simdjson::NUMBER_ERROR) {
          return false;
        }
      }
    }

    return true;
  }
};

template <typename T>
struct SIMDJsonArrayLengthFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(int64_t& len, const arg_type<Json>& json) {
    ParserContext ctx(json.data(), json.size());

    try {
      ctx.parseDocument();
    } catch (const simdjson::simdjson_error&) {
      return false;
    }

    if (ctx.jsonDoc.type() != simdjson::ondemand::json_type::array) {
      return false;
    }

    try {
      len = ctx.jsonDoc.count_elements();
    } catch (const simdjson::simdjson_error&) {
      return false;
    }

    return true;
  }
};

// jsonExtractScalar(json, json_path) -> varchar
// Like jsonExtract(), but returns the result value as a string (as opposed
// to being encoded as JSON). The value referenced by json_path must be a scalar
// (boolean, number or string)
template <typename T>
struct SIMDJsonExtractScalarFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Json>& json,
      const arg_type<Varchar>& jsonPath) {
    return callImpl(result, json, jsonPath) == simdjson::SUCCESS;
  }

 private:
  FOLLY_ALWAYS_INLINE bool callImpl(
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
    SIMDJSON_TRY(simdJsonExtract(json, extractor, consumer));

    if (resultStr.has_value()) {
      result.copy_from(*resultStr);
      return simdjson::SUCCESS;
    } else {
      return simdjson::NO_SUCH_FIELD;
    }
  }
};

template <typename T>
struct SIMDJsonExtractFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  bool call(
      out_type<Json>& result,
      const arg_type<Json>& json,
      const arg_type<Varchar>& jsonPath) {
    return callImpl(result, json, jsonPath) == simdjson::SUCCESS;
  }

 private:
  simdjson::error_code callImpl(
      out_type<Json>& result,
      const arg_type<Json>& json,
      const arg_type<Varchar>& jsonPath) {
    static constexpr std::string_view kNullString{"null"};
    std::string results;
    size_t resultSize = 0;
    auto consumer = [&results, &resultSize](auto& v) {
      // Add the separator for the JSON array.
      if (resultSize++ > 0) {
        results += ",";
      }
      // We could just convert v to a string using to_json_string directly, but
      // in that case the JSON wouldn't be parsed (it would just return the
      // contents directly) and we might miss invalid JSON.
      SIMDJSON_ASSIGN_OR_RAISE(auto vtype, v.type());
      switch (vtype) {
        case simdjson::ondemand::json_type::object: {
          SIMDJSON_ASSIGN_OR_RAISE(
              auto jsonStr, simdjson::to_json_string(v.get_object()));
          results += jsonStr;
          break;
        }
        case simdjson::ondemand::json_type::array: {
          SIMDJSON_ASSIGN_OR_RAISE(
              auto jsonStr, simdjson::to_json_string(v.get_array()));
          results += jsonStr;
          break;
        }
        case simdjson::ondemand::json_type::string:
        case simdjson::ondemand::json_type::number:
        case simdjson::ondemand::json_type::boolean: {
          SIMDJSON_ASSIGN_OR_RAISE(auto jsonStr, simdjson::to_json_string(v));
          results += jsonStr;
          break;
        }
        case simdjson::ondemand::json_type::null:
          results += kNullString;
          break;
      }
      return simdjson::SUCCESS;
    };

    auto& extractor = SIMDJsonExtractor::getInstance(jsonPath);
    SIMDJSON_TRY(simdJsonExtract(json, extractor, consumer));

    if (resultSize == 0) {
      if (extractor.isDefinitePath()) {
        // If the path didn't map to anything in the JSON object, return null.
        return simdjson::NO_SUCH_FIELD;
      }

      result.copy_from("[]");
    } else if (resultSize == 1 && extractor.isDefinitePath()) {
      // If there was only one value mapped to by the path, don't wrap it in an
      // array.
      result.copy_from(results);
    } else {
      // Add the square brackets to make it a valid JSON array.
      result.reserve(2 + results.size());
      result.append("[");
      result.append(results);
      result.append("]");
    }
    return simdjson::SUCCESS;
  }
};

template <typename T>
struct SIMDJsonSizeFunction {
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
    SIMDJSON_TRY(simdJsonExtract(json, extractor, consumer));

    if (resultCount == 0) {
      // If the path didn't map to anything in the JSON object, return null.
      return simdjson::NO_SUCH_FIELD;
    }

    result = resultCount == 1 ? singleResultSize : resultCount;

    return simdjson::SUCCESS;
  }
};

} // namespace facebook::velox::functions
