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
#include "simdjson/singleheader/simdjson.h"
#include "velox/functions/Macros.h"
#include "velox/functions/UDFOutputString.h"
#include "velox/functions/prestosql/json/JsonPathTokenizer.h"
#include "velox/functions/prestosql/json/SIMDJsonExtractor.h"
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

    try {
      for (auto&& v : ctx.jsonDoc) {
        if constexpr (std::is_same_v<TInput, bool>) {
          if (v.type() == simdjson::ondemand::json_type::boolean &&
              v.get_bool() == value) {
            result = true;
            break;
          }
        } else if constexpr (std::is_same_v<TInput, int64_t>) {
          if (v.type() == simdjson::ondemand::json_type::number &&
              ((v.get_number_type() ==
                    simdjson::ondemand::number_type::signed_integer &&
                v.get_int64() == value) ||
               (v.get_number_type() ==
                    simdjson::ondemand::number_type::unsigned_integer &&
                v.get_uint64() == value))) {
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
      }
    } catch (const simdjson::simdjson_error&) {
      return false;
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
    bool resultPopulated = false;
    std::optional<std::string> resultStr;
    auto consumer = [&resultStr, &resultPopulated](auto& v) {
      if (resultPopulated) {
        // We should just get a single value, if we see multiple, it's an error
        // and we should return null.
        resultStr = std::nullopt;
        return;
      }

      resultPopulated = true;

      switch (v.type()) {
        case simdjson::ondemand::json_type::boolean:
          resultStr = v.get_bool().value() ? "true" : "false";
          break;
        case simdjson::ondemand::json_type::string:
          resultStr = v.get_string().value();
          break;
        case simdjson::ondemand::json_type::object:
        case simdjson::ondemand::json_type::array:
        case simdjson::ondemand::json_type::null:
          // Do nothing.
          break;
        default:
          resultStr = simdjson::to_json_string(v).value();
      }
    };

    if (!simdJsonExtract(json, jsonPath, consumer)) {
      // If there's an error parsing the JSON, return null.
      return false;
    }

    if (resultStr.has_value()) {
      result.copy_from(*resultStr);
      return true;
    } else {
      return false;
    }
  }
};

} // namespace facebook::velox::functions
