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
#include "simdjson.h"
#include "velox/functions/Macros.h"
#include "velox/functions/UDFOutputString.h"
#include "velox/functions/prestosql/json/JsonPathTokenizer.h"
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
    result = false;

    ctx.parseDocument();
    if (ctx.jsonDoc.type() == simdjson::ondemand::json_type::number ||
        ctx.jsonDoc.type() == simdjson::ondemand::json_type::string ||
        ctx.jsonDoc.type() == simdjson::ondemand::json_type::boolean ||
        ctx.jsonDoc.type() == simdjson::ondemand::json_type::null) {
      result = true;
    }
  }
};

} // namespace facebook::velox::functions
