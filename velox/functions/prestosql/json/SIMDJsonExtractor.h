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

#include <string>

#include "folly/Range.h"
#include "folly/dynamic.h"
#include "velox/common/base/Exceptions.h"
#include "velox/functions/prestosql/json/JsonPathTokenizer.h"
#include "velox/functions/prestosql/json/SIMDJsonUtil.h"
#include "velox/type/StringView.h"

namespace facebook::velox::functions {

class SIMDJsonExtractor {
 public:
  template <typename TConsumer>
  simdjson::error_code extract(
      simdjson::ondemand::value& json,
      TConsumer& consumer,
      size_t tokenStartIndex = 0);

  /// Returns true if this extractor was initialized with the trivial path "$".
  bool isRootOnlyPath() {
    return tokens_.empty();
  }

  /// Returns true if this extractor was initialized with a path that's
  /// guaranteed to match at most one entry.
  bool isDefinitePath() {
    for (const auto& token : tokens_) {
      if (token == "*") {
        return false;
      }
    }

    return true;
  }

  /// Use this method to get an instance of SIMDJsonExtractor given a JSON path.
  /// Given the nature of the cache, it's important this is only used by
  /// the callers of simdJsonExtract.
  static SIMDJsonExtractor& getInstance(folly::StringPiece path);

 private:
  // Shouldn't instantiate directly - use getInstance().
  explicit SIMDJsonExtractor(const std::string& path) {
    if (!tokenize(path)) {
      VELOX_USER_FAIL("Invalid JSON path: {}", path);
    }
  }

  bool tokenize(const std::string& path);

  // Max number of extractors cached in extractorCache.
  static const uint32_t kMaxCacheSize{32};

  std::vector<std::string> tokens_;
};

simdjson::error_code extractObject(
    simdjson::ondemand::value& jsonObj,
    const std::string& key,
    std::optional<simdjson::ondemand::value>& ret);

simdjson::error_code extractArray(
    simdjson::ondemand::value& jsonValue,
    const std::string& index,
    std::optional<simdjson::ondemand::value>& ret);

template <typename TConsumer>
simdjson::error_code SIMDJsonExtractor::extract(
    simdjson::ondemand::value& json,
    TConsumer& consumer,
    size_t tokenStartIndex) {
  simdjson::ondemand::value input = json;
  // Temporary extraction result holder.
  std::optional<simdjson::ondemand::value> result;

  for (int tokenIndex = tokenStartIndex; tokenIndex < tokens_.size();
       tokenIndex++) {
    auto& token = tokens_[tokenIndex];
    if (input.type() == simdjson::ondemand::json_type::object) {
      SIMDJSON_TRY(extractObject(input, token, result));
    } else if (input.type() == simdjson::ondemand::json_type::array) {
      if (token == "*") {
        for (auto child : input.get_array()) {
          if (tokenIndex == tokens_.size() - 1) {
            // If this is the last token in the path, consume each element in
            // the array.
            SIMDJSON_TRY(consumer(child.value()));
          } else {
            // If not, then recursively call the extract function on each
            // element in the array.
            SIMDJSON_TRY(extract(child.value(), consumer, tokenIndex + 1));
          }
        }

        return simdjson::SUCCESS;
      } else {
        SIMDJSON_TRY(extractArray(input, token, result));
      }
    }
    if (!result) {
      return simdjson::SUCCESS;
    }

    input = result.value();
    result.reset();
  }

  return consumer(input);
};

/**
 * Extract element(s) from a JSON object using the given path.
 * @param json: A JSON object
 * @param path: Path to locate a JSON object. Following operators are supported.
 *              "$"      Root member of a JSON structure no matter if it's an
 *                       object, an array, or a scalar.
 *              "."      Child operator to get a child object.
 *              "[]"     Subscript operator for array.
 *              "*"      Wildcard for [], get all the elements of an array.
 * @param consumer: Function to consume the extracted elements. Should be able
 *                  to take an argument that can either be a
 *                  simdjson::ondemand::document or a simdjson::ondemand::value.
 *                  Note that once consumer returns, it should be assumed that
 *                  the argument passed in is no longer valid, so do not attempt
 *                  to store it as is in the consumer.
 * @return Return simdjson::SUCCESS on success.
 *         If any errors are encountered parsing the JSON, returns the error.
 */
template <typename TConsumer>
simdjson::error_code simdJsonExtract(
    const velox::StringView& json,
    SIMDJsonExtractor& extractor,
    TConsumer&& consumer) {
  simdjson::padded_string paddedJson(json.data(), json.size());
  SIMDJSON_ASSIGN_OR_RAISE(auto jsonDoc, simdjsonParse(paddedJson));

  if (extractor.isRootOnlyPath()) {
    // If the path is just to return the original object, call consumer on the
    // document.  Note, we cannot convert this to a value as this is not
    // supported if the object is a scalar.
    return consumer(jsonDoc);
  }
  SIMDJSON_ASSIGN_OR_RAISE(auto value, jsonDoc.get_value());
  return extractor.extract(value, std::forward<TConsumer>(consumer));
}

} // namespace facebook::velox::functions
