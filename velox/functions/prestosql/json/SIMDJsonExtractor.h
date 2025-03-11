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
  /**
   * Extract element(s) from a JSON object using the given path.
   * See class comments in JsonPathTokenizer.h for more details on how json path
   * is interpreted and has notable differences compared to Jayway (used by
   * Presto java).
   * @param json: A json string of type simdjson::padded_string
   * @param path: Path to locate a JSON object. Following operators are
   * supported.
   *              "$"      Root member of a JSON structure no matter if it's an
   *                       object, an array, or a scalar.
   *              "."      Child operator to get a child object.
   *              "[]"     Subscript operator for array or object.
   *              "*"      Wildcard operator, gets all the elements of an array
   *                       or all values in an object.
   *              ".."     Recursive descent operator, where it visits the input
   *                       node and each of its descendants such that nodes of
   * any array are visited in array order, and nodes are visited before their
   * descendants.
   * @param consumer: Function to consume the extracted elements. Should be able
   *                  to take an argument that can either be a
   *                  simdjson::ondemand::document or a
   *                  simdjson::ondemand::value. Note: If the consumer holds
   *                  onto string_view(s) generated from applying
   *                  simdjson::to_json_string to the arguments, then those
   *                  string_view(s) will reference the original `json`
   *                  padded_string and remain valid as long as it remains
   *                  in scope.
   * @param isDefinitePath is an output param that will get set to
   *                       false if a token is evaluated which can return
   *                       multiple results like '*'.
   * @return Return simdjson::SUCCESS on success.
   *         If any errors are encountered parsing the JSON, returns the error.
   */
  template <typename TConsumer>
  simdjson::error_code extract(
      const simdjson::padded_string& json,
      TConsumer& consumer,
      bool& isDefinitePath);

  /// Returns true if this extractor was initialized with the trivial path "$".
  bool isRootOnlyPath() {
    return tokens_.empty();
  }

  /// Use this method to get an instance of SIMDJsonExtractor given a JSON path.
  /// Uses a thread local cache, therefore the caller must ensure the returned
  /// instance is not passed between threads.
  static SIMDJsonExtractor& getInstance(folly::StringPiece path);

 private:
  // Shouldn't instantiate directly - use getInstance().
  explicit SIMDJsonExtractor(const std::string& path) {
    if (!tokenize(path)) {
      VELOX_USER_FAIL("Invalid JSON path: {}", path);
    }
  }

  bool tokenize(const std::string& path);

  template <typename TConsumer>
  simdjson::error_code extractInternal(
      simdjson::ondemand::value& json,
      TConsumer& consumer,
      bool& isDefinitePath,
      size_t tokenStartIndex = 0);

  // Implementation for the recursive operator (..) where it visits the input
  // node and each of its descendants such that nodes of any array are visited
  // in array order, and nodes are visited before their descendants.
  template <typename TConsumer>
  simdjson::error_code visitRecursive(
      simdjson::ondemand::value& json,
      TConsumer& consumer,
      bool& isDefinitePath,
      size_t startTokenIdx);

  // Max number of extractors cached in extractorCache.
  static const uint32_t kMaxCacheSize{32};

  std::vector<JsonPathTokenizer::Token> tokens_;
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
    const simdjson::padded_string& paddedJson,
    TConsumer& consumer,
    bool& isDefinitePath) {
  SIMDJSON_ASSIGN_OR_RAISE(auto jsonDoc, simdjsonParse(paddedJson));
  SIMDJSON_ASSIGN_OR_RAISE(auto isScalar, jsonDoc.is_scalar());
  if (isScalar) {
    // Note, we cannot convert this to a value as this is not supported if the
    // object is a scalar.
    if (isRootOnlyPath()) {
      return consumer(jsonDoc);
    }
    VELOX_CHECK_GT(tokens_.size(), 0);
    auto& selector = tokens_[0].selector;
    if (tokens_[0].selector == JsonPathTokenizer::Selector::WILDCARD ||
        selector == JsonPathTokenizer::Selector::RECURSIVE) {
      isDefinitePath = false;
    }
    return simdjson::SUCCESS;
  }
  SIMDJSON_ASSIGN_OR_RAISE(auto value, jsonDoc.get_value());
  return extractInternal(value, consumer, isDefinitePath, 0);
}

template <typename TConsumer>
simdjson::error_code SIMDJsonExtractor::extractInternal(
    simdjson::ondemand::value& json,
    TConsumer& consumer,
    bool& isDefinitePath,
    size_t tokenStartIndex) {
  simdjson::ondemand::value input = json;
  // Temporary extraction result holder.
  std::optional<simdjson::ondemand::value> result;
  for (int tokenIndex = tokenStartIndex; tokenIndex < tokens_.size();
       tokenIndex++) {
    auto& token = tokens_[tokenIndex];
    auto& selector = token.selector;
    if (selector == JsonPathTokenizer::Selector::WILDCARD ||
        selector == JsonPathTokenizer::Selector::RECURSIVE) {
      isDefinitePath = false;
    }
    if (input.type() == simdjson::ondemand::json_type::object) {
      if (selector == JsonPathTokenizer::Selector::WILDCARD) {
        SIMDJSON_ASSIGN_OR_RAISE(auto jsonObj, input.get_object());
        for (auto field : jsonObj) {
          simdjson::ondemand::value val = field.value();
          if (tokenIndex == tokens_.size() - 1) {
            // Consume each element in the object.
            SIMDJSON_TRY(consumer(val));
          } else {
            // If not, then recursively call the extractInternal function on
            // each element in the object.
            SIMDJSON_TRY(
                extractInternal(val, consumer, isDefinitePath, tokenIndex + 1));
          }
        }
        return simdjson::SUCCESS;
      } else if (selector == JsonPathTokenizer::Selector::RECURSIVE) {
        SIMDJSON_TRY(
            visitRecursive(input, consumer, isDefinitePath, tokenIndex + 1));
      } else if (
          selector == JsonPathTokenizer::Selector::KEY ||
          selector == JsonPathTokenizer::Selector::KEY_OR_INDEX) {
        SIMDJSON_TRY(extractObject(input, token.value, result));
      }
    } else if (input.type() == simdjson::ondemand::json_type::array) {
      if (selector == JsonPathTokenizer::Selector::WILDCARD) {
        for (auto child : input.get_array()) {
          if (tokenIndex == tokens_.size() - 1) {
            // Consume each element in the object.
            SIMDJSON_TRY(consumer(child.value()));
          } else {
            // If not, then recursively call the extractInternal function on
            // each element in the array.
            SIMDJSON_TRY(extractInternal(
                child.value(), consumer, isDefinitePath, tokenIndex + 1));
          }
        }
        return simdjson::SUCCESS;
      } else if (selector == JsonPathTokenizer::Selector::RECURSIVE) {
        SIMDJSON_TRY(
            visitRecursive(input, consumer, isDefinitePath, tokenIndex + 1));
      } else if (selector == JsonPathTokenizer::Selector::KEY_OR_INDEX) {
        SIMDJSON_TRY(extractArray(input, token.value, result));
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

template <typename TConsumer>
simdjson::error_code SIMDJsonExtractor::visitRecursive(
    simdjson::ondemand::value& json,
    TConsumer& consumer,
    bool& isDefinitePath,
    size_t startTokenIdx) {
  // SIMDJson doesn't support iterating and extracting an ondemand value
  // multiple times which is required for the recursive operator. Therefore, we
  // need to extract the json string and use it with a new local parser object.
  // Moreover, since the extracted string was already padded we can avoid
  // creating a new copy with padding and utilize reusePaddedStringView() to
  // create a no-copy padded_string_view.
  SIMDJSON_ASSIGN_OR_RAISE(auto jsonString, simdjson::to_json_string(json));
  simdjson::padded_string_view paddedJson = reusePaddedStringView(jsonString);
  simdjson::ondemand::parser localParser;
  SIMDJSON_ASSIGN_OR_RAISE(auto jsonDoc, localParser.iterate(paddedJson));
  simdjson::ondemand::value jsonDocVal = jsonDoc.get_value();
  // Visit the current node.
  SIMDJSON_TRY(
      extractInternal(jsonDocVal, consumer, isDefinitePath, startTokenIdx));

  // Reset the local parser for the next round of iteration where we visit the
  // children.
  SIMDJSON_ASSIGN_OR_RAISE(jsonDoc, localParser.iterate(paddedJson));
  jsonDocVal = jsonDoc.get_value();
  if (jsonDocVal.type() == simdjson::ondemand::json_type::object) {
    SIMDJSON_ASSIGN_OR_RAISE(auto jsonObj, jsonDocVal.get_object());
    for (auto field : jsonObj) {
      simdjson::ondemand::value val = field.value();
      if (val.type() != simdjson::ondemand::json_type::object &&
          val.type() != simdjson::ondemand::json_type::array) {
        continue;
      }
      SIMDJSON_TRY(
          visitRecursive(val, consumer, isDefinitePath, startTokenIdx));
    }
  } else if (jsonDocVal.type() == simdjson::ondemand::json_type::array) {
    for (auto child : jsonDocVal.get_array()) {
      simdjson::ondemand::value val = child.value();
      if (val.type() != simdjson::ondemand::json_type::object &&
          val.type() != simdjson::ondemand::json_type::array) {
        continue;
      }
      SIMDJSON_TRY(
          visitRecursive(val, consumer, isDefinitePath, startTokenIdx));
    }
  }
  return simdjson::SUCCESS;
}
} // namespace facebook::velox::functions
