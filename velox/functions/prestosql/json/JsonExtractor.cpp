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

#include "velox/functions/prestosql/json/JsonExtractor.h"

#include <cctype>
#include <unordered_map>
#include <vector>

#include "boost/algorithm/string/trim.hpp"
#include "folly/String.h"
#include "folly/json.h"
#include "velox/common/base/Exceptions.h"
#include "velox/functions/prestosql/json/JsonPathTokenizer.h"

namespace facebook::velox::functions {

namespace {

using JsonVector = std::vector<const folly::dynamic*>;

class JsonExtractor {
 public:
  // Use this method to get an instance of JsonExtractor given a json path.
  static JsonExtractor& getInstance(folly::StringPiece path) {
    // Pre-process
    auto trimedPath = folly::trimWhitespace(path).str();

    std::shared_ptr<JsonExtractor> op;
    if (kExtractorCache.count(trimedPath)) {
      op = kExtractorCache.at(trimedPath);
    } else {
      if (kExtractorCache.size() == kMaxCacheNum) {
        // TODO: Blindly evict the first one, use better policy
        kExtractorCache.erase(kExtractorCache.begin());
      }
      op = std::make_shared<JsonExtractor>(trimedPath);
      kExtractorCache[trimedPath] = op;
    }
    return *op;
  }

  folly::Optional<folly::dynamic> extract(const folly::dynamic& json);

  // Shouldn't instantiate directly - use getInstance().
  explicit JsonExtractor(const std::string& path) {
    if (!tokenize(path)) {
      VELOX_USER_FAIL("Invalid JSON path: {}", path);
    }
  }

 private:
  bool tokenize(const std::string& path) {
    if (path.empty()) {
      return false;
    }
    if (!kTokenizer.reset(path)) {
      return false;
    }

    while (kTokenizer.hasNext()) {
      if (auto token = kTokenizer.getNext()) {
        tokens_.push_back(token.value());
      } else {
        tokens_.clear();
        return false;
      }
    }
    return true;
  }

  // Cache tokenize operations in JsonExtractor across invocations in the same
  // thread for the same JsonPath.
  thread_local static std::
      unordered_map<std::string, std::shared_ptr<JsonExtractor>>
          kExtractorCache;
  thread_local static JsonPathTokenizer kTokenizer;

  // Max extractor number in extractor cache
  static const uint32_t kMaxCacheNum{32};

  std::vector<std::string> tokens_;
};

thread_local std::unordered_map<std::string, std::shared_ptr<JsonExtractor>>
    JsonExtractor::kExtractorCache;
thread_local JsonPathTokenizer JsonExtractor::kTokenizer;

void extractObject(
    const folly::dynamic* jsonObj,
    const std::string& key,
    JsonVector& ret) {
  auto val = jsonObj->get_ptr(key);
  if (val) {
    ret.push_back(val);
  }
}

void extractArray(
    const folly::dynamic* jsonArray,
    const std::string& key,
    JsonVector& ret) {
  auto arrayLen = jsonArray->size();
  if (key == "*") {
    for (size_t i = 0; i < arrayLen; ++i) {
      ret.push_back(jsonArray->get_ptr(i));
    }
  } else {
    auto rv = folly::tryTo<int32_t>(key);
    if (rv.hasValue()) {
      auto idx = rv.value();
      if (idx >= 0 && idx < arrayLen) {
        ret.push_back(jsonArray->get_ptr(idx));
      }
    }
  }
}

folly::Optional<folly::dynamic> JsonExtractor::extract(
    const folly::dynamic& json) {
  JsonVector input;
  // Temporary extraction result holder, swap with input after
  // each iteration.
  JsonVector result;
  input.push_back(&json);

  for (auto& token : tokens_) {
    for (auto& jsonObj : input) {
      if (jsonObj->isObject()) {
        extractObject(jsonObj, token, result);
      } else if (jsonObj->isArray()) {
        extractArray(jsonObj, token, result);
      }
    }
    if (result.empty()) {
      return folly::none;
    }
    input.clear();
    result.swap(input);
  }

  auto len = input.size();
  if (0 == len) {
    return folly::none;
  } else if (1 == len) {
    return *input.front();
  } else {
    folly::dynamic array = folly::dynamic::array;
    for (auto obj : input) {
      array.push_back(*obj);
    }
    return array;
  }
}

bool isScalarType(const folly::Optional<folly::dynamic>& json) {
  return json.has_value() && !json->isObject() && !json->isArray() &&
      !json->isNull();
}

} // namespace

folly::Optional<folly::dynamic> jsonExtract(
    folly::StringPiece json,
    folly::StringPiece path) {
  try {
    // If extractor fails to parse the path, this will throw a VeloxUserError,
    // and we want to let this exception bubble up to the client. We only catch
    // json parsing failures (in which cases we return folly::none instead of
    // throw).
    auto& extractor = JsonExtractor::getInstance(path);
    return extractor.extract(folly::parseJson(json));
  } catch (const folly::json::parse_error&) {
  } catch (const folly::ConversionError&) {
    // Folly might throw a conversion error while parsing the input json. In
    // this case, let it return null.
  }
  return folly::none;
}

folly::Optional<folly::dynamic> jsonExtract(
    const std::string& json,
    const std::string& path) {
  return jsonExtract(folly::StringPiece(json), folly::StringPiece(path));
}

folly::Optional<folly::dynamic> jsonExtract(
    const folly::dynamic& json,
    folly::StringPiece path) {
  try {
    return JsonExtractor::getInstance(path).extract(json);
  } catch (const folly::json::parse_error&) {
  }
  return folly::none;
}

folly::Optional<std::string> jsonExtractScalar(
    folly::StringPiece json,
    folly::StringPiece path) {
  auto res = jsonExtract(json, path);
  // Not a scalar value
  if (isScalarType(res)) {
    if (res->isBool()) {
      return res->asBool() ? std::string{"true"} : std::string{"false"};
    } else {
      return res->asString();
    }
  }
  return folly::none;
}

folly::Optional<std::string> jsonExtractScalar(
    const std::string& json,
    const std::string& path) {
  folly::StringPiece jsonPiece{json};
  folly::StringPiece pathPiece{path};

  return jsonExtractScalar(jsonPiece, pathPiece);
}

} // namespace facebook::velox::functions
