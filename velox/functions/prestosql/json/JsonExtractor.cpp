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

// Cache tokenize operations in JsonExtractor across invocations in the same
// thread for the same JsonPath.
thread_local std::unordered_map<std::string, std::shared_ptr<JsonExtractor>>
    kExtractorCache;

thread_local JsonPathTokenizer kTokenizer;

static const std::string kIgnoreChars = " \n\t\r\\";
// Max extractor number in extractor cache
static const uint32_t kMaxCacheNum = 32;

namespace {

folly::Optional<folly::dynamic> jsonExtractInternal(
    const folly::dynamic* json,
    folly::StringPiece path) {
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

  return op->extract(json);
}

bool isScalarType(const folly::Optional<folly::dynamic>& json) {
  return json.has_value() && !json->isObject() && !json->isArray() &&
      !json->isNull();
}

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
} // namespace

JsonExtractor::JsonExtractor(const std::string& path)
    : isValid_(false), path_(path) {
  tokenize();
}

void JsonExtractor::tokenize() {
  if (path_.empty()) {
    return;
  }
  folly::StringPiece jsonPath(path_);
  if (!kTokenizer.reset(path_)) {
    return;
  }

  while (kTokenizer.hasNext()) {
    if (auto token = kTokenizer.getNext()) {
      tokens_.push_back(token.value());
    } else {
      tokens_.clear();
      return;
    }
  }
  isValid_ = true;
}

folly::Optional<folly::dynamic> JsonExtractor::extract(
    const folly::dynamic* json) {
  VELOX_USER_CHECK(isValid_, "Invalid JSON path: {}", path_);

  JsonVector input;
  // Temporary extraction result holder, swap with input after
  // each iteration.
  JsonVector result;
  input.push_back(json);

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

folly::Optional<folly::dynamic> jsonExtract(
    folly::StringPiece json,
    folly::StringPiece path) {
  try {
    auto jsonObj = folly::parseJson(json);
    return jsonExtractInternal(&jsonObj, path);
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
    return jsonExtractInternal(&json, path);
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
