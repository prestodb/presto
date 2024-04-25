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

#include "velox/functions/prestosql/json/SIMDJsonExtractor.h"

namespace facebook::velox::functions {
namespace {
using JsonVector = std::vector<simdjson::ondemand::value>;
}

/* static */ SIMDJsonExtractor& SIMDJsonExtractor::getInstance(
    folly::StringPiece path) {
  // Cache tokenize operations in JsonExtractor across invocations in the same
  // thread for the same JsonPath.
  thread_local static std::
      unordered_map<std::string, std::shared_ptr<SIMDJsonExtractor>>
          extractorCache;
  // Pre-process
  auto trimmedPath = folly::trimWhitespace(path).str();

  std::shared_ptr<SIMDJsonExtractor> op;
  if (extractorCache.count(trimmedPath)) {
    return *extractorCache.at(trimmedPath);
  }

  if (extractorCache.size() == kMaxCacheSize) {
    // TODO: Blindly evict the first one, use better policy
    extractorCache.erase(extractorCache.begin());
  }

  auto it =
      extractorCache.emplace(trimmedPath, new SIMDJsonExtractor(trimmedPath));
  return *it.first->second;
}

bool SIMDJsonExtractor::tokenize(const std::string& path) {
  thread_local static JsonPathTokenizer tokenizer;

  if (path.empty()) {
    return false;
  }

  if (!tokenizer.reset(path)) {
    return false;
  }

  while (tokenizer.hasNext()) {
    if (auto token = tokenizer.getNext()) {
      tokens_.emplace_back(token.value());
    } else {
      tokens_.clear();
      return false;
    }
  }

  return true;
}

simdjson::error_code extractObject(
    simdjson::ondemand::value& jsonValue,
    const std::string& key,
    std::optional<simdjson::ondemand::value>& ret) {
  SIMDJSON_ASSIGN_OR_RAISE(auto jsonObj, jsonValue.get_object());
  for (auto field : jsonObj) {
    SIMDJSON_ASSIGN_OR_RAISE(auto currentKey, field.unescaped_key());
    if (currentKey == key) {
      ret.emplace(field.value());
      return simdjson::SUCCESS;
    }
  }
  return simdjson::SUCCESS;
}

simdjson::error_code extractArray(
    simdjson::ondemand::value& jsonValue,
    const std::string& index,
    std::optional<simdjson::ondemand::value>& ret) {
  SIMDJSON_ASSIGN_OR_RAISE(auto jsonArray, jsonValue.get_array());
  auto rv = folly::tryTo<int32_t>(index);
  if (rv.hasValue()) {
    auto val = jsonArray.at(rv.value());
    if (!val.error()) {
      ret.emplace(std::move(val));
    }
  }
  return simdjson::SUCCESS;
}
} // namespace facebook::velox::functions
