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

#include <string>
#include "velox/type/Type.h"

namespace facebook::velox::functions::prestosql {

TypePtr typeFromString(
    const std::string& type,
    bool failIfNotRegistered = true) {
  auto upper = type;
  std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
  if (upper == "INT") {
    upper = "INTEGER";
  } else if (upper == "DOUBLE PRECISION") {
    upper = "DOUBLE";
  }
  auto inferredType = getType(upper, {});
  if (failIfNotRegistered == true && inferredType == nullptr) {
    VELOX_UNSUPPORTED("Failed to parse type [{}]. Type not registered.", type);
  }
  return inferredType;
}

TypePtr customTypeWithChildren(
    const std::string& name,
    const std::vector<TypePtr>& children) {
  std::vector<TypeParameter> params;
  params.reserve(children.size());
  for (auto& child : children) {
    params.emplace_back(child);
  }
  auto type = getType(name, params);
  VELOX_CHECK_NOT_NULL(
      type, "Failed to parse custom type with children [{}]", name);
  return type;
}

std::pair<std::string, std::shared_ptr<const Type>> inferTypeWithSpaces(
    std::vector<std::string>& words,
    bool cannotHaveFieldName = false) {
  VELOX_CHECK_GE(words.size(), 2);
  const auto& fieldName = words[0];
  const auto allWords = folly::join(" ", words);
  // Fail if cannotHaveFieldName = true.
  auto type = typeFromString(allWords, cannotHaveFieldName);
  if (type) {
    return std::make_pair("", type);
  }
  return std::make_pair(
      fieldName, typeFromString(allWords.substr(fieldName.size() + 1)));
}

// The values map of the enum type is passed into this function in the format:
// "[["CURIOUS",-2], ["HAPPY",0]]" as an array of key-value pairs (as opposed to
// a JSON map like "{"CURIOUS":-2, "HAPPY":0}") so that the function can fail
// on duplicate keys and values. folly::parseJson on a JSON map will silently
// drop duplicate elements.
std::unordered_map<std::string, int64_t> parseMapFromString(
    const std::string& input) {
  folly::dynamic obj = folly::parseJson(input);
  VELOX_CHECK(
      obj.isArray(),
      "Expected an array of key-value pairs for input: {}",
      input);
  std::unordered_map<std::string, int64_t> result;
  std::unordered_set<int> seenValues;

  for (const auto& pair : obj) {
    VELOX_CHECK(
        pair.isArray() && pair.size() == 2 && pair[0].isString() &&
            pair[1].isInt(),
        "Failed to parse map: {}, each element must be a [string key, int value] pair");

    std::string key = pair[0].asString();
    int64_t value = pair[1].asInt();

    VELOX_CHECK(
        !result.contains(key),
        "Failed to parse map: {}, duplicate key found: {}",
        input,
        key);
    VELOX_CHECK(
        seenValues.insert(value).second,
        "Failed to parse map: {}, duplicate value found: {}",
        input,
        value);

    result[key] = value;
  }
  return result;
}

TypePtr getEnumType(
    const std::string& enumType,
    const std::string& enumName,
    const std::string& valuesMap) {
  std::vector<TypeParameter> params;
  LongEnumParameter longEnumParameter(enumName, parseMapFromString(valuesMap));
  params.emplace_back(TypeParameter(longEnumParameter));
  if (enumType == "BigintEnum") {
    return getType("BIGINT_ENUM", params);
  }

  VELOX_UNREACHABLE("Invalid type {}, expected BigintEnum", enumType);
}
} // namespace facebook::velox::functions::prestosql
