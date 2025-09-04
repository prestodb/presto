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

#include <boost/algorithm/string.hpp>
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

namespace {
std::array<int, 256> base32CharMap() {
  std::array<int, 256> map{};
  map.fill(-1);
  const std::string base32Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

  for (size_t i = 0; i < base32Chars.size(); ++i) {
    char upper = base32Chars[i];
    char lower = std::tolower(upper);
    map[static_cast<unsigned char>(upper)] = static_cast<int>(i);
    map[static_cast<unsigned char>(lower)] = static_cast<int>(i);
  }
  return map;
}

static const std::array<int, 256> base32Map = base32CharMap();

std::string base32Decode(const std::string& encoded) {
  std::vector<uint8_t> decodedBytes;
  int buffer = 0;
  int bitsLeft = 0;

  for (char c : encoded) {
    // Skip padding and whitespace.
    if (c == '=' || std::isspace(static_cast<unsigned char>(c))) {
      continue;
    }

    int val = base32Map[static_cast<unsigned char>(c)];
    VELOX_CHECK_NE(
        val,
        -1,
        "Failed to parse value {}, invalid Base32 character: {}",
        encoded,
        c);

    buffer = (buffer << 5) | val;
    bitsLeft += 5;
    if (bitsLeft >= 8) {
      bitsLeft -= 8;
      decodedBytes.push_back((buffer >> bitsLeft) & 0xFF);
    }
  }
  std::string decoded(decodedBytes.begin(), decodedBytes.end());

  return decoded;
}

// The values map of the enum type is passed into this function in the format:
// "[["CURIOUS",-2], ["HAPPY",0]]" as an array of key-value pairs (as opposed
// to a JSON map like "{"CURIOUS":-2, "HAPPY":0}") so that the function can
// fail on duplicate keys and values. folly::parseJson on a JSON map will
// silently drop duplicate elements.
template <typename T>
std::unordered_map<std::string, T> parseMapFromString(
    const std::string& input) {
  folly::dynamic obj = folly::parseJson(input);
  VELOX_CHECK(
      obj.isArray(),
      "Expected an array of key-value pairs for input: {}",
      input);
  std::unordered_map<std::string, T> result;
  std::unordered_set<T> seenValues;

  for (const auto& pair : obj) {
    VELOX_CHECK(
        pair.isArray() && pair.size() == 2 && pair[0].isString(),
        "Failed to parse map: {}, each element must be have a string key.",
        input);
    std::string key = boost::algorithm::to_upper_copy(pair[0].asString());
    VELOX_CHECK(
        !result.contains(key),
        "Failed to parse map: {}, duplicate key found: {}",
        input,
        key);
    T value;
    if constexpr (std::is_same_v<T, int64_t>) {
      VELOX_CHECK(
          pair[1].isInt(),
          "Failed to parse map: {}, each element must have an integer value.",
          input);
      value = pair[1].asInt();
    } else if constexpr (std::is_same_v<T, std::string>) {
      VELOX_CHECK(
          pair[1].isString(),
          "Failed to parse map: {}, each element must have a string value.",
          input);
      // For a VarcharEnum map where the values are strings, the values from the
      // Presto coordinator are received as base32-encoded strings to match the
      // standard Presto TypeSignature format.
      // Hence decode the value before creating a VarcharEnumParameter.
      // https://github.com/prestodb/presto/blob/master/presto-common/src/main/java/com/facebook/presto/common/type/VarcharEnumType.java#L128
      value = base32Decode(pair[1].asString());
    } else {
      VELOX_UNSUPPORTED(
          "parseMapFromString is only supported for int64_t and std::string values.");
    }
    VELOX_CHECK(
        seenValues.insert(value).second,
        "Failed to parse map: {}, duplicate value found: {}",
        input,
        value);

    result[key] = value;
  }
  return result;
}
} // namespace

TypePtr getEnumType(
    const std::string& enumType,
    const std::string& enumName,
    const std::string& valuesMap) {
  std::vector<TypeParameter> params;
  if (enumType == "BigintEnum") {
    LongEnumParameter longEnumParameter(
        enumName, parseMapFromString<int64_t>(valuesMap));
    params.emplace_back(TypeParameter(longEnumParameter));
    return getType("BIGINT_ENUM", params);
  } else if (enumType == "VarcharEnum") {
    VarcharEnumParameter varcharEnumParameter(
        enumName, parseMapFromString<std::string>(valuesMap));
    params.emplace_back(TypeParameter(varcharEnumParameter));
    return getType("VARCHAR_ENUM", params);
  }

  VELOX_UNREACHABLE(
      "Invalid type {}, expected BigintEnum or VarcharEnum", enumType);
}
} // namespace facebook::velox::functions::prestosql
