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

namespace facebook::velox {

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
  if (failIfNotRegistered) {
    VELOX_CHECK(
        inferredType, "Failed to parse type [{}]. Type not registered.", type);
  }
  return inferredType;
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

} // namespace facebook::velox
