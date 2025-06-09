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
#include "velox/functions/sparksql/CharVarcharUtils.h"

namespace facebook::velox::functions::sparksql {

void trimTrailingSpaces(
    exec::StringWriter& output,
    StringView inputStr,
    int32_t numChars,
    uint32_t limit) {
  const auto numTailSpacesToTrim = numChars - limit;
  VELOX_USER_CHECK_GT(numTailSpacesToTrim, 0);

  auto curPos = inputStr.end() - 1;
  const auto trimTo = inputStr.end() - numTailSpacesToTrim;

  while (curPos >= trimTo && stringImpl::isAsciiSpace(*curPos)) {
    curPos--;
  }
  // Get the length of the trimmed string in characters.
  const auto trimmedSize = numChars - std::distance(curPos + 1, inputStr.end());

  VELOX_USER_CHECK_LE(
      trimmedSize, limit, "Exceeds allowed length limitation: {}", limit);
  output.setNoCopy(
      StringView(inputStr.data(), std::distance(inputStr.begin(), curPos + 1)));
}

} // namespace facebook::velox::functions::sparksql
