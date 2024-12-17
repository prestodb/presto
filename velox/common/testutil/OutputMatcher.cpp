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

#include "velox/common/testutil/OutputMatcher.h"

#include <vector>

#include <folly/String.h>
#include <gtest/gtest.h>
#include <re2/re2.h>

void OutputMatcher::compareOutputs(
    const std::string& testName,
    const std::string& result,
    const std::vector<ExpectedLine>& expectedRegex) {
  std::string line;
  std::string eline;
  std::istringstream iss(result);
  int lineCount = 0;
  int expectedLineIndex = 0;
  for (; std::getline(iss, line);) {
    lineCount++;
    std::vector<std::string> potentialLines;
    auto expectedLine = expectedRegex.at(expectedLineIndex++);
    while (!RE2::FullMatch(line, expectedLine.line)) {
      potentialLines.push_back(expectedLine.line);
      if (!expectedLine.optional) {
        ASSERT_FALSE(true) << "Output did not match " << "Source:" << testName
                           << ", Line number:" << lineCount
                           << ", Line: " << line << ", Expected Line one of: "
                           << folly::join(",", potentialLines);
      }
      expectedLine = expectedRegex.at(expectedLineIndex++);
    }
  }
  for (int i = expectedLineIndex; i < expectedRegex.size(); i++) {
    ASSERT_TRUE(expectedRegex[expectedLineIndex].optional);
  }
}
