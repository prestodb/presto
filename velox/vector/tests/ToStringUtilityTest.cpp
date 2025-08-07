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

#include <gtest/gtest.h>
#include "velox/type/Type.h"

namespace facebook::velox {
namespace {
TEST(ToStringUtil, stringifyTruncatedElementList) {
  const auto indexAsString = [](std::stringstream& ss, size_t i) {
    ss << folly::to<std::string>(i);
  };

  // no item
  EXPECT_EQ(stringifyTruncatedElementList(0, indexAsString), "<empty>");
  // exact item
  EXPECT_EQ(stringifyTruncatedElementList(5, indexAsString), "{0, 1, 2, 3, 4}");
  // more items
  EXPECT_EQ(
      stringifyTruncatedElementList(5, indexAsString, 2), "{0, 1, ...3 more}");
  // less items
  EXPECT_EQ(stringifyTruncatedElementList(3, indexAsString), "{0, 1, 2}");
}
} // namespace
} // namespace facebook::velox
