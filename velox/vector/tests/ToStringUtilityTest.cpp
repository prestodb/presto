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
#include <sstream>
#include "folly/Conv.h"
#include "velox/vector/TypeAliases.h"

namespace facebook::velox {

// Forward declaration, which is defined in ComplexVector.cpp
std::string stringifyTruncatedElementList(
    vector_size_t start,
    vector_size_t size,
    vector_size_t limit,
    std::string_view delimiter,
    const std::function<void(std::stringstream&, vector_size_t)>&
        stringifyElementCB);

namespace {
TEST(ToStringUtil, stringifyTruncatedElementList) {
  const auto indexAsString = [](std::stringstream& ss, vector_size_t i) {
    ss << folly::to<std::string>(i);
  };

  // no item
  EXPECT_EQ(
      stringifyTruncatedElementList(0, 0, 0, ", ", indexAsString), "<empty>");
  // exact item
  EXPECT_EQ(
      stringifyTruncatedElementList(0, 5, 5, ", ", indexAsString),
      "5 elements starting at 0 {0, 1, 2, 3, 4}");
  // more items
  EXPECT_EQ(
      stringifyTruncatedElementList(1, 3, 2, ", ", indexAsString),
      "3 elements starting at 1 {1, 2, ...}");
  // less items
  EXPECT_EQ(
      stringifyTruncatedElementList(1, 3, 5, ", ", indexAsString),
      "3 elements starting at 1 {1, 2, 3}");
  // zero limit.
  EXPECT_EQ(
      stringifyTruncatedElementList(1, 5, 0, ", ", indexAsString),
      "5 elements starting at 1 {...}");
  // different delimiter
  EXPECT_EQ(
      stringifyTruncatedElementList(0, 3, 5, "<delimiter>", indexAsString),
      "3 elements starting at 0 {0<delimiter>1<delimiter>2}");
}
} // namespace
} // namespace facebook::velox
