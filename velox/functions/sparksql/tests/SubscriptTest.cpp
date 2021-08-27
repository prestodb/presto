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

#include <optional>

#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

class SubscriptTest : public SparkFunctionBaseTest {
 protected:
  template <typename T = int64_t>
  std::optional<T> subscriptSimple(
      const std::string& expression,
      const std::vector<VectorPtr>& parameters) {
    auto result =
        evaluate<SimpleVector<T>>(expression, makeRowVector(parameters));
    if (result->size() != 1) {
      throw std::invalid_argument(
          "subscriptSimple expects a single output row.");
    }
    if (result->isNullAt(0)) {
      return std::nullopt;
    }
    return result->valueAt(0);
  }
};

} // namespace

// Spark's subscript ("a[1]") behavior:
// #1 - start indices at 0.
// #2 - allow out of bounds access for arrays (return null).
// #3 - do not allow negative indices (return null).
TEST_F(SubscriptTest, allFlavors2) {
  auto arrayVector = makeArrayVector<int64_t>({{10, 11, 12}});

  // Create a simple vector containing a single map ([10=>10, 11=>11, 12=>12]).
  auto keyAt = [](auto idx) { return idx + 10; };
  auto sizeAt = [](auto) { return 3; };
  auto mapValueAt = [](auto idx) { return idx + 10; };
  auto mapVector =
      makeMapVector<int64_t, int64_t>(1, sizeAt, keyAt, mapValueAt);

  // #1
  EXPECT_EQ(subscriptSimple("getarrayitem(C0, 0)", {arrayVector}), 10);
  EXPECT_EQ(subscriptSimple("getarrayitem(C0, 1)", {arrayVector}), 11);
  EXPECT_EQ(subscriptSimple("getarrayitem(C0, 2)", {arrayVector}), 12);

  // #2
  EXPECT_EQ(
      subscriptSimple("getarrayitem(C0, 3)", {arrayVector}), std::nullopt);
  EXPECT_EQ(
      subscriptSimple("getarrayitem(C0, 4)", {arrayVector}), std::nullopt);
  EXPECT_EQ(
      subscriptSimple("getmapvalue(C0, 1001)", {mapVector}), std::nullopt);

  // #3
  EXPECT_EQ(
      subscriptSimple("getarrayitem(C0, -1)", {arrayVector}), std::nullopt);
  EXPECT_EQ(
      subscriptSimple("getarrayitem(C0, -2)", {arrayVector}), std::nullopt);
  EXPECT_EQ(
      subscriptSimple("getarrayitem(C0, -3)", {arrayVector}), std::nullopt);
  EXPECT_EQ(
      subscriptSimple("getarrayitem(C0, -4)", {arrayVector}), std::nullopt);
}
} // namespace facebook::velox::functions::sparksql::test
