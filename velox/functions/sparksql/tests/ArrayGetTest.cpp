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

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

namespace facebook::velox::functions::sparksql::test {
namespace {

class ArrayGetTest : public SparkFunctionBaseTest {
 protected:
  template <typename T>
  std::optional<T> arrayGet(
      const ArrayVectorPtr& arrayVector,
      const std::optional<int32_t>& index) {
    auto input =
        makeRowVector({arrayVector, makeConstant(index, arrayVector->size())});
    return evaluateOnce<T>("get(c0, c1)", input);
  }
};

TEST_F(ArrayGetTest, basic) {
  auto arrayVector = makeNullableArrayVector<int32_t>({{1, 2, std::nullopt}});
  EXPECT_EQ(arrayGet<int32_t>(arrayVector, -1), std::nullopt);
  EXPECT_EQ(arrayGet<int32_t>(arrayVector, 0), 1);
  EXPECT_EQ(arrayGet<int32_t>(arrayVector, 1), 2);
  EXPECT_EQ(arrayGet<int32_t>(arrayVector, 2), std::nullopt);
  EXPECT_EQ(arrayGet<int32_t>(arrayVector, 3), std::nullopt);
  EXPECT_EQ(arrayGet<int32_t>(arrayVector, std::nullopt), std::nullopt);
}
} // namespace
} // namespace facebook::velox::functions::sparksql::test
