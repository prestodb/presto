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
#include "velox/common/base/tests/GTestUtils.h"

#include "velox/functions/lib/CheckNestedNulls.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::functions::test {
class CheckNestedNullsTest : public testing::Test,
                             public facebook::velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(CheckNestedNullsTest, array) {
  auto baseVector = makeArrayVectorFromJson<int32_t>({
      "null",
      "[6, 7]",
      "[2, null]",
  });
  DecodedVector decodedVector(*baseVector);
  const auto* indices = decodedVector.indices();

  for (bool throwOnNestedNulls : {true, false}) {
    ASSERT_TRUE(
        checkNestedNulls(decodedVector, indices, 0, throwOnNestedNulls));
    ASSERT_FALSE(
        checkNestedNulls(decodedVector, indices, 1, throwOnNestedNulls));
  }

  VELOX_ASSERT_THROW(
      checkNestedNulls(decodedVector, indices, 2, true),
      "ARRAY comparison not supported for values that contain nulls");
  ASSERT_FALSE(checkNestedNulls(decodedVector, indices, 2, false));
}

TEST_F(CheckNestedNullsTest, map) {
  auto baseVector = makeMapVectorFromJson<int32_t, int32_t>({
      "null",
      "{4: 7, 1: 2}",
      "{6: 8, 8: null}",
  });
  DecodedVector decodedVector(*baseVector);
  const auto* indices = decodedVector.indices();

  for (bool throwOnNestedNulls : {true, false}) {
    ASSERT_TRUE(
        checkNestedNulls(decodedVector, indices, 0, throwOnNestedNulls));
    ASSERT_FALSE(
        checkNestedNulls(decodedVector, indices, 1, throwOnNestedNulls));
  }

  VELOX_ASSERT_THROW(
      checkNestedNulls(decodedVector, indices, 2, true),
      "MAP comparison not supported for values that contain nulls");
  ASSERT_FALSE(checkNestedNulls(decodedVector, indices, 2, false));
}

TEST_F(CheckNestedNullsTest, row) {
  auto baseVector = makeRowVector(
      {
          makeFlatVector<StringView>({
              "a"_sv,
              "b"_sv,
              "c"_sv,
          }),
          makeNullableFlatVector<StringView>({
              "aa"_sv,
              "bb"_sv,
              std::nullopt,
          }),
      },
      [](vector_size_t row) { return row == 0; });
  DecodedVector decodedVector(*baseVector);
  const auto* indices = decodedVector.indices();

  for (bool throwOnNestedNulls : {true, false}) {
    ASSERT_TRUE(
        checkNestedNulls(decodedVector, indices, 0, throwOnNestedNulls));
    ASSERT_FALSE(
        checkNestedNulls(decodedVector, indices, 1, throwOnNestedNulls));
  }

  VELOX_ASSERT_THROW(
      checkNestedNulls(decodedVector, indices, 2, true),
      "ROW comparison not supported for values that contain nulls");
  ASSERT_FALSE(checkNestedNulls(decodedVector, indices, 2, false));
}
} // namespace facebook::velox::functions::test
