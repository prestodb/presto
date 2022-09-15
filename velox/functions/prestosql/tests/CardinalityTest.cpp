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

#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions::test;

namespace {

class CardinalityTest : public FunctionBaseTest {
 protected:
  void testArrayCardinality(
      std::function<vector_size_t(vector_size_t /* row */)> sizeAt,
      std::function<bool(vector_size_t /*row */)> isNullAt) {
    vector_size_t numRows = 100;
    auto arrayVector =
        makeArrayVector<int64_t>(numRows, sizeAt, valueAt, isNullAt);
    auto result = evaluate<SimpleVector<int64_t>>(
        "cardinality(c0)", makeRowVector({arrayVector}));
    for (vector_size_t i = 0; i < numRows; ++i) {
      EXPECT_EQ(result->isNullAt(i), arrayVector->isNullAt(i)) << "at " << i;
      if (!arrayVector->isNullAt(i)) {
        EXPECT_EQ(result->valueAt(i), sizeAt(i)) << "at " << i;
      }
    }
  }

  void testMapCardinality(
      std::function<vector_size_t(vector_size_t /* row */)> sizeAt,
      std::function<bool(vector_size_t /*row */)> isNullAt) {
    vector_size_t numRows = 100;
    auto mapVector = makeMapVector<int64_t, int64_t>(
        numRows, sizeAt, valueAt, valueAt, isNullAt);
    auto result = evaluate<SimpleVector<int64_t>>(
        "cardinality(c0)", makeRowVector({mapVector}));
    for (vector_size_t i = 0; i < numRows; ++i) {
      EXPECT_EQ(result->isNullAt(i), mapVector->isNullAt(i)) << "at " << i;
      if (!mapVector->isNullAt(i)) {
        EXPECT_EQ(result->valueAt(i), sizeAt(i)) << "at " << i;
      }
    }
  }

  static inline vector_size_t valueAt(vector_size_t idx) {
    return idx + 1;
  }
};
} // namespace

TEST_F(CardinalityTest, array) {
  auto sizeAt = [](vector_size_t row) { return 1 + row % 7; };
  testArrayCardinality(sizeAt, nullptr);
  testArrayCardinality(sizeAt, nullEvery(5));
}

TEST_F(CardinalityTest, map) {
  auto sizeAt = [](vector_size_t row) { return 1 + row % 7; };
  testMapCardinality(sizeAt, nullptr);
  testMapCardinality(sizeAt, nullEvery(5));
}
