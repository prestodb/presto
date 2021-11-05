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

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "velox/expression/VectorUdfTypeSystem.h"
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

namespace {

using namespace facebook::velox;

class ArrayViewTest : public functions::test::FunctionBaseTest {
 protected:
  std::vector<std::vector<std::optional<int64_t>>> arrayDataBigInt = {
      {0, 1, 2, 4},
      {99, 98},
      {101, std::nullopt},
      {10001, 12345676, std::nullopt},
  };
};

TEST_F(ArrayViewTest, intArray) {
  auto arrayVector = makeNullableArrayVector(arrayDataBigInt);
  DecodedVector decoded;
  exec::VectorReader<Array<int64_t>> reader(
      exec::detail::decode<exec::ArrayView<int64_t>>(
          decoded, *arrayVector.get()));

  auto testItem = [&](int i, int j, auto item) {
    // Test has_value.
    ASSERT_EQ(arrayDataBigInt[i][j].has_value(), item.has_value());

    // Test bool implicit cast.
    ASSERT_EQ(arrayDataBigInt[i][j].has_value(), item);

    if (arrayDataBigInt[i][j].has_value()) {
      // Test * operator.
      ASSERT_EQ(arrayDataBigInt[i][j].value(), *item) << i << j;

      // Test value().
      ASSERT_EQ(arrayDataBigInt[i][j].value(), item.value());
      ASSERT_TRUE(item == arrayDataBigInt[i][j].value());
    }
    ASSERT_TRUE(item == arrayDataBigInt[i][j]);
  };

  for (auto i = 0; i < arrayVector->size(); i++) {
    auto arrayView = reader[i];
    auto j = 0;

    // Test iterate loop.
    for (auto item : arrayView) {
      testItem(i, j, item);
      j++;
    }
    ASSERT_EQ(j, arrayDataBigInt[i].size());

    // Test iterate loop 2.
    auto it = arrayView.begin();
    j = 0;
    while (it != arrayView.end()) {
      testItem(i, j, *it);
      j++;
      ++it;
    }
    ASSERT_EQ(j, arrayDataBigInt[i].size());

    // Test index based loop.
    for (j = 0; j < arrayView.size(); j++) {
      testItem(i, j, arrayView[j]);
    }
    ASSERT_EQ(j, arrayDataBigInt[i].size());

    // Test loop iterator with <.
    j = 0;
    for (auto it = arrayView.begin(); it < arrayView.end(); it++) {
      testItem(i, j, *it);
      j++;
    }
    ASSERT_EQ(j, arrayDataBigInt[i].size());
  }
}
} // namespace
