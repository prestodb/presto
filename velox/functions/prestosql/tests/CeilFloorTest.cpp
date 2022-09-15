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

class CeilFloorTest : public functions::test::FunctionBaseTest {
 protected:
  template <typename T>
  void runCeilFloorTest(
      const std::shared_ptr<const Type>& type,
      const std::string& query,
      const std::vector<T>& input,
      const std::vector<T>& output) {
    int32_t testSize = input.size();

    // Create input by using the passed input vector
    auto column = BaseVector::create(type, testSize, execCtx_.pool());
    T* data = column->as<FlatVector<T>>()->mutableRawValues();
    for (int32_t i = 0; i < testSize; ++i) {
      data[i] = input[i];
    }

    // Call evaluate to run the query on the created input
    auto result = evaluate<SimpleVector<T>>(query, makeRowVector({column}));
    for (int32_t i = 0; i < testSize; ++i) {
      ASSERT_EQ(result->valueAt(i), output[i]);
    }
  }
};

TEST_F(CeilFloorTest, floor) {
  runCeilFloorTest<double_t>(
      DOUBLE(),
      "floor(c0)",
      {0.0, 1.1, -2.2, 3.3, -4.4, 5.5, -6.6, 7.7, -8.8, 9.9},
      {0, 1, -3, 3, -5, 5, -7, 7, -9, 9});

  runCeilFloorTest<float_t>(
      REAL(),
      "floor(c0)",
      {0.0, 1.1, -2.2, 3.3, -4.4, 5.5, -6.6, 7.7, -8.8, 9.9},
      {0, 1, -3, 3, -5, 5, -7, 7, -9, 9});

  runCeilFloorTest<int64_t>(
      BIGINT(),
      "floor(c0)",
      {0, 1, -2, 3, -4, 5, -6, 7, -8, 9},
      {0, 1, -2, 3, -4, 5, -6, 7, -8, 9});
}

TEST_F(CeilFloorTest, ceil) {
  runCeilFloorTest<double_t>(
      DOUBLE(),
      "ceil(c0)",
      {0.0, 1.1, -2.2, 3.3, -4.4, 5.5, -6.6, 7.7, -8.8, 9.9},
      {0.0, 2, -2, 4, -4, 6, -6, 8, -8, 10});

  runCeilFloorTest<float_t>(
      REAL(),
      "ceil(c0)",
      {0.0, 1.1, -2.2, 3.3, -4.4, 5.5, -6.6, 7.7, -8.8, 9.9},
      {0.0, 2, -2, 4, -4, 6, -6, 8, -8, 10});

  runCeilFloorTest<int64_t>(
      BIGINT(),
      "ceil(c0)",
      {0, 1, -2, 3, -4, 5, -6, 7, -8, 9},
      {0, 1, -2, 3, -4, 5, -6, 7, -8, 9});
}

TEST_F(CeilFloorTest, ceiling) {
  runCeilFloorTest<double_t>(
      DOUBLE(),
      "ceiling(c0)",
      {0.0, 1.1, -2.2, 3.3, -4.4, 5.5, -6.6, 7.7, -8.8, 9.9},
      {0.0, 2, -2, 4, -4, 6, -6, 8, -8, 10});

  runCeilFloorTest<float_t>(
      REAL(),
      "ceiling(c0)",
      {0.0, 1.1, -2.2, 3.3, -4.4, 5.5, -6.6, 7.7, -8.8, 9.9},
      {0.0, 2, -2, 4, -4, 6, -6, 8, -8, 10});

  runCeilFloorTest<int64_t>(
      BIGINT(),
      "ceiling(c0)",
      {0, 1, -2, 3, -4, 5, -6, 7, -8, 9},
      {0, 1, -2, 3, -4, 5, -6, 7, -8, 9});
}
