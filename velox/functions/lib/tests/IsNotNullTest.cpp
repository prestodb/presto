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
#include "velox/exec/tests/utils/FunctionUtils.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

namespace facebook::velox::functions {
void registerIsNotNull() {
  facebook::velox::exec::test::registerTypeResolver();
  VELOX_REGISTER_VECTOR_FUNCTION(udf_is_not_null, "isnotnull");
}
}; // namespace facebook::velox::functions

using namespace facebook::velox;

class IsNotNullTest : public functions::test::FunctionBaseTest {
 public:
  static void SetUpTestCase() {
    facebook::velox::functions::registerIsNotNull();
  }
  template <typename T>
  std::optional<bool> isnotnull(std::optional<T> arg) {
    return evaluateOnce<bool>("isnotnull(c0)", arg);
  }
};

TEST_F(IsNotNullTest, singleValues) {
  EXPECT_FALSE(isnotnull<int32_t>(std::nullopt).value());
  EXPECT_FALSE(isnotnull<double>(std::nullopt).value());
  EXPECT_TRUE(isnotnull<int64_t>(1).value());
  EXPECT_TRUE(isnotnull<float>(1.5).value());
  EXPECT_TRUE(isnotnull<std::string>("1").value());
  EXPECT_TRUE(isnotnull<double>(std::numeric_limits<double>::max()).value());
}

// The rest of the file is copied and modified from IsNullTest.cpp

TEST_F(IsNotNullTest, basicVectors) {
  vector_size_t size = 20;

  // all nulls
  auto allNulls = makeFlatVector<int32_t>(
      size, [](vector_size_t /*row*/) { return 0; }, vectorMaker_.nullEvery(1));
  auto result =
      evaluate<SimpleVector<bool>>("isnotnull(c0)", makeRowVector({allNulls}));
  for (int i = 0; i < size; ++i) {
    EXPECT_FALSE(result->valueAt(i)) << "at " << i;
  }

  // nulls in odd positions: 0, null, 2, null,...
  auto oddNulls = makeFlatVector<int32_t>(
      size,
      [](vector_size_t row) { return row; },
      vectorMaker_.nullEvery(2, 1));
  result =
      evaluate<SimpleVector<bool>>("isnotnull(c0)", makeRowVector({oddNulls}));
  for (int i = 0; i < size; ++i) {
    EXPECT_EQ(result->valueAt(i), i % 2 == 0) << "at " << i;
  }

  // no nulls
  auto noNulls =
      makeFlatVector<int32_t>(size, [](vector_size_t row) { return row; });
  result =
      evaluate<SimpleVector<bool>>("isnotnull(c0)", makeRowVector({noNulls}));
  for (int i = 0; i < size; ++i) {
    EXPECT_TRUE(result->valueAt(i)) << "at " << i;
  }
}

TEST_F(IsNotNullTest, somePositions) {
  vector_size_t size = 20;

  // nulls in odd positions: 0, null, 2, null,...
  auto oddNulls = makeFlatVector<int32_t>(
      size,
      [](vector_size_t row) { return row; },
      vectorMaker_.nullEvery(2, 1));
  auto result = BaseVector::create(BOOLEAN(), size, execCtx_.pool());
  auto flatResult = std::dynamic_pointer_cast<FlatVector<bool>>(result);
  for (int i = 0; i < size; i++) {
    flatResult->set(i, true);
  }

  // select odd rows 1, 3, 5,...
  SelectivityVector oddRows(size);
  for (int i = 0; i < size; i++) {
    oddRows.setValid(i, i % 2 == 1);
  }

  flatResult = evaluate<FlatVector<bool>>(
      "isnotnull(c0)", makeRowVector({oddNulls}), oddRows, result);
  for (int i = 0; i < size; i += 2) {
    EXPECT_EQ(flatResult->valueAt(i), i % 2 == 0) << "at " << i;
  }

  // select even rows 0, 2, 4...
  SelectivityVector evenRows(size);
  for (int i = 0; i < size; i++) {
    evenRows.setValid(i, i % 2 == 0);
  }
  flatResult = evaluate<FlatVector<bool>>(
      "isnotnull(c0)", makeRowVector({oddNulls}), evenRows, result);
  for (int i = 0; i < size; ++i) {
    EXPECT_TRUE(flatResult->valueAt(i)) << "at " << i;
  }
}
