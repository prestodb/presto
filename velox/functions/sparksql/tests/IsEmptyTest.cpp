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

#include <algorithm>
#include <limits>
#include <optional>
#include <vector>

#include "velox/functions/prestosql/tests/FunctionBaseTest.h"
#include "velox/functions/sparksql/Register.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "velox/type/Variant.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/tests/VectorMaker.h"
#include "velox/vector/tests/VectorTestBase.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

using facebook::velox::functions::test::FunctionBaseTest;

class IsEmptyTest : public SparkFunctionBaseTest {
 protected:
  void testIsEmptyVector(
      const VectorPtr& input,
      const std::vector<bool>& expected) {
    auto result =
        evaluate<SimpleVector<bool>>("is_empty(c0)", makeRowVector({input}));

    velox::test::assertEqualVectors(makeFlatVector(expected), result);
  }

  template <typename T>
  std::optional<bool> scalarIsEmptyTest(std::optional<T> arg) {
    std::optional<bool> ret = evaluateOnce<bool>("is_empty(c0)", arg);
    return ret;
  }
};

TEST_F(IsEmptyTest, nullTest) {
  // FB_IS_EMPTY(NULL)
  EXPECT_EQ(scalarIsEmptyTest<int64_t>(std::nullopt), 1);
  EXPECT_EQ(scalarIsEmptyTest<double>(std::nullopt), 1);
  EXPECT_EQ(scalarIsEmptyTest<bool>(std::nullopt), 1);
  EXPECT_EQ(scalarIsEmptyTest<std::string>(std::nullopt), 1);
}

TEST_F(IsEmptyTest, intTest) {
  // FB_IS_EMPTY(0)
  EXPECT_EQ(scalarIsEmptyTest<int8_t>(0), 1);
  EXPECT_EQ(scalarIsEmptyTest<int16_t>(0), 1);
  EXPECT_EQ(scalarIsEmptyTest<int32_t>(0), 1);
  EXPECT_EQ(scalarIsEmptyTest<int64_t>(0), 1);
  EXPECT_EQ(scalarIsEmptyTest<int8_t>(0), 1);
  EXPECT_EQ(scalarIsEmptyTest<int16_t>(321), 0);
  EXPECT_EQ(scalarIsEmptyTest<int32_t>(3456), 0);
  EXPECT_EQ(scalarIsEmptyTest<int64_t>(100000), 0);
}

TEST_F(IsEmptyTest, realTest) {
  // FB_IS_EMPTY(0)
  EXPECT_EQ(scalarIsEmptyTest<float>(0), 1);
  EXPECT_EQ(scalarIsEmptyTest<double>(0), 1);
  EXPECT_EQ(scalarIsEmptyTest<float>(-0), 1);
  EXPECT_EQ(scalarIsEmptyTest<double>(-0), 1);
  EXPECT_EQ(scalarIsEmptyTest<double>(2.3), 0);
}

TEST_F(IsEmptyTest, boolTest) {
  EXPECT_EQ(scalarIsEmptyTest<bool>(false), 1);
  EXPECT_EQ(scalarIsEmptyTest<bool>(true), 0);
}

TEST_F(IsEmptyTest, stringTest) {
  EXPECT_EQ(scalarIsEmptyTest<std::string>(""), 1);
  EXPECT_EQ(scalarIsEmptyTest<std::string>("null"), 1);
  EXPECT_EQ(scalarIsEmptyTest<std::string>("fAlse"), 1);
  EXPECT_EQ(scalarIsEmptyTest<std::string>("Not False"), 0);
}

template <typename T>
using NestedVector = std::vector<std::vector<T>>;

TEST_F(IsEmptyTest, intArrayTest) {
  auto nullInput = makeNullableArrayVector(NestedVector<std::optional<int>>{
      {1, 2},
      {1},
      {2},
  });
  auto expected = std::vector<bool>{false, false, false};
  testIsEmptyVector(nullInput, expected);
  auto intInput = makeNullableArrayVector(NestedVector<std::optional<int>>{
      {},
      {1, 30},
      {13, 2245},
  });
  auto expected2 = std::vector<bool>{true, false, false};
  testIsEmptyVector(intInput, expected2);

  using array_type = std::optional<std::vector<std::optional<int32_t>>>;
  array_type array1 = {{}};
  array_type array2 = std::nullopt;
  array_type array3 = {{1, 2}};
  auto input3 = makeNestedArrayVector<int32_t>({{array1, array2, array3}});
  auto expected3 = std::vector<bool>{false};
  testIsEmptyVector(input3, expected3);
}

TEST_F(IsEmptyTest, intMapTest) {
  using M = std::vector<std::pair<int32_t, std::optional<int32_t>>>;
  auto nullMapInput = NestedVector<M>{
      {},
      {},
      {},
  };
  auto input = makeArrayOfMapVector(nullMapInput);
  auto expected = std::vector<bool>{true, true, true};
  testIsEmptyVector(input, expected);
  auto intMapInput = NestedVector<M>{
      {},
      {M{{2, 11}, {0, 10}}, M{{1, 11}, {1, 10}}, M{{1, 11}, {3, 10}}},
      {M{{1, 11}, {3, 10}}, M{{1, 13}, {3, 12}}},
  };
  auto input2 = makeArrayOfMapVector(intMapInput);
  auto expected2 = std::vector<bool>{true, false, false};
  testIsEmptyVector(input2, expected2);
}

TEST_F(IsEmptyTest, rowTest) {
  auto rowType = ROW({INTEGER(), VARCHAR()});
  auto nullRow = NestedVector<variant>{
      {},
      {},
  };
  auto expected = std::vector<bool>{true, true};
  testIsEmptyVector(makeArrayOfRowVector(rowType, nullRow), expected);

  variant nullInt = variant(TypeKind::INTEGER);
  auto nonNullRow = NestedVector<variant>{
      {},
      {variant::row({2, "red"}), variant::row({1, "blue"})},
      {variant::row({1, "green"}), variant::row({nullInt, "red"})},
  };
  auto input2 = makeArrayOfRowVector(rowType, nonNullRow);
  auto expected2 = std::vector<bool>{true, false, false};
  testIsEmptyVector(makeArrayOfRowVector(rowType, nonNullRow), expected2);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
