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
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

namespace {

// Class to test the array_duplicates operator.
class ArrayDuplicatesTest : public FunctionBaseTest {
 protected:
  // Evaluate an expression.
  void testExpr(
      const VectorPtr& expected,
      const std::string& expression,
      const std::vector<VectorPtr>& input) {
    auto result = evaluate<ArrayVector>(expression, makeRowVector(input));
    assertEqualVectors(expected, result);
  }

  // Execute test for bigint type.
  void testBigint(const std::string& funcName) {
    auto array = makeNullableArrayVector<int64_t>({
        {},
        {1,
         std::numeric_limits<int64_t>::min(),
         std::numeric_limits<int64_t>::max()},
        {std::nullopt},
        {1, 2, 3},
        {2, 1, 1, -2},
        {1, 1, 1},
        {-1, std::nullopt, -1, -1},
        {std::nullopt, std::nullopt, std::nullopt},
        {1, -2, -2, 8, -2, 4, 8, 1},
        {std::numeric_limits<int64_t>::max(),
         std::numeric_limits<int64_t>::max(),
         1,
         std::nullopt,
         0,
         1,
         std::nullopt,
         0},
    });

    auto expected = makeNullableArrayVector<int64_t>({
        {},
        {},
        {},
        {},
        {1},
        {1},
        {-1},
        {std::nullopt},
        {-2, 1, 8},
        {std::nullopt, 0, 1, std::numeric_limits<int64_t>::max()},
    });

    testExpr(expected, fmt::format("{}(C0)", funcName), {array});
  }

  void testinlineStringArrays(const std::string& funcName) {
    using S = StringView;

    auto array = makeNullableArrayVector<StringView>({
        {},
        {S("")},
        {std::nullopt},
        {S("a"), S("b")},
        {S("a"), std::nullopt, S("b")},
        {S("a"), S("a")},
        {S("b"), S("a"), S("b"), S("a"), S("a")},
        {std::nullopt, std::nullopt},
        {S("b"), std::nullopt, S("a"), S("a"), std::nullopt, S("b")},
    });

    auto expected = makeNullableArrayVector<StringView>({
        {},
        {},
        {},
        {},
        {},
        {S("a")},
        {S("a"), S("b")},
        {std::nullopt},
        {std::nullopt, S("a"), S("b")},
    });

    testExpr(expected, fmt::format("{}(C0)", funcName), {array});
  }

  void teststringArrays(const std::string& funcName) {
    using S = StringView;

    auto array = makeNullableArrayVector<StringView>({
        {S("red shiny car ahead"), S("blue clear sky above")},
        {S("blue clear sky above"),
         S("yellow rose flowers"),
         std::nullopt,
         S("blue clear sky above"),
         S("orange beautiful sunset")},
        {
            S("red shiny car ahead"),
            std::nullopt,
            S("purple is an elegant color"),
            S("red shiny car ahead"),
            S("green plants make us happy"),
            S("purple is an elegant color"),
            std::nullopt,
            S("purple is an elegant color"),
        },
    });

    auto expected = makeNullableArrayVector<StringView>({
        {},
        {S("blue clear sky above")},
        {std::nullopt,
         S("purple is an elegant color"),
         S("red shiny car ahead")},
    });

    testExpr(expected, fmt::format("{}(C0)", funcName), {array});
  }

  void testNonContiguousRows(const std::string& funcName) {
    auto c0 = makeFlatVector<int64_t>(4, [](auto row) { return row; });
    auto c1 = makeArrayVector<int64_t>({
        {1, 1, 2, 3, 3},
        {1, 1, 2, 3, 4, 4},
        {1, 1, 2, 3, 4, 5, 5},
        {1, 1, 2, 3, 3, 4, 5, 6, 6},
    });

    auto c2 = makeArrayVector<int64_t>({
        {0, 0, 1, 1, 2, 3, 3},
        {0, 0, 1, 1, 2, 3, 4, 4},
        {0, 0, 1, 1, 2, 3, 4, 5, 5},
        {0, 0, 1, 1, 2, 3, 4, 5, 6, 6},
    });

    auto expected = makeArrayVector<int64_t>({
        {1, 3},
        {0, 1, 4},
        {1, 5},
        {0, 1, 6},
    });

    auto result = evaluate<ArrayVector>(
        fmt::format("if(c0 % 2 = 0, {}(c1), {}(c2))", funcName, funcName),
        makeRowVector({c0, c1, c2}));
    assertEqualVectors(expected, result);
  }

  void testConstant(const std::string& funcName) {
    vector_size_t size = 1'000;
    auto data =
        makeArrayVector<int64_t>({{1, 2, 3}, {4, 5, 4, 5}, {6, 6, 6, 6}});

    auto evaluateConstant = [&](vector_size_t row, const VectorPtr& vector) {
      return evaluate(
          fmt::format("{}(c0)", funcName),
          makeRowVector({BaseVector::wrapInConstant(size, row, vector)}));
    };

    auto result = evaluateConstant(0, data);
    auto expected = makeConstantArray<int64_t>(size, {});
    assertEqualVectors(expected, result);

    result = evaluateConstant(1, data);
    expected = makeConstantArray<int64_t>(size, {4, 5});
    assertEqualVectors(expected, result);

    result = evaluateConstant(2, data);
    expected = makeConstantArray<int64_t>(size, {6});
    assertEqualVectors(expected, result);
  }
};

} // namespace

// Test integer arrays.
TEST_F(ArrayDuplicatesTest, integerArrays) {
  testBigint("array_duplicates");
  testBigint("array_dupes");
}

// Test inline (short) strings.
TEST_F(ArrayDuplicatesTest, inlineStringArrays) {
  testinlineStringArrays("array_duplicates");
  testinlineStringArrays("array_dupes");
}

// Test non-inline (> 12 character length) strings.
TEST_F(ArrayDuplicatesTest, stringArrays) {
  teststringArrays("array_duplicates");
  teststringArrays("array_dupes");
}

TEST_F(ArrayDuplicatesTest, nonContiguousRows) {
  testNonContiguousRows("array_duplicates");
  testNonContiguousRows("array_dupes");
}

TEST_F(ArrayDuplicatesTest, constant) {
  testConstant("array_duplicates");
  testConstant("array_dupes");
}
