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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

namespace facebook::velox::functions {
namespace {

class FindFirstTest : public functions::test::FunctionBaseTest {
 protected:
  void verify(
      const std::string& expression,
      const RowVectorPtr& input,
      const VectorPtr& expected) {
    SCOPED_TRACE(expression);
    auto result = evaluate(expression, input);
    velox::test::assertEqualVectors(expected, result);
  }
};

TEST_F(FindFirstTest, basic) {
  auto data = makeRowVector({
      makeArrayVectorFromJson<int32_t>({
          "[1, 2, 3, 4]",
          "null",
          "[]",
          "[null, 10, 20, null, 30, null, 40]",
      }),
  });

  // find_first: x > 0.
  VectorPtr expected =
      makeNullableFlatVector<int32_t>({1, std::nullopt, std::nullopt, 10});
  verify("find_first(c0, x -> (x > 0))", data, expected);

  expected =
      makeNullableFlatVector<int64_t>({1, std::nullopt, std::nullopt, 2});
  verify("find_first_index(c0, x -> (x > 0))", data, expected);

  // find_first: x > 10.
  expected = makeNullableFlatVector<int32_t>(
      {std::nullopt, std::nullopt, std::nullopt, 20});
  verify("find_first(c0, x -> (x > 10))", data, expected);

  expected = makeNullableFlatVector<int64_t>(
      {std::nullopt, std::nullopt, std::nullopt, 3});
  verify("find_first_index(c0, x -> (x > 10))", data, expected);

  // find_first: x < 0.
  expected = makeNullConstant(TypeKind::INTEGER, 4);
  verify("find_first(c0, x -> (x < 0))", data, expected);

  expected = makeNullConstant(TypeKind::BIGINT, 4);
  verify("find_first_index(c0, x -> (x < 0))", data, expected);

  // find_first x > 2 starting with 2nd element.
  expected =
      makeNullableFlatVector<int32_t>({3, std::nullopt, std::nullopt, 10});
  verify("find_first(c0, 2, x -> (x > 2))", data, expected);

  expected =
      makeNullableFlatVector<int64_t>({3, std::nullopt, std::nullopt, 2});
  verify("find_first_index(c0, 2, x -> (x > 2))", data, expected);

  // find_first x > 2 starting with 5-th element.
  expected = makeNullableFlatVector<int32_t>(
      {std::nullopt, std::nullopt, std::nullopt, 30});
  verify("find_first(c0, 5, x -> (x > 2))", data, expected);

  expected = makeNullableFlatVector<int64_t>(
      {std::nullopt, std::nullopt, std::nullopt, 5});
  verify("find_first_index(c0, 5, x -> (x > 2))", data, expected);

  // first_first x > 0 from the end of the array.
  expected =
      makeNullableFlatVector<int32_t>({4, std::nullopt, std::nullopt, 40});
  verify("find_first(c0, -1, x -> (x > 0))", data, expected);

  expected =
      makeNullableFlatVector<int64_t>({4, std::nullopt, std::nullopt, 7});
  verify("find_first_index(c0, -1, x -> (x > 0))", data, expected);

  // first_first x > 0 from the 2-nd to last element of the array.
  expected =
      makeNullableFlatVector<int32_t>({3, std::nullopt, std::nullopt, 30});
  verify("find_first(c0, -2, x -> (x > 0))", data, expected);

  expected =
      makeNullableFlatVector<int64_t>({3, std::nullopt, std::nullopt, 5});
  verify("find_first_index(c0, -2, x -> (x > 0))", data, expected);

  expected = makeNullConstant(TypeKind::INTEGER, 4);
  verify("find_first(c0, cast(null as INTEGER), x -> (x > 0))", data, expected);

  expected = makeNullConstant(TypeKind::BIGINT, 4);
  verify(
      "find_first_index(c0, cast(null as INTEGER), x -> (x > 0))",
      data,
      expected);
}

TEST_F(FindFirstTest, firstMatchIsNull) {
  auto data = makeRowVector({
      makeArrayVectorFromJson<int32_t>({
          "[1, null, 2]",
      }),
  });

  VELOX_ASSERT_THROW(
      evaluate("find_first(c0, x -> (x is null))", data),
      "find_first found NULL as the first match");
}

TEST_F(FindFirstTest, predicateFailures) {
  auto data = makeRowVector({makeArrayVectorFromJson<int32_t>({
      "[1, 2, 3, 0]",
      "[-1, 3, 0, 5]",
      "[5, 6, 7, 0]",
  })});

  VELOX_ASSERT_THROW(
      evaluate("find_first(c0, x -> (10 / x > 2))", data), "division by zero");
  VELOX_ASSERT_THROW(
      evaluate("find_first(c0, 2, x -> (10 / x > 2))", data),
      "division by zero");
  VELOX_ASSERT_THROW(
      evaluate("find_first_index(c0, x -> (10 / x > 2))", data),
      "division by zero");
  VELOX_ASSERT_THROW(
      evaluate("find_first_index(c0, 2, x -> (10 / x > 2))", data),
      "division by zero");

  VectorPtr expected = makeNullableFlatVector<int32_t>({
      1,
      3,
      std::nullopt,
  });

  verify("find_first(c0, x -> (try(10 / x) > 2))", data, expected);
  verify("try(find_first(c0, x -> (10 / x > 2)))", data, expected);

  expected = makeNullableFlatVector<int32_t>({
      2,
      3,
      std::nullopt,
  });
  verify("find_first(c0, -3, x -> (10 / x > 2))", data, expected);

  expected = makeNullableFlatVector<int64_t>({
      1,
      2,
      std::nullopt,
  });
  verify("find_first_index(c0, x -> (try(10 / x) > 2))", data, expected);
  verify("try(find_first_index(c0, x -> (10 / x > 2)))", data, expected);

  expected = makeNullableFlatVector<int64_t>({
      2,
      2,
      std::nullopt,
  });
  verify("find_first_index(c0, -3, x -> (10 / x > 2))", data, expected);
}

TEST_F(FindFirstTest, invalidIndex) {
  auto data = makeRowVector({
      makeArrayVectorFromJson<int32_t>({
          "[1, 2, 3]",
          "[-1, 2]",
          "[-2, -3, -4]",
          "[-5, -6]",
      }),
      makeFlatVector<int32_t>({2, 0, 0, 1}),
  });

  // Index 0 is not valid. Expect an error.
  VELOX_ASSERT_THROW(
      evaluate("find_first(c0, c1, x -> (x > 0))", data),
      "SQL array indices start at 1. Got 0.");

  // There are 2 rows with invalid index. Mark array argument in one of these
  // rows as NULL. Still expect the other row to trigger an error.
  data->childAt(0)->setNull(1, true);
  VELOX_ASSERT_THROW(
      evaluate("find_first(c0, c1, x -> (x > 0))", data),
      "SQL array indices start at 1. Got 0.");

  // Mark array argument in the other row as NULL. Expect no errors.
  data->childAt(0)->setNull(2, true);
  auto expected = makeNullableFlatVector<int32_t>(
      {2, std::nullopt, std::nullopt, std::nullopt});
  verify("find_first(c0, c1, x -> (x > 0))", data, expected);

  // All null or empty arrays.
  data = makeRowVector({
      makeArrayVectorFromJson<int32_t>({
          "[]",
          "null",
          "[]",
          "null",
      }),
      makeFlatVector<int32_t>({2, 0, 0, 1}),
  });

  // Index 0 is not valid. Expect an error in the 3rd row.
  VELOX_ASSERT_THROW(
      evaluate("find_first(c0, c1, x -> (x > 0))", data),
      "SQL array indices start at 1. Got 0.");

  // Mark 3rd row null. Expect no error.
  data->childAt(1)->setNull(2, true);
  expected = makeAllNullFlatVector<int32_t>(4);
  verify("find_first(c0, c1, x -> (x > 0))", data, expected);
}

// Verify that null arrays with non-zero offsets/sizes are processed correctly.
TEST_F(FindFirstTest, nulls) {
  auto data = makeRowVector({
      makeArrayVectorFromJson<int32_t>({
          "[1, 2, 3]",
          "[-1, 2]",
          "[-2, -3, -4]",
          "[-5, -6]",
      }),
  });

  // find_first: x > 0.
  VectorPtr expected =
      makeNullableFlatVector<int32_t>({1, 2, std::nullopt, std::nullopt});
  verify("find_first(c0, x -> (x > 0))", data, expected);

  // Mark [-1, 2] array as null. Expect null result.
  data->childAt(0)->setNull(1, true);
  expected = makeNullableFlatVector<int32_t>(
      {1, std::nullopt, std::nullopt, std::nullopt});
  verify("find_first(c0, x -> (x > 0))", data, expected);

  // Mark all arrays null. Expect all-null results.
  for (auto i = 0; i < data->size(); ++i) {
    data->childAt(0)->setNull(i, true);
  }

  expected = makeAllNullFlatVector<int32_t>(data->size());
  verify("find_first(c0, x -> (x > 0))", data, expected);
}

// Verify evaluation on a subset of rows in ArrayVector with only null and empty
// arrays..
TEST_F(FindFirstTest, emptyArrays) {
  auto data = makeRowVector({
      makeArrayVectorFromJson<int32_t>({
          "[]",
          "[]",
          "null",
          "[]",
          "[]",
          "null",
      }),
  });

  // Evaluate on all rows, but the first one.
  SelectivityVector rows(data->size());
  rows.setValid(0, false);
  rows.updateBounds();

  auto result = evaluate("find_first(c0, x -> (x > 0))", data, rows);
  auto expected = makeNullConstant(TypeKind::INTEGER, data->size());
  velox::test::assertEqualVectors(expected, result);
}

} // namespace
} // namespace facebook::velox::functions
