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
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/SelectivityVector.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions::test;

namespace {

class ArrayContainsTest : public FunctionBaseTest {};

TEST_F(ArrayContainsTest, integerNoNulls) {
  auto arrayVector = makeArrayVector<int64_t>(
      {{1, 2, 3, 4}, {3, 4, 5}, {}, {5, 6, 7, 8, 9}, {7}, {10, 9, 8, 7}});

  auto testContains = [&](std::optional<int64_t> search,
                          const std::vector<std::optional<bool>>& expected) {
    auto result = evaluate<SimpleVector<bool>>(
        "contains(c0, c1)",
        makeRowVector({
            arrayVector,
            makeConstant(search, arrayVector->size()),
        }));

    assertEqualVectors(makeNullableFlatVector<bool>(expected), result);
  };

  testContains(1, {true, false, false, false, false, false});
  testContains(3, {true, true, false, false, false, false});
  testContains(5, {false, true, false, true, false, false});
  testContains(7, {false, false, false, true, true, true});
  testContains(-2, {false, false, false, false, false, false});
  testContains(
      std::nullopt,
      {std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt});
}

TEST_F(ArrayContainsTest, integerWithNulls) {
  auto arrayVector = makeNullableArrayVector<int64_t>(
      {{1, 2, 3, 4},
       {3, 4, 5},
       {},
       {5, 6, std::nullopt, 7, 8, 9},
       {7, std::nullopt},
       {10, 9, 8, 7}});

  auto testContains = [&](std::optional<int64_t> search,
                          const std::vector<std::optional<bool>>& expected) {
    auto result = evaluate<SimpleVector<bool>>(
        "contains(c0, c1)",
        makeRowVector({
            arrayVector,
            makeConstant(search, arrayVector->size()),
        }));

    assertEqualVectors(makeNullableFlatVector<bool>(expected), result);
  };

  testContains(1, {true, false, false, std::nullopt, std::nullopt, false});
  testContains(3, {true, true, false, std::nullopt, std::nullopt, false});
  testContains(5, {false, true, false, true, std::nullopt, false});
  testContains(7, {false, false, false, true, true, true});
  testContains(-2, {false, false, false, std::nullopt, std::nullopt, false});
  testContains(
      std::nullopt,
      {std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt});
}

TEST_F(ArrayContainsTest, varcharNoNulls) {
  std::vector<std::string> colors = {
      "red", "green", "blue", "yellow", "orange", "purple"};

  using S = StringView;

  auto arrayVector = makeArrayVector<StringView>({
      {S("red"), S("blue")},
      {S("blue"), S("yellow"), S("orange")},
      {},
      {S("red"), S("purple"), S("green")},
  });

  auto testContains = [&](std::optional<const char*> search,
                          const std::vector<std::optional<bool>>& expected) {
    auto result = evaluate<SimpleVector<bool>>(
        "contains(c0, c1)",
        makeRowVector({
            arrayVector,
            makeConstant(search, arrayVector->size()),
        }));

    assertEqualVectors(makeNullableFlatVector<bool>(expected), result);
  };

  testContains("red", {true, false, false, true});
  testContains("blue", {true, true, false, false});
  testContains("yellow", {false, true, false, false});
  testContains("green", {false, false, false, true});
  testContains("crimson red", {false, false, false, false});
  testContains(
      std::nullopt, {std::nullopt, std::nullopt, std::nullopt, std::nullopt});
}

TEST_F(ArrayContainsTest, varcharWithNulls) {
  std::vector<std::string> colors = {
      "red", "green", "blue", "yellow", "orange", "purple"};

  using S = StringView;

  auto arrayVector = makeNullableArrayVector<StringView>({
      {S("red"), S("blue")},
      {std::nullopt, S("blue"), S("yellow"), S("orange")},
      {},
      {S("red"), S("purple"), S("green")},
  });

  auto testContains = [&](std::optional<const char*> search,
                          const std::vector<std::optional<bool>>& expected) {
    auto result = evaluate<SimpleVector<bool>>(
        "contains(c0, c1)",
        makeRowVector({
            arrayVector,
            makeConstant(search, arrayVector->size()),
        }));

    assertEqualVectors(makeNullableFlatVector<bool>(expected), result);
  };

  testContains("red", {true, std::nullopt, false, true});
  testContains("blue", {true, true, false, false});
  testContains("yellow", {false, true, false, false});
  testContains("green", {false, std::nullopt, false, true});
  testContains("crimson red", {false, std::nullopt, false, false});
  testContains(
      std::nullopt, {std::nullopt, std::nullopt, std::nullopt, std::nullopt});
}

TEST_F(ArrayContainsTest, booleanNoNulls) {
  auto arrayVector = makeArrayVector<bool>({
      {true, false},
      {true},
      {false},
      {},
      {true, false, true},
      {false, false, false},
  });

  auto testContains = [&](std::optional<bool> search,
                          const std::vector<std::optional<bool>>& expected) {
    auto result = evaluate<SimpleVector<bool>>(
        "contains(c0, c1)",
        makeRowVector({
            arrayVector,
            makeConstant(search, arrayVector->size()),
        }));

    assertEqualVectors(makeNullableFlatVector<bool>(expected), result);
  };

  testContains(true, {true, true, false, false, true, false});
  testContains(false, {true, false, true, false, true, true});
  testContains(
      std::nullopt,
      {std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt});
}

TEST_F(ArrayContainsTest, booleanWithNulls) {
  auto arrayVector = makeNullableArrayVector<bool>({
      {true, false},
      {true},
      {false, std::nullopt},
      {},
      {true, false, std::nullopt, true},
      {false, false, false},
  });

  auto testContains = [&](std::optional<bool> search,
                          const std::vector<std::optional<bool>>& expected) {
    auto result = evaluate<SimpleVector<bool>>(
        "contains(c0, c1)",
        makeRowVector({
            arrayVector,
            makeConstant(search, arrayVector->size()),
        }));

    assertEqualVectors(makeNullableFlatVector<bool>(expected), result);
  };

  testContains(true, {true, true, std::nullopt, false, true, false});
  testContains(false, {true, false, true, false, true, true});
  testContains(
      std::nullopt,
      {std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt});
}

TEST_F(ArrayContainsTest, row) {
  std::vector<std::vector<variant>> data = {
      {
          variant::row({1, "red"}),
          variant::row({2, "blue"}),
          variant::row({3, "green"}),
      },
      {
          variant::row({2, "blue"}),
          variant(TypeKind::ROW), // null
          variant::row({5, "green"}),
      },
      {},
      {
          variant::row({1, "yellow"}),
          variant::row({2, "blue"}),
          variant::row({4, "green"}),
          variant::row({5, "purple"}),
      },
  };

  auto rowType = ROW({INTEGER(), VARCHAR()});
  auto arrayVector = makeArrayOfRowVector(rowType, data);

  auto testContains = [&](int32_t n,
                          const char* color,
                          const std::vector<std::optional<bool>>& expected) {
    auto search =
        makeConstantRow(rowType, variant::row({n, color}), arrayVector->size());

    auto result = evaluate<SimpleVector<bool>>(
        "contains(c0, c1)", makeRowVector({arrayVector, search}));

    assertEqualVectors(makeNullableFlatVector<bool>(expected), result);
  };

  testContains(1, "red", {true, std::nullopt, false, false});
  testContains(2, "blue", {true, true, false, true});
  testContains(4, "green", {false, std::nullopt, false, true});
  testContains(5, "green", {false, true, false, false});
  testContains(1, "purple", {false, std::nullopt, false, false});
}

TEST_F(ArrayContainsTest, preDefinedResults) {
  auto arrayVector = makeArrayVector<int64_t>(
      {{1, 2, 3, 4}, {3, 4, 5}, {}, {5, 6, 7, 8, 9}, {7}, {10, 9, 8, 7}});

  auto testContains = [&](std::optional<int64_t> search,
                          const std::vector<std::optional<bool>>& expected) {
    VectorPtr result = makeFlatVector<bool>(6);
    SelectivityVector rows(6);
    rows.resize(6);

    evaluate<SimpleVector<bool>>(
        "contains(c0, c1)",
        makeRowVector({
            arrayVector,
            makeConstant(search, arrayVector->size()),
        }),
        rows,
        result);

    assertEqualVectors(makeNullableFlatVector<bool>(expected), result);
  };

  testContains(1, {true, false, false, false, false, false});
  testContains(3, {true, true, false, false, false, false});
  testContains(5, {false, true, false, true, false, false});
  testContains(7, {false, false, false, true, true, true});
  testContains(-2, {false, false, false, false, false, false});
  testContains(
      std::nullopt,
      {std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt});
}

TEST_F(ArrayContainsTest, preAllocatedNulls) {
  auto arrayVector = makeArrayVector<int64_t>(
      {{1, 2, 3, 4}, {3, 4, 5}, {}, {5, 6, 7, 8, 9}, {7}, {10, 9, 8, 7}});

  auto testContains = [&](std::optional<int64_t> search,
                          const std::vector<std::optional<bool>>& expected) {
    VectorPtr result = makeFlatVector<bool>(6);
    SelectivityVector rows(6);
    rows.resize(6);
    result->setNull(0, true);

    evaluate<SimpleVector<bool>>(
        "contains(c0, c1)",
        makeRowVector({
            arrayVector,
            makeConstant(search, arrayVector->size()),
        }),
        rows,
        result);

    assertEqualVectors(makeNullableFlatVector<bool>(expected), result);
  };

  testContains(1, {true, false, false, false, false, false});
  testContains(3, {true, true, false, false, false, false});
  testContains(5, {false, true, false, true, false, false});
  testContains(7, {false, false, false, true, true, true});
  testContains(-2, {false, false, false, false, false, false});
  testContains(
      std::nullopt,
      {std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt});
}

} // namespace
