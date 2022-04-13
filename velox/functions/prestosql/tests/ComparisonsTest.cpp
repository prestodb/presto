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
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

using namespace facebook::velox;

class ComparisonsTest : public functions::test::FunctionBaseTest {};

TEST_F(ComparisonsTest, between) {
  std::vector<std::tuple<int32_t, bool>> testData = {
      {0, false}, {1, true}, {4, true}, {5, true}, {10, false}, {-1, false}};

  auto result = evaluate<SimpleVector<bool>>(
      "c0 between 1 and 5",
      makeRowVector({makeFlatVector<int32_t, 0>(testData)}));

  for (int i = 0; i < testData.size(); ++i) {
    EXPECT_EQ(result->valueAt(i), std::get<1>(testData[i])) << "at " << i;
  }
}

TEST_F(ComparisonsTest, betweenVarchar) {
  using S = StringView;

  const auto between = [&](std::optional<std::string> s) {
    auto expr = "c0 between 'mango' and 'pear'";
    if (s.has_value()) {
      return evaluateOnce<bool>(expr, std::optional(S(s.value())));
    } else {
      return evaluateOnce<bool>(expr, std::optional<S>());
    }
  };

  EXPECT_EQ(std::nullopt, between(std::nullopt));
  EXPECT_EQ(false, between(""));
  EXPECT_EQ(false, between("apple"));
  EXPECT_EQ(false, between("pineapple"));
  EXPECT_EQ(true, between("mango"));
  EXPECT_EQ(true, between("orange"));
  EXPECT_EQ(true, between("pear"));
}

TEST_F(ComparisonsTest, betweenDate) {
  auto parseDate = [](const std::string& dateStr) {
    Date returnDate;
    parseTo(dateStr, returnDate);
    return returnDate;
  };
  std::vector<std::tuple<Date, bool>> testData = {
      {parseDate("2019-05-01"), false},
      {parseDate("2019-06-01"), true},
      {parseDate("2019-07-01"), true},
      {parseDate("2020-05-31"), true},
      {parseDate("2020-06-01"), true},
      {parseDate("2020-07-01"), false}};

  auto result = evaluate<SimpleVector<bool>>(
      "c0 between cast(\'2019-06-01\' as date) and cast(\'2020-06-01\' as date)",
      makeRowVector({makeFlatVector<Date, 0>(testData)}));

  for (int i = 0; i < testData.size(); ++i) {
    EXPECT_EQ(result->valueAt(i), std::get<1>(testData[i])) << "at " << i;
  }
}

TEST_F(ComparisonsTest, eqArray) {
  auto test =
      [&](const std::optional<std::vector<std::optional<int64_t>>>& array1,
          const std::optional<std::vector<std::optional<int64_t>>>& array2,
          std::optional<bool> expected) {
        auto vector1 = vectorMaker_.arrayVectorNullable<int64_t>({array1});
        auto vector2 = vectorMaker_.arrayVectorNullable<int64_t>({array2});
        auto result = evaluate<SimpleVector<bool>>(
            "c0 == c1", makeRowVector({vector1, vector2}));

        ASSERT_EQ(expected.has_value(), !result->isNullAt(0));
        if (expected.has_value()) {
          ASSERT_EQ(expected.value(), result->valueAt(0));
        }
      };

  test(std::nullopt, std::nullopt, std::nullopt);
  test(std::nullopt, {{1}}, std::nullopt);
  test({{1}}, std::nullopt, std::nullopt);

  test({{}}, {{}}, true);

  test({{1, 2, 3}}, {{1, 2, 3}}, true);
  test({{1, 2, 3}}, {{1, 2, 4}}, false);

  // Checking the first element is enough to determine the result of the
  // compare.
  test({{1, std::nullopt}}, {{6, 2}}, false);

  test({{1, std::nullopt}}, {{1, 2}}, std::nullopt);

  // Different size arrays.
  test({{}}, {{std::nullopt, std::nullopt}}, false);
  test({{1, 2}}, {{1, 2, std::nullopt}}, false);
  test(
      {{std::nullopt, std::nullopt}},
      {{std::nullopt, std::nullopt, std::nullopt}},
      false);

  test(
      {{std::nullopt, std::nullopt}},
      {{std::nullopt, std::nullopt}},
      std::nullopt);
}

TEST_F(ComparisonsTest, eqMap) {
  using map_t =
      std::optional<std::vector<std::pair<int64_t, std::optional<int64_t>>>>;
  auto test =
      [&](const map_t& map1, const map_t& map2, std::optional<bool> expected) {
        auto vector1 = makeNullableMapVector<int64_t, int64_t>({map1});
        auto vector2 = makeNullableMapVector<int64_t, int64_t>({map2});

        auto result = evaluate<SimpleVector<bool>>(
            "c0 == c1", makeRowVector({vector1, vector2}));

        ASSERT_EQ(expected.has_value(), !result->isNullAt(0));

        if (expected.has_value()) {
          ASSERT_EQ(expected.value(), result->valueAt(0));
        }
      };

  test({{{1, 2}, {3, 4}}}, {{{1, 2}, {3, 4}}}, true);

  // Elements checked in sorted order.
  test({{{3, 4}, {1, 2}}}, {{{1, 2}, {3, 4}}}, true);

  test({{}}, {{}}, true);

  test({{{1, 2}, {3, 5}}}, {{{1, 2}, {3, 4}}}, false);

  test({{{1, 2}, {3, 4}}}, {{{11, 2}, {3, 4}}}, false);

  // Null map entries.
  test(std::nullopt, {{{1, 2}, {3, 4}}}, std::nullopt);
  test({{{1, 2}, {3, 4}}}, std::nullopt, std::nullopt);

  // Null in values should be read.
  test({{{1, std::nullopt}, {3, 4}}}, {{{1, 2}, {3, 4}}}, std::nullopt);

  test({{{1, 2}, {3, 4}}}, {{{1, 2}, {3, std::nullopt}}}, std::nullopt);

  // Compare will find results before reading null.

  // Keys are same, but first value is different.
  test({{{1, 2}, {3, 4}}}, {{{1, 100}, {3, std::nullopt}}}, false);
  test({{{3, 4}, {1, 2}}}, {{{3, std::nullopt}, {1, 100}}}, false);

  // Keys are different.
  test(
      {{{1, std::nullopt}, {2, std::nullopt}}},
      {{{1, std::nullopt}, {3, std::nullopt}}},
      false);
  test(
      {{{2, std::nullopt}, {1, std::nullopt}}},
      {{{1, std::nullopt}, {3, std::nullopt}}},
      false);

  // Different sizes.
  test({{{1, 2}, {10, std::nullopt}}}, {{{1, std::nullopt}}}, false);
  test({{{1, 2}, {10, std::nullopt}}}, {{{1, std::nullopt}}}, false);
}

TEST_F(ComparisonsTest, eqRow) {
  auto test =
      [&](const std::tuple<std::optional<int64_t>, std::optional<int64_t>>&
              row1,
          const std::tuple<std::optional<int64_t>, std::optional<int64_t>>&
              row2,
          std::optional<bool> expected) {
        auto vector1 = vectorMaker_.rowVector(
            {vectorMaker_.flatVectorNullable<int64_t>({std::get<0>(row1)}),
             vectorMaker_.flatVectorNullable<int64_t>({std::get<1>(row1)})});

        auto vector2 = vectorMaker_.rowVector(
            {vectorMaker_.flatVectorNullable<int64_t>({std::get<0>(row2)}),
             vectorMaker_.flatVectorNullable<int64_t>({std::get<1>(row2)})});

        auto result = evaluate<SimpleVector<bool>>(
            "c0 == c1", makeRowVector({vector1, vector2}));

        ASSERT_EQ(expected.has_value(), !result->isNullAt(0));

        if (expected.has_value()) {
          ASSERT_EQ(expected.value(), result->valueAt(0));
        }
      };

  test({1, 2}, {2, 3}, false);
  test({1, 2}, {1, 2}, true);

  test({2, std::nullopt}, {1, 2}, false);

  test({1, 2}, {1, std::nullopt}, std::nullopt);
  test({1, std::nullopt}, {1, 2}, std::nullopt);
  test({1, 2}, {std::nullopt, 2}, std::nullopt);
}

TEST_F(ComparisonsTest, eqNestedComplex) {
  // Comapre Row(Array<Array<int>>, int, Map<int, int>)
  using array_type = std::optional<std::vector<std::optional<int64_t>>>;
  array_type array1 = {{1, 2}};
  array_type array2 = {{}};
  array_type array3 = {{1, 100, 2}};

  auto vector1 = makeNestedArrayVector<int64_t>({{array1, array2, array3}});
  auto vector2 = makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6});
  auto vector3 = makeMapVector<int64_t, int64_t>({{{1, 2}, {3, 4}}});
  auto row1 = makeRowVector({vector1, vector2, vector3});

  // True result.
  auto result =
      evaluate<SimpleVector<bool>>("c0 == c1", makeRowVector({row1, row1}));
  ASSERT_EQ(result->isNullAt(0), false);
  ASSERT_EQ(result->valueAt(0), true);

  // False result.
  {
    auto differentMap = makeMapVector<int64_t, int64_t>({{{1, 2}, {3, 10}}});
    auto row2 = makeRowVector({vector1, vector2, differentMap});

    auto result =
        evaluate<SimpleVector<bool>>("c0 == c1", makeRowVector({row1, row2}));
    ASSERT_EQ(result->isNullAt(0), false);
    ASSERT_EQ(result->valueAt(0), false);
  }

  // Null result.
  {
    auto mapWithNull =
        makeMapVector<int64_t, int64_t>({{{1, 2}, {3, std::nullopt}}});
    auto row2 = makeRowVector({vector1, vector2, mapWithNull});

    auto result =
        evaluate<SimpleVector<bool>>("c0 == c1", makeRowVector({row1, row2}));
    ASSERT_EQ(result->isNullAt(0), true);
  }
}
