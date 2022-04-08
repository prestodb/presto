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

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <optional>

#include "velox/vector/tests/VectorTestBase.h"

namespace facebook::velox {
class VectorCompareTest : public testing::Test,
                          public velox::test::VectorTestBase {
 public:
  bool static constexpr kExpectNull = true;
  bool static constexpr kExpectNotNull = false;
  bool static constexpr kEqualsOnly = true;

  void testCompareWithStopAtNull(
      const VectorPtr& vector,
      vector_size_t index1,
      vector_size_t index2,
      bool expectNull,
      bool equalsOnly = false) {
    CompareFlags testFlags;
    testFlags.stopAtNull = true;
    testFlags.equalsOnly = equalsOnly;

    ASSERT_EQ(
        expectNull,
        !vector->compare(vector.get(), index1, index2, testFlags).has_value());

    ASSERT_TRUE(vector->compare(vector.get(), index1, index2, CompareFlags())
                    .has_value());
  }
};

TEST_F(VectorCompareTest, compareStopAtNullFlat) {
  auto flatVector = vectorMaker_.flatVectorNullable<int32_t>({1, std::nullopt});

  testCompareWithStopAtNull(flatVector, 0, 0, kExpectNotNull);
  testCompareWithStopAtNull(flatVector, 0, 1, kExpectNull);
  testCompareWithStopAtNull(flatVector, 1, 0, kExpectNull);
  testCompareWithStopAtNull(flatVector, 1, 1, kExpectNull);
}

TEST_F(VectorCompareTest, compareStopAtNullArray) {
  auto test = [&](const std::optional<std::vector<std::optional<int64_t>>>&
                      array1,
                  const std::optional<std::vector<std::optional<int64_t>>>&
                      array2,
                  bool expectNull,
                  bool equalsOnly = false) {
    auto vector = vectorMaker_.arrayVectorNullable<int64_t>({array1, array2});
    testCompareWithStopAtNull(vector, 0, 1, expectNull, equalsOnly);
  };

  test(std::nullopt, std::nullopt, kExpectNull);
  test(std::nullopt, {{1}}, kExpectNull);
  test({{1}}, std::nullopt, kExpectNull);

  test({{1, 2, 3}}, {{1, 2, 3}}, kExpectNotNull);

  // Checking the first element is enough to determine the result of the
  // compare.
  test({{1, std::nullopt}}, {{6, 2}}, kExpectNotNull);

  test({{1, std::nullopt}}, {{1, 2}}, kExpectNull);

  // When two arrays are of different sizes the checked elements are:
  // equalsOnly=true  -> none.
  // equalsOnly=false -> the size of the smallest.
  test({{}}, {{std::nullopt, std::nullopt}}, kExpectNotNull);
  test({{1, 2}}, {{1, 2, std::nullopt}}, kExpectNotNull);
  test({{std::nullopt}}, {{std::nullopt, std::nullopt}}, kExpectNull);

  // Since the two arrays are of different size and equalsOnly is enabled, no
  // elements is read and hence no null encountered.
  test(
      {{std::nullopt, std::nullopt}},
      {{std::nullopt, std::nullopt, std::nullopt}},
      kExpectNotNull,
      kEqualsOnly);

  // Since kEqualsOnly = false, the first two elements will be read.
  test(
      {{std::nullopt, std::nullopt}},
      {{std::nullopt, std::nullopt, std::nullopt}},
      kExpectNull);

  test(
      {{std::nullopt, std::nullopt}},
      {{std::nullopt, std::nullopt}},
      kExpectNull,
      kEqualsOnly);

  test(
      {{std::nullopt, std::nullopt}},
      {{std::nullopt, std::nullopt}},
      kExpectNull);
}

TEST_F(VectorCompareTest, compareStopAtNullMap) {
  using map_t =
      std::optional<std::vector<std::pair<int64_t, std::optional<int64_t>>>>;
  auto test = [&](const map_t& map1,
                  const map_t& map2,
                  bool expectNull,
                  bool equalsOnly = false) {
    auto vector = makeNullableMapVector<int64_t, int64_t>({map1, map2});
    testCompareWithStopAtNull(vector, 0, 1, expectNull, equalsOnly);
  };

  test({{{1, 2}, {3, 4}}}, {{{1, 2}, {3, 4}}}, kExpectNotNull);

  // Null map entries.
  test(std::nullopt, {{{1, 2}, {3, 4}}}, kExpectNull);
  test({{{1, 2}, {3, 4}}}, std::nullopt, kExpectNull);

  // Null in values should be read.
  test({{{1, std::nullopt}, {3, 4}}}, {{{1, 2}, {3, 4}}}, kExpectNull);
  test({{{1, 2}, {3, 4}}}, {{{1, 2}, {3, std::nullopt}}}, kExpectNull);
  test(
      {{{1, std::nullopt}, {2, std::nullopt}}},
      {{{1, std::nullopt}, {2, std::nullopt}}},
      kExpectNull);

  // Compare will find results before reading null.
  // All keys are compared before values.
  test({{{1, std::nullopt}, {3, 4}}}, {{{2, 2}, {3, 4}}}, kExpectNotNull);
  test(
      {{{1, std::nullopt}, {2, std::nullopt}}},
      {{{1, std::nullopt}, {3, std::nullopt}}},
      kExpectNotNull);
  test(
      {{{2, std::nullopt}, {1, std::nullopt}}},
      {{{1, std::nullopt}, {3, std::nullopt}}},
      kExpectNotNull);

  // Different sizes.
  test({{{1, 2}, {1, std::nullopt}}}, {{{1, std::nullopt}}}, kExpectNotNull);
  test(
      {{{1, 2}, {1, std::nullopt}}},
      {{{1, std::nullopt}}},
      kExpectNotNull,
      kEqualsOnly);
}

TEST_F(VectorCompareTest, compareStopAtNullRow) {
  auto test =
      [&](const std::tuple<std::optional<int64_t>, std::optional<int64_t>>&
              row1,
          const std::tuple<std::optional<int64_t>, std::optional<int64_t>>&
              row2,
          bool expectNull,
          bool equalsOnly = false) {
        auto vector = vectorMaker_.rowVector(
            {vectorMaker_.flatVectorNullable<int64_t>(
                 {std::get<0>(row1), std::get<0>(row2)}),
             vectorMaker_.flatVectorNullable<int64_t>(
                 {std::get<1>(row1), std::get<1>(row2)})});

        testCompareWithStopAtNull(vector, 0, 1, expectNull, equalsOnly);
      };

  test({1, 2}, {2, 3}, kExpectNotNull);
  test({1, 2}, {1, 2}, kExpectNotNull);
  test({2, std::nullopt}, {1, 2}, kExpectNotNull);

  test({1, 2}, {1, std::nullopt}, kExpectNull);
  test({1, std::nullopt}, {1, 2}, kExpectNull);
  test({1, 2}, {std::nullopt, 2}, kExpectNull);
}
} // namespace facebook::velox
