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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/type/tests/utils/CustomTypesForTesting.h"
#include "velox/vector/tests/utils/VectorMaker.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox {

static constexpr int32_t kNeq = 1;
static constexpr int32_t kEq = 0;

class VectorCompareTest : public testing::Test,
                          public velox::test::VectorTestBase {
 public:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  bool static constexpr kExpectNull = true;
  bool static constexpr kExpectNotNull = false;
  bool static constexpr kEqualsOnly = true;

  CompareFlags kEquality = CompareFlags::equality(
      CompareFlags::NullHandlingMode::kNullAsIndeterminate);

  CompareFlags kOrderingAsc = {
      .ascending = true,
      .nullHandlingMode = CompareFlags::NullHandlingMode::kNullAsIndeterminate};
  CompareFlags kOrderingDesc = {
      .ascending = false,
      .nullHandlingMode = CompareFlags::NullHandlingMode::kNullAsIndeterminate};
  void testCompare(
      const VectorPtr& vector,
      vector_size_t index1,
      vector_size_t index2,
      CompareFlags testFlags,
      std::optional<int32_t> expectedResult) {
    testCompare(vector, index1, vector, index2, testFlags, expectedResult);
  }

  void testCompare(
      const VectorPtr& vector1,
      vector_size_t index1,
      const VectorPtr& vector2,
      vector_size_t index2,
      CompareFlags testFlags,
      std::optional<int32_t> expectedResult) {
    if (testFlags.equalsOnly && expectedResult.has_value() &&
        expectedResult.value() != kEq) {
      // If we are checking for non-equality we just check that output is 0.
      ASSERT_NE(
          0,
          vector1->compare(vector2.get(), index1, index2, testFlags).value());

      // Test that NullAsIndeterminate results matche NullAsValue results when
      // results are determined.
      testFlags.nullHandlingMode = CompareFlags::NullHandlingMode::kNullAsValue;
      ASSERT_NE(
          0,
          vector1->compare(vector2.get(), index1, index2, testFlags).value());
    } else {
      ASSERT_EQ(
          expectedResult,
          vector1->compare(vector2.get(), index1, index2, testFlags));

      // Test that NullAsIndeterminate results matche NullAsValue results when
      // results are determined.
      if (expectedResult.has_value()) {
        testFlags.nullHandlingMode =
            CompareFlags::NullHandlingMode::kNullAsValue;

        ASSERT_EQ(
            expectedResult,
            vector1->compare(vector2.get(), index1, index2, testFlags));
      }
    }
  }

  void testOrderingThrow(
      const VectorPtr& vector,
      vector_size_t index1,
      vector_size_t index2) {
    testOrderingThrow(vector, index1, vector, index2);
    testOrderingThrow(vector, index2, vector, index1);
  }

  void testOrderingThrow(
      const VectorPtr& vector1,
      vector_size_t index1,
      const VectorPtr& vector2,
      vector_size_t index2) {
    VELOX_ASSERT_THROW(
        testCompare(
            vector1, index1, vector2, index2, kOrderingDesc, kIndeterminate),
        "Ordering nulls is not supported");
    VELOX_ASSERT_THROW(
        testCompare(
            vector1, index1, vector2, index2, kOrderingAsc, kIndeterminate),
        "Ordering nulls is not supported");
  }

  void testCustomComparison(
      const std::vector<VectorPtr>& vectors,
      const std::vector<vector_size_t>& offsets) {
    // Tests BaseVector::compare() for custom types that supply custom
    // comparison. It assumes:
    //   vectors[2][offsets[2]] is null or is a complex type that only contains
    //     nulls
    //   vectors[0][offsets[0]] < vectors[1][offsets[1]]
    //   vectors[3][offsets[3]] == vectors[0][offsets[0]]
    //   vectors[4][offsets[4]] == vectors[1][offsets[1]]

    ASSERT_EQ(vectors.size(), 5);
    ASSERT_EQ(offsets.size(), 5);

    // Test equality.
    {
      testCompare(
          vectors[0], offsets[0], vectors[0], offsets[0], kEquality, kEq);
      testCompare(
          vectors[0], offsets[0], vectors[1], offsets[1], kEquality, kNeq);
      testCompare(
          vectors[1], offsets[1], vectors[0], offsets[0], kEquality, kNeq);
      testCompare(
          vectors[0], offsets[0], vectors[3], offsets[3], kEquality, kEq);
      testCompare(
          vectors[3], offsets[3], vectors[0], offsets[0], kEquality, kEq);
      testCompare(
          vectors[0], offsets[0], vectors[4], offsets[4], kEquality, kNeq);
      testCompare(
          vectors[4], offsets[4], vectors[0], offsets[0], kEquality, kNeq);
      testCompare(
          vectors[1], offsets[1], vectors[3], offsets[3], kEquality, kNeq);
      testCompare(
          vectors[3], offsets[3], vectors[1], offsets[1], kEquality, kNeq);
      testCompare(
          vectors[1], offsets[1], vectors[4], offsets[4], kEquality, kEq);
      testCompare(
          vectors[4], offsets[4], vectors[1], offsets[1], kEquality, kEq);
      testCompare(
          vectors[0],
          offsets[0],
          vectors[2],
          offsets[2],
          kEquality,
          kIndeterminate);
      testCompare(
          vectors[2],
          offsets[2],
          vectors[0],
          offsets[0],
          kEquality,
          kIndeterminate);
      testCompare(
          vectors[2],
          offsets[2],
          vectors[2],
          offsets[2],
          kEquality,
          kIndeterminate);
    }

    // Test ordering.
    if (vectors[0]->type()->isOrderable()) {
      testCompare(
          vectors[0], offsets[0], vectors[0], offsets[0], kOrderingDesc, 0);
      testCompare(
          vectors[0], offsets[0], vectors[1], offsets[1], kOrderingDesc, 1);
      testCompare(
          vectors[1], offsets[1], vectors[0], offsets[0], kOrderingDesc, -1);
      testCompare(
          vectors[0], offsets[0], vectors[3], offsets[3], kOrderingDesc, 0);
      testCompare(
          vectors[3], offsets[3], vectors[0], offsets[0], kOrderingDesc, 0);
      testCompare(
          vectors[0], offsets[0], vectors[4], offsets[4], kOrderingDesc, 1);
      testCompare(
          vectors[4], offsets[4], vectors[0], offsets[0], kOrderingDesc, -1);
      testCompare(
          vectors[1], offsets[1], vectors[3], offsets[3], kOrderingDesc, -1);
      testCompare(
          vectors[3], offsets[3], vectors[1], offsets[1], kOrderingDesc, 1);
      testCompare(
          vectors[1], offsets[1], vectors[4], offsets[4], kOrderingDesc, 0);
      testCompare(
          vectors[4], offsets[4], vectors[1], offsets[1], kOrderingDesc, 0);

      testCompare(
          vectors[0], offsets[0], vectors[0], offsets[0], kOrderingAsc, 0);
      testCompare(
          vectors[0], offsets[0], vectors[1], offsets[1], kOrderingAsc, -1);
      testCompare(
          vectors[1], offsets[1], vectors[0], offsets[0], kOrderingAsc, 1);
      testCompare(
          vectors[0], offsets[0], vectors[3], offsets[3], kOrderingAsc, 0);
      testCompare(
          vectors[3], offsets[3], vectors[0], offsets[0], kOrderingAsc, 0);
      testCompare(
          vectors[0], offsets[0], vectors[4], offsets[4], kOrderingAsc, -1);
      testCompare(
          vectors[4], offsets[4], vectors[0], offsets[0], kOrderingAsc, 1);
      testCompare(
          vectors[1], offsets[1], vectors[3], offsets[3], kOrderingAsc, 1);
      testCompare(
          vectors[3], offsets[3], vectors[1], offsets[1], kOrderingAsc, -1);
      testCompare(
          vectors[1], offsets[1], vectors[4], offsets[4], kOrderingAsc, 0);
      testCompare(
          vectors[4], offsets[4], vectors[1], offsets[1], kOrderingAsc, 0);
    }

    // Test hashing.
    {
      ASSERT_EQ(
          vectors[0]->hashValueAt(offsets[0]), vectors[0]->hashValueAt(0));
      ASSERT_NE(
          vectors[0]->hashValueAt(offsets[0]), vectors[1]->hashValueAt(1));
      ASSERT_NE(
          vectors[1]->hashValueAt(offsets[1]), vectors[0]->hashValueAt(0));
      ASSERT_EQ(
          vectors[0]->hashValueAt(offsets[0]), vectors[3]->hashValueAt(3));
      ASSERT_EQ(
          vectors[3]->hashValueAt(offsets[3]), vectors[0]->hashValueAt(0));
      ASSERT_NE(
          vectors[0]->hashValueAt(offsets[0]), vectors[4]->hashValueAt(4));
      ASSERT_NE(
          vectors[4]->hashValueAt(offsets[4]), vectors[0]->hashValueAt(0));
      ASSERT_NE(
          vectors[1]->hashValueAt(offsets[1]), vectors[3]->hashValueAt(3));
      ASSERT_NE(
          vectors[3]->hashValueAt(offsets[3]), vectors[1]->hashValueAt(1));
      ASSERT_EQ(
          vectors[1]->hashValueAt(offsets[1]), vectors[4]->hashValueAt(4));
      ASSERT_EQ(
          vectors[4]->hashValueAt(offsets[4]), vectors[1]->hashValueAt(1));
      ASSERT_NE(
          vectors[0]->hashValueAt(offsets[0]), vectors[2]->hashValueAt(2));
      ASSERT_NE(
          vectors[2]->hashValueAt(offsets[2]), vectors[0]->hashValueAt(0));
      ASSERT_EQ(
          vectors[2]->hashValueAt(offsets[2]), vectors[2]->hashValueAt(2));
    }
  }
};

TEST_F(VectorCompareTest, compareNullAsIndeterminateFlat) {
  auto flatVector =
      vectorMaker_.flatVectorNullable<int32_t>({1, std::nullopt, 3});

  // Test equality.
  {
    testCompare(flatVector, 0, 0, kEquality, kEq);
    testCompare(flatVector, 0, 2, kEquality, kNeq);
    testCompare(flatVector, 0, 1, kEquality, kIndeterminate);
    testCompare(flatVector, 1, 0, kEquality, kIndeterminate);
    testCompare(flatVector, 1, 1, kEquality, kIndeterminate);
  }

  // Test ordering with nulls.
  {
    testOrderingThrow(flatVector, 0, 1);
    testOrderingThrow(flatVector, 1, 0);
    testOrderingThrow(flatVector, 1, 1);
  }

  // Test ordering.
  {
    testCompare(flatVector, 0, 0, kOrderingDesc, 0);
    testCompare(flatVector, 0, 2, kOrderingDesc, 1);
    testCompare(flatVector, 2, 0, kOrderingDesc, -1);

    testCompare(flatVector, 0, 0, kOrderingAsc, 0);
    testCompare(flatVector, 0, 2, kOrderingAsc, -1);
    testCompare(flatVector, 2, 0, kOrderingAsc, 1);
  }
}

// Test SimpleVector<ComplexType>::compare()
TEST_F(VectorCompareTest, compareNullAsIndeterminateSimpleOfComplex) {
  auto flatVector = vectorMaker_.arrayVectorNullable<int32_t>(
      {{{1, 2, 3}}, std::nullopt, {{1, 2, 4}}});

  auto constantVectorNull = BaseVector::wrapInConstant(2, 1, flatVector);
  auto constantVectorOne = BaseVector::wrapInConstant(2, 0, flatVector);
  auto constantVectorTwo = BaseVector::wrapInConstant(2, 2, flatVector);

  auto indices = makeIndices({0, 1, 2});
  auto dictionary =
      BaseVector::wrapInDictionary(nullptr, indices, 3, flatVector);

  // Test equality with nulls.
  {
    CompareFlags equalityFlags = CompareFlags::equality(
        CompareFlags::NullHandlingMode::kNullAsIndeterminate);

    // Constat vector.
    testCompare(
        constantVectorNull, 0, constantVectorOne, 1, kEquality, kIndeterminate);

    testCompare(
        constantVectorOne, 0, constantVectorNull, 1, kEquality, kIndeterminate);

    testCompare(
        constantVectorNull,
        0,
        constantVectorNull,
        1,
        kEquality,
        kIndeterminate);

    // Dictionary vector.
    testCompare(dictionary, 0, 1, kEquality, kIndeterminate);
    testCompare(dictionary, 1, 0, kEquality, kIndeterminate);
    testCompare(dictionary, 1, 1, kEquality, kIndeterminate);
  }

  // Test ordering with nulls.
  {
    // Constat vector.
    testOrderingThrow(constantVectorNull, 0, constantVectorOne, 1);

    testOrderingThrow(constantVectorOne, 0, constantVectorNull, 1);

    testOrderingThrow(constantVectorNull, 0, constantVectorNull, 1);

    // Dictionary vector.
    testOrderingThrow(dictionary, 0, 1);
    testOrderingThrow(dictionary, 1, 0);
    testOrderingThrow(dictionary, 1, 1);
  }

  // Some tests without nulls.
  testCompare(constantVectorOne, 0, constantVectorOne, 1, kEquality, kEq);
  testCompare(constantVectorOne, 0, constantVectorOne, 1, kOrderingDesc, 0);
  testCompare(constantVectorOne, 0, constantVectorOne, 1, kOrderingAsc, 0);

  testCompare(constantVectorOne, 0, constantVectorTwo, 1, kEquality, kNeq);
  testCompare(constantVectorOne, 0, constantVectorTwo, 1, kOrderingDesc, 1);
  testCompare(constantVectorOne, 0, constantVectorTwo, 1, kOrderingAsc, -1);

  testCompare(dictionary, 0, 2, kOrderingAsc, -1);
  testCompare(dictionary, 0, 2, kOrderingDesc, 1);
  testCompare(dictionary, 2, 2, kEquality, kEq);
}

TEST_F(VectorCompareTest, compareNullAsIndeterminateArray) {
  auto test = [&](const std::string& array1,
                  const std::string& array2,
                  CompareFlags flags,
                  std::optional<int32_t> expected) {
    auto vector = vectorMaker_.arrayVectorFromJson<int64_t>({array1, array2});
    testCompare(vector, 0, 1, flags, expected);
  };

  auto orderingThrow = [&](const std::string& array1,
                           const std::string& array2) {
    auto vector = vectorMaker_.arrayVectorFromJson<int64_t>({array1, array2});
    testOrderingThrow(vector, 0, 1);
  };

  // Top level nulls.
  orderingThrow("null", "null");
  orderingThrow("null", "[]");
  orderingThrow("[]", "null");

  test("null", "[1, 2]", kEquality, kIndeterminate);
  test("null", "null", kEquality, kIndeterminate);
  test("[]", "null", kEquality, kIndeterminate);

  // Testing equality.
  {
    // False dominates indeterminate.
    test("[null, 1]", "[null, 2]", kEquality, kNeq);
    test("[1, null]", "[2, null]", kEquality, kNeq);

    // If the mode is equality and the arrays have different lengths, then
    // values are not read.
    test("[null]", "[null, null]", kEquality, kNeq);
    test("[]", "[null, null]", kEquality, kNeq);
    test("[1]", "[2, null, null]", kEquality, kNeq);

    // Indeterminate dominates true.
    test("[1, null]", "[1, null]", kEquality, kIndeterminate);
    test("[1, 1, 2]", "[1, 1, null]", kEquality, kIndeterminate);
    test("[1, 1, 2]", "[null, 1, 2]", kEquality, kIndeterminate);
    test("[null,1 , 2]", "[1, 1, 2]", kEquality, kIndeterminate);

    test("[1, 2]", "[1, 2]", kEquality, kEq);
    test("[1, 2]", "[1, 3]", kEquality, kNeq);
    test("[1, 2]", "[1, 2, 3]", kEquality, kNeq);
  }

  // Testing ordering.
  {
    orderingThrow("[null]", "[null]");
    orderingThrow("[null]", "[1]");
    orderingThrow("[null]", "[1, 2]");
    orderingThrow("[1, 2, null]", "[1, 2, null]");

    // Only elements between 0 and min size are read.
    test("[1]", "[1, null, 3]", kOrderingAsc, -2);
    test("[1]", "[1, null, 3]", kOrderingDesc, 2);
    test("[1, null, 3]", "[1]", kOrderingAsc, 2);
    test("[1, null, 3]", "[1]", kOrderingDesc, -2);

    // If order is determined before null is seen, no throw happen and null is
    // not read.
    test("[1, 2, null]", "[1, 3, null]", kOrderingAsc, -1);
    test("[1, 2, null]", "[1, 3, null]", kOrderingDesc, 1);
    test("[1, 3, null]", "[1, 2, null]", kOrderingAsc, 1);
    test("[1, 3, null]", "[1, 2, null]", kOrderingDesc, -1);
  }
}

TEST_F(VectorCompareTest, compareNullAsIndeterminateNullMap) {
  auto test = [&](const std::string& map1,
                  const std::string& map2,
                  CompareFlags flags,
                  std::optional<int32_t> expected) {
    auto vector = makeMapVectorFromJson<int64_t, int64_t>({map1, map2});
    testCompare(vector, 0, 1, flags, expected);
  };

  // Map is NOT orderable in NullAsIndeterminate mode.
  VELOX_ASSERT_THROW(
      test("{}", "{}", kOrderingAsc, kIndeterminate),
      "Map is not orderable type");
  VELOX_ASSERT_THROW(
      test("{}", "{}", kOrderingDesc, kIndeterminate),
      "Map is not orderable type");

  // Map is orderable in NullAsValue mode.
  test("{}", "{}", CompareFlags{.ascending = true}, 0);
  test("{}", "{}", CompareFlags{.ascending = false}, 0);

  // Only need to check equality.

  // Top level nulls.
  test("null", "{}", kEquality, kIndeterminate);

  // If keys are different, values are not looked at. Note that keys cant be
  // null or contain nested nulls in presto.
  test("{1: null, 2: null}", "{1: null, 3: null}", kEquality, kNeq);
  test("{1: null, 2: null}", "{1: null}", kEquality, kNeq);

  // If keys are equal, values are compared as arrays.
  // == [null, null] = [null, null]
  test("{1: null, 2: null}", "{1: null, 2: null}", kEquality, kIndeterminate);

  // == [1, null] = [2, null]
  test("{1: 1, 2: null}", "{1: 2, 2: null}", kEquality, kNeq);
  test("{1: 2, 2: null}", "{1: 1, 2: null}", kEquality, kNeq);

  // == [null, 1] = [null, 2]
  test("{1: null, 2: 1}", "{1: null, 2: 2}", kEquality, kNeq);
  test("{1: null, 2: 2}", "{1: null, 2: 1}", kEquality, kNeq);
}

TEST_F(VectorCompareTest, compareNullAsIndeterminateRow) {
  auto test =
      [&](const std::tuple<std::optional<int64_t>, std::optional<int64_t>>&
              row1,
          const std::tuple<std::optional<int64_t>, std::optional<int64_t>>&
              row2,
          CompareFlags testFlags,
          const std::optional<int32_t> expected) {
        auto vector = vectorMaker_.rowVector(
            {vectorMaker_.flatVectorNullable<int64_t>(
                 {std::get<0>(row1), std::get<0>(row2)}),
             vectorMaker_.flatVectorNullable<int64_t>(
                 {std::get<1>(row1), std::get<1>(row2)})});
        testCompare(vector, 0, 1, testFlags, expected);
      };

  auto orderingThrow =
      [&](const std::tuple<std::optional<int64_t>, std::optional<int64_t>>&
              row1,
          const std::tuple<std::optional<int64_t>, std::optional<int64_t>>&
              row2) {
        auto vector = vectorMaker_.rowVector(
            {vectorMaker_.flatVectorNullable<int64_t>(
                 {std::get<0>(row1), std::get<0>(row2)}),
             vectorMaker_.flatVectorNullable<int64_t>(
                 {std::get<1>(row1), std::get<1>(row2)})});
        testOrderingThrow(vector, 0, 1);
      };

  // Top level nulls.
  {
    auto vector = vectorMaker_.rowVector(
        {vectorMaker_.flatVector<int64_t>(0),
         vectorMaker_.flatVector<int64_t>(0)});
    ASSERT_EQ(vector->size(), 0);
    vector->appendNulls(2);
    testCompare(vector, 0, 1, kEquality, kIndeterminate);
    testOrderingThrow(vector, 0, 1);
  }

  const auto kNull = std::nullopt;

  // Testing equality.
  {
    // False dominates indeterminate.
    test({kNull, 1}, {kNull, 2}, kEquality, kNeq);
    test({kNull, 2}, {kNull, 1}, kEquality, kNeq);

    // Indeterminate dominates true.
    test({1, kNull}, {1, kNull}, kEquality, kIndeterminate);
    test({kNull, 1}, {kNull, 1}, kEquality, kIndeterminate);

    test({1, 2}, {1, 2}, kEquality, kEq);
    test({1, 2}, {1, 3}, kEquality, kNeq);
  }

  // Testing ordering.
  {
    orderingThrow({kNull, kNull}, {kNull, kNull});
    orderingThrow({1, kNull}, {1, kNull});
    orderingThrow({1, kNull}, {kNull, kNull});
    orderingThrow({kNull, kNull}, {1, kNull});

    // If order is determined before null is seen, no throw happens and null is
    // not read.
    test({1, kNull}, {2, kNull}, kOrderingAsc, -1);
    test({1, kNull}, {2, kNull}, kOrderingDesc, 1);
    test({2, kNull}, {1, kNull}, kOrderingDesc, -1);
    test({2, kNull}, {1, kNull}, kOrderingAsc, 1);
  }
}

TEST_F(VectorCompareTest, CompareWithNullChildVector) {
  auto pool = memory::memoryManager()->addLeafPool();
  test::VectorMaker maker{pool.get()};
  auto rowType = ROW({"a", "b", "c"}, {INTEGER(), INTEGER(), INTEGER()});
  const auto& rowVector1 = std::make_shared<RowVector>(
      pool_.get(),
      rowType,
      BufferPtr(nullptr),
      3,
      std::vector<VectorPtr>{
          maker.flatVector<int32_t>({1, 2, 3, 4}),
          nullptr,
          maker.flatVector<int32_t>({1, 2, 3, 4})});

  const auto& rowVector2 = std::make_shared<RowVector>(
      pool_.get(),
      rowType,
      BufferPtr(nullptr),
      3,
      std::vector<VectorPtr>{
          maker.flatVector<int32_t>({1, 2, 3, 4}),
          nullptr,
          maker.flatVector<int32_t>({1, 2, 3, 4})});
  test::assertEqualVectors(rowVector1, rowVector2);
}

TEST_F(VectorCompareTest, customComparisonFlat) {
  auto flatVector = vectorMaker_.flatVectorNullable<int64_t>(
      {0, 1, std::nullopt, 256, 257},
      test::BIGINT_TYPE_WITH_CUSTOM_COMPARISON());

  testCustomComparison(std::vector<VectorPtr>(5, flatVector), {0, 1, 2, 3, 4});
}

TEST_F(VectorCompareTest, customComparisonConstant) {
  auto flatVector = vectorMaker_.flatVectorNullable<int64_t>(
      {0, 1, std::nullopt, 256, 257},
      test::BIGINT_TYPE_WITH_CUSTOM_COMPARISON());

  auto constantVectorNull = BaseVector::wrapInConstant(1, 2, flatVector);
  auto constantVectorOne = BaseVector::wrapInConstant(1, 0, flatVector);
  auto constantVectorTwo = BaseVector::wrapInConstant(1, 1, flatVector);
  auto constantVectorThree = BaseVector::wrapInConstant(1, 3, flatVector);
  auto constantVectorFour = BaseVector::wrapInConstant(1, 4, flatVector);

  testCustomComparison(
      {constantVectorOne,
       constantVectorTwo,
       constantVectorNull,
       constantVectorThree,
       constantVectorFour},
      std::vector<vector_size_t>(5, 0));
}

TEST_F(VectorCompareTest, customComparisonDictionary) {
  auto flatVector = vectorMaker_.flatVectorNullable<int64_t>(
      {0, 1, std::nullopt, 256, 257},
      test::BIGINT_TYPE_WITH_CUSTOM_COMPARISON());

  auto indices = makeIndices({0, 1, 2, 3, 4});
  auto dictionary =
      BaseVector::wrapInDictionary(nullptr, indices, 5, flatVector);

  testCustomComparison(std::vector<VectorPtr>(5, dictionary), {0, 1, 2, 3, 4});
}

TEST_F(VectorCompareTest, customComparisonArray) {
  auto arrayVector = makeNullableArrayVector<int64_t>(
      {{0}, {1}, {std::nullopt}, {256}, {257}},
      ARRAY(test::BIGINT_TYPE_WITH_CUSTOM_COMPARISON()));

  testCustomComparison(std::vector<VectorPtr>(5, arrayVector), {0, 1, 2, 3, 4});
}

TEST_F(VectorCompareTest, customComparisonMap) {
  auto mapVector = makeNullableMapVector<int64_t, int64_t>(
      {{{{0, 0}}},
       {{{1, 1}}},
       {{{0, std::nullopt}}},
       {{{256, 256}}},
       {{{257, 257}}}},
      MAP(test::BIGINT_TYPE_WITH_CUSTOM_COMPARISON(),
          test::BIGINT_TYPE_WITH_CUSTOM_COMPARISON()));

  testCustomComparison(std::vector<VectorPtr>(5, mapVector), {0, 1, 2, 3, 4});
}

TEST_F(VectorCompareTest, customComparisonRow) {
  auto rowVector = makeRowVector(
      {"a"},
      {vectorMaker_.flatVectorNullable<int64_t>(
          {0, 1, std::nullopt, 256, 257},
          test::BIGINT_TYPE_WITH_CUSTOM_COMPARISON())});

  testCustomComparison(std::vector<VectorPtr>(5, rowVector), {0, 1, 2, 3, 4});
}

} // namespace facebook::velox
