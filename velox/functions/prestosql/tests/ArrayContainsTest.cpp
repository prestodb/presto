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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions::test;

namespace {

class ArrayContainsTest : public FunctionBaseTest {
 public:
  void testContainsConstantKey(
      const ArrayVectorPtr& arrayVector,
      const std::vector<int64_t>& search,
      const std::vector<std::optional<bool>>& expected) {
    auto constSearch =
        BaseVector::wrapInConstant(1, 0, makeArrayVector<int64_t>({search}));
    auto result =
        evaluate("contains(c0, c1)", makeRowVector({arrayVector, constSearch}));
    assertEqualVectors(makeNullableFlatVector<bool>(expected), result);
  }

  template <typename T>
  void testContains(
      const ArrayVectorPtr& arrayVector,
      T search,
      const std::vector<std::optional<bool>>& expected) {
    auto result = evaluate(
        "contains(c0, c1)",
        makeRowVector({
            arrayVector,
            makeConstant(search, arrayVector->size()),
        }));

    assertEqualVectors(makeNullableFlatVector<bool>(expected), result);
  };

  void testContainsGeneric(
      const VectorPtr& arrayVector,
      const VectorPtr& search,
      const std::vector<std::optional<bool>>& expected) {
    auto result = evaluate(
        "contains(c0, c1)",
        makeRowVector({
            arrayVector,
            search,
        }));

    assertEqualVectors(makeNullableFlatVector<bool>(expected), result);
  };

  template <typename T>
  void testFloatingPointNaNs() {
    static const T kNaN = std::numeric_limits<T>::quiet_NaN();
    static const T kSNaN = std::numeric_limits<T>::signaling_NaN();
    {
      auto arrayVector =
          makeArrayVector<T>({{1, 2, 3, 4}, {3, 4, kNaN}, {5, 6, 7, 8, kSNaN}});
      // Test fast path for flat.
      testContains(arrayVector, kNaN, {false, true, true});
      // Test code path for generic encoded vectors.
      auto indices = makeIndices(arrayVector->size(), folly::identity);
      auto dictOverArray = wrapInDictionary(indices, arrayVector);
      testContainsGeneric(
          dictOverArray,
          makeConstant(kNaN, arrayVector->size()),
          {false, true, true});
    }

    // Test code path for complex-type elements.
    {
      RowTypePtr rowType;
      if constexpr (std::is_same_v<T, float>) {
        rowType = ROW({REAL(), VARCHAR()});
      } else {
        static_assert(std::is_same_v<T, double>);
        rowType = ROW({DOUBLE(), VARCHAR()});
      }
      using ArrayOfRow = std::vector<std::optional<std::tuple<T, std::string>>>;
      std::vector<ArrayOfRow> data = {
          {{{1, "red"}}, {{2, "blue"}}, {{3, "green"}}},
          {{{1, "red"}}, {{kNaN, "blue"}}, {{3, "green"}}},
          {{{1, "red"}}, {{kSNaN, "blue"}}, {{3, "green"}}}};
      auto arrayVector = makeArrayOfRowVector(data, rowType);
      const auto searchVector =
          makeConstantRow(rowType, variant::row({kNaN, "blue"}), 2);
      testContainsGeneric(arrayVector, searchVector, {false, true, true});
    }
  }
};

TEST_F(ArrayContainsTest, integerNoNulls) {
  auto arrayVector = makeArrayVector<int64_t>(
      {{1, 2, 3, 4}, {3, 4, 5}, {}, {5, 6, 7, 8, 9}, {7}, {10, 9, 8, 7}});

  testContains(arrayVector, 1, {true, false, false, false, false, false});
  testContains(arrayVector, 3, {true, true, false, false, false, false});
  testContains(arrayVector, 5, {false, true, false, true, false, false});
  testContains(arrayVector, 7, {false, false, false, true, true, true});
  testContains(arrayVector, -2, {false, false, false, false, false, false});
  testContains(
      arrayVector,
      std::optional<int64_t>(std::nullopt),
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

  testContains(
      arrayVector, 1, {true, false, false, std::nullopt, std::nullopt, false});
  testContains(
      arrayVector, 3, {true, true, false, std::nullopt, std::nullopt, false});
  testContains(arrayVector, 5, {false, true, false, true, std::nullopt, false});
  testContains(arrayVector, 7, {false, false, false, true, true, true});
  testContains(
      arrayVector,
      -2,
      {false, false, false, std::nullopt, std::nullopt, false});
  testContains(
      arrayVector,
      std::optional<int64_t>(std::nullopt),
      {std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt});
}

TEST_F(ArrayContainsTest, varcharNoNulls) {
  auto arrayVector = makeArrayVector<StringView>({
      {"red"_sv, "blue"_sv},
      {"blue"_sv, "yellow"_sv, "orange"_sv},
      {},
      {"red"_sv, "purple"_sv, "green"_sv},
  });

  testContains(arrayVector, "red"_sv, {true, false, false, true});
  testContains(arrayVector, "blue"_sv, {true, true, false, false});
  testContains(arrayVector, "yellow"_sv, {false, true, false, false});
  testContains(arrayVector, "green"_sv, {false, false, false, true});
  testContains(arrayVector, "crimson red"_sv, {false, false, false, false});
  testContains(
      arrayVector,
      std::optional<StringView>(std::nullopt),
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt});
}

TEST_F(ArrayContainsTest, varcharWithNulls) {
  auto arrayVector = makeNullableArrayVector<StringView>({
      {"red"_sv, "blue"_sv},
      {std::nullopt, "blue"_sv, "yellow"_sv, "orange"_sv},
      {},
      {"red"_sv, "purple"_sv, "green"_sv},
  });

  testContains(arrayVector, "red"_sv, {true, std::nullopt, false, true});
  testContains(arrayVector, "blue"_sv, {true, true, false, false});
  testContains(arrayVector, "yellow"_sv, {false, true, false, false});
  testContains(arrayVector, "green"_sv, {false, std::nullopt, false, true});
  testContains(
      arrayVector, "crimson red"_sv, {false, std::nullopt, false, false});
  testContains(
      arrayVector,
      std::optional<StringView>(std::nullopt),
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt});
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

  testContains(arrayVector, true, {true, true, false, false, true, false});
  testContains(arrayVector, false, {true, false, true, false, true, true});
  testContains(
      arrayVector,
      std::optional<bool>(std::nullopt),
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

  testContains(
      arrayVector, true, {true, true, std::nullopt, false, true, false});
  testContains(arrayVector, false, {true, false, true, false, true, true});
  testContains(
      arrayVector,
      std::optional<bool>(std::nullopt),
      {std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt});
}

TEST_F(ArrayContainsTest, row) {
  std::vector<std::vector<std::optional<std::tuple<int32_t, std::string>>>>
      data = {
          {{{1, "red"}}, {{2, "blue"}}, {{3, "green"}}},
          {{{2, "blue"}}, std::nullopt, {{5, "green"}}},
          {},
          {{{1, "yellow"}}, {{2, "blue"}}, {{4, "green"}}, {{5, "purple"}}},
      };

  auto rowType = ROW({INTEGER(), VARCHAR()});
  auto arrayVector = makeArrayOfRowVector(data, rowType);

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

  testContains(arrayVector, 1, {true, false, false, false, false, false});
  testContains(arrayVector, 1, {true, false, false, false, false, false});
  testContains(arrayVector, 3, {true, true, false, false, false, false});
  testContains(arrayVector, 5, {false, true, false, true, false, false});
  testContains(arrayVector, 7, {false, false, false, true, true, true});
  testContains(arrayVector, -2, {false, false, false, false, false, false});
  testContains(
      arrayVector,
      std::optional<int64_t>(std::nullopt),
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

TEST_F(ArrayContainsTest, constantEncodingElements) {
  // ArrayVector with ConstantVector<Array> elements.
  auto baseVector = makeArrayVector<int64_t>(
      {{1, 2, 3, 4}, {3, 4, 5}, {6}, {5, 6, 7, 8, 9}, {7}, {10, 9, 8, 7}});
  const vector_size_t kTopLevelVectorSize = baseVector->size() * 2;
  auto constantVector =
      BaseVector::wrapInConstant(kTopLevelVectorSize, 0, baseVector);
  auto arrayVector = makeArrayVector({0, 3}, constantVector);

  testContainsConstantKey(arrayVector, {1, 2, 3, 4}, {true, true});
  testContainsConstantKey(arrayVector, {3, 4}, {false, false});
  testContainsConstantKey(arrayVector, {5, 6, 7, 8, 9}, {false, false});
}

TEST_F(ArrayContainsTest, dictionaryEncodingElements) {
  // ArrayVector with DictionaryVector<Array> elements.
  auto baseVector =
      makeArrayVector<int64_t>({{1, 2, 3, 4}, {3, 4, 5}, {10, 9, 8, 7}});
  auto baseVectorSize = baseVector->size();
  const vector_size_t kTopLevelVectorSize = baseVectorSize * 2;
  BufferPtr indices = allocateIndices(kTopLevelVectorSize, pool_.get());
  auto rawIndices = indices->asMutable<vector_size_t>();
  for (size_t i = 0; i < kTopLevelVectorSize; ++i) {
    rawIndices[i] = i % baseVectorSize;
  }
  auto dictVector = BaseVector::wrapInDictionary(
      nullptr, indices, kTopLevelVectorSize, baseVector);
  auto arrayVector = makeArrayVector({0, baseVectorSize + 1}, dictVector);
  // arrayVector is
  // {
  //    [[1, 2, 3, 4], [3, 4, 5], [10, 9, 8, 7], [1, 2, 3, 4]],
  //    [[3, 4, 5], [10, 9, 8, 7]]
  // }
  testContainsConstantKey(arrayVector, {1, 2, 3, 4}, {true, false});
  testContainsConstantKey(arrayVector, {3, 4, 5}, {true, true});
}

TEST_F(ArrayContainsTest, arrayCheckNulls) {
  facebook::velox::functions::prestosql::registerInternalFunctions();

  static const std::string kErrorMessage =
      "contains does not support arrays with elements that contain null";
  auto contains = [&](const std::string& search,
                      const auto& data,
                      bool internal = false) {
    const auto searchBase = makeArrayVectorFromJson<int32_t>({search});
    const auto searchConstant =
        BaseVector::wrapInConstant(data->size(), 0, searchBase);
    std::string call = internal ? "\"$internal$contains\"" : "contains";
    const auto result = evaluate(
        fmt::format("{}(c0, c1)", call), makeRowVector({data, searchConstant}));
    return result->template asFlatVector<bool>()->valueAt(0);
  };

  {
    // Null at the end of the array.
    const auto baseVector = makeArrayVectorFromJson<int32_t>({
        "[1, 1]",
        "[2, 2]",
        "[3, null]",
        "[4, 4]",
        "[5, 5]",
        "[6, 6]",
    });
    const auto data = makeArrayVector({0, 3}, baseVector);

    // No null equal.
    ASSERT_FALSE(contains("[7, null]", data));
    ASSERT_FALSE(contains("[7, null]", data, true));
    // Null equal, [3, null] vs [3, 3].
    VELOX_ASSERT_THROW(contains("[3, 3]", data), kErrorMessage);
    ASSERT_FALSE(contains("[3, 3]", data, true));
    // Null equal, [6, 6] vs [6, null].
    VELOX_ASSERT_THROW(contains("[6, null]", data), kErrorMessage);
    ASSERT_FALSE(contains("[6, null]", data, true));
    // [3, null] = [3, null] is true in $internal$contains.
    ASSERT_TRUE(contains("[3, null]", data, true));
  }

  {
    // Null at the beginning of the array.
    // data is [[null, 3]].
    const auto data =
        makeNullableNestedArrayVector<int32_t>({{{{{std::nullopt, 3}}}}});

    // [null] = [null, 3] is false.
    ASSERT_FALSE(contains("[null]", data));
    ASSERT_FALSE(contains("[null]", data, true));
    //  [null, 4] = [null, 3] is false.
    ASSERT_FALSE(contains("[null, 4]", data));
    ASSERT_FALSE(contains("[null, 4]", data, true));
    //  [null, 4] = [1, 1] is false.
    ASSERT_FALSE(contains("[1, 1]", data));
    ASSERT_FALSE(contains("[1, 1]", data, true));

    // [null, 3] = [null, 3] is indeterminate.
    VELOX_ASSERT_THROW(contains("[null, 3]", data), kErrorMessage);
    // [null, 3] = [null, null] is indeterminate.
    VELOX_ASSERT_THROW(contains("[null, null]", data), kErrorMessage);
    ASSERT_FALSE(contains("[null, null]", data, true));
    // [null, 3] = [null, 3] is true in $internal$contains.
    ASSERT_TRUE(contains("[null, 3]", data, true));
  }
}

TEST_F(ArrayContainsTest, rowCheckNulls) {
  facebook::velox::functions::prestosql::registerInternalFunctions();

  const auto baseVector = makeRowVector({
      makeNullableFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
      makeNullableFlatVector<int32_t>({1, 2, std::nullopt, 4, 5, 6}),
  });
  const auto data = makeArrayVector({0, 3}, baseVector);

  auto contains = [&](const std::vector<std::optional<int32_t>>& search,
                      bool internal = false) {
    const auto searchBase = makeRowVector({
        makeNullableFlatVector<int32_t>({search.at(0)}),
        makeNullableFlatVector<int32_t>({search.at(1)}),
    });
    const auto searchConstant =
        BaseVector::wrapInConstant(data->size(), 0, searchBase);
    std::string call = internal ? "\"$internal$contains\"" : "contains";
    const auto result = evaluate(
        fmt::format("{}(c0, c1)", call), makeRowVector({data, searchConstant}));
    return result->asFlatVector<bool>()->valueAt(0);
  };

  static const std::string kErrorMessage =
      "contains does not support arrays with elements that contain null";
  // No null equal.
  ASSERT_FALSE(contains({7, std::nullopt}));
  ASSERT_FALSE(contains({7, std::nullopt}, true));
  // Null equal, (3, null) vs (3, 3).
  VELOX_ASSERT_THROW(contains({3, 3}), kErrorMessage);
  ASSERT_FALSE(contains({3, 3}, true));
  // Null equal, (6, 6) vs (6, null).
  VELOX_ASSERT_THROW(contains({6, std::nullopt}), kErrorMessage);
  ASSERT_FALSE(contains({6, std::nullopt}, true));
  // (3, null) = (3, null) is true in $internal$contains.
  ASSERT_TRUE(contains({3, std::nullopt}, true));
}

TEST_F(ArrayContainsTest, floatNaNs) {
  testFloatingPointNaNs<float>();
  testFloatingPointNaNs<double>();
}
} // namespace
