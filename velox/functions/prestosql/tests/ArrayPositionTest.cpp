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
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions::test;

namespace {

template <typename T>
using TwoDimVector = std::vector<std::vector<std::optional<T>>>;

class ArrayPositionTest : public FunctionBaseTest {
 protected:
  void evalExpr(
      const std::vector<VectorPtr>& input,
      const std::string& expression,
      const VectorPtr& expected) {
    auto result =
        evaluate<SimpleVector<int64_t>>(expression, makeRowVector(input));

    assertEqualVectors(expected, result);
  }

  template <typename T>
  void testPosition(
      const TwoDimVector<T>& array,
      const std::optional<T>& search,
      const std::vector<std::optional<int64_t>>& expected) {
    evalExpr(
        {makeNullableArrayVector<T>(array), makeConstant(search, array.size())},
        "array_position(c0, c1)",
        makeNullableFlatVector<int64_t>(expected));
  }

  template <typename T>
  void testPosition(
      const ArrayVectorPtr& arrayVector,
      const std::optional<T>& search,
      const std::vector<std::optional<int64_t>>& expected) {
    evalExpr(
        {arrayVector,
         makeConstant(
             search, arrayVector->size(), arrayVector->type()->childAt(0))},
        "array_position(c0, c1)",
        makeNullableFlatVector<int64_t>(expected));
  }

  void testPosition(
      const ArrayVectorPtr& arrayVector,
      const std::vector<int64_t>& search,
      const std::vector<std::optional<int64_t>>& expected,
      std::optional<int64_t> instanceOpt = std::nullopt) {
    auto constSearch =
        BaseVector::wrapInConstant(1, 0, makeArrayVector<int64_t>({search}));
    if (instanceOpt.has_value()) {
      auto instanceResult = evaluate(
          "array_position(c0, c1, c2)",
          makeRowVector(
              {arrayVector,
               constSearch,
               makeConstant(instanceOpt.value(), arrayVector->size())}));
      assertEqualVectors(
          makeNullableFlatVector<int64_t>(expected), instanceResult);
    } else {
      auto result = evaluate(
          "array_position(c0, c1)", makeRowVector({arrayVector, constSearch}));
      assertEqualVectors(makeNullableFlatVector<int64_t>(expected), result);
    }
  }

  template <typename T>
  void testPositionDictEncoding(
      const TwoDimVector<T>& array,
      const std::optional<T>& search,
      const std::vector<std::optional<int64_t>>& expected) {
    auto arrayVector = makeNullableArrayVector<T>(array);
    auto size = arrayVector->size();

    auto newSize = size * 2;
    auto indices = makeIndices(newSize, [](auto row) { return row / 2; });
    auto dictArrayVector = wrapInDictionary(indices, newSize, arrayVector);

    auto expectedVector = makeNullableFlatVector<int64_t>(expected);
    auto dictExpectedVector =
        wrapInDictionary(indices, newSize, expectedVector);

    FlatVectorPtr<T> searchVector;
    if (search.has_value()) {
      searchVector =
          makeFlatVector<T>(size, [&](auto row) { return search.value(); });
    } else {
      searchVector = makeFlatVector<T>(
          size,
          [&](auto row) { return search.value(); },
          [](auto row) { return true; });
    }

    // Encode search vector using a different instance of indices
    // to prevent peeling.
    auto searchIndices = makeIndices(newSize, [](auto row) { return row / 2; });
    auto dictSearchVector =
        wrapInDictionary(searchIndices, newSize, searchVector);

    evalExpr(
        {dictArrayVector, dictSearchVector},
        "array_position(c0, c1)",
        dictExpectedVector);
  }

  template <typename T>
  void testPositionDictEncoding(
      const ArrayVectorPtr& arrayVector,
      const std::optional<T>& search,
      const std::vector<std::optional<int64_t>>& expected) {
    testPositionDictEncoding(
        arrayVector,
        makeConstant(
            search, arrayVector->size(), arrayVector->type()->childAt(0)),
        expected);
  }

  void testPositionDictEncoding(
      const ArrayVectorPtr& arrayVector,
      const VectorPtr& searchVector,
      const std::vector<std::optional<int64_t>>& expected) {
    auto newSize = arrayVector->size() * 2;
    auto indices = makeIndices(newSize, [](auto row) { return row / 2; });
    auto dictArrayVector = wrapInDictionary(indices, newSize, arrayVector);

    auto expectedVector = makeNullableFlatVector<int64_t>(expected);
    auto dictExpectedVector =
        wrapInDictionary(indices, newSize, expectedVector);

    // Encode search vector using a different instance of indices
    // to prevent peeling.
    auto searchIndices = makeIndices(newSize, [](auto row) { return row / 2; });
    auto dictSearchVector =
        wrapInDictionary(searchIndices, newSize, searchVector);

    evalExpr(
        {dictArrayVector, dictSearchVector},
        "array_position(c0, c1)",
        dictExpectedVector);
  }

  template <typename T>
  void testPositionWithInstance(
      const TwoDimVector<T>& array,
      const std::optional<T>& search,
      const int64_t instance,
      const std::vector<std::optional<int64_t>>& expected) {
    evalExpr(
        {makeNullableArrayVector<T>(array),
         makeConstant(search, array.size()),
         makeConstant(instance, array.size())},
        "array_position(c0, c1, c2)",
        makeNullableFlatVector<int64_t>(expected));
  }

  template <typename T>
  void testPositionWithInstance(
      const ArrayVectorPtr& array,
      const std::optional<T>& search,
      const int64_t instance,
      const std::vector<std::optional<int64_t>>& expected) {
    evalExpr(
        {array,
         makeConstant(search, array->size(), array->type()->childAt(0)),
         makeConstant(instance, array->size())},
        "array_position(c0, c1, c2)",
        makeNullableFlatVector<int64_t>(expected));
  }

  template <typename T>
  void testPositionWithInstanceNoNulls(
      const std::vector<std::vector<T>>& array,
      const std::optional<T>& search,
      const int64_t instance,
      const std::vector<std::optional<int64_t>>& expected) {
    evalExpr(
        {makeArrayVector<T>(array),
         makeConstant(search, array.size()),
         makeConstant(instance, array.size())},
        "array_position(c0, c1, c2)",
        makeNullableFlatVector<int64_t>(expected));
  }

  template <typename T>
  void testPositionDictEncodingWithInstance(
      const TwoDimVector<T>& array,
      const std::optional<T>& search,
      const int64_t instance,
      const std::vector<std::optional<int64_t>>& expected) {
    testPositionDictEncodingWithInstance(
        makeNullableArrayVector<T>(array), search, instance, expected);
  }

  template <typename T>
  void testPositionDictEncodingWithInstance(
      const ArrayVectorPtr& arrayVector,
      const std::optional<T>& search,
      const int64_t instance,
      const std::vector<std::optional<int64_t>>& expected) {
    auto size = arrayVector->size();

    auto newSize = size * 2;
    auto indices = makeIndices(newSize, [](auto row) { return row / 2; });
    auto dictArrayVector = wrapInDictionary(indices, newSize, arrayVector);

    auto expectedVector = makeNullableFlatVector<int64_t>(expected);
    auto dictExpectedVector =
        wrapInDictionary(indices, newSize, expectedVector);

    FlatVectorPtr<T> searchVector;
    if (search.has_value()) {
      searchVector = makeFlatVector<T>(
          size,
          [&](auto row) { return search.value(); },
          nullptr,
          arrayVector->type()->childAt(0));
    } else {
      searchVector = makeFlatVector<T>(
          size,
          [&](auto row) { return search.value(); },
          [](auto row) { return true; },
          arrayVector->type()->childAt(0));
    }

    // Encode search and instance vectors using a different instance of
    // indices to prevent peeling.
    auto searchIndices = makeIndices(newSize, [](auto row) { return row / 2; });
    auto dictSearchVector =
        wrapInDictionary(searchIndices, newSize, searchVector);

    auto instanceVector =
        makeFlatVector<int64_t>(size, [=](auto row) { return instance; });
    auto dictInstanceVector =
        wrapInDictionary(searchIndices, newSize, instanceVector);

    evalExpr(
        {dictArrayVector, dictSearchVector, dictInstanceVector},
        "array_position(c0, c1, c2)",
        dictExpectedVector);
  }

  void testPositionComplexTypeDictionaryEncodingWithInstance(
      const ArrayVectorPtr& arrayVector,
      const VectorPtr& searchVector,
      const int64_t instance,
      const std::vector<std::optional<int64_t>>& expected) {
    auto newSize = arrayVector->size() * 2;
    auto indices = makeIndices(newSize, [](auto row) { return row / 2; });
    auto dictArrayVector = wrapInDictionary(indices, newSize, arrayVector);

    auto expectedVector = makeNullableFlatVector<int64_t>(expected);
    auto dictExpectedVector =
        wrapInDictionary(indices, newSize, expectedVector);

    // Encode search and instance vectors using a different instance of
    // indices to prevent peeling.
    auto searchIndices = makeIndices(newSize, [](auto row) { return row / 2; });
    auto dictSearchVector =
        wrapInDictionary(searchIndices, newSize, searchVector);

    auto instanceVector = makeFlatVector<int64_t>(
        arrayVector->size(), [=](auto row) { return instance; });
    auto dictInstanceVector =
        wrapInDictionary(searchIndices, newSize, instanceVector);

    evalExpr(
        {dictArrayVector, dictSearchVector, dictInstanceVector},
        "array_position(c0, c1, c2)",
        dictExpectedVector);
  }

  // Verify that all NaNs are treated as equal and are identifiable in the
  // input array.
  template <typename T>
  void testFloatingPointNaN() {
    static const T kNaN = std::numeric_limits<T>::quiet_NaN();
    static const T kSNaN = std::numeric_limits<T>::signaling_NaN();

    // Test NaN in a simple array.
    ArrayVectorPtr arrayVectorWithNulls = makeNullableArrayVector<T>({
        {1, std::nullopt, kNaN, 4, kNaN, 5, 6, kNaN, 7},
        {1, std::nullopt, kSNaN, 4, kNaN, 5, 6, kNaN, 7},
    });
    // This exercises the optimization for null free input.
    ArrayVectorPtr arrayVectorWithoutNulls = makeArrayVector<T>({
        {1, 3, kNaN, 4, kNaN, 5, 6, kNaN, 7},
        {1, 3, kSNaN, 4, kNaN, 5, 6, kNaN, 7},
    });

    for (auto& arrayVectorPtr :
         {arrayVectorWithNulls, arrayVectorWithoutNulls}) {
      for (const T& nan : {kNaN, kSNaN}) {
        testPosition<T>(arrayVectorPtr, nan, {3, 3});
        testPositionWithInstance<T>(arrayVectorPtr, nan, 1, {3, 3});
        testPositionWithInstance<T>(arrayVectorPtr, nan, 2, {5, 5});
        testPositionWithInstance<T>(arrayVectorPtr, nan, -1, {8, 8});
      }
    }

    // Test NaN withing a array of complex type.
    std::vector<std::vector<std::optional<std::tuple<double, std::string>>>>
        data = {
            {{{1, "red"}}, {{kNaN, "blue"}}, {{3, "green"}}},
            {{{kNaN, "blue"}}, std::nullopt, {{5, "green"}}},
            {},
            {std::nullopt},
            {{{1, "yellow"}},
             {{kNaN, "blue"}},
             {{4, "green"}},
             {{5, "purple"}}},
        };

    auto rowType = ROW({DOUBLE(), VARCHAR()});
    auto arrayVector = makeArrayOfRowVector(data, rowType);
    auto size = arrayVector->size();

    auto testPositionOfRow =
        [&](double n,
            const char* color,
            const std::vector<std::optional<int64_t>>& expected) {
          auto expectedVector = makeNullableFlatVector<int64_t>(expected);
          auto searchVector =
              makeConstantRow(rowType, variant::row({n, color}), size);

          evalExpr(
              {arrayVector,
               makeConstantRow(rowType, variant::row({n, color}), size)},
              "array_position(c0, c1)",
              makeNullableFlatVector<int64_t>(expected));
        };

    testPositionOfRow(1, "red", {1, 0, 0, 0, 0});
    testPositionOfRow(kNaN, "blue", {2, 1, 0, 0, 2});
    testPositionOfRow(kSNaN, "blue", {2, 1, 0, 0, 2});
  }
};

TEST_F(ArrayPositionTest, integer) {
  TwoDimVector<int64_t> arrayVector{
      {1, std::nullopt, 3, 4},
      {3, 4, 5},
      {},
      {std::nullopt},
      {5, 6, std::nullopt, 8, 9},
      {7},
      {3, 9, 8, 7}};

  testPosition<int64_t>(arrayVector, 3, {3, 1, 0, 0, 0, 0, 1});
  testPosition<int64_t>(arrayVector, 4, {4, 2, 0, 0, 0, 0, 0});
  testPosition<int64_t>(arrayVector, 5, {0, 3, 0, 0, 1, 0, 0});
  testPosition<int64_t>(arrayVector, 7, {0, 0, 0, 0, 0, 1, 4});
  testPosition<int64_t>(
      arrayVector,
      std::nullopt,
      {std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt});

  testPositionDictEncoding<int64_t>(arrayVector, 3, {3, 1, 0, 0, 0, 0, 1});
  testPositionDictEncoding<int64_t>(arrayVector, 4, {4, 2, 0, 0, 0, 0, 0});
  testPositionDictEncoding<int64_t>(arrayVector, 5, {0, 3, 0, 0, 1, 0, 0});
  testPositionDictEncoding<int64_t>(arrayVector, 7, {0, 0, 0, 0, 0, 1, 4});
  testPositionDictEncoding<int64_t>(
      arrayVector,
      std::nullopt,
      {std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt});
}

TEST_F(ArrayPositionTest, varchar) {
  using S = StringView;

  TwoDimVector<S> arrayVector{
      {S{"red"}, S{"blue"}},
      {std::nullopt, S{"yellow"}, S{"orange"}},
      {},
      {std::nullopt},
      {S{"red"}, S{"purple"}, S{"green"}},
  };

  testPosition<S>(arrayVector, S{"red"}, {1, 0, 0, 0, 1});
  testPosition<S>(arrayVector, S{"blue"}, {2, 0, 0, 0, 0});
  testPosition<S>(arrayVector, S{"yellow"}, {0, 2, 0, 0, 0});
  testPosition<S>(arrayVector, S{"green"}, {0, 0, 0, 0, 3});
  testPosition<S>(arrayVector, S{"crimson red"}, {0, 0, 0, 0, 0});
  testPosition<S>(
      arrayVector,
      std::nullopt,
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt});

  testPositionDictEncoding<S>(arrayVector, S{"red"}, {1, 0, 0, 0, 1});
  testPositionDictEncoding<S>(arrayVector, S{"blue"}, {2, 0, 0, 0, 0});
  testPositionDictEncoding<S>(arrayVector, S{"yellow"}, {0, 2, 0, 0, 0});
  testPositionDictEncoding<S>(arrayVector, S{"green"}, {0, 0, 0, 0, 3});
  testPositionDictEncoding<S>(arrayVector, S{"crimson red"}, {0, 0, 0, 0, 0});
  testPositionDictEncoding<S>(
      arrayVector,
      std::nullopt,
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt});
}

TEST_F(ArrayPositionTest, boolean) {
  TwoDimVector<bool> arrayVector{
      {true, false},
      {true},
      {false},
      {},
      {std::nullopt},
      {true, std::nullopt, true},
      {false, false, false},
  };

  testPosition<bool>(arrayVector, true, {1, 1, 0, 0, 0, 1, 0});
  testPosition<bool>(arrayVector, false, {2, 0, 1, 0, 0, 0, 1});
  testPosition<bool>(
      arrayVector,
      std::nullopt,
      {std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt});

  testPositionDictEncoding<bool>(arrayVector, true, {1, 1, 0, 0, 0, 1, 0});
  testPositionDictEncoding<bool>(arrayVector, false, {2, 0, 1, 0, 0, 0, 1});
  testPositionDictEncoding<bool>(
      arrayVector,
      std::nullopt,
      {std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt});
}

TEST_F(ArrayPositionTest, row) {
  std::vector<std::vector<std::optional<std::tuple<int32_t, std::string>>>>
      data = {
          {{{1, "red"}}, {{2, "blue"}}, {{3, "green"}}},
          {{{2, "blue"}}, std::nullopt, {{5, "green"}}},
          {},
          {std::nullopt},
          {{{1, "yellow"}}, {{2, "blue"}}, {{4, "green"}}, {{5, "purple"}}},
      };

  auto rowType = ROW({INTEGER(), VARCHAR()});
  auto arrayVector = makeArrayOfRowVector(data, rowType);
  auto size = arrayVector->size();

  auto testPositionOfRow =
      [&](int32_t n,
          const char* color,
          const std::vector<std::optional<int64_t>>& expected) {
        auto expectedVector = makeNullableFlatVector<int64_t>(expected);
        auto searchVector =
            makeConstantRow(rowType, variant::row({n, color}), size);

        evalExpr(
            {arrayVector,
             makeConstantRow(rowType, variant::row({n, color}), size)},
            "array_position(c0, c1)",
            makeNullableFlatVector<int64_t>(expected));
      };

  testPositionOfRow(1, "red", {1, 0, 0, 0, 0});
  testPositionOfRow(2, "blue", {2, 1, 0, 0, 2});
  testPositionOfRow(4, "green", {0, 0, 0, 0, 3});
  testPositionOfRow(5, "green", {0, 3, 0, 0, 0});
  testPositionOfRow(1, "purple", {0, 0, 0, 0, 0});

  auto testPositionOfRowDictionaryEncoding =
      [&](int32_t n,
          const char* color,
          const std::vector<std::optional<int64_t>>& expected) {
        auto nVector =
            makeFlatVector<int32_t>(size, [&](auto row) { return n; });
        auto colorVector = makeFlatVector<StringView>(
            size, [&](auto row) { return StringView{color}; });
        auto searchVector = makeRowVector({nVector, colorVector});

        testPositionDictEncoding(arrayVector, searchVector, expected);
      };

  testPositionOfRowDictionaryEncoding(1, "red", {1, 0, 0, 0, 0});
  testPositionOfRowDictionaryEncoding(2, "blue", {2, 1, 0, 0, 2});
  testPositionOfRowDictionaryEncoding(4, "green", {0, 0, 0, 0, 3});
  testPositionOfRowDictionaryEncoding(5, "green", {0, 3, 0, 0, 0});
  testPositionOfRowDictionaryEncoding(1, "purple", {0, 0, 0, 0, 0});
}

TEST_F(ArrayPositionTest, array) {
  std::vector<std::optional<int64_t>> a{1, 2, 3};
  std::vector<std::optional<int64_t>> b{4, 5};
  std::vector<std::optional<int64_t>> c{6, 7, 8};
  std::vector<std::optional<int64_t>> d{1, 2};
  std::vector<std::optional<int64_t>> e{4, 3};
  auto arrayVector = makeNullableNestedArrayVector<int64_t>(
      {{{{a}, {b}, {c}}},
       {{{d}, {e}}},
       {{{d}, {a}, {b}}},
       {{{c}, {e}}},
       {{{{}}}},
       {{{std::vector<std::optional<int64_t>>{std::nullopt}}}}});

  auto testPositionOfArray =
      [&](const TwoDimVector<int64_t>& search,
          const std::vector<std::optional<int64_t>>& expected) {
        evalExpr(
            {arrayVector, makeNullableArrayVector<int64_t>(search)},
            "array_position(c0, c1)",
            makeNullableFlatVector<int64_t>(expected));
      };

  testPositionOfArray({a, a, a, a, a, a}, {1, 0, 2, 0, 0, 0});
  testPositionOfArray({b, e, a, e, e, e}, {2, 2, 2, 2, 0, 0});
  testPositionOfArray(
      {b, {std::nullopt}, a, {}, {}, {std::nullopt}}, {2, 0, 2, 0, 1, 1});

  auto testPositionOfArrayDictionaryEncoding =
      [&](const TwoDimVector<int64_t>& search,
          const std::vector<std::optional<int64_t>>& expected) {
        auto searchVector = makeNullableArrayVector<int64_t>(search);
        testPositionDictEncoding(arrayVector, searchVector, expected);
      };

  testPositionOfArrayDictionaryEncoding({a, a, a, a, a, a}, {1, 0, 2, 0, 0, 0});
  testPositionOfArrayDictionaryEncoding({b, e, a, e, e, e}, {2, 2, 2, 2, 0, 0});
  testPositionOfArrayDictionaryEncoding(
      {b, {std::nullopt}, a, {}, {}, {std::nullopt}}, {2, 0, 2, 0, 1, 1});
}

TEST_F(ArrayPositionTest, map) {
  using S = StringView;
  using P = std::pair<int64_t, std::optional<S>>;

  std::vector<P> a{P{1, S{"red"}}, P{2, S{"blue"}}, P{3, S{"green"}}};
  std::vector<P> b{P{1, S{"yellow"}}, P{2, S{"orange"}}};
  std::vector<P> c{P{1, S{"red"}}, P{2, S{"yellow"}}, P{3, S{"purple"}}};
  std::vector<P> d{P{0, std::nullopt}};
  std::vector<std::vector<std::vector<P>>> data{{a, b, c}, {b}, {c, a}};
  auto arrayVector = makeArrayOfMapVector<int64_t, S>(data);

  auto testPositionOfMap =
      [&](const std::vector<std::vector<P>>& search,
          const std::vector<std::optional<int64_t>>& expected) {
        evalExpr(
            {arrayVector, makeMapVector<int64_t, S>(search)},
            "array_position(c0, c1)",
            makeNullableFlatVector<int64_t>(expected));
      };

  testPositionOfMap({a, a, a}, {1, 0, 2});
  testPositionOfMap({b, b, a}, {2, 1, 2});
  testPositionOfMap({d, d, d}, {0, 0, 0});

  auto testPositionOfMapDictionaryEncoding =
      [&](const std::vector<std::vector<P>>& search,
          const std::vector<std::optional<int64_t>>& expected) {
        auto searchVector = makeMapVector<int64_t, S>(search);
        testPositionDictEncoding(arrayVector, searchVector, expected);
      };

  testPositionOfMapDictionaryEncoding({a, a, a}, {1, 0, 2});
  testPositionOfMapDictionaryEncoding({b, b, a}, {2, 1, 2});
  testPositionOfMapDictionaryEncoding({d, d, d}, {0, 0, 0});
}

TEST_F(ArrayPositionTest, integerWithInstance) {
  TwoDimVector<int64_t> arrayVector{
      {1, 2, std::nullopt, 4},
      {3, 4, 4},
      {},
      {std::nullopt},
      {5, 2, 4, 2, 9},
      {7},
      {10, 4, 8, 4}};

  testPositionWithInstance<int64_t>(arrayVector, 4, 2, {0, 3, 0, 0, 0, 0, 4});
  testPositionWithInstance<int64_t>(arrayVector, 4, -1, {4, 3, 0, 0, 3, 0, 4});
  testPositionWithInstance<int64_t>(arrayVector, 2, 2, {0, 0, 0, 0, 4, 0, 0});
  testPositionWithInstance<int64_t>(arrayVector, 2, 3, {0, 0, 0, 0, 0, 0, 0});
  testPositionWithInstance<int64_t>(arrayVector, 2, -1, {2, 0, 0, 0, 4, 0, 0});
  testPositionWithInstance<int64_t>(
      arrayVector,
      std::nullopt,
      1,
      {std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt});

  testPositionDictEncodingWithInstance<int64_t>(
      arrayVector, 4, 2, {0, 3, 0, 0, 0, 0, 4});
  testPositionDictEncodingWithInstance<int64_t>(
      arrayVector, 4, -1, {4, 3, 0, 0, 3, 0, 4});
  testPositionDictEncodingWithInstance<int64_t>(
      arrayVector, 2, 2, {0, 0, 0, 0, 4, 0, 0});
  testPositionDictEncodingWithInstance<int64_t>(
      arrayVector, 2, 3, {0, 0, 0, 0, 0, 0, 0});
  testPositionDictEncodingWithInstance<int64_t>(
      arrayVector, 2, -1, {2, 0, 0, 0, 4, 0, 0});
  testPositionDictEncodingWithInstance<int64_t>(
      arrayVector,
      std::nullopt,
      1,
      {std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt});
}

TEST_F(ArrayPositionTest, varcharWithInstance) {
  using S = StringView;

  TwoDimVector<S> arrayVector{
      {S{"red"}, S{"blue"}, S{"red"}},
      {std::nullopt, S{"yellow"}, S{"yellow"}},
      {},
      {std::nullopt},
      {S{"red"}, S{"green"}, S{"purple"}, S{"green"}},
  };

  testPositionWithInstance<S>(arrayVector, S{"red"}, 2, {3, 0, 0, 0, 0});
  testPositionWithInstance<S>(arrayVector, S{"red"}, -1, {3, 0, 0, 0, 1});
  testPositionWithInstance<S>(arrayVector, S{"yellow"}, 2, {0, 3, 0, 0, 0});
  testPositionWithInstance<S>(arrayVector, S{"green"}, -2, {0, 0, 0, 0, 2});
  testPositionWithInstance<S>(arrayVector, S{"green"}, 3, {0, 0, 0, 0, 0});
  testPositionWithInstance<S>(
      arrayVector, S{"crimson red"}, 1, {0, 0, 0, 0, 0});
  testPositionWithInstance<S>(
      arrayVector,
      std::nullopt,
      1,
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt});

  testPositionDictEncodingWithInstance<S>(
      arrayVector, S{"red"}, 2, {3, 0, 0, 0, 0});
  testPositionDictEncodingWithInstance<S>(
      arrayVector, S{"red"}, -1, {3, 0, 0, 0, 1});
  testPositionDictEncodingWithInstance<S>(
      arrayVector, S{"yellow"}, 2, {0, 3, 0, 0, 0});
  testPositionDictEncodingWithInstance<S>(
      arrayVector, S{"green"}, -2, {0, 0, 0, 0, 2});
  testPositionDictEncodingWithInstance<S>(
      arrayVector, S{"green"}, 3, {0, 0, 0, 0, 0});
  testPositionDictEncodingWithInstance<S>(
      arrayVector, S{"crimson red"}, 1, {0, 0, 0, 0, 0});
  testPositionDictEncodingWithInstance<S>(
      arrayVector,
      std::nullopt,
      1,
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt});
}

TEST_F(ArrayPositionTest, booleanWithInstance) {
  TwoDimVector<bool> arrayVector{
      {true, false},
      {true},
      {false},
      {},
      {std::nullopt},
      {true, std::nullopt, true},
      {false, false, false},
  };

  testPositionWithInstance<bool>(arrayVector, true, 2, {0, 0, 0, 0, 0, 3, 0});
  testPositionWithInstance<bool>(arrayVector, true, -1, {1, 1, 0, 0, 0, 3, 0});
  testPositionWithInstance<bool>(arrayVector, false, 3, {0, 0, 0, 0, 0, 0, 3});
  testPositionWithInstance<bool>(arrayVector, false, 4, {0, 0, 0, 0, 0, 0, 0});
  testPositionWithInstance<bool>(arrayVector, false, -2, {0, 0, 0, 0, 0, 0, 2});
  testPositionWithInstance<bool>(
      arrayVector,
      std::nullopt,
      1,
      {std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt});

  testPositionDictEncodingWithInstance<bool>(
      arrayVector, true, 2, {0, 0, 0, 0, 0, 3, 0});
  testPositionDictEncodingWithInstance<bool>(
      arrayVector, true, -1, {1, 1, 0, 0, 0, 3, 0});
  testPositionDictEncodingWithInstance<bool>(
      arrayVector, false, 3, {0, 0, 0, 0, 0, 0, 3});
  testPositionDictEncodingWithInstance<bool>(
      arrayVector, false, 4, {0, 0, 0, 0, 0, 0, 0});
  testPositionDictEncodingWithInstance<bool>(
      arrayVector, false, -2, {0, 0, 0, 0, 0, 0, 2});
  testPositionDictEncodingWithInstance<bool>(
      arrayVector,
      std::nullopt,
      1,
      {std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt});
}

TEST_F(ArrayPositionTest, primitiveWithInstanceFastPath) {
  std::vector<std::vector<int64_t>> int64s = {
      {1, 2, 3, 4},
      {3, 4, 4},
      {},
      {6},
      {5, 2, 4, 2, 9},
      {7},
      {10, 4, 8, 4},
  };
  testPositionWithInstanceNoNulls<int64_t>(int64s, 4, 2, {0, 3, 0, 0, 0, 0, 4});
  std::vector<std::vector<bool>> bools = {
      {true, false},
      {true},
      {false},
      {},
      {false},
      {true, false, true},
      {false, false, false},
  };
  testPositionWithInstanceNoNulls<bool>(bools, true, 2, {0, 0, 0, 0, 0, 3, 0});
}

TEST_F(ArrayPositionTest, rowWithInstance) {
  std::vector<std::vector<std::optional<std::tuple<int32_t, std::string>>>>
      data = {
          {{{1, "red"}}, {{2, "blue"}}, {{3, "green"}}, {{1, "red"}}},
          {{{2, "blue"}}, std::nullopt, {{5, "green"}}, {{2, "blue"}}},
          {},
          {std::nullopt},
          {
              {{1, "yellow"}},
              {{2, "blue"}},
              {{4, "green"}},
              {{2, "blue"}},
              {{2, "blue"}},
              {{5, "purple"}},
          },
      };

  auto rowType = ROW({INTEGER(), VARCHAR()});
  auto arrayVector = makeArrayOfRowVector(data, rowType);
  auto size = arrayVector->size();

  auto testPositionOfRow =
      [&](int32_t n,
          const char* color,
          const int64_t instance,
          const std::vector<std::optional<int64_t>>& expected) {
        auto expectedVector = makeNullableFlatVector<int64_t>(expected);
        auto searchVector =
            makeConstantRow(rowType, variant::row({n, color}), size);
        auto instanceVector = makeConstant(instance, size);

        evalExpr(
            {arrayVector,
             makeConstantRow(rowType, variant::row({n, color}), size),
             instanceVector},
            "array_position(c0, c1, c2)",
            makeNullableFlatVector<int64_t>(expected));
      };

  testPositionOfRow(1, "red", 1, {1, 0, 0, 0, 0});
  testPositionOfRow(1, "red", -1, {4, 0, 0, 0, 0});
  testPositionOfRow(2, "blue", 3, {0, 0, 0, 0, 5});
  testPositionOfRow(2, "blue", -2, {0, 1, 0, 0, 4});

  auto testPositionOfRowDictionaryEncoding =
      [&](int32_t n,
          const char* color,
          const int64_t instance,
          const std::vector<std::optional<int64_t>>& expected) {
        auto nVector =
            makeFlatVector<int32_t>(size, [&](auto row) { return n; });
        auto colorVector = makeFlatVector<StringView>(
            size, [&](auto row) { return StringView{color}; });
        auto searchVector = makeRowVector({nVector, colorVector});
        testPositionComplexTypeDictionaryEncodingWithInstance(
            arrayVector, searchVector, instance, expected);
      };

  testPositionOfRowDictionaryEncoding(1, "red", 1, {1, 0, 0, 0, 0});
  testPositionOfRowDictionaryEncoding(1, "red", -1, {4, 0, 0, 0, 0});
  testPositionOfRowDictionaryEncoding(2, "blue", 3, {0, 0, 0, 0, 5});
  testPositionOfRowDictionaryEncoding(2, "blue", -2, {0, 1, 0, 0, 4});
}

TEST_F(ArrayPositionTest, arrayWithInstance) {
  std::vector<std::optional<int64_t>> a{1, 2, 3};
  std::vector<std::optional<int64_t>> b{4, 5};
  std::vector<std::optional<int64_t>> c{6, 7, 8};
  std::vector<std::optional<int64_t>> d{1, 2};
  std::vector<std::optional<int64_t>> e{4, 3};
  auto arrayVector = makeNullableNestedArrayVector<int64_t>(
      {{{{a}, {b}, {a}}},
       {{{d}, {e}}},
       {{{d}, {a}, {a}}},
       {{{c}, {c}}},
       {{{{}}}},
       {{{std::vector<std::optional<int64_t>>{std::nullopt}}}}});

  auto testPositionOfArray =
      [&](const TwoDimVector<int64_t>& search,
          const int64_t instance,
          const std::vector<std::optional<int64_t>>& expected) {
        evalExpr(
            {arrayVector,
             makeNullableArrayVector<int64_t>(search),
             makeConstant(instance, arrayVector->size())},
            "array_position(c0, c1, c2)",
            makeNullableFlatVector<int64_t>(expected));
      };

  testPositionOfArray({a, a, a, a, a, a}, 1, {1, 0, 2, 0, 0, 0});
  testPositionOfArray({a, a, a, a, a, a}, -1, {3, 0, 3, 0, 0, 0});
  testPositionOfArray({b, e, a, e, e, e}, -1, {2, 2, 3, 0, 0, 0});
  testPositionOfArray(
      {b, {std::nullopt}, a, {}, {}, {std::nullopt}}, 1, {2, 0, 2, 0, 1, 1});

  auto testPositionOfArrayDictionaryEncoding =
      [&](const TwoDimVector<int64_t>& search,
          const int64_t instance,
          const std::vector<std::optional<int64_t>>& expected) {
        auto searchVector = makeNullableArrayVector<int64_t>(search);
        testPositionComplexTypeDictionaryEncodingWithInstance(
            arrayVector, searchVector, instance, expected);
      };

  testPositionOfArrayDictionaryEncoding(
      {a, a, a, a, a, a}, 1, {1, 0, 2, 0, 0, 0});
  testPositionOfArrayDictionaryEncoding(
      {a, a, a, a, a, a}, -1, {3, 0, 3, 0, 0, 0});
  testPositionOfArrayDictionaryEncoding(
      {b, e, a, e, e, e}, -1, {2, 2, 3, 0, 0, 0});
  testPositionOfArrayDictionaryEncoding(
      {b, {std::nullopt}, a, {}, {}, {std::nullopt}}, 1, {2, 0, 2, 0, 1, 1});
}

TEST_F(ArrayPositionTest, mapWithInstance) {
  using S = StringView;
  using P = std::pair<int64_t, std::optional<S>>;

  std::vector<P> a{P{1, S{"red"}}, P{2, S{"blue"}}, P{3, S{"green"}}};
  std::vector<P> b{P{1, S{"yellow"}}, P{2, S{"orange"}}};
  std::vector<P> c{P{1, S{"red"}}, P{2, S{"yellow"}}, P{3, S{"purple"}}};
  std::vector<P> d{P{0, std::nullopt}};
  std::vector<std::vector<std::vector<P>>> data{{a, b, b}, {b, c}, {c, a, c}};
  auto arrayVector = makeArrayOfMapVector<int64_t, S>(data);

  auto testPositionOfMap =
      [&](const std::vector<std::vector<P>>& search,
          const int64_t instance,
          const std::vector<std::optional<int64_t>>& expected) {
        evalExpr(
            {arrayVector,
             makeMapVector<int64_t, S>(search),
             makeConstant(instance, arrayVector->size())},
            "array_position(c0, c1, c2)",
            makeNullableFlatVector<int64_t>(expected));
      };

  testPositionOfMap({a, a, a}, 1, {1, 0, 2});
  testPositionOfMap({a, a, a}, 2, {0, 0, 0});
  testPositionOfMap({b, b, c}, -1, {3, 1, 3});
  testPositionOfMap({d, d, d}, 1, {0, 0, 0});

  auto testPositionOfMapDictionaryEncoding =
      [&](const std::vector<std::vector<P>>& search,
          const int64_t instance,
          const std::vector<std::optional<int64_t>>& expected) {
        auto searchVector = makeMapVector<int64_t, S>(search);
        testPositionComplexTypeDictionaryEncodingWithInstance(
            arrayVector, searchVector, instance, expected);
      };

  testPositionOfMapDictionaryEncoding({a, a, a}, 1, {1, 0, 2});
  testPositionOfMapDictionaryEncoding({a, a, a}, 2, {0, 0, 0});
  testPositionOfMapDictionaryEncoding({b, b, c}, -1, {3, 1, 3});
  testPositionOfMapDictionaryEncoding({d, d, d}, 1, {0, 0, 0});
}

TEST_F(ArrayPositionTest, invalidInstance) {
  auto input = makeRowVector({
      makeArrayVector<int64_t>({{1, 2, 3}, {4, 5}}),
      makeFlatVector<int64_t>({1, 4}),
      makeFlatVector<int32_t>({0, 1}),
  });

  // Flat element and instance vectors.
  VELOX_ASSERT_THROW(
      evaluate("array_position(c0, c1, c2)", input),
      "array_position cannot take a 0-valued instance argument");

  auto result = evaluate("try(array_position(c0, c1, c2))", input);
  assertEqualVectors(
      makeNullableFlatVector<int64_t>({std::nullopt, 1}), result);

  // Constant element and instance vectors.
  VELOX_ASSERT_THROW(
      evaluate("array_position(c0, 1, 0)", input),
      "array_position cannot take a 0-valued instance argument");

  result = evaluate("try(array_position(c0, 1, 0))", input);
  assertEqualVectors(makeNullConstant(TypeKind::BIGINT, 2), result);
}

TEST_F(ArrayPositionTest, constantEncodingElements) {
  // ArrayVector with ConstantVector<Array> elements.
  auto baseVector = makeArrayVector<int64_t>(
      {{1, 2, 3, 4},
       {3, 4, 5},
       {},
       {5, 6, 7, 8, 9},
       {1, 2, 3, 4},
       {10, 9, 8, 7}});
  const vector_size_t kTopLevelVectorSize = baseVector->size() * 2;
  auto constantVector =
      BaseVector::wrapInConstant(kTopLevelVectorSize, 0, baseVector);
  auto arrayVector = makeArrayVector({0, 2, 7}, constantVector);

  testPosition(arrayVector, {1, 2, 3, 4}, {1, 1, 1});
  testPosition(arrayVector, {3, 4, 5}, {0, 0, 0});
  testPosition(arrayVector, {1, 2, 3, 4}, {1, 1, 1}, 1);
  testPosition(arrayVector, {1, 2, 3, 4}, {2, 5, kTopLevelVectorSize - 7}, -1);
}

TEST_F(ArrayPositionTest, dictionaryEncodingElements) {
  // ArrayVector with DictionaryVector<Array> elements.
  auto baseVector = makeArrayVector<int64_t>(
      {{1, 2, 3, 4}, {3, 4, 5}, {1, 2, 3, 4}, {10, 9, 8, 7}});
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
  //    [[1, 2, 3, 4], [3, 4, 5], [1, 2, 3, 4], [10, 9, 8, 7], [1, 2, 3, 4]],
  //    [[3, 4, 5], [1, 2, 3, 4], [10, 9, 8, 7]]
  // }
  testPosition(arrayVector, {1, 2, 3, 4}, {1, 2}, std::nullopt);
  testPosition(arrayVector, {3, 4, 5}, {2, 1}, std::nullopt);
  testPosition(arrayVector, {1, 2, 3, 4}, {1, 2}, 1);
  testPosition(arrayVector, {1, 2, 3, 4}, {5, 2}, -1);
}

TEST_F(ArrayPositionTest, floatNaN) {
  testFloatingPointNaN<float>();
  testFloatingPointNaN<double>();
}

TEST_F(ArrayPositionTest, timestampWithTimeZone) {
  auto arrayVector = makeArrayVector(
      {0, 4, 7, 7, 8, 13, 14},
      makeNullableFlatVector<int64_t>(
          {pack(1, 1),
           std::nullopt,
           pack(3, 2),
           pack(4, 3),
           pack(3, 4),
           pack(4, 5),
           pack(5, 6),
           std::nullopt,
           pack(5, 7),
           pack(6, 8),
           std::nullopt,
           pack(8, 9),
           pack(9, 10),
           pack(7, 11),
           pack(3, 12),
           pack(9, 13),
           pack(8, 14),
           pack(7, 15)},
          TIMESTAMP_WITH_TIME_ZONE()));

  testPosition<int64_t>(arrayVector, pack(3, 2), {3, 1, 0, 0, 0, 0, 1});
  testPosition<int64_t>(arrayVector, pack(4, 3), {4, 2, 0, 0, 0, 0, 0});
  testPosition<int64_t>(arrayVector, pack(5, 6), {0, 3, 0, 0, 1, 0, 0});
  testPosition<int64_t>(arrayVector, pack(7, 11), {0, 0, 0, 0, 0, 1, 4});
  testPosition<int64_t>(
      arrayVector,
      std::nullopt,
      {std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt});

  testPositionDictEncoding<int64_t>(
      arrayVector, pack(3, 2), {3, 1, 0, 0, 0, 0, 1});
  testPositionDictEncoding<int64_t>(
      arrayVector, pack(4, 3), {4, 2, 0, 0, 0, 0, 0});
  testPositionDictEncoding<int64_t>(
      arrayVector, pack(5, 6), {0, 3, 0, 0, 1, 0, 0});
  testPositionDictEncoding<int64_t>(
      arrayVector, pack(7, 11), {0, 0, 0, 0, 0, 1, 4});
  testPositionDictEncoding<int64_t>(
      arrayVector,
      std::nullopt,
      {std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt});

  // Test wrapped in a complex vector.
  arrayVector = makeArrayVector(
      {0, 4, 7, 7, 8, 13, 14},
      makeRowVector({makeNullableFlatVector<int64_t>(
          {pack(1, 1),
           std::nullopt,
           pack(3, 2),
           pack(4, 3),
           pack(3, 4),
           pack(4, 5),
           pack(5, 6),
           std::nullopt,
           pack(5, 7),
           pack(6, 8),
           std::nullopt,
           pack(8, 9),
           pack(9, 10),
           pack(7, 11),
           pack(3, 12),
           pack(9, 13),
           pack(8, 14),
           pack(7, 15)},
          TIMESTAMP_WITH_TIME_ZONE())}));

  auto testPositionOfRow = [&](int64_t n,
                               const std::vector<int64_t>& expected) {
    const auto expectedVector = makeFlatVector<int64_t>(expected);
    const auto searchVector = BaseVector::wrapInConstant(
        arrayVector->size(),
        0,
        makeRowVector({makeFlatVector(
            std::vector<int64_t>{n}, TIMESTAMP_WITH_TIME_ZONE())}));

    evalExpr(
        {arrayVector, searchVector}, "array_position(c0, c1)", expectedVector);
  };

  testPositionOfRow(pack(3, 2), {3, 1, 0, 0, 0, 0, 1});
  testPositionOfRow(pack(4, 3), {4, 2, 0, 0, 0, 0, 0});
  testPositionOfRow(pack(5, 6), {0, 3, 0, 0, 1, 0, 0});
  testPositionOfRow(pack(7, 11), {0, 0, 0, 0, 0, 1, 4});
}

TEST_F(ArrayPositionTest, timestampWithTimeZoneWithInstance) {
  auto arrayVector = makeArrayVector(
      {0, 4, 7, 7, 8, 13, 14},
      makeNullableFlatVector<int64_t>(
          {pack(1, 1),
           pack(2, 2),
           std::nullopt,
           pack(4, 3),
           pack(3, 4),
           pack(4, 5),
           pack(4, 6),
           std::nullopt,
           pack(5, 7),
           pack(2, 8),
           pack(4, 9),
           pack(2, 10),
           pack(9, 11),
           pack(7, 12),
           pack(10, 13),
           pack(4, 14),
           pack(8, 15),
           pack(4, 16)},
          TIMESTAMP_WITH_TIME_ZONE()));

  testPositionWithInstance<int64_t>(
      arrayVector, pack(4, 3), 2, {0, 3, 0, 0, 0, 0, 4});
  testPositionWithInstance<int64_t>(
      arrayVector, pack(4, 3), -1, {4, 3, 0, 0, 3, 0, 4});
  testPositionWithInstance<int64_t>(
      arrayVector, pack(2, 2), 2, {0, 0, 0, 0, 4, 0, 0});
  testPositionWithInstance<int64_t>(
      arrayVector, pack(2, 2), 3, {0, 0, 0, 0, 0, 0, 0});
  testPositionWithInstance<int64_t>(
      arrayVector, pack(2, 2), -1, {2, 0, 0, 0, 4, 0, 0});
  testPositionWithInstance<int64_t>(
      arrayVector,
      std::nullopt,
      1,
      {std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt});

  testPositionDictEncodingWithInstance<int64_t>(
      arrayVector, pack(4, 3), 2, {0, 3, 0, 0, 0, 0, 4});
  testPositionDictEncodingWithInstance<int64_t>(
      arrayVector, pack(4, 3), -1, {4, 3, 0, 0, 3, 0, 4});
  testPositionDictEncodingWithInstance<int64_t>(
      arrayVector, pack(2, 2), 2, {0, 0, 0, 0, 4, 0, 0});
  testPositionDictEncodingWithInstance<int64_t>(
      arrayVector, pack(2, 2), 3, {0, 0, 0, 0, 0, 0, 0});
  testPositionDictEncodingWithInstance<int64_t>(
      arrayVector, pack(2, 2), -1, {2, 0, 0, 0, 4, 0, 0});
  testPositionDictEncodingWithInstance<int64_t>(
      arrayVector,
      std::nullopt,
      1,
      {std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt});

  // Test wrapped in a complex vector.
  arrayVector = makeArrayVector(
      {0, 4, 7, 7, 8, 13, 14},
      makeRowVector({makeNullableFlatVector<int64_t>(
          {pack(1, 1),
           pack(2, 2),
           std::nullopt,
           pack(4, 3),
           pack(3, 4),
           pack(4, 5),
           pack(4, 6),
           std::nullopt,
           pack(5, 7),
           pack(2, 8),
           pack(4, 9),
           pack(2, 10),
           pack(9, 11),
           pack(7, 12),
           pack(10, 13),
           pack(4, 14),
           pack(8, 15),
           pack(4, 16)},
          TIMESTAMP_WITH_TIME_ZONE())}));

  auto testPositionOfRowWithInstance =
      [&](int64_t n,
          const int32_t instance,
          const std::vector<int64_t>& expected) {
        const auto expectedVector = makeFlatVector<int64_t>(expected);
        const auto searchVector = BaseVector::wrapInConstant(
            arrayVector->size(),
            0,
            makeRowVector({makeFlatVector(
                std::vector<int64_t>{n}, TIMESTAMP_WITH_TIME_ZONE())}));
        const auto instanceVector = makeConstant(instance, arrayVector->size());

        evalExpr(
            {arrayVector, searchVector, instanceVector},
            "array_position(c0, c1, c2)",
            expectedVector);
      };

  testPositionOfRowWithInstance(pack(4, 3), 2, {0, 3, 0, 0, 0, 0, 4});
  testPositionOfRowWithInstance(pack(4, 3), -1, {4, 3, 0, 0, 3, 0, 4});
  testPositionOfRowWithInstance(pack(2, 2), 2, {0, 0, 0, 0, 4, 0, 0});
  testPositionOfRowWithInstance(pack(2, 2), 3, {0, 0, 0, 0, 0, 0, 0});
  testPositionOfRowWithInstance(pack(2, 2), -1, {2, 0, 0, 0, 4, 0, 0});
}

} // namespace
