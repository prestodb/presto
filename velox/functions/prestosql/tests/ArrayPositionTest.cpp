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

  void testPositionComplexTypeDictionaryEncoding(
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
  void testPositionDictEncodingWithInstance(
      const TwoDimVector<T>& array,
      const std::optional<T>& search,
      const int64_t instance,
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

    // Encode search and instance vectors using a different instance of indices
    // to prevent peeling.
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

    // Encode search and instance vectors using a different instance of indices
    // to prevent peeling.
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
  std::vector<std::vector<variant>> data{
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
          variant(TypeKind::ROW), // null
      },
      {
          variant::row({1, "yellow"}),
          variant::row({2, "blue"}),
          variant::row({4, "green"}),
          variant::row({5, "purple"}),
      },
  };

  auto rowType = ROW({INTEGER(), VARCHAR()});
  auto arrayVector = makeArrayOfRowVector(rowType, data);
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

        testPositionComplexTypeDictionaryEncoding(
            arrayVector, searchVector, expected);
      };

  testPositionOfRowDictionaryEncoding(1, "red", {1, 0, 0, 0, 0});
  testPositionOfRowDictionaryEncoding(2, "blue", {2, 1, 0, 0, 2});
  testPositionOfRowDictionaryEncoding(4, "green", {0, 0, 0, 0, 3});
  testPositionOfRowDictionaryEncoding(5, "green", {0, 3, 0, 0, 0});
  testPositionOfRowDictionaryEncoding(1, "purple", {0, 0, 0, 0, 0});
}

TEST_F(ArrayPositionTest, array) {
  auto O = [](const std::vector<std::optional<int64_t>>& data) {
    return std::make_optional(data);
  };

  std::vector<std::optional<int64_t>> a{1, 2, 3};
  std::vector<std::optional<int64_t>> b{4, 5};
  std::vector<std::optional<int64_t>> c{6, 7, 8};
  std::vector<std::optional<int64_t>> d{1, 2};
  std::vector<std::optional<int64_t>> e{4, 3};
  auto arrayVector = makeNestedArrayVector<int64_t>(
      {{O(a), O(b), O(c)},
       {O(d), O(e)},
       {O(d), O(a), O(b)},
       {O(c), O(e)},
       {O({})},
       {O({std::nullopt})}});

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
        testPositionComplexTypeDictionaryEncoding(
            arrayVector, searchVector, expected);
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
        testPositionComplexTypeDictionaryEncoding(
            arrayVector, searchVector, expected);
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

TEST_F(ArrayPositionTest, rowWithInstance) {
  std::vector<std::vector<variant>> data{
      {variant::row({1, "red"}),
       variant::row({2, "blue"}),
       variant::row({3, "green"}),
       variant::row({1, "red"})},
      {variant::row({2, "blue"}),
       variant(TypeKind::ROW), // null
       variant::row({5, "green"}),
       variant::row({2, "blue"})},
      {},
      {
          variant(TypeKind::ROW), // null
      },
      {
          variant::row({1, "yellow"}),
          variant::row({2, "blue"}),
          variant::row({4, "green"}),
          variant::row({2, "blue"}),
          variant::row({2, "blue"}),
          variant::row({5, "purple"}),
      },
  };

  auto rowType = ROW({INTEGER(), VARCHAR()});
  auto arrayVector = makeArrayOfRowVector(rowType, data);
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
  auto O = [](const std::vector<std::optional<int64_t>>& data) {
    return std::make_optional(data);
  };

  std::vector<std::optional<int64_t>> a{1, 2, 3};
  std::vector<std::optional<int64_t>> b{4, 5};
  std::vector<std::optional<int64_t>> c{6, 7, 8};
  std::vector<std::optional<int64_t>> d{1, 2};
  std::vector<std::optional<int64_t>> e{4, 3};
  auto arrayVector = makeNestedArrayVector<int64_t>(
      {{O(a), O(b), O(a)},
       {O(d), O(e)},
       {O(d), O(a), O(a)},
       {O(c), O(c)},
       {O({})},
       {O({std::nullopt})}});

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
} // namespace
