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
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "velox/common/base/VeloxException.h"
#include "velox/expression/VectorUdfTypeSystem.h"
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

namespace {

using namespace facebook::velox;
using namespace facebook::velox::test;

DecodedVector* decode(DecodedVector& decoder, const BaseVector& vector) {
  SelectivityVector rows(vector.size());
  decoder.decode(vector, rows);
  return &decoder;
}

template <bool returnsOptionalValues>
class ArrayViewTest : public functions::test::FunctionBaseTest {
  template <typename T>
  exec::ArrayView<returnsOptionalValues, T> read(
      exec::VectorReader<Array<T>>& reader,
      size_t offset) {
    if constexpr (returnsOptionalValues) {
      return reader[offset];
    } else {
      return reader.readNullFree(offset);
    }
  }

 protected:
  // What value to use for NULL in the test data.  If the view type is
  // not returnsOptionalValues, we use 0 as an arbitrary value.
  std::optional<int64_t> nullValue =
      returnsOptionalValues ? std::nullopt : std::make_optional(0);
  std::vector<std::vector<std::optional<int64_t>>> arrayDataBigInt = {
      {},
      {nullValue},
      {nullValue, 1},
      {nullValue, nullValue, nullValue},
      {0, 1, 2, 4},
      {99, 98},
      {101, nullValue},
      {10001, 12345676, nullValue},
  };

  void intArrayTest(
      std::function<void(
          int,
          int,
          typename exec::ArrayView<returnsOptionalValues, int64_t>::Element)>
          testItem) {
    auto arrayVector = makeNullableArrayVector(arrayDataBigInt);
    DecodedVector decoded;
    exec::VectorReader<Array<int64_t>> reader(
        decode(decoded, *arrayVector.get()));

    for (auto i = 0; i < arrayVector->size(); i++) {
      auto arrayView = read(reader, i);
      auto j = 0;

      // Test iterate loop.
      for (auto item : arrayView) {
        testItem(i, j, item);
        j++;
      }
      ASSERT_EQ(j, arrayDataBigInt[i].size());

      // Test iterate loop 2.
      auto it = arrayView.begin();
      j = 0;
      while (it != arrayView.end()) {
        testItem(i, j, *it);
        j++;
        ++it;
      }
      ASSERT_EQ(j, arrayDataBigInt[i].size());

      // Test index based loop.
      for (j = 0; j < arrayView.size(); j++) {
        testItem(i, j, arrayView[j]);
      }
      ASSERT_EQ(j, arrayDataBigInt[i].size());

      // Test loop iterator with <.
      j = 0;
      for (it = arrayView.begin(); it < arrayView.end(); it++) {
        testItem(i, j, *it);
        j++;
      }
      ASSERT_EQ(j, arrayDataBigInt[i].size());

      // Test loop iteration in reverse with post decrement
      j = arrayDataBigInt[i].size() - 1;
      for (it = arrayView.end() - 1; it >= arrayView.begin(); it--) {
        testItem(i, j, *it);
        j--;
      }
      // This is unintuitive but because we decrement after accessing each
      // element, since j starts as one less than the size of the array, it
      // should finish at -1.
      ASSERT_EQ(j, -1);

      // Test iterate with pre decrement
      it = arrayView.end() - 1;
      j = arrayDataBigInt[i].size() - 1;
      while (it >= arrayView.begin()) {
        testItem(i, j, *it);
        j--;
        --it;
      }
      // This is unintuitive but because we decrement after accessing each
      // element, since j starts as one less than the size of the array, it
      // should finish at -1.
      ASSERT_EQ(j, -1);
    }
  }

  void encodedTest() {
    std::vector<std::vector<std::optional<int32_t>>> intArray = {
        {1},
        {2, 3},
    };
    VectorPtr arrayVector = makeNullableArrayVector(intArray);
    // Wrap in dictionary.
    auto vectorSize = arrayVector->size();
    BufferPtr indices =
        AlignedBuffer::allocate<vector_size_t>(vectorSize, pool_.get());
    auto rawIndices = indices->asMutable<vector_size_t>();
    // Assign indices such that array is reversed.
    for (size_t i = 0; i < vectorSize; ++i) {
      rawIndices[i] = vectorSize - 1 - i;
    }
    arrayVector = BaseVector::wrapInDictionary(
        BufferPtr(nullptr), indices, vectorSize, arrayVector);

    DecodedVector decoded;
    exec::VectorReader<Array<int32_t>> reader(decode(decoded, *arrayVector));

    ASSERT_EQ(read(reader, 0).size(), 2);
    ASSERT_EQ(read(reader, 1).size(), 1);
  }

  void iteratorDifferenceTest() {
    std::vector<std::vector<std::optional<int32_t>>> intArray{
        {1}, {2, 3}, {4, 5, 6}, {7, 8, 9, 10}, {11, 12, 13, 14, 15}};
    auto arrayVector = makeNullableArrayVector(intArray);
    DecodedVector decoded;
    exec::VectorReader<Array<int32_t>> reader(
        decode(decoded, *arrayVector.get()));

    for (auto i = 0; i < arrayVector->size(); i++) {
      auto arrayView = read(reader, i);
      auto it = arrayView.begin();

      for (int j = 0; j < arrayView.size(); j++) {
        auto it2 = arrayView.begin();
        for (int k = 0; k <= j; k++) {
          ASSERT_EQ(it - it2, j - k);
          ASSERT_EQ(it2 - it, k - j);
          it2++;
        }
        it++;
      }
    }
  }

  void iteratorAdditionTest() {
    std::vector<std::vector<std::optional<int32_t>>> intArray{
        {1}, {2, 3}, {4, 5, 6}, {7, 8, 9, 10}, {11, 12, 13, 14, 15}};
    auto arrayVector = makeNullableArrayVector(intArray);
    DecodedVector decoded;
    exec::VectorReader<Array<int32_t>> reader(
        decode(decoded, *arrayVector.get()));

    for (auto i = 0; i < arrayVector->size(); i++) {
      auto arrayView = read(reader, i);
      auto it = arrayView.begin();

      for (int j = 0; j < arrayView.size(); j++) {
        auto it2 = arrayView.begin();
        for (int k = 0; k < arrayView.size(); k++) {
          ASSERT_EQ(it, it2 + (j - k));
          ASSERT_EQ(it, (j - k) + it2);
          auto it3 = it2;
          it3 += j - k;
          ASSERT_EQ(it, it3);
          it2++;
        }
        it++;
      }
    }
  }

  void iteratorSubtractionTest() {
    std::vector<std::vector<std::optional<int32_t>>> intArray{
        {1}, {2, 3}, {4, 5, 6}, {7, 8, 9, 10}, {11, 12, 13, 14, 15}};
    auto arrayVector = makeNullableArrayVector(intArray);
    DecodedVector decoded;
    exec::VectorReader<Array<int32_t>> reader(
        decode(decoded, *arrayVector.get()));

    for (auto i = 0; i < arrayVector->size(); i++) {
      auto arrayView = read(reader, i);
      auto it = arrayView.begin();

      for (int j = 0; j < arrayView.size(); j++) {
        auto it2 = arrayView.begin();
        for (int k = 0; k < arrayView.size(); k++) {
          ASSERT_EQ(it, it2 - (k - j));
          auto it3 = it2;
          it3 -= k - j;
          ASSERT_EQ(it, it3);
          it2++;
        }
        it++;
      }
    }
  }

  void iteratorSubscriptTest() {
    std::vector<std::vector<std::optional<int32_t>>> intArray{
        {1}, {2, 3}, {4, 5, 6}, {7, 8, 9, 10}, {11, 12, 13, 14, 15}};
    auto arrayVector = makeNullableArrayVector(intArray);
    DecodedVector decoded;
    exec::VectorReader<Array<int32_t>> reader(
        decode(decoded, *arrayVector.get()));

    for (auto i = 0; i < arrayVector->size(); i++) {
      auto arrayView = read(reader, i);
      auto it = arrayView.begin();

      for (int j = 0; j < arrayView.size(); j++) {
        auto it2 = arrayView.begin();
        for (int k = 0; k < arrayView.size(); k++) {
          ASSERT_EQ(*it, it2[j - k]);
          it2++;
        }
        it++;
      }
    }
  }
};

class NullableArrayViewTest : public ArrayViewTest<true> {};

class NullFreeArrayViewTest : public ArrayViewTest<false> {};

TEST_F(NullableArrayViewTest, intArray) {
  auto testItem = [&](int i, int j, auto item) {
    // Test has_value.
    ASSERT_EQ(arrayDataBigInt[i][j].has_value(), item.has_value());

    // Test bool implicit cast.
    ASSERT_EQ(arrayDataBigInt[i][j].has_value(), static_cast<bool>(item));

    if (arrayDataBigInt[i][j].has_value()) {
      // Test * operator.
      ASSERT_EQ(arrayDataBigInt[i][j].value(), *item) << i << j;

      // Test value().
      ASSERT_EQ(arrayDataBigInt[i][j].value(), item.value());
    }
    ASSERT_TRUE(item == arrayDataBigInt[i][j]);
  };

  intArrayTest(testItem);
}

TEST_F(NullableArrayViewTest, encoded) {
  encodedTest();
}

TEST_F(NullableArrayViewTest, notNullContainer) {
  auto arrayVector = makeNullableArrayVector(arrayDataBigInt);
  DecodedVector decoded;
  exec::VectorReader<Array<int64_t>> reader(
      decode(decoded, *arrayVector.get()));

  for (auto i = 0; i < arrayVector->size(); i++) {
    auto arrayView = reader[i];
    int j = 0;
    for (auto value : arrayView.skipNulls()) {
      while (j < arrayDataBigInt[i].size() &&
             arrayDataBigInt[i][j] == std::nullopt) {
        j++;
      }
      ASSERT_EQ(value, arrayDataBigInt[i][j].value());
      j++;
    }
  }
}

TEST_F(NullableArrayViewTest, arrowOperatorForOptional) {
  std::vector<std::vector<StringView>> data = {
      {""_sv, "a"_sv, "aa"_sv, "aaa"_sv},
      {""_sv, "b"_sv, "bb"_sv},
      {""_sv, "c"_sv},
  };
  auto arrayVector = makeArrayVector(data);
  DecodedVector decoded;
  exec::VectorReader<Array<Varchar>> reader(
      decode(decoded, *arrayVector.get()));

  auto arrayView = reader[0];
  auto totalSize = 0;
  for (const auto& string : arrayView) {
    totalSize += string->size();
  }
  ASSERT_EQ(totalSize, 6);
}

TEST_F(NullableArrayViewTest, itIncrementSafe) {
  auto arrayVector = makeNullableArrayVector(arrayDataBigInt);
  DecodedVector decoded;
  exec::VectorReader<Array<int64_t>> reader(
      decode(decoded, *arrayVector.get()));

  {
    // Test that it++ does not invalidate references for ArrayView::Iterator
    // obtained by *.
    auto arrayViewIt = reader[4].begin();
    const auto& optionalValRef = *arrayViewIt;
    auto valBefore = optionalValRef;
    arrayViewIt++;
    auto valAfter = optionalValRef;
    EXPECT_EQ(valBefore, valAfter);
    EXPECT_EQ(valBefore, std::optional<int64_t>{0});
  }

  {
    // Test that it++ does not invalidate references for ArrayView::Iterator
    // obtained by throw ->.
    auto arrayViewIt = reader[4].begin();
    const auto& valueRef = arrayViewIt->value();
    auto valBefore = valueRef;
    arrayViewIt++;
    auto valAfter = valueRef;
    EXPECT_EQ(valBefore, valAfter);
    EXPECT_EQ(valBefore, 0);
  };

  {
    // Test that it++ does not invalidate references for
    // ArrayView::SkipNullContainer::Iterator.
    auto skipNullsIt = reader[4].skipNulls().begin();
    const auto& notNullRef = *skipNullsIt;
    auto valBefore = notNullRef;
    skipNullsIt++;
    auto valAfter = notNullRef;
    EXPECT_EQ(valBefore, valAfter);
    EXPECT_EQ(valBefore, 0);
  }
}

// Function that takes an array of arrays as input.
template <typename T>
struct NestedArrayF {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      int64_t& out,
      const arg_type<Array<Array<int64_t>>>& input) {
    out = 0;
    for (const auto& inner : input) {
      if (inner) {
        for (const auto& v : inner.value()) {
          if (v) {
            out += v.value();
          }
        }
      }
    }

    // Test operator -> in ArrayView.
    int64_t outTest = 0;
    for (auto it = input.begin(); it < input.end(); it++) {
      for (auto it2 = it->value().begin(); it2 < it->value().end(); it2++) {
        outTest += it2->value();
      }
    }

    // Test operator -> in OptionalValueAccessor.
    int64_t outTest2 = 0;
    for (auto it = input.begin(); it < input.end(); it++) {
      auto optionalOfArrayView = *it;
      for (auto it2 = optionalOfArrayView->begin();
           it2 < optionalOfArrayView->end();
           it2++) {
        outTest2 += it2->value();
      }
    }

    EXPECT_EQ(outTest, out);
    EXPECT_EQ(outTest2, out);

    return true;
  }
};

TEST_F(NullableArrayViewTest, nestedArray) {
  registerFunction<NestedArrayF, int64_t, Array<Array<int64_t>>>(
      {"nested_array_func"});
  std::vector<std::vector<int64_t>> arrayData = {
      {0, 1, 2, 4},
      {99, 98},
      {101, 42},
      {10001, 12345676},
  };

  size_t rows = arrayData.size();
  auto arrayVector = makeArrayVector(arrayData);
  auto result = evaluate<FlatVector<int64_t>>(
      "nested_array_func(array_constructor(c0, c0))",
      makeRowVector({arrayVector}));

  auto expected = makeFlatVector<int64_t>(rows, [&](auto row) {
    return 2 * std::accumulate(arrayData[row].begin(), arrayData[row].end(), 0);
  });

  assertEqualVectors(expected, result);
}

TEST_F(NullableArrayViewTest, iteratorDifference) {
  iteratorDifferenceTest();
}

TEST_F(NullableArrayViewTest, iteratorAddition) {
  iteratorAdditionTest();
}

TEST_F(NullableArrayViewTest, iteratorSubtraction) {
  iteratorSubtractionTest();
}

TEST_F(NullableArrayViewTest, iteratorSubscript) {
  iteratorSubscriptTest();
}

TEST_F(NullFreeArrayViewTest, intArray) {
  auto testItem = [&](int i, int j, auto item) {
    ASSERT_TRUE(item == arrayDataBigInt[i][j]);
  };

  intArrayTest(testItem);
}

TEST_F(NullFreeArrayViewTest, encoded) {
  encodedTest();
}

TEST_F(NullFreeArrayViewTest, materialize) {
  auto result = evaluate(
      "array_constructor(1, 2, 3, 4, 5)",
      makeRowVector({makeFlatVector<int64_t>(1)}));

  DecodedVector decoded;
  exec::VectorReader<Array<int64_t>> reader(decode(decoded, *result.get()));

  ASSERT_EQ(
      reader.readNullFree(0).materialize(),
      std::vector<int64_t>({1, 2, 3, 4, 5}));
}

TEST_F(NullableArrayViewTest, materialize) {
  auto result = evaluate(
      "array_constructor(1, 2, NULL, 4, NULL)",
      makeRowVector({makeFlatVector<int64_t>(1)}));

  DecodedVector decoded;
  exec::VectorReader<Array<int64_t>> reader(decode(decoded, *result.get()));

  ASSERT_EQ(
      reader[0].materialize(),
      std::vector<std::optional<int64_t>>(
          {1, 2, std::nullopt, 4, std::nullopt}));
}

TEST_F(NullableArrayViewTest, materializeNested) {
  auto result = evaluate(
      "array_constructor(array_constructor(1), array_constructor(1, NULL), NULL)",
      makeRowVector({makeFlatVector<int64_t>(1)}));

  DecodedVector decoded;
  exec::VectorReader<Array<Array<int64_t>>> reader(
      decode(decoded, *result.get()));
  ASSERT_EQ(
      reader[0].materialize(),
      std::vector<std::optional<std::vector<std::optional<int64_t>>>>(
          {{{{1}}, {{1, std::nullopt}}, std::nullopt}}));
}

// Function that takes an array of arrays as input.
template <typename T>
struct MakeOpaqueFunc {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(std::shared_ptr<int64_t>& out) {
    out = std::make_shared<int64_t>(1);
  }
};

TEST_F(NullableArrayViewTest, materializeArrayWithOpaque) {
  registerFunction<MakeOpaqueFunc, std::shared_ptr<int64_t>>({"make_opaque"});

  auto result = evaluate(
      "array_constructor(make_opaque(), null)",
      makeRowVector({makeFlatVector<int64_t>(1)}));

  DecodedVector decoded;
  exec::VectorReader<Array<std::shared_ptr<int64_t>>> reader(
      decode(decoded, *result.get()));

  std::vector<std::optional<int64_t>> array = reader[0].materialize();
  ASSERT_EQ(array.size(), 2);
  ASSERT_EQ(array[0].value(), 1);

  ASSERT_FALSE(array[1].has_value());
}

TEST_F(NullFreeArrayViewTest, iteratorDifference) {
  iteratorDifferenceTest();
}

TEST_F(NullFreeArrayViewTest, iteratorAddition) {
  iteratorAdditionTest();
}

TEST_F(NullFreeArrayViewTest, iteratorSubtraction) {
  iteratorSubtractionTest();
}

TEST_F(NullFreeArrayViewTest, iteratorSubscript) {
  iteratorSubscriptTest();
}
} // namespace
