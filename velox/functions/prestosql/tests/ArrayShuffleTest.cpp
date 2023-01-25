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

#include <folly/container/F14Set.h>
#include <algorithm>
#include <numeric>
#include <optional>
#include <random>
#include <sstream>

#include "velox/expression/VectorReaders.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

namespace {
template <typename T>
std::string printArray(const std::vector<std::optional<T>>& input) {
  std::stringstream out;
  out << "[";
  std::for_each(input.begin(), input.end(), [&out](const auto& e) {
    if (e.has_value()) {
      out << e.value();
    } else {
      out << "NULL";
    }
    out << ", ";
  });
  out << "]";
  return out.str();
}

template <typename T>
std::string printArray(
    const std::vector<std::optional<std::vector<std::optional<T>>>>& input) {
  std::stringstream out;
  out << "[";
  std::for_each(input.begin(), input.end(), [&out](const auto& e) {
    if (e.has_value()) {
      out << printArray(e.value());
    } else {
      out << "NULL";
    }
    out << ", ";
  });
  out << "]";
  return out.str();
}

template <typename T>
std::string printArray(const std::vector<T>& input) {
  std::stringstream out;
  out << "[";
  std::for_each(
      input.begin(), input.end(), [&out](const auto& e) { out << e << ", "; });
  out << "]";
  return out.str();
}
} // namespace

namespace {
class ArrayShuffleTest : public FunctionBaseTest {
 protected:
  template <typename T>
  void testShuffle(const VectorPtr& input) {
    DecodedVector decodedExpected(*input.get());
    exec::VectorReader<Array<T>> readerExpected(&decodedExpected);

    auto actualVector =
        evaluate<ArrayVector>("shuffle(C0)", makeRowVector({input}));
    // Validate each row from the actual decoded ArrayVector is a permutation
    // of the corresponding row from the expected decoded ArrayVector.
    DecodedVector decodedActual(*actualVector.get());
    exec::VectorReader<Array<T>> readerActual(&decodedActual);

    for (auto i = 0; i < input->size(); i++) {
      // Must materialize into std::vector, otherwise it will throw error
      // because ArrayView doesn't support std::is_permutation() yet.
      auto actualArray = readerActual[i].materialize();
      auto expectedArray = readerExpected[i].materialize();

      // Assert the two vectors contain the same elements (ignoring order).
      ASSERT_TRUE(std::is_permutation(
          actualArray.begin(), actualArray.end(), expectedArray.begin()))
          << "Actual array " << printArray(actualArray) << " at " << i
          << " must produce a permutation of expected array "
          << printArray(expectedArray);
    }
  }

  /// To test shuffle's randomness, we need the following steps:
  /// 1. Shuffle kNumShuffleTimes times with range-based input
  ///    {0, 1, ..., kNumDistinctValues-1}, and it should be at least
  ///    kUniquenessRate% times different in total.
  /// 2. Verify that results are a permutation of the input, i.e.
  ///    no value is missing and no extra value is present.
  /// NOTE:
  /// The combination of tests above is a straightforward way of
  /// verifying the shuffle's (1) randomness and (2) and correctness.
  /// However, it doesn't guarantee (3) uniformity.
  void testShuffleRandomness(VectorEncoding::Simple encoding) {
    // Generate a range-based array N {0, 1, ..., n-1} as the input for
    // test shuffle randomness purpose.
    // 1. For flat encoding: we generate an ArrayVector with all identical
    // elements (N) and its size equals to t.
    // 2. For constant encoding: we generate a ConstantVector with
    // its valueVector = N  and its size equals to t.
    // 3. For dict encoding: we generate a DictionaryVector with dictValues
    // being an ArrayVector (with all identical elements: N and size=2t) and
    // indices with size=2t (by duplicating the first half of N).
    const int32_t kNumShuffleTimes = 100;
    const int32_t kNumDistinctValues = 10;
    const double kUniquenessRate = .7;

    std::vector<int32_t> inputData(kNumDistinctValues);
    std::iota(inputData.begin(), inputData.end(), 0);

    VectorPtr inputVector;
    switch (encoding) {
      case VectorEncoding::Simple::FLAT: {
        std::vector<std::vector<int32_t>> flatData(kNumShuffleTimes);
        std::fill(flatData.begin(), flatData.end(), inputData);
        inputVector = makeArrayVector<int32_t>(flatData);
        break;
      }
      case VectorEncoding::Simple::CONSTANT: {
        auto valueVector = makeArrayVector<int32_t>({inputData});
        inputVector =
            BaseVector::wrapInConstant(kNumShuffleTimes, 0, valueVector);
        break;
      }
      case VectorEncoding::Simple::DICTIONARY: {
        vector_size_t dictSize = kNumShuffleTimes * 2;
        std::vector<std::vector<int32_t>> baseData(dictSize);
        std::fill(baseData.begin(), baseData.end(), inputData);
        auto dictValues = makeArrayVector<int32_t>(baseData);

        std::vector<int32_t> indicesData(dictSize);
        // Test duplicate indices.
        std::iota(
            indicesData.begin(), indicesData.begin() + kNumShuffleTimes, 0);
        std::iota(indicesData.begin() + kNumShuffleTimes, indicesData.end(), 0);
        auto indices = makeIndices(indicesData);
        inputVector = wrapInDictionary(indices, dictValues);
        break;
      }
      default:
        VELOX_FAIL(
            "Unsupported vector encoding: {}",
            VectorEncoding::mapSimpleToName(encoding));
    }

    DecodedVector decodedExpected(*inputVector.get());
    exec::VectorReader<Array<int32_t>> readerExpected(&decodedExpected);

    using materialize_t =
        typename exec::ArrayView<false, int32_t>::materialize_t;
    folly::F14FastSet<materialize_t> distinctValueSet;

    auto actualVector =
        evaluate<ArrayVector>("shuffle(C0)", makeRowVector({inputVector}));

    DecodedVector decodedActual(*actualVector.get());
    exec::VectorReader<Array<int32_t>> readerActual(&decodedActual);

    for (auto i = 0; i < actualVector->size(); i++) {
      auto actualArray = readerActual.readNullFree(i).materialize();
      auto expectedArray = readerExpected.readNullFree(i).materialize();

      ASSERT_TRUE(std::is_permutation(
          actualArray.begin(), actualArray.end(), expectedArray.begin()))
          << "Actual " << inputVector->encoding() << " array "
          << printArray(actualArray) << " at " << i
          << " must produce a permutation of expected array "
          << printArray(expectedArray);

      distinctValueSet.insert(actualArray);
    }

    // Shuffled arrays should be kUniquenessRate% different in total.
    const int32_t kThreshold = (int32_t)(kNumShuffleTimes * kUniquenessRate);
    auto numDistinctValues = distinctValueSet.size();
    ASSERT_TRUE(numDistinctValues >= kThreshold)
        << "Shuffle " << inputVector->encoding()
        << " array must yield >= " << kThreshold
        << " distinct values, but only got " << numDistinctValues;
  }
};
} // namespace

TEST_F(ArrayShuffleTest, bigintArrays) {
  auto input = makeNullableArrayVector<int64_t>(
      {{},
       {std::nullopt},
       {std::nullopt, std::nullopt},
       {-1, 0, 1},
       {std::nullopt, 0, 0},
       {std::numeric_limits<int64_t>::max(),
        std::numeric_limits<int64_t>::min(),
        0,
        1,
        2,
        3}});
  testShuffle<int64_t>(input);
}

TEST_F(ArrayShuffleTest, nestedArrays) {
  using innerArrayType = std::vector<std::optional<int64_t>>;
  using outerArrayType =
      std::vector<std::optional<std::vector<std::optional<int64_t>>>>;
  innerArrayType a{1, 2, 3, 4};
  innerArrayType b{5, 6};
  innerArrayType c{6, 7, 8};
  outerArrayType row1{{a}, {b}};
  outerArrayType row2{std::nullopt, std::nullopt, {a}, {b}, {c}};
  outerArrayType row3{{{}}};
  outerArrayType row4{{{std::nullopt}}};
  auto input =
      makeNullableNestedArrayVector<int64_t>({{row1}, {row2}, {row3}, {row4}});

  testShuffle<Array<int64_t>>(input);
}

TEST_F(ArrayShuffleTest, sortAndShuffle) {
  auto input = makeNullableArrayVector<int64_t>(
      {{-1, 0, std::nullopt, 1, std::nullopt},
       {4, 1, 5, 3, 2},
       {std::numeric_limits<int64_t>::max(),
        std::numeric_limits<int64_t>::min(),
        4,
        1,
        3,
        2,
        std::nullopt}});
  auto inputVector = makeRowVector({input});
  auto result1 = evaluate<ArrayVector>("array_sort(C0)", inputVector);
  auto result2 = evaluate<ArrayVector>("array_sort(shuffle(C0))", inputVector);

  assertEqualVectors(result1, result2);
}

TEST_F(ArrayShuffleTest, constantEncoding) {
  vector_size_t size = 100;
  // Test empty array, array with null element,
  // array with duplicate elements, and array with distinct values.
  auto valueVector = makeNullableArrayVector<int64_t>(
      {{}, {std::nullopt, 0}, {5, 5}, {1, 2, 3}});

  for (auto i = 0; i < valueVector->size(); i++) {
    auto input = BaseVector::wrapInConstant(size, i, valueVector);
    testShuffle<int64_t>(input);
  }
}

TEST_F(ArrayShuffleTest, dictEncoding) {
  // Test dict with repeated elements: {1,2,3} x 3, {4,5} x 2.
  auto base = makeNullableArrayVector<int64_t>(
      {{0},
       {1, 2, 3},
       {4, 5, std::nullopt},
       {1, 2, 3},
       {1, 2, 3},
       {4, 5, std::nullopt}});
  // Test repeated index elements and indices filtering (filter out element at
  // index 0).
  auto indices = makeIndices({3, 3, 4, 2, 2, 1, 1, 1});
  auto input = wrapInDictionary(indices, base);

  testShuffle<int64_t>(input);
}

TEST_F(ArrayShuffleTest, flatEncodingRandomness) {
  testShuffleRandomness(VectorEncoding::Simple::FLAT);
}

TEST_F(ArrayShuffleTest, constantEncodingRandomness) {
  testShuffleRandomness(VectorEncoding::Simple::CONSTANT);
}

TEST_F(ArrayShuffleTest, dictEncodingRandomness) {
  testShuffleRandomness(VectorEncoding::Simple::DICTIONARY);
}
