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

#include "velox/expression/VectorReaders.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

using namespace facebook::velox::test;

class ArrayShuffleTest : public SparkFunctionBaseTest {
 protected:
  void testShuffle(
      const VectorPtr& input,
      const VectorPtr& expected,
      int64_t seed,
      int32_t partitionId = 0) {
    setSparkPartitionId(partitionId);
    assertEqualVectors(
        evaluate(fmt::format("shuffle(c0, {})", seed), makeRowVector({input})),
        expected);
  }

  template <typename T>
  void testShuffle(
      const VectorPtr& input,
      const std::vector<std::string>& expected,
      int64_t seed,
      int32_t partitionId = 0) {
    testShuffle(input, makeArrayVectorFromJson<T>(expected), seed, partitionId);
  }

  template <typename T>
  void testShuffle(
      const std::string& input,
      const std::string& expected,
      int64_t seed,
      int32_t partitionId = 0) {
    testShuffle<T>(
        makeArrayVectorFromJson<T>({input}), {expected}, seed, partitionId);
  }
};

TEST_F(ArrayShuffleTest, basic) {
  testShuffle<int64_t>("[1, 2, 3, 4, 5]", "[3, 5, 4, 1, 2]", 0);
  testShuffle<std::string>(
      R"(["a", "b", "c", "d"])", R"(["a", "c", "b", "d"])", 0);

  // Assert results are different with different seeds / partition ids.
  testShuffle<int64_t>("[1, 2, 3, 4, 5]", "[2, 1, 3, 4, 5]", 0, 1);
  testShuffle<int64_t>("[1, 2, 3, 4, 5]", "[4, 1, 3, 5, 2]", 2, 0);
}

TEST_F(ArrayShuffleTest, nestedArrays) {
  auto input = makeNestedArrayVectorFromJson<int64_t>(
      {"[[1, 2, 3, 4], [5, 6]]",
       "[null, null, [1, 2, 3, 4], [5, 6], [6, 7, 8]]",
       "[[]]",
       "[[null]]"});
  auto result = makeNestedArrayVectorFromJson<int64_t>(
      {"[[1, 2, 3, 4], [5, 6]]",
       "[[1, 2, 3, 4], null, [5, 6], null, [6, 7, 8]]",
       "[[]]",
       "[[null]]"});
  testShuffle(input, result, 0);
}

TEST_F(ArrayShuffleTest, constantEncoding) {
  vector_size_t size = 3;
  // Test empty array, array with null element,
  // array with duplicate elements, and array with distinct values.
  auto valueVector = makeArrayVectorFromJson<int64_t>(
      {"[]", "[null, 0]", "[5, 5]", "[1, 2, 3]"});
  std::vector<std::vector<std::string>> result = {
      {"[]", "[]", "[]"},
      {"[null, 0]", "[null, 0]", "[null, 0]"},
      {"[5, 5]", "[5, 5]", "[5, 5]"},
      {"[3, 2, 1]", "[3, 2, 1]", "[1, 3, 2]"}};
  for (auto i = 0; i < valueVector->size(); i++) {
    auto input = BaseVector::wrapInConstant(size, i, valueVector);
    testShuffle<int64_t>(input, result[i], 0);
  }
}

TEST_F(ArrayShuffleTest, dictEncoding) {
  // Test dict with repeated elements: {1,2,3} x 3, {4,5} x 2.
  auto base = makeArrayVectorFromJson<int64_t>(
      {"[0]",
       "[1, 2 ,3]",
       "[4, 5, null]",
       "[1, 2, 3]",
       "[1, 2, 3]",
       "[4, 5, null]"});
  std::vector<std::string> result = {
      "[3, 2, 1]",
      "[3, 2 ,1]",
      "[1, 3, 2]",
      "[4, 5, null]",
      "[null, 5, 4]",
      "[1, 2, 3]",
      "[3, 2, 1]",
      "[1, 2, 3]"};
  // Test repeated index elements and indices filtering (filter out element at
  // index 0).
  auto indices = makeIndices({3, 3, 4, 2, 2, 1, 1, 1});
  auto input = wrapInDictionary(indices, base);
  testShuffle<int64_t>(input, result, 0);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
