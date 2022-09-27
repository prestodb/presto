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
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

class TransformTest : public functions::test::FunctionBaseTest {};

TEST_F(TransformTest, basic) {
  vector_size_t size = 1'000;
  auto input = makeRowVector(
      {makeArrayVector<int64_t>(size, modN(5), modN(7), nullEvery(11))});

  auto result = evaluate<ArrayVector>("transform(c0, x -> x + 5)", input);

  auto expectedResult = makeArrayVector<int64_t>(
      size,
      modN(5),
      [](vector_size_t row) { return row % 7 + 5; },
      nullEvery(11));
  assertEqualVectors(expectedResult, result);
}

TEST_F(TransformTest, differentResultType) {
  vector_size_t size = 1'000;
  auto input = makeRowVector(
      {makeArrayVector<int64_t>(size, modN(5), modN(7), nullEvery(11))});

  auto result = evaluate<ArrayVector>("transform(c0, x -> (x % 2 = 0))", input);

  auto expectedResult = makeArrayVector<bool>(
      size,
      modN(5),
      [](auto row) { return (row % 7) % 2 == 0; },
      nullEvery(11));
  assertEqualVectors(expectedResult, result);
}

// Test different lambdas applied to different rows
TEST_F(TransformTest, conditional) {
  vector_size_t size = 1'000;

  // make 2 columns: the array to transform and a boolean that decided which
  // lambda to use
  auto isNullAt = nullEvery(11);
  auto inputArray = makeArrayVector<int64_t>(size, modN(5), modN(7), isNullAt);
  auto condition =
      makeFlatVector<bool>(size, [](auto row) { return row % 3 == 1; });
  auto input = makeRowVector({condition, inputArray});

  auto result = evaluate<ArrayVector>(
      "transform(c1, if (c0, x -> x + 5, x -> x - 3))", input);

  // make 2 expected vectors: one for rows where condition is true and another
  // for rows where condition is false
  auto expectedPlus5 = makeArrayVector<int64_t>(
      size, modN(5), [](auto row) { return row % 7 + 5; }, isNullAt);
  auto expectedMinus3 = makeArrayVector<int64_t>(
      size, modN(5), [](auto row) { return row % 7 - 3; }, isNullAt);
  ASSERT_EQ(size, result->size());
  for (auto i = 0; i < size; i++) {
    if (i % 3 == 1) {
      ASSERT_TRUE(expectedPlus5->equalValueAt(result.get(), i, i))
          << "at " << i << ": " << expectedPlus5->toString(i) << " vs. "
          << result->toString(i);

    } else {
      ASSERT_TRUE(expectedMinus3->equalValueAt(result.get(), i, i))
          << "at " << i << ": " << expectedMinus3->toString(i) << " vs. "
          << result->toString(i);
    }
  }
}

TEST_F(TransformTest, dictionaryWithUniqueValues) {
  vector_size_t size = 1'000;
  auto inputArray =
      makeArrayVector<int32_t>(size, modN(5), modN(7), nullEvery(11));

  auto indices = makeIndicesInReverse(size);
  auto input = makeRowVector(
      {makeFlatVector<int16_t>(size, [](auto /* row */) { return 5; }),
       wrapInDictionary(indices, size, inputArray)});

  auto result = evaluate<BaseVector>("transform(c1, x -> x + c0)", input);

  auto expectedResult = wrapInDictionary(
      indices,
      size,
      makeArrayVector<int32_t>(
          size, modN(5), [](auto row) { return row % 7 + 5; }, nullEvery(11)));
  assertEqualVectors(expectedResult, result);
}

TEST_F(TransformTest, dictionaryWithDuplicates) {
  vector_size_t size = 1'000;

  // make an array vector where each row repeats a few times
  auto baseArray =
      makeArrayVector<int32_t>(size / 2, modN(5), modN(7), nullEvery(11));

  BufferPtr indices = allocateIndices(size, execCtx_.pool());
  auto rawIndices = indices->asMutable<vector_size_t>();
  for (auto i = 0; i < size; ++i) {
    rawIndices[i] = i / 2;
  }

  auto array = wrapInDictionary(indices, size, baseArray);

  // make a capture with unique values
  auto capture = makeFlatVector<int32_t>(size, [](auto row) { return row; });

  auto input = makeRowVector({capture, array});

  auto result = evaluate<BaseVector>("transform(c1, x -> x + c0)", input);

  auto flatArray = flatten(array);
  input = makeRowVector({capture, flatArray});
  auto expectedResult =
      evaluate<BaseVector>("transform(c1, x -> x + c0)", input);

  assertEqualVectors(expectedResult, result);
}
