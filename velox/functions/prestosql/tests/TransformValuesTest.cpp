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

class TransformValuesTest : public functions::test::FunctionBaseTest {};

TEST_F(TransformValuesTest, basic) {
  vector_size_t size = 1'000;
  auto input = makeRowVector({
      makeMapVector<int32_t, int64_t>(
          size,
          [](auto row) { return row % 5; },
          [](auto row) { return row % 7; },
          [](auto row) { return row % 11; },
          nullEvery(13)),
  });
  registerLambda(
      "plus5",
      rowType("unused_k", INTEGER(), "v", BIGINT()),
      input->type(),
      "v + 5");

  auto result =
      evaluate<MapVector>("transform_values(c0, function('plus5'))", input);

  auto expectedResult = makeMapVector<int32_t, int64_t>(
      size,
      [](auto row) { return row % 5; },
      [](auto row) { return row % 7; },
      [](auto row) { return row % 11 + 5; },
      nullEvery(13));
  assertEqualVectors(expectedResult, result);

  registerLambda(
      "key+value",
      rowType("k", INTEGER(), "v", BIGINT()),
      input->type(),
      "k + v");

  result =
      evaluate<MapVector>("transform_values(c0, function('key+value'))", input);

  expectedResult = makeMapVector<int32_t, int64_t>(
      size,
      [](auto row) { return row % 5; },
      [](auto row) { return row % 7; },
      [](auto row) { return row % 7 + row % 11; },
      nullEvery(13));
  assertEqualVectors(expectedResult, result);
}

TEST_F(TransformValuesTest, differentResultType) {
  vector_size_t size = 1'000;
  auto input = makeRowVector({
      makeMapVector<int32_t, int64_t>(
          size,
          [](auto row) { return row % 5; },
          [](auto row) { return row % 7; },
          [](auto row) { return row % 11; },
          nullEvery(13)),
  });
  registerLambda(
      "gt3",
      rowType("unused_k", INTEGER(), "v", BIGINT()),
      input->type(),
      "v > 3");

  auto result =
      evaluate<MapVector>("transform_values(c0, function('gt3'))", input);

  auto expectedResult = makeMapVector<int32_t, bool>(
      size,
      [](auto row) { return row % 5; },
      [](auto row) { return row % 7; },
      [](auto row) { return row % 11 > 3; },
      nullEvery(13));
  assertEqualVectors(expectedResult, result);
}

// Test different lambdas applied to different rows.
TEST_F(TransformValuesTest, conditional) {
  vector_size_t size = 1'000;

  // Make 2 columns: the map to transform and a boolean that decided which
  // lambda to use.
  auto inputMap = makeMapVector<int32_t, int64_t>(
      size,
      [](auto row) { return row % 5; },
      [](auto row) { return row % 7; },
      [](auto row) { return row % 11; },
      nullEvery(13));
  auto condition =
      makeFlatVector<bool>(size, [](auto row) { return row % 3 == 1; });
  auto input = makeRowVector({condition, inputMap});
  auto signature = rowType("unused_k", INTEGER(), "v", BIGINT());
  registerLambda("plus5", signature, input->type(), "v + 5");
  registerLambda("minus3", signature, input->type(), "v - 3");

  auto result = evaluate<MapVector>(
      "transform_values(c1, if (c0, function('plus5'), function('minus3')))",
      input);

  // Make 2 expected vectors: one for rows where condition is true and another
  // for rows where condition is false.
  auto expectedPlus5 = makeMapVector<int32_t, int64_t>(
      size,
      [](auto row) { return row % 5; },
      [](auto row) { return row % 7; },
      [](auto row) { return row % 11 + 5; },
      nullEvery(13));
  auto expectedMinus3 = makeMapVector<int32_t, int64_t>(
      size,
      [](auto row) { return row % 5; },
      [](auto row) { return row % 7; },
      [](auto row) { return row % 11 - 3; },
      nullEvery(13));
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

TEST_F(TransformValuesTest, dictionaryWithUniqueValues) {
  vector_size_t size = 1'000;

  auto indices = makeIndicesInReverse(size);
  auto input = makeRowVector(
      {makeFlatVector<int16_t>(size, [](auto /* row */) { return 5; }),
       wrapInDictionary(
           indices,
           size,
           makeMapVector<int32_t, int64_t>(
               size,
               [](auto row) { return row % 5; },
               [](auto row) { return row % 7; },
               [](auto row) { return row % 11; },
               nullEvery(13)))});

  registerLambda(
      "plus5",
      rowType("unused_k", INTEGER(), "v", BIGINT()),
      input->type(),
      "v + c0");

  auto result =
      evaluate<BaseVector>("transform_values(c1, function('plus5'))", input);

  auto expectedResult = wrapInDictionary(
      indices,
      size,
      makeMapVector<int32_t, int64_t>(
          size,
          [](auto row) { return row % 5; },
          [](auto row) { return row % 7; },
          [](auto row) { return row % 11 + 5; },
          nullEvery(13)));
  assertEqualVectors(expectedResult, result);
}

TEST_F(TransformValuesTest, dictionaryWithDuplicates) {
  vector_size_t size = 1'000;

  // Make a map vector where each row repeats twice.
  BufferPtr indices = makeIndices(size, [](auto row) { return row / 2; });
  auto inputMap = wrapInDictionary(
      indices,
      size,
      makeMapVector<int32_t, int64_t>(
          size,
          [](auto row) { return row % 5; },
          [](auto row) { return row % 7; },
          [](auto row) { return row % 11; },
          nullEvery(13)));

  // Make a capture with unique values.
  auto capture = makeFlatVector<int32_t>(size, [](auto row) { return row; });

  auto input = makeRowVector({capture, inputMap});

  registerLambda(
      "v+c0",
      rowType("unused_k", INTEGER(), "v", BIGINT()),
      input->type(),
      "v + c0");

  auto result =
      evaluate<BaseVector>("transform_values(c1, function('v+c0'))", input);

  auto expectedResult = evaluate<BaseVector>(
      "transform_values(c1, function('v+c0'))",
      makeRowVector({capture, flatten(inputMap)}));

  assertEqualVectors(expectedResult, result);
}
