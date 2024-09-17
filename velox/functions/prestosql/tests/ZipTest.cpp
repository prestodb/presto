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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions::test;

namespace {

class ZipTest : public FunctionBaseTest {};

/// Test if we can zip two integer arrays.
TEST_F(ZipTest, simpleInt) {
  auto firstVector = makeNullableArrayVector<int64_t>(
      {{1, 2, 3, 4}, {3, 4, 5}, {std::nullopt}});
  auto secondVector =
      makeArrayVector<int64_t>({{2, 2, 2}, {1, 1, 1}, {3, 3, 3}});

  auto result = evaluate<ArrayVector>(
      "zip(c0, c1)",
      makeRowVector({
          firstVector,
          secondVector,
      }));

  auto firstResult = makeNullableFlatVector<int64_t>(
      {1, 2, 3, 4, 3, 4, 5, std::nullopt, std::nullopt, std::nullopt});
  auto secondResult = makeNullableFlatVector<int64_t>(
      {2, 2, 2, std::nullopt, 1, 1, 1, 3, 3, 3});

  auto rowVector = makeRowVector({firstResult, secondResult});

  // create the expected ArrayVector
  auto expected = makeArrayVector({0, 4, 7}, rowVector);

  assertEqualVectors(expected, result);
}

/// Test if we can zip across different data types.
TEST_F(ZipTest, combineInt) {
  using S = StringView;
  auto firstVector = makeArrayVector<int64_t>({{1, 1, 1, 1}, {2, 2, 2}, {}});
  auto secondVector = makeArrayVector<StringView>(
      {{S("a"), S("a"), S("a")}, {S("b"), S("b"), S("b")}, {S("c"), S("c")}});

  auto result = evaluate<ArrayVector>(
      "zip(c0, c1)",
      makeRowVector({
          firstVector,
          secondVector,
      }));

  auto firstResult = makeNullableFlatVector<int64_t>(
      {1, 1, 1, 1, 2, 2, 2, std::nullopt, std::nullopt});
  auto secondResult = makeNullableFlatVector<std::string>(
      {S("a"),
       S("a"),
       S("a"),
       std::nullopt,
       S("b"),
       S("b"),
       S("b"),
       S("c"),
       S("c")});

  auto rowVector = makeRowVector({firstResult, secondResult});

  // create the expected ArrayVector
  auto expected = makeArrayVector({0, 4, 7}, rowVector);

  assertEqualVectors(expected, result);
}

/// Test if we can zip with vectors containing null and empty Arrays
TEST_F(ZipTest, nullEmptyArray) {
  auto firstVector = makeNullableArrayVector<int32_t>({
      {{1, 1, 1, 1}},
      {{}},
      std::nullopt,
  });

  auto secondVector = makeArrayVector<int32_t>({
      {0, 0, 0},
      {4, 4},
      {5, 5},
  });

  auto result = evaluate(
      "zip(c0, c1)",
      makeRowVector({
          firstVector,
          secondVector,
      }));

  auto firstResult =
      makeNullableFlatVector<int32_t>({1, 1, 1, 1, std::nullopt, std::nullopt});
  auto secondResult =
      makeNullableFlatVector<int32_t>({0, 0, 0, std::nullopt, 4, 4});

  auto rowVector = makeRowVector({firstResult, secondResult});

  // create the expected ArrayVector
  auto expected = makeArrayVector({0, 4, 6}, rowVector, {2});

  assertEqualVectors(expected, result);
}

// Verify that zip takes between 2 and 7 arguments.
TEST_F(ZipTest, arity) {
  std::vector<VectorPtr> inputs;
  for (auto i = 0; i < 7; i++) {
    if (i % 2 == 0) {
      inputs.push_back(makeArrayVectorFromJson<int32_t>({
          "[1, 1, 1, 1]",
          "[2, 2, 2]",
          "[3, 3]",
      }));
    } else {
      inputs.push_back(makeArrayVectorFromJson<double>({
          "[1.1, 1.1, 1.1, 1.1]",
          "[2.2, 2.2, 2.2]",
          "[3.3, 3.3]",
      }));
    }
  }

  std::vector<VectorPtr> inputElements;
  for (auto i = 0; i < 7; i++) {
    inputElements.push_back(inputs[i]->as<ArrayVector>()->elements());
  }

  for (auto i = 2; i <= 7; ++i) {
    std::ostringstream expression;
    expression << "zip(c0";
    for (auto j = 1; j < i; ++j) {
      expression << ", c" + std::to_string(j);
    }
    expression << ")";

    SCOPED_TRACE(expression.str());
    auto result = evaluate(expression.str(), makeRowVector(inputs));

    std::vector<VectorPtr> children;
    for (auto j = 0; j < i; ++j) {
      children.push_back(inputElements[j]);
    }

    auto expected = makeArrayVector({0, 4, 7}, makeRowVector(children));
    assertEqualVectors(expected, result);
  }

  // Verify that 0, 1, or 8 args are not supported.
  VELOX_ASSERT_THROW(
      evaluate("zip()", makeRowVector(inputs)),
      "Scalar function signature is not supported: zip");
  VELOX_ASSERT_THROW(
      evaluate("zip(c0)", makeRowVector(inputs)),
      "Scalar function signature is not supported: zip");
  VELOX_ASSERT_THROW(
      evaluate("zip(c0, c1, c2, c3, c0, c1, c2, c3)", makeRowVector(inputs)),
      "Scalar function signature is not supported: zip");
}

/// Test if we can zip on complex types
TEST_F(ZipTest, complexTypes) {
  auto baseVector = makeArrayVector<int64_t>(
      {{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}});

  // Create an array of array vector using above base vector
  auto arrayOfArrays = makeArrayVector({0, 2, 4}, baseVector);

  auto secondVector = makeArrayVector<int32_t>({{0, 0}, {4, 4}, {5, 5}});

  auto result = evaluate<ArrayVector>(
      "zip(c0, c1)",
      makeRowVector({
          arrayOfArrays,
          secondVector,
      }));

  auto secondResult = makeNullableFlatVector<int32_t>({0, 0, 4, 4, 5, 5});
  auto rowVector = makeRowVector({baseVector, secondResult});
  // create the expected ArrayVector
  auto expected = makeArrayVector({0, 2, 4}, rowVector);

  assertEqualVectors(expected, result);
}

/// Test if we can zip two integer arrays with dictionary encoded elements.
TEST_F(ZipTest, dictionaryElements) {
  auto firstIndices = makeIndices(9, [](vector_size_t row) { return row; });
  auto firstElements = makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6, 7, 8});
  auto firstElementsDictionary =
      wrapInDictionary(firstIndices, 9, firstElements);
  auto firstVector = makeArrayVector({0, 3, 6}, firstElementsDictionary);

  // Use different indices.
  auto secondIndices =
      makeIndices(9, [](vector_size_t row) { return 8 - row; });
  auto secondElements =
      makeFlatVector<int32_t>({10, 11, 12, 13, 14, 15, 16, 17, 18});
  auto secondElementsDictionary =
      wrapInDictionary(secondIndices, 9, secondElements);
  auto secondVector = makeArrayVector({0, 3, 6}, secondElementsDictionary);

  auto result = evaluate<ArrayVector>(
      "zip(c0, c1)",
      makeRowVector({
          firstVector,
          secondVector,
      }));

  auto firstResult = makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6, 7, 8});
  auto secondResult =
      makeFlatVector<int32_t>({18, 17, 16, 15, 14, 13, 12, 11, 10});

  auto rowVector = makeRowVector({firstResult, secondResult});

  // create the expected ArrayVector
  auto expected = makeArrayVector({0, 3, 6}, rowVector);

  assertEqualVectors(expected, result);
}

/// Test if we can zip two dictionary encoded integer arrays
TEST_F(ZipTest, dictionaryArrays) {
  auto firstElements = makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6, 7, 8});
  auto firstIndices = makeIndices(3, [](vector_size_t row) { return row; });
  auto firstVector = makeArrayVector({0, 3, 6}, firstElements);
  auto firstVectorDictionary = wrapInDictionary(firstIndices, 3, firstVector);

  // Use different indices.
  auto secondElements =
      makeFlatVector<int32_t>({10, 11, 12, 13, 14, 15, 16, 17, 18});
  auto secondIndices =
      makeIndices(3, [](vector_size_t row) { return 2 - row; });
  auto secondVector = makeArrayVector({0, 3, 6}, secondElements);
  auto secondVectorDictionary =
      wrapInDictionary(secondIndices, 3, secondVector);

  auto result = evaluate<ArrayVector>(
      "zip(c0, c1)",
      makeRowVector({
          firstVectorDictionary,
          secondVectorDictionary,
      }));

  auto firstResult = makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6, 7, 8});
  auto secondResult =
      makeFlatVector<int32_t>({16, 17, 18, 13, 14, 15, 10, 11, 12});

  auto rowVector = makeRowVector({firstResult, secondResult});

  // create the expected ArrayVector
  auto expected = makeArrayVector({0, 3, 6}, rowVector);

  assertEqualVectors(expected, result);
}

/// Test if we can zip two flat vectors of arrays with rows starting at
/// different offsets
TEST_F(ZipTest, flatArraysWithDifferentOffsets) {
  auto firstElements = makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6, 7, 8});
  auto firstVector = makeArrayVector({0, 3, 6}, firstElements);

  // Use different offsets.
  auto secondElements =
      makeFlatVector<int32_t>({10, 11, 12, 13, 14, 15, 16, 17, 18});
  const auto size = 3;
  BufferPtr offsetsBuffer = allocateOffsets(size, pool_.get());
  BufferPtr sizesBuffer = allocateSizes(size, pool_.get());
  auto rawOffsets = offsetsBuffer->asMutable<vector_size_t>();
  auto rawSizes = sizesBuffer->asMutable<vector_size_t>();

  rawOffsets[0] = 6;
  rawOffsets[1] = 3;
  rawOffsets[2] = 0;

  for (int i = 0; i < size; i++) {
    rawSizes[i] = 3;
  }

  auto secondVector = std::make_shared<ArrayVector>(
      pool_.get(),
      ARRAY(INTEGER()),
      nullptr,
      size,
      offsetsBuffer,
      sizesBuffer,
      secondElements);

  auto result = evaluate<ArrayVector>(
      "zip(c0, c1)",
      makeRowVector({
          firstVector,
          secondVector,
      }));

  auto firstResult = makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6, 7, 8});
  auto secondResult =
      makeFlatVector<int32_t>({16, 17, 18, 13, 14, 15, 10, 11, 12});

  auto rowVector = makeRowVector({firstResult, secondResult});

  // create the expected ArrayVector
  auto expected = makeArrayVector({0, 3, 6}, rowVector);

  assertEqualVectors(expected, result);
}

/// Test if we can zip two flat vectors of arrays with overlapping ranges of
/// elements.
TEST_F(ZipTest, DISABLED_flatArraysWithOverlappingRanges) {
  const auto size = 3;
  BufferPtr offsetsBuffer = allocateOffsets(size, pool_.get());
  BufferPtr sizesBuffer = allocateSizes(size, pool_.get());
  auto rawOffsets = offsetsBuffer->asMutable<vector_size_t>();
  auto rawSizes = sizesBuffer->asMutable<vector_size_t>();

  rawOffsets[0] = 0;
  rawOffsets[1] = 1;
  rawOffsets[2] = 2;

  for (int i = 0; i < size; i++) {
    rawSizes[i] = 3;
  }

  auto firstElements = makeFlatVector<int32_t>({0, 1, 2, 3, 4});
  auto firstVector = std::make_shared<ArrayVector>(
      pool_.get(),
      ARRAY(INTEGER()),
      nullptr,
      size,
      offsetsBuffer,
      sizesBuffer,
      firstElements);

  auto secondElements = makeFlatVector<int32_t>({10, 11, 12, 13, 14});
  auto secondVector = std::make_shared<ArrayVector>(
      pool_.get(),
      ARRAY(INTEGER()),
      nullptr,
      size,
      offsetsBuffer,
      sizesBuffer,
      secondElements);

  auto result = evaluate<ArrayVector>(
      "zip(c0, c1)",
      makeRowVector({
          firstVector,
          secondVector,
      }));

  auto firstResult = makeFlatVector<int32_t>({0, 1, 2, 1, 2, 3, 2, 3, 4});
  auto secondResult =
      makeFlatVector<int32_t>({10, 11, 12, 11, 12, 13, 12, 13, 14});

  auto rowVector = makeRowVector({firstResult, secondResult});

  // create the expected ArrayVector
  auto expected = makeArrayVector({0, 3, 6}, rowVector);

  assertEqualVectors(expected, result);
}

/// Test if we can zip two flat vectors of arrays with a gap in the range of
/// elements.
TEST_F(ZipTest, flatArraysWithGapInElements) {
  const auto size = 3;
  BufferPtr offsetsBuffer = allocateOffsets(size, pool_.get());
  BufferPtr sizesBuffer = allocateSizes(size, pool_.get());
  auto rawOffsets = offsetsBuffer->asMutable<vector_size_t>();
  auto rawSizes = sizesBuffer->asMutable<vector_size_t>();

  rawOffsets[0] = 0;
  rawOffsets[1] = 6;
  rawOffsets[2] = 9;

  for (int i = 0; i < size; i++) {
    rawSizes[i] = 3;
  }

  auto firstElements =
      makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11});
  auto firstVector = std::make_shared<ArrayVector>(
      pool_.get(),
      ARRAY(INTEGER()),
      nullptr,
      size,
      offsetsBuffer,
      sizesBuffer,
      firstElements);

  auto secondElements =
      makeFlatVector<int32_t>({10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21});
  auto secondVector = std::make_shared<ArrayVector>(
      pool_.get(),
      ARRAY(INTEGER()),
      nullptr,
      size,
      offsetsBuffer,
      sizesBuffer,
      secondElements);

  auto result = evaluate<ArrayVector>(
      "zip(c0, c1)",
      makeRowVector({
          firstVector,
          secondVector,
      }));

  auto firstResult = makeFlatVector<int32_t>({0, 1, 2, 6, 7, 8, 9, 10, 11});
  auto secondResult =
      makeFlatVector<int32_t>({10, 11, 12, 16, 17, 18, 19, 20, 21});

  auto rowVector = makeRowVector({firstResult, secondResult});

  // create the expected ArrayVector
  auto expected = makeArrayVector({0, 3, 6}, rowVector);

  assertEqualVectors(expected, result);
}

} // namespace
