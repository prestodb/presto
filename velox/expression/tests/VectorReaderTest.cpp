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

#include "velox/expression/VectorReaders.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

using facebook::velox::exec::LocalDecodedVector;
namespace facebook::velox {

DecodedVector* decode(DecodedVector& decoder, const BaseVector& vector) {
  SelectivityVector rows(vector.size());
  decoder.decode(vector, rows);
  return &decoder;
}

class VectorReaderTest : public functions::test::FunctionBaseTest {};

TEST_F(VectorReaderTest, scalarContainsNull) {
  // Vector is:
  // [NULL, 1, 2, 3, 4, NULL, 6, 7, 8, 9]
  auto vector = makeFlatVector<int32_t>(
      10,
      [](vector_size_t row) { return row; },
      [](vector_size_t row) { return row % 5 == 0; });
  DecodedVector decoded;
  exec::VectorReader<int32_t> reader(decode(decoded, *vector.get()));

  ASSERT_FALSE(reader.containsNull(1));
  ASSERT_FALSE(reader.containsNull(2));
  ASSERT_FALSE(reader.containsNull(3));
  ASSERT_TRUE(reader.containsNull(0));
  ASSERT_TRUE(reader.containsNull(5));

  // Value before is NULL.
  ASSERT_FALSE(reader.containsNull(1, 2));
  // Value after is NULL.
  ASSERT_FALSE(reader.containsNull(2, 5));
  // Up to the last value.
  ASSERT_FALSE(reader.containsNull(7, 10));
  // First value is NULL.
  ASSERT_TRUE(reader.containsNull(0, 3));
  // NULL value in middle of range.
  ASSERT_TRUE(reader.containsNull(4, 7));
  // Last value is NULL.
  ASSERT_TRUE(reader.containsNull(1, 6));
}

TEST_F(VectorReaderTest, dictionaryEncodedScalarContainsNull) {
  // Vector is:
  // [NULL, 3, 6, 9, 2, 5, NULL, 1, NULL, 7]
  vector_size_t size = 10;
  auto baseVector = makeFlatVector<int32_t>(
      size,
      [](vector_size_t row) { return row; },
      [](vector_size_t row) { return row % 4 == 0; });
  BufferPtr indices =
      AlignedBuffer::allocate<vector_size_t>(size, execCtx_.pool());
  auto rawIndices = indices->asMutable<vector_size_t>();
  for (auto i = 0; i < size; ++i) {
    // 13 and 10 are coprime so this shuffles the indices.
    rawIndices[i] = (i * 13) % size;
  }
  auto scalarVector = wrapInDictionary(indices, size, baseVector);

  DecodedVector decoded;
  exec::VectorReader<int32_t> reader(decode(decoded, *scalarVector.get()));

  ASSERT_FALSE(reader.containsNull(1));
  ASSERT_FALSE(reader.containsNull(2));
  ASSERT_FALSE(reader.containsNull(3));
  ASSERT_TRUE(reader.containsNull(0));
  ASSERT_TRUE(reader.containsNull(6));

  // Value before is NULL.
  ASSERT_FALSE(reader.containsNull(1, 6));
  // Value after is NULL.
  ASSERT_FALSE(reader.containsNull(7, 8));
  // Up to the last value.
  ASSERT_FALSE(reader.containsNull(9, 10));
  // First value is NULL.
  ASSERT_TRUE(reader.containsNull(0, 3));
  // NULL value in middle of range.
  ASSERT_TRUE(reader.containsNull(4, 8));
  // Last value is NULL.
  ASSERT_TRUE(reader.containsNull(7, 9));
}

TEST_F(VectorReaderTest, mapContainsNull) {
  // Vector is:
  // [
  //   {},
  //   NULL,
  //   {0: 0, 1: NULL},
  //   {2: 2, 3: 3, 4: 4},
  //   {},
  //   {5: NULL},
  //   NULL,
  //   {6: 6, 7: 7, 8: 8},
  //   {},
  //   {9: NULL}
  // ]
  auto vector = makeMapVector<int32_t, float>(
      10,
      [](vector_size_t row) { return row % 4; },
      [](vector_size_t idx) { return idx; },
      [](vector_size_t idx) { return idx; },
      [](vector_size_t row) { return row % 5 == 1; },
      [](vector_size_t idx) { return idx % 4 == 1; });
  DecodedVector decoded;
  exec::VectorReader<Map<int32_t, float>> reader(
      decode(decoded, *vector.get()));
  reader.setChildrenMayHaveNulls();

  // Empty Map.
  ASSERT_FALSE(reader.containsNull(0));
  // Non-empty maps without NULLs.
  ASSERT_FALSE(reader.containsNull(3));
  ASSERT_FALSE(reader.containsNull(7));
  // Map is NULL.
  ASSERT_TRUE(reader.containsNull(1));
  // Map contains NULL value.
  ASSERT_TRUE(reader.containsNull(2));

  // Next map is NULL.
  ASSERT_FALSE(reader.containsNull(0, 1));
  // Previous map is NULL.
  ASSERT_FALSE(reader.containsNull(7, 9));
  // Surrounded by NULL and map containing NULL.
  ASSERT_FALSE(reader.containsNull(3, 5));
  // NULL in middle of range.
  ASSERT_TRUE(reader.containsNull(0, 4));
  // First map contains NULL.
  ASSERT_TRUE(reader.containsNull(2, 5));
  // Last map contains NULL.
  ASSERT_TRUE(reader.containsNull(7, 10));
}

TEST_F(VectorReaderTest, dictionaryEncodedMapContainsNull) {
  // Vector is:
  // [
  //   {},
  //   {2: 2, 3: 3, 4: 4},
  //   NULL,
  //   {9: NULL}
  //   {0: 0, 1: NULL},
  //   {5: NULL},
  //   {},
  //   NULL,
  //   {},
  //   {6: 6, 7: 7, 8: 8},
  // ]
  vector_size_t size = 10;
  auto baseVector = makeMapVector<int32_t, float>(
      size,
      [](vector_size_t row) { return row % 4; },
      [](vector_size_t idx) { return idx; },
      [](vector_size_t idx) { return idx; },
      [](vector_size_t row) { return row % 5 == 1; },
      [](vector_size_t idx) { return idx % 4 == 1; });
  BufferPtr indices =
      AlignedBuffer::allocate<vector_size_t>(size, execCtx_.pool());
  auto rawIndices = indices->asMutable<vector_size_t>();
  for (auto i = 0; i < size; ++i) {
    // 13 and 10 are coprime so this shuffles the indices.
    rawIndices[i] = (i * 13) % size;
  }
  auto mapVector = wrapInDictionary(indices, size, baseVector);

  DecodedVector decoded;
  exec::VectorReader<Map<int32_t, float>> reader(
      decode(decoded, *mapVector.get()));
  reader.setChildrenMayHaveNulls();

  // Empty Map.
  ASSERT_FALSE(reader.containsNull(0));
  // Non-empty maps without NULLs.
  ASSERT_FALSE(reader.containsNull(1));
  ASSERT_FALSE(reader.containsNull(9));
  // Map is NULL.
  ASSERT_TRUE(reader.containsNull(7));
  // Map contains NULL value.
  ASSERT_TRUE(reader.containsNull(4));

  // Next map is NULL.
  ASSERT_FALSE(reader.containsNull(0, 2));
  // Previous map is NULL.
  ASSERT_FALSE(reader.containsNull(8, 10));
  // Surrounded by NULL and map containing NULL.
  ASSERT_FALSE(reader.containsNull(6, 7));
  // NULL in middle of range.
  ASSERT_TRUE(reader.containsNull(6, 10));
  // First map contains NULL.
  ASSERT_TRUE(reader.containsNull(5, 7));
  // Last map is NULL.
  ASSERT_TRUE(reader.containsNull(0, 3));
}

TEST_F(VectorReaderTest, arrayContainsNull) {
  // Vector is:
  // [
  //   [],
  //   [NULL],
  //   NULL,
  //   [1, 2, 3],
  //   [4, 5, NULL, 7],
  //   [],
  //   [8],
  //   NULL,
  //   [9, 10, 11],
  //   [NULL, 13, 14, 15]
  // ]
  auto vector = makeArrayVector<int32_t>(
      10,
      [](vector_size_t row) { return row % 5; },
      [](vector_size_t idx) { return idx; },
      [](vector_size_t row) { return row % 5 == 2; },
      [](vector_size_t idx) { return idx % 6 == 0; });
  DecodedVector decoded;
  exec::VectorReader<Array<int32_t>> reader(decode(decoded, *vector.get()));
  reader.setChildrenMayHaveNulls();

  // Empty Array.
  ASSERT_FALSE(reader.containsNull(0));
  // Non-empty arrays without NULLs.
  ASSERT_FALSE(reader.containsNull(3));
  ASSERT_FALSE(reader.containsNull(8));
  // Array is NULL.
  ASSERT_TRUE(reader.containsNull(2));
  // Array contains NULL value.
  ASSERT_TRUE(reader.containsNull(4));

  // Next array is NULL.
  ASSERT_FALSE(reader.containsNull(5, 7));
  // Previous array is NULL.
  ASSERT_FALSE(reader.containsNull(3, 4));
  // Surrounded by NULL and array containing NULL.
  ASSERT_FALSE(reader.containsNull(8, 9));
  // NULL in middle of range.
  ASSERT_TRUE(reader.containsNull(0, 4));
  // First array contains NULL.
  ASSERT_TRUE(reader.containsNull(4, 7));
  // Last array contains NULL.
  ASSERT_TRUE(reader.containsNull(8, 10));
}

TEST_F(VectorReaderTest, dictionaryEncodedArrayContainsNull) {
  // Vector is:
  // [
  //   [],
  //   [1, 2, 3],
  //   [8],
  //   [NULL, 13, 14, 15]
  //   NULL,
  //   [],
  //   [9, 10, 11],
  //   [NULL],
  //   [4, 5, NULL, 7],
  //   NULL,
  // ]
  vector_size_t size = 10;
  auto baseVector = makeArrayVector<int32_t>(
      size,
      [](vector_size_t row) { return row % 5; },
      [](vector_size_t idx) { return idx; },
      [](vector_size_t row) { return row % 5 == 2; },
      [](vector_size_t idx) { return idx % 6 == 0; });
  BufferPtr indices =
      AlignedBuffer::allocate<vector_size_t>(size, execCtx_.pool());
  auto rawIndices = indices->asMutable<vector_size_t>();
  for (auto i = 0; i < size; ++i) {
    // 13 and 10 are coprime so this shuffles the indices.
    rawIndices[i] = (i * 13) % size;
  }
  auto arrayVector = wrapInDictionary(indices, size, baseVector);

  DecodedVector decoded;
  exec::VectorReader<Array<int32_t>> reader(
      decode(decoded, *arrayVector.get()));
  reader.setChildrenMayHaveNulls();

  // Empty Array.
  ASSERT_FALSE(reader.containsNull(0));
  // Non-empty arrays without NULLs.
  ASSERT_FALSE(reader.containsNull(1));
  ASSERT_FALSE(reader.containsNull(6));
  // Array is NULL.
  ASSERT_TRUE(reader.containsNull(4));
  // Array contains NULL value.
  ASSERT_TRUE(reader.containsNull(8));

  // Next contains NULL.
  ASSERT_FALSE(reader.containsNull(6, 7));
  // Previous array is NULL.
  ASSERT_FALSE(reader.containsNull(5, 6));
  // Surrounded by NULL and array containing NULL.
  ASSERT_FALSE(reader.containsNull(5, 7));
  // NULL in middle of range.
  ASSERT_TRUE(reader.containsNull(2, 6));
  // First array is NULL.
  ASSERT_TRUE(reader.containsNull(4, 7));
  // Last array contains NULL.
  ASSERT_TRUE(reader.containsNull(5, 8));
}

TEST_F(VectorReaderTest, rowContainsNull) {
  // Vector is:
  // [
  //   {field1: NULL, field2: 0},
  //   {field1: 1, field2: NULL},
  //   {field1: 2, field2: 2},
  //   NULL,
  //   {field1: 4, field2: 4},
  //   {field1: NULL, field2: 5},
  //   {field1: 6, field2: NULL},
  //   {field1: 7, field2: 7},
  //   NULL,
  //   {field1: 9, field2: 9},
  // ]
  vector_size_t size = 10;
  auto field1Vector = makeFlatVector<int32_t>(
      size,
      [](vector_size_t row) { return row; },
      [](vector_size_t row) { return row % 5 == 0; });
  auto field2Vector = makeFlatVector<float>(
      size,
      [](vector_size_t row) { return row; },
      [](vector_size_t row) { return row % 5 == 1; });
  auto vector = makeRowVector(
      {field1Vector, field2Vector},
      [](vector_size_t idx) { return idx % 5 == 3; });
  DecodedVector decoded;
  exec::VectorReader<Row<int32_t, float>> reader(
      decode(decoded, *vector.get()));

  // Row without NULLs.
  ASSERT_FALSE(reader.containsNull(4));
  ASSERT_FALSE(reader.containsNull(7));
  // Row is NULL.
  ASSERT_TRUE(reader.containsNull(3));
  // Row contains NULL field.
  ASSERT_TRUE(reader.containsNull(1));

  // Next row is NULL.
  ASSERT_FALSE(reader.containsNull(2, 3));
  // Previous row contains NULL.
  ASSERT_FALSE(reader.containsNull(7, 8));
  // Surrounded by NULL and row containing NULL.
  ASSERT_FALSE(reader.containsNull(4, 5));
  // NULL in middle of range.
  ASSERT_TRUE(reader.containsNull(7, 10));
  // First row contains NULL.
  ASSERT_TRUE(reader.containsNull(1, 3));
  // Last row contains NULL.
  ASSERT_TRUE(reader.containsNull(4, 6));
}

TEST_F(VectorReaderTest, dictionaryEncodedRowContainsNull) {
  // Vector is:
  // [
  //   {field1: NULL, field2: 0},
  //   NULL,
  //   {field1: 6, field2: NULL},
  //   {field1: 9, field2: 9},
  //   {field1: 2, field2: 2},
  //   {field1: NULL, field2: 5},
  //   NULL,
  //   {field1: 1, field2: NULL},
  //   {field1: 4, field2: 4},
  //   {field1: 7, field2: 7},
  // ]
  vector_size_t size = 10;
  auto field1Vector = makeFlatVector<int32_t>(
      size,
      [](vector_size_t row) { return row; },
      [](vector_size_t row) { return row % 5 == 0; });
  auto field2Vector = makeFlatVector<float>(
      size,
      [](vector_size_t row) { return row; },
      [](vector_size_t row) { return row % 5 == 1; });
  auto baseVector = makeRowVector(
      {field1Vector, field2Vector},
      [](vector_size_t idx) { return idx % 5 == 3; });
  BufferPtr indices =
      AlignedBuffer::allocate<vector_size_t>(size, execCtx_.pool());
  auto rawIndices = indices->asMutable<vector_size_t>();
  for (auto i = 0; i < size; ++i) {
    // 13 and 10 are coprime so this shuffles the indices.
    rawIndices[i] = (i * 13) % size;
  }
  auto rowVector = wrapInDictionary(indices, size, baseVector);

  DecodedVector decoded;
  exec::VectorReader<Row<int32_t, float>> reader(
      decode(decoded, *rowVector.get()));

  // Row without NULLs.
  ASSERT_FALSE(reader.containsNull(4));
  ASSERT_FALSE(reader.containsNull(8));
  // Row is NULL.
  ASSERT_TRUE(reader.containsNull(1));
  // Row contains NULL field.
  ASSERT_TRUE(reader.containsNull(2));

  // Next row contains NULL.
  ASSERT_FALSE(reader.containsNull(4, 5));
  // Previous row contains NULL.
  ASSERT_FALSE(reader.containsNull(8, 10));
  // Surrounded by rows containing NULL.
  ASSERT_FALSE(reader.containsNull(3, 5));
  // NULL in middle of range.
  ASSERT_TRUE(reader.containsNull(4, 9));
  // First row contains NULL.
  ASSERT_TRUE(reader.containsNull(7, 10));
  // Last row contains NULL.
  ASSERT_TRUE(reader.containsNull(3, 6));
}

TEST_F(VectorReaderTest, variadicContainsNull) {
  // Vector is:
  // [
  //   [NULL, 0, 0],
  //   [1, NULL, 1],
  //   [2, 2, NULL],
  //   [3, 3, 3],
  //   [4, 4, 4],
  //   [NULL, 5, 5],
  //   [6, NULL, 6],
  //   [7, 7, NULL],
  //   [8, 8, 8],
  //   [9, 9, 9],
  // ]
  vector_size_t size = 10;
  auto field1Vector = makeFlatVector<int32_t>(
      size,
      [](vector_size_t row) { return row; },
      [](vector_size_t row) { return row % 5 == 0; });
  auto field2Vector = makeFlatVector<int32_t>(
      size,
      [](vector_size_t row) { return row; },
      [](vector_size_t row) { return row % 5 == 1; });
  auto field3Vector = makeFlatVector<int32_t>(
      size,
      [](vector_size_t row) { return row; },
      [](vector_size_t row) { return row % 5 == 2; });
  SelectivityVector rows(10);
  exec::EvalCtx ctx(&execCtx_);
  std::vector<std::optional<LocalDecodedVector>> args;
  for (const auto& vector : {field1Vector, field2Vector, field3Vector}) {
    args.emplace_back(LocalDecodedVector(ctx, *vector, rows));
  }
  exec::VectorReader<Variadic<int32_t>> reader(args, 0);

  // Row without NULLs.
  ASSERT_FALSE(reader.containsNull(3));
  ASSERT_FALSE(reader.containsNull(9));
  // Row contains NULL arg.
  ASSERT_TRUE(reader.containsNull(1));

  // Next row contains NULL arg.
  ASSERT_FALSE(reader.containsNull(4, 5));
  // Previous row contains NULL arg.
  ASSERT_FALSE(reader.containsNull(8, 10));
  // Surrounded by rows containing NULL arg.
  ASSERT_FALSE(reader.containsNull(3, 5));
  // Row containing NULL arg in middle of range.
  ASSERT_TRUE(reader.containsNull(4, 9));
  // First row contains NULL arg.
  ASSERT_TRUE(reader.containsNull(2, 5));
  // Last row contains NULL arg.
  ASSERT_TRUE(reader.containsNull(3, 6));
}

TEST_F(VectorReaderTest, dictionaryEncodedVariadicContainsNull) {
  // Vector is:
  // [
  //   [NULL, 0, 0],
  //   [9, 3, 7],
  //   [8, NULL, 4],
  //   [7, 9, 1],
  //   [6, 2, NULL],
  //   [NULL, 5, 5],
  //   [4, 8, 2],
  //   [3, NULL, 9],
  //   [2, 4, 6],
  //   [1, 7, NULL],
  // ]
  vector_size_t size = 10;
  auto field1BaseVector = makeFlatVector<int32_t>(
      size,
      [](vector_size_t row) { return row; },
      [](vector_size_t row) { return row % 5 == 0; });
  auto field2BaseVector = makeFlatVector<int32_t>(
      size,
      [](vector_size_t row) { return row; },
      [](vector_size_t row) { return row % 5 == 1; });
  auto field3BaseVector = makeFlatVector<int32_t>(
      size,
      [](vector_size_t row) { return row; },
      [](vector_size_t row) { return row % 5 == 3; });
  BufferPtr field1indices =
      AlignedBuffer::allocate<vector_size_t>(size, execCtx_.pool());
  BufferPtr field2indices =
      AlignedBuffer::allocate<vector_size_t>(size, execCtx_.pool());
  BufferPtr field3indices =
      AlignedBuffer::allocate<vector_size_t>(size, execCtx_.pool());
  auto field1RawIndices = field1indices->asMutable<vector_size_t>();
  auto field2RawIndices = field2indices->asMutable<vector_size_t>();
  auto field3RawIndices = field3indices->asMutable<vector_size_t>();
  for (auto i = 0; i < size; ++i) {
    // 19, 13, 17 and 10 are coprime so this shuffles the indices.
    field1RawIndices[i] = (i * 19) % size;
    field2RawIndices[i] = (i * 13) % size;
    field3RawIndices[i] = (i * 17) % size;
  }
  auto field1Vector = wrapInDictionary(field1indices, size, field1BaseVector);
  auto field2Vector = wrapInDictionary(field2indices, size, field2BaseVector);
  auto field3Vector = wrapInDictionary(field3indices, size, field3BaseVector);

  SelectivityVector rows(10);
  exec::EvalCtx ctx(&execCtx_);
  std::vector<std::optional<LocalDecodedVector>> args;
  for (const auto& vector : {field1Vector, field2Vector, field3Vector}) {
    args.emplace_back(LocalDecodedVector(ctx, *vector, rows));
  }
  exec::VectorReader<Variadic<int32_t>> reader(args, 0);

  // Row without NULLs.
  ASSERT_FALSE(reader.containsNull(3));
  ASSERT_FALSE(reader.containsNull(8));
  // Row contains NULL arg.
  ASSERT_TRUE(reader.containsNull(2));

  // Next row contains NULL arg.
  ASSERT_FALSE(reader.containsNull(1, 2));
  // Previous row contains NULL arg.
  ASSERT_FALSE(reader.containsNull(3, 4));
  // Surrounded by rows containing NULL arg.
  ASSERT_FALSE(reader.containsNull(6, 7));
  // Row containing NULL arg in middle of range.
  ASSERT_TRUE(reader.containsNull(6, 9));
  // First row contains NULL arg.
  ASSERT_TRUE(reader.containsNull(2, 4));
  // Last row contains NULL arg.
  ASSERT_TRUE(reader.containsNull(3, 5));
}

TEST_F(VectorReaderTest, genericContainsNull) {
  // Vector is:
  // [NULL, 1, 2, 3, 4, NULL, 6, 7, 8, 9]
  auto vector = makeFlatVector<int32_t>(
      10,
      [](vector_size_t row) { return row; },
      [](vector_size_t row) { return row % 5 == 0; });
  DecodedVector decoded;
  exec::VectorReader<Any> reader(decode(decoded, *vector.get()));

  ASSERT_THROW(reader.containsNull(1), VeloxUserError);
  // TODO (kevinwilfong): Add these back once generics are supported, and add
  // test for generics that are containers.
  // ASSERT_FALSE(reader.containsNull(1));
  // ASSERT_FALSE(reader.containsNull(2));
  // ASSERT_FALSE(reader.containsNull(3));
  // ASSERT_TRUE(reader.containsNull(0));
  // ASSERT_TRUE(reader.containsNull(5));

  // // Value before is NULL.
  // ASSERT_FALSE(reader.containsNull(1, 2));
  // // Value after is NULL.
  // ASSERT_FALSE(reader.containsNull(2, 5));
  // // Up to the last value.
  // ASSERT_FALSE(reader.containsNull(7, 10));
  // // First value is NULL.
  // ASSERT_TRUE(reader.containsNull(0, 3));
  // // NULL value in middle of range.
  // ASSERT_TRUE(reader.containsNull(4, 7));
  // // Last value is NULL.
  // ASSERT_TRUE(reader.containsNull(1, 6));
}
} // namespace facebook::velox
