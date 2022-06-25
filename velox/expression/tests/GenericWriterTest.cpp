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

#include <velox/vector/ComplexVector.h>
#include "velox/expression/VectorWriters.h"
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"
#include "velox/type/Type.h"
#include "velox/vector/tests/VectorTestBase.h"

using namespace facebook::velox::exec;

namespace facebook::velox {
namespace {

class GenericWriterTest : public functions::test::FunctionBaseTest {};

TEST_F(GenericWriterTest, boolean) {
  VectorPtr result;
  BaseVector::ensureWritable(SelectivityVector(5), BOOLEAN(), pool(), &result);

  VectorWriter<Any> writer;
  writer.init(*result);

  for (int i = 0; i < 5; ++i) {
    writer.setOffset(i);

    auto& current = writer.current();
    current.castTo<bool>() = i % 2;

    writer.commit(true);
  }
  writer.finish();
  test::assertEqualVectors(
      makeFlatVector<bool>({false, true, false, true, false}), result);

  writer.setOffset(0);
  auto& current = writer.current();
  ASSERT_THROW(current.castTo<int32_t>(), VeloxUserError);

  ASSERT_NO_THROW(current.tryCastTo<int32_t>());
  ASSERT_TRUE(current.tryCastTo<int32_t>() == nullptr);
}

TEST_F(GenericWriterTest, integer) {
  VectorPtr result;
  BaseVector::ensureWritable(SelectivityVector(100), BIGINT(), pool(), &result);

  VectorWriter<Any> writer;
  writer.init(*result);

  for (int i = 0; i < 100; ++i) {
    writer.setOffset(i);

    auto& current = writer.current();
    current.castTo<int64_t>() = i + 1000;

    writer.commit(true);
  }
  writer.finish();
  test::assertEqualVectors(
      makeFlatVector<int64_t>(100, [](auto row) { return row + 1000; }),
      result);
}

TEST_F(GenericWriterTest, varchar) {
  VectorPtr result;
  BaseVector::ensureWritable(SelectivityVector(6), VARCHAR(), pool(), &result);

  VectorWriter<Any> writer;
  writer.init(*result);

  for (int i = 0; i < 5; ++i) {
    writer.setOffset(i);

    auto& current = writer.current().castTo<Varchar>();
    current.copy_from(std::to_string(i + 10));

    writer.commit(true);
  }
  writer.setOffset(5);
  writer.current().castTo<Varchar>().copy_from(std::to_string(5 + 10));
  writer.commitNull();

  writer.finish();
  test::assertEqualVectors(
      makeNullableFlatVector<StringView>(
          {"10"_sv, "11"_sv, "12"_sv, "13"_sv, "14"_sv, std::nullopt}),
      result);
}

TEST_F(GenericWriterTest, array) {
  VectorPtr result;
  BaseVector::ensureWritable(
      SelectivityVector(5), ARRAY(BIGINT()), pool(), &result);

  VectorWriter<Any> writer;
  writer.init(*result);
  for (int i = 0; i < 5; ++i) {
    writer.setOffset(i);

    auto& current = writer.current().castTo<Array<Any>>();

    current.add_item().castTo<int64_t>() = i * 3;
    *current.add_item().tryCastTo<int64_t>() = i * 3 + 1;
    current.add_item().castTo<int64_t>() = i * 3 + 2;

    writer.commit(true);
  }
  writer.finish();

  ASSERT_EQ(result->as<ArrayVector>()->elements()->size(), 15);

  auto data = makeNullableArrayVector<int64_t>(
      {{0, 1, 2}, {3, 4, 5}, {6, 7, 8}, {9, 10, 11}, {12, 13, 14}});
  test::assertEqualVectors(data, result);

  writer.setOffset(0);
  auto& current = writer.current();
  ASSERT_THROW(current.castTo<double>(), VeloxUserError);

  ASSERT_NO_THROW(current.tryCastTo<double>());
  ASSERT_TRUE(current.tryCastTo<double>() == nullptr);
}

TEST_F(GenericWriterTest, map) {
  VectorPtr result;
  BaseVector::ensureWritable(
      SelectivityVector(4), MAP(VARCHAR(), BIGINT()), pool(), &result);

  VectorWriter<Any> writer;
  writer.init(*result);

  for (int i = 0; i < 4; ++i) {
    writer.setOffset(i);

    auto& current = writer.current().castTo<Map<Any, Any>>();

    auto tuple = current.add_item();
    std::get<0>(tuple).castTo<Varchar>().copy_from(std::to_string(i * 2));
    *std::get<1>(tuple).tryCastTo<int64_t>() = i * 2;

    auto& key = current.add_null();
    key.castTo<Varchar>().copy_from(std::to_string(i * 2 + 1));

    writer.commit(true);
  }
  writer.finish();

  ASSERT_EQ(result->as<MapVector>()->mapKeys()->size(), 8);
  ASSERT_EQ(result->as<MapVector>()->mapValues()->size(), 8);

  auto data = makeNullableMapVector<StringView, int64_t>(
      {{{{"0"_sv, 0}, {"1"_sv, std::nullopt}}},
       {{{"2"_sv, 2}, {"3"_sv, std::nullopt}}},
       {{{"4"_sv, 4}, {"5"_sv, std::nullopt}}},
       {{{"6"_sv, 6}, {"7"_sv, std::nullopt}}}});
  test::assertEqualVectors(data, result);
}

TEST_F(GenericWriterTest, nested) {
  VectorPtr result;
  BaseVector::ensureWritable(
      SelectivityVector(3), MAP(BIGINT(), ARRAY(BIGINT())), pool(), &result);

  VectorWriter<Any> writer;
  writer.init(*result);

  for (int i = 0; i < 3; ++i) {
    writer.setOffset(i);

    auto& current = writer.current().castTo<Map<Any, Any>>();

    auto pair = current.add_item();
    std::get<0>(pair).castTo<int64_t>() = i * 3;

    auto& array = std::get<1>(pair).castTo<Array<Any>>();
    array.add_item().castTo<int64_t>() = i * 3 + 1;
    *array.add_item().tryCastTo<int64_t>() = i * 3 + 2;
    array.add_null();

    writer.commit(true);
  }
  writer.finish();

  ASSERT_EQ(result->as<MapVector>()->mapKeys()->size(), 3);
  ASSERT_EQ(result->as<MapVector>()->mapValues()->size(), 3);
  ASSERT_EQ(
      result->as<MapVector>()
          ->mapValues()
          ->as<ArrayVector>()
          ->elements()
          ->size(),
      9);

  auto arrayVector = makeNullableArrayVector<int64_t>(
      {{1, 2, std::nullopt}, {4, 5, std::nullopt}, {7, 8, std::nullopt}});
  auto keyVector = makeNullableFlatVector<int64_t>({0, 3, 6});

  auto offsets = AlignedBuffer::allocate<vector_size_t>(3, pool());
  auto sizes = AlignedBuffer::allocate<vector_size_t>(3, pool());
  auto rawOffsets = offsets->asMutable<vector_size_t>();
  auto rawSizes = sizes->asMutable<vector_size_t>();

  rawSizes[0] = rawSizes[1] = rawSizes[2] = 1;
  rawOffsets[0] = 0;
  rawOffsets[1] = 1;
  rawOffsets[2] = 2;

  auto mapVector = std::make_shared<MapVector>(
      pool(),
      MAP(BIGINT(), ARRAY(BIGINT())),
      nullptr,
      3,
      offsets,
      sizes,
      keyVector,
      arrayVector,
      0);

  test::assertEqualVectors(mapVector, result);
}

TEST_F(GenericWriterTest, commitNull) {
  VectorPtr result;
  BaseVector::ensureWritable(SelectivityVector(3), BIGINT(), pool(), &result);

  VectorWriter<Any> writer;
  writer.init(*result);

  for (int i = 0; i < 3; ++i) {
    writer.setOffset(i);
    writer.commitNull();
  }
  writer.finish();

  auto data = makeNullableFlatVector<int64_t>(
      {std::nullopt, std::nullopt, std::nullopt});
  test::assertEqualVectors(data, result);
}

TEST_F(GenericWriterTest, handleMisuse) {
  auto initializeAndAddElements = [](const VectorPtr& vector,
                                     VectorWriter<Any>& writer) {
    writer.init(*vector);
    writer.setOffset(0);

    auto& current = writer.current().castTo<Array<Any>>();
    current.add_item().castTo<int64_t>() = 1;
    current.add_item().castTo<int64_t>() = 2;
  };

  // Test finish without commit.
  VectorPtr result;
  BaseVector::ensureWritable(
      SelectivityVector(1), ARRAY(BIGINT()), pool(), &result);

  VectorWriter<Any> writer1;
  initializeAndAddElements(result, writer1);

  writer1.finish();
  ASSERT_EQ(result->as<ArrayVector>()->elements()->size(), 0);

  // Test commitNull after adding elements.
  VectorWriter<Any> writer2;
  initializeAndAddElements(result, writer2);

  writer2.commitNull();
  writer2.finish();

  ASSERT_EQ(result->as<ArrayVector>()->elements()->size(), 0);
  ASSERT_TRUE(result->as<ArrayVector>()->isNullAt(0));
}

} // namespace
} // namespace facebook::velox
