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
#include "velox/serializers/PrestoSerializer.h"
#include <gtest/gtest.h>
#include "velox/common/memory/ByteStream.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/tests/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

class PrestoSerializerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = memory::getDefaultScopedMemoryPool();
    serde_ = std::make_unique<serializer::presto::PrestoVectorSerde>();
    vectorMaker_ = std::make_unique<test::VectorMaker>(pool_.get());
  }

  void sanityCheckEstimateSerializedSize(
      RowVectorPtr rowVector,
      const folly::Range<const IndexRange*>& ranges) {
    auto numRows = rowVector->size();
    std::vector<vector_size_t> rowSizes(numRows, 0);
    std::vector<vector_size_t*> rawRowSizes(numRows);
    for (auto i = 0; i < numRows; i++) {
      rawRowSizes[i] = &rowSizes[i];
    }
    serde_->estimateSerializedSize(rowVector, ranges, rawRowSizes.data());
  }

  void serialize(RowVectorPtr rowVector, std::ostream* output) {
    auto numRows = rowVector->size();

    std::vector<IndexRange> rows(numRows);
    for (int i = 0; i < numRows; i++) {
      rows[i] = IndexRange{i, 1};
    }

    sanityCheckEstimateSerializedSize(
        rowVector, folly::Range(rows.data(), numRows));

    auto arena =
        std::make_unique<StreamArena>(memory::MappedMemory::getInstance());
    auto rowType = std::dynamic_pointer_cast<const RowType>(rowVector->type());
    auto serializer = serde_->createSerializer(rowType, numRows, arena.get());

    serializer->append(rowVector, folly::Range(rows.data(), numRows));
    facebook::velox::serializer::presto::PrestoOutputStreamListener listener;
    OStreamOutputStream out(output, &listener);
    serializer->flush(&out);
  }

  std::unique_ptr<ByteStream> toByteStream(const std::string& input) {
    auto byteStream = std::make_unique<ByteStream>();
    ByteRange byteRange{
        reinterpret_cast<uint8_t*>(const_cast<char*>(input.data())),
        (int32_t)input.length(),
        0};
    byteStream->resetInput({byteRange});
    return byteStream;
  }

  RowVectorPtr deserialize(
      std::shared_ptr<const RowType> rowType,
      const std::string& input) {
    auto byteStream = toByteStream(input);

    RowVectorPtr result;
    serde_->deserialize(byteStream.get(), pool_.get(), rowType, &result);
    return result;
  }

  RowVectorPtr makeTestVector(vector_size_t size) {
    auto a = vectorMaker_->flatVector<int64_t>(
        size, [](vector_size_t row) { return row; });
    auto b = vectorMaker_->flatVector<double>(
        size, [](vector_size_t row) { return row * 0.1; });

    std::vector<VectorPtr> childVectors = {a, b};

    return vectorMaker_->rowVector(childVectors);
  }

  void testRoundTrip(VectorPtr vector) {
    auto rowVector = vectorMaker_->rowVector({vector});
    std::ostringstream out;
    serialize(rowVector, &out);

    auto rowType = std::dynamic_pointer_cast<const RowType>(rowVector->type());
    auto deserialized = deserialize(rowType, out.str());
    assertEqualVectors(deserialized, rowVector);
  }

  std::unique_ptr<memory::MemoryPool> pool_;
  std::unique_ptr<VectorSerde> serde_;
  std::unique_ptr<test::VectorMaker> vectorMaker_;
};

TEST_F(PrestoSerializerTest, basic) {
  vector_size_t numRows = 1'000;
  auto rowVector = makeTestVector(numRows);

  std::ostringstream out;
  serialize(rowVector, &out);

  auto rowType = std::dynamic_pointer_cast<const RowType>(rowVector->type());
  auto deserialized = deserialize(rowType, out.str());
  assertEqualVectors(deserialized, rowVector);
}

/// Test serialization of a dictionary vector that adds nulls to the base
/// vector.
TEST_F(PrestoSerializerTest, dictionaryWithExtraNulls) {
  vector_size_t size = 1'000;

  auto base =
      vectorMaker_->flatVector<int64_t>(10, [](auto row) { return row; });

  BufferPtr nulls = AlignedBuffer::allocate<bool>(size, pool_.get());
  auto rawNulls = nulls->asMutable<uint64_t>();
  for (auto i = 0; i < size; i++) {
    bits::setNull(rawNulls, i, i % 5 == 0);
  }

  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(size, pool_.get());
  auto rawIndices = indices->asMutable<vector_size_t>();
  for (auto i = 0; i < size; i++) {
    if (i % 5 != 0) {
      rawIndices[i] = i % 10;
    }
  }

  auto dictionary = BaseVector::wrapInDictionary(nulls, indices, size, base);
  testRoundTrip(dictionary);
}

TEST_F(PrestoSerializerTest, emptyPage) {
  auto rowVector = vectorMaker_->rowVector(ROW({"a"}, {BIGINT()}), 0);

  std::ostringstream out;
  serialize(rowVector, &out);

  auto rowType = std::dynamic_pointer_cast<const RowType>(rowVector->type());
  auto deserialized = deserialize(rowType, out.str());
  assertEqualVectors(deserialized, rowVector);
}

TEST_F(PrestoSerializerTest, emptyArray) {
  auto arrayVector = vectorMaker_->arrayVector<int32_t>(
      1'000,
      [](vector_size_t row) { return row % 5; },
      [](vector_size_t row) { return row; });

  testRoundTrip(arrayVector);
}

TEST_F(PrestoSerializerTest, emptyMap) {
  auto mapVector = vectorMaker_->mapVector<int32_t, int32_t>(
      1'000,
      [](vector_size_t row) { return row % 5; },
      [](vector_size_t row) { return row; },
      [](vector_size_t row) { return row * 2; });

  testRoundTrip(mapVector);
}

TEST_F(PrestoSerializerTest, timestampWithTimeZone) {
  auto timestamp = vectorMaker_->flatVector<int64_t>(
      100, [](auto row) { return 10'000 + row; });
  auto timezone =
      vectorMaker_->flatVector<int16_t>(100, [](auto row) { return row % 37; });

  auto vector = std::make_shared<RowVector>(
      pool_.get(),
      TIMESTAMP_WITH_TIME_ZONE(),
      BufferPtr(nullptr),
      100,
      std::vector<VectorPtr>{timestamp, timezone});

  testRoundTrip(vector);

  // Add some nulls.
  for (auto i = 0; i < 100; i += 7) {
    vector->setNull(i, true);
  }
  testRoundTrip(vector);
}

TEST_F(PrestoSerializerTest, unknown) {
  const vector_size_t size = 123;
  auto constantVector =
      BaseVector::createConstant(variant(TypeKind::UNKNOWN), 123, pool_.get());
  testRoundTrip(constantVector);

  auto flatVector = BaseVector::create(UNKNOWN(), size, pool_.get());
  for (auto i = 0; i < size; i++) {
    flatVector->setNull(i, true);
  }
  testRoundTrip(flatVector);
}

TEST_F(PrestoSerializerTest, multiPage) {
  std::ostringstream out;

  // page 1
  auto a = makeTestVector(1'234);
  serialize(a, &out);

  // page 2
  auto b = makeTestVector(538);
  serialize(b, &out);

  // page 3
  auto c = makeTestVector(2'048);
  serialize(c, &out);

  auto bytes = out.str();

  auto rowType = std::dynamic_pointer_cast<const RowType>(a->type());
  auto byteStream = toByteStream(bytes);

  RowVectorPtr deserialized;
  serde_->deserialize(byteStream.get(), pool_.get(), rowType, &deserialized);
  ASSERT_FALSE(byteStream->atEnd());
  assertEqualVectors(deserialized, a);

  serde_->deserialize(byteStream.get(), pool_.get(), rowType, &deserialized);
  assertEqualVectors(deserialized, b);
  ASSERT_FALSE(byteStream->atEnd());

  serde_->deserialize(byteStream.get(), pool_.get(), rowType, &deserialized);
  assertEqualVectors(deserialized, c);
  ASSERT_TRUE(byteStream->atEnd());
}
