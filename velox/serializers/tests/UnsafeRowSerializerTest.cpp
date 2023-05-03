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
#include "velox/serializers/UnsafeRowSerializer.h"
#include <gtest/gtest.h>
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

class UnsafeRowSerializerTest : public ::testing::Test,
                                public test::VectorTestBase {
 protected:
  void SetUp() override {
    pool_ = memory::addDefaultLeafMemoryPool();
    serde_ = std::make_unique<serializer::spark::UnsafeRowVectorSerde>();
  }

  void serialize(RowVectorPtr rowVector, std::ostream* output) {
    auto numRows = rowVector->size();

    std::vector<IndexRange> rows(numRows);
    for (int i = 0; i < numRows; i++) {
      rows[i] = IndexRange{i, 1};
    }

    auto arena = std::make_unique<StreamArena>(pool_.get());
    auto rowType = std::dynamic_pointer_cast<const RowType>(rowVector->type());
    auto serializer = serde_->createSerializer(rowType, numRows, arena.get());

    serializer->append(rowVector, folly::Range(rows.data(), numRows));
    OStreamOutputStream out(output);
    serializer->flush(&out);
  }

  std::unique_ptr<ByteStream> toByteStream(const std::string_view& input) {
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
      const std::string_view& input) {
    auto byteStream = toByteStream(input);

    RowVectorPtr result;
    serde_->deserialize(byteStream.get(), pool_.get(), rowType, &result);
    return result;
  }

  void testRoundTrip(RowVectorPtr rowVector) {
    std::ostringstream out;
    serialize(rowVector, &out);

    auto rowType = std::dynamic_pointer_cast<const RowType>(rowVector->type());
    auto deserialized = deserialize(rowType, out.str());
    test::assertEqualVectors(deserialized, rowVector);
  }

  void
  testSerialize(RowVectorPtr rowVector, int8_t* expectedData, size_t dataSize) {
    std::ostringstream out;
    serialize(rowVector, &out);
    EXPECT_EQ(std::memcmp(expectedData, out.str().data(), dataSize), 0);
  }

  void
  testDeserialize(int8_t* data, size_t dataSize, RowVectorPtr expectedVector) {
    auto results = deserialize(
        asRowType(expectedVector->type()),
        std::string_view(reinterpret_cast<const char*>(data), dataSize));
    test::assertEqualVectors(expectedVector, results);
  }

  std::shared_ptr<memory::MemoryPool> pool_;
  std::unique_ptr<VectorSerde> serde_;
};

// These expected binary buffers were samples taken using Spark's java code.
TEST_F(UnsafeRowSerializerTest, tinyint) {
  int8_t data[20] = {0, 0, 0,   16, 0, 0, 0, 0, 0, 0,
                     0, 0, 123, 0,  0, 0, 0, 0, 0, 0};
  auto expected = makeRowVector({makeFlatVector(std::vector<int8_t>{123})});

  testSerialize(expected, data, 20);
  testDeserialize(data, 20, expected);
}

TEST_F(UnsafeRowSerializerTest, bigint) {
  int8_t data[20] = {0, 0, 0,  16, 0,   0,   0, 0, 0, 0,
                     0, 0, 62, 28, -36, -33, 2, 0, 0, 0};
  auto expected =
      makeRowVector({makeFlatVector(std::vector<int64_t>{12345678910})});

  testSerialize(expected, data, 20);
  testDeserialize(data, 20, expected);
}

TEST_F(UnsafeRowSerializerTest, double) {
  int8_t data[20] = {0, 0, 0,   16, 0,  0,  0,   0,  0,    0,
                     0, 0, 125, 63, 53, 94, -70, 73, -109, 64};
  auto expected =
      makeRowVector({makeFlatVector(std::vector<double>{1234.432})});

  testSerialize(expected, data, 20);
  testDeserialize(data, 20, expected);
}

TEST_F(UnsafeRowSerializerTest, boolean) {
  int8_t data[20] = {0, 0, 0, 16, 0, 0, 0, 0, 0, 0,
                     0, 0, 1, 0,  0, 0, 0, 0, 0, 0};
  auto expected = makeRowVector({makeFlatVector(std::vector<bool>{true})});

  testSerialize(expected, data, 20);
  testDeserialize(data, 20, expected);
}

TEST_F(UnsafeRowSerializerTest, string) {
  int8_t data[28] = {0, 0, 0,  24, 0, 0, 0,  0,  0,  0,  0,  0, 5, 0,
                     0, 0, 16, 0,  0, 0, 72, 69, 76, 76, 79, 0, 0, 0};
  auto expected =
      makeRowVector({makeFlatVector(std::vector<StringView>{"HELLO"})});

  testSerialize(expected, data, 28);
  testDeserialize(data, 28, expected);
}

TEST_F(UnsafeRowSerializerTest, null) {
  int8_t data[20] = {0, 0, 0, 16, 1, 0, 0, 0, 0, 0,
                     0, 0, 0, 0,  0, 0, 0, 0, 0, 0};
  auto expected = makeRowVector({makeNullableFlatVector(
      std::vector<std::optional<int64_t>>{std::nullopt})});

  testSerialize(expected, data, 20);
  testDeserialize(data, 20, expected);
}

TEST_F(UnsafeRowSerializerTest, manyRows) {
  int8_t data[140] = {0, 0, 0,  24, 0, 0, 0,   0,   0,   0,   0,   0,  4,   0,
                      0, 0, 16, 0,  0, 0, 109, 97,  110, 121, 0,   0,  0,   0,
                      0, 0, 0,  24, 0, 0, 0,   0,   0,   0,   0,   0,  4,   0,
                      0, 0, 16, 0,  0, 0, 114, 111, 119, 115, 0,   0,  0,   0,
                      0, 0, 0,  24, 0, 0, 0,   0,   0,   0,   0,   0,  2,   0,
                      0, 0, 16, 0,  0, 0, 105, 110, 0,   0,   0,   0,  0,   0,
                      0, 0, 0,  24, 0, 0, 0,   0,   0,   0,   0,   0,  1,   0,
                      0, 0, 16, 0,  0, 0, 97,  0,   0,   0,   0,   0,  0,   0,
                      0, 0, 0,  24, 0, 0, 0,   0,   0,   0,   0,   0,  7,   0,
                      0, 0, 16, 0,  0, 0, 112, 97,  121, 108, 111, 97, 100, 0};
  auto expected = makeRowVector({makeFlatVector(
      std::vector<StringView>{"many", "rows", "in", "a", "payload"})});

  testSerialize(expected, data, 140);
  testDeserialize(data, 140, expected);
}

TEST_F(UnsafeRowSerializerTest, types) {
  auto rowType = ROW(
      {BOOLEAN(),
       TINYINT(),
       SMALLINT(),
       INTEGER(),
       BIGINT(),
       REAL(),
       DOUBLE(),
       VARCHAR(),
       TIMESTAMP(),
       ROW({VARCHAR(), INTEGER()}),
       ARRAY(INTEGER()),
       ARRAY(INTEGER()),
       MAP(VARCHAR(), ARRAY(INTEGER()))});

  VectorFuzzer::Options opts;
  opts.vectorSize = 5;
  opts.nullRatio = 0.1;
  opts.containerHasNulls = false;
  opts.dictionaryHasNulls = false;
  opts.stringVariableLength = true;
  opts.stringLength = 20;
  opts.containerVariableLength = false;

  // Spark uses microseconds to store timestamp
  opts.timestampPrecision =
      VectorFuzzer::Options::TimestampPrecision::kMicroSeconds;
  opts.containerLength = 10;

  auto seed = folly::Random::rand32();

  LOG(ERROR) << "Seed: " << seed;
  SCOPED_TRACE(fmt::format("seed: {}", seed));
  VectorFuzzer fuzzer(opts, pool_.get(), seed);

  auto data = fuzzer.fuzzRow(rowType);
  testRoundTrip(data);
}

TEST_F(UnsafeRowSerializerTest, date) {
  auto rowVector = makeRowVector({
      makeFlatVector<Date>({Date(0), Date(1)}),
  });

  testRoundTrip(rowVector);
}
