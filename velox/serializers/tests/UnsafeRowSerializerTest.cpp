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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/row/UnsafeRowFast.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook;
using namespace facebook::velox;

struct TestParam {
  common::CompressionKind compressionKind;
  bool appendRow;

  TestParam(common::CompressionKind _compressionKind, bool _appendRow)
      : compressionKind(_compressionKind), appendRow(_appendRow) {}
};

class UnsafeRowSerializerTest : public ::testing::Test,
                                public velox::test::VectorTestBase,
                                public testing::WithParamInterface<TestParam> {
 public:
  static std::vector<TestParam> getTestParams() {
    static std::vector<TestParam> testParams = {
        {common::CompressionKind::CompressionKind_NONE, false},
        {common::CompressionKind::CompressionKind_ZLIB, true},
        {common::CompressionKind::CompressionKind_SNAPPY, false},
        {common::CompressionKind::CompressionKind_ZSTD, true},
        {common::CompressionKind::CompressionKind_LZ4, false},
        {common::CompressionKind::CompressionKind_GZIP, true}};
    return testParams;
  }

 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool();
    deregisterVectorSerde();
    deregisterNamedVectorSerde(VectorSerde::Kind::kCompactRow);
    serializer::spark::UnsafeRowVectorSerde::registerVectorSerde();
    serializer::spark::UnsafeRowVectorSerde::registerNamedVectorSerde();
    ASSERT_EQ(getVectorSerde()->kind(), VectorSerde::Kind::kUnsafeRow);
    ASSERT_EQ(
        getNamedVectorSerde(VectorSerde::Kind::kUnsafeRow)->kind(),
        VectorSerde::Kind::kUnsafeRow);
    appendRow_ = GetParam().appendRow;
    compressionKind_ = GetParam().compressionKind;
    options_ = std::make_unique<VectorSerde::Options>(compressionKind_, 0.8);
  }

  void TearDown() override {
    deregisterVectorSerde();
    deregisterNamedVectorSerde(VectorSerde::Kind::kUnsafeRow);
  }

  void serialize(RowVectorPtr rowVector, std::ostream* output) {
    const auto streamInitialSize = output->tellp();
    const auto numRows = rowVector->size();

    std::vector<IndexRange> ranges(numRows);
    for (int i = 0; i < numRows; i++) {
      ranges[i] = IndexRange{i, 1};
    }

    std::unique_ptr<row::UnsafeRowFast> unsafeRow;
    std::vector<vector_size_t> serializedRowSizes(numRows);
    std::vector<vector_size_t*> serializedRowSizesPtr(numRows);
    std::vector<vector_size_t> rows(numRows);
    std::iota(rows.begin(), rows.end(), 0);
    for (auto i = 0; i < numRows; ++i) {
      serializedRowSizesPtr[i] = &serializedRowSizes[i];
    }
    if (appendRow_) {
      unsafeRow = std::make_unique<row::UnsafeRowFast>(rowVector);
      getVectorSerde()->estimateSerializedSize(
          unsafeRow.get(), rows, serializedRowSizesPtr.data());
    }

    auto arena = std::make_unique<StreamArena>(pool_.get());
    auto rowType = std::dynamic_pointer_cast<const RowType>(rowVector->type());
    auto serializer = getVectorSerde()->createIterativeSerializer(
        rowType, numRows, arena.get(), options_.get());

    if (appendRow_) {
      serializer->append(*unsafeRow, rows, serializedRowSizes);
    } else {
      Scratch scratch;
      serializer->append(
          rowVector, folly::Range(ranges.data(), numRows), scratch);
    }

    auto size = serializer->maxSerializedSize();
    OStreamOutputStream out(output);
    serializer->flush(&out);
    if (!needCompression()) {
      ASSERT_EQ(size, output->tellp() - streamInitialSize);
    } else {
      ASSERT_GT(size, output->tellp() - streamInitialSize);
    }
  }

  std::unique_ptr<ByteInputStream> toByteStream(
      const std::vector<std::string_view>& inputs) {
    std::vector<ByteRange> ranges;
    ranges.reserve(inputs.size());

    for (const auto& input : inputs) {
      ranges.push_back(
          {reinterpret_cast<uint8_t*>(const_cast<char*>(input.data())),
           (int32_t)input.length(),
           0});
    }
    return std::make_unique<BufferInputStream>(std::move(ranges));
  }

  RowVectorPtr deserialize(
      std::shared_ptr<const RowType> rowType,
      const std::vector<std::string_view>& input) {
    auto byteStream = toByteStream(input);

    RowVectorPtr result;
    getVectorSerde()->deserialize(
        byteStream.get(), pool_.get(), rowType, &result, options_.get());
    return result;
  }

  void testRoundTrip(RowVectorPtr rowVector) {
    std::ostringstream out;
    serialize(rowVector, &out);

    auto rowType = std::dynamic_pointer_cast<const RowType>(rowVector->type());
    auto deserialized = deserialize(rowType, {out.str()});
    test::assertEqualVectors(deserialized, rowVector);
  }

  void
  testSerialize(RowVectorPtr rowVector, int8_t* expectedData, size_t dataSize) {
    std::ostringstream out;
    serialize(rowVector, &out);
    if (!needCompression()) {
      // Check the data after header.
      EXPECT_EQ(
          std::memcmp(expectedData, out.str().data() + kHeaderSize, dataSize),
          0);
    }
  }

  void testDeserialize(
      const std::vector<std::string_view>& input,
      RowVectorPtr expectedVector) {
    if (needCompression()) {
      return;
    }
    // Construct the header to make deserialization work.
    std::vector<std::string_view> uncompressedInput = input;
    char header[kHeaderSize] = {0};
    int32_t uncompressedSize = 0;
    for (const auto& in : input) {
      uncompressedSize += in.size();
    }
    auto* headerPtr = reinterpret_cast<int32_t*>(&header);
    headerPtr[0] = uncompressedSize;
    headerPtr[1] = uncompressedSize;
    header[kHeaderSize - 1] = 0;

    uncompressedInput.insert(
        uncompressedInput.begin(), std::string_view(header, kHeaderSize));
    auto results =
        deserialize(asRowType(expectedVector->type()), uncompressedInput);
    test::assertEqualVectors(expectedVector, results);
  }

  void
  testDeserialize(int8_t* data, size_t dataSize, RowVectorPtr expectedVector) {
    testDeserialize(
        {std::string_view(reinterpret_cast<const char*>(data), dataSize)},
        expectedVector);
  }

  bool needCompression() {
    return compressionKind_ != common::CompressionKind::CompressionKind_NONE;
  }

  std::shared_ptr<memory::MemoryPool> pool_;

 private:
  static constexpr int32_t kHeaderSize = sizeof(int32_t) * 2 + sizeof(char);
  common::CompressionKind compressionKind_;
  std::unique_ptr<VectorSerde::Options> options_;
  bool appendRow_;
};

// These expected binary buffers were samples taken using Spark's java code.
TEST_P(UnsafeRowSerializerTest, tinyint) {
  int8_t data[20] = {0, 0, 0,   16, 0, 0, 0, 0, 0, 0,
                     0, 0, 123, 0,  0, 0, 0, 0, 0, 0};
  auto expected = makeRowVector({makeFlatVector(std::vector<int8_t>{123})});

  testSerialize(expected, data, 20);
  testDeserialize(data, 20, expected);
}

TEST_P(UnsafeRowSerializerTest, bigint) {
  int8_t data[20] = {0, 0, 0,  16, 0,   0,   0, 0, 0, 0,
                     0, 0, 62, 28, -36, -33, 2, 0, 0, 0};
  auto expected =
      makeRowVector({makeFlatVector(std::vector<int64_t>{12345678910})});

  testSerialize(expected, data, 20);
  testDeserialize(data, 20, expected);
}

TEST_P(UnsafeRowSerializerTest, double) {
  int8_t data[20] = {0, 0, 0,   16, 0,  0,  0,   0,  0,    0,
                     0, 0, 125, 63, 53, 94, -70, 73, -109, 64};
  auto expected =
      makeRowVector({makeFlatVector(std::vector<double>{1234.432})});

  testSerialize(expected, data, 20);
  testDeserialize(data, 20, expected);
}

TEST_P(UnsafeRowSerializerTest, boolean) {
  int8_t data[20] = {0, 0, 0, 16, 0, 0, 0, 0, 0, 0,
                     0, 0, 1, 0,  0, 0, 0, 0, 0, 0};
  auto expected = makeRowVector({makeFlatVector(std::vector<bool>{true})});

  testSerialize(expected, data, 20);
  testDeserialize(data, 20, expected);
}

TEST_P(UnsafeRowSerializerTest, string) {
  int8_t data[28] = {0, 0, 0,  24, 0, 0, 0,  0,  0,  0,  0,  0, 5, 0,
                     0, 0, 16, 0,  0, 0, 72, 69, 76, 76, 79, 0, 0, 0};
  auto expected =
      makeRowVector({makeFlatVector(std::vector<StringView>{"HELLO"})});

  testSerialize(expected, data, 28);
  testDeserialize(data, 28, expected);
}

TEST_P(UnsafeRowSerializerTest, null) {
  int8_t data[20] = {0, 0, 0, 16, 1, 0, 0, 0, 0, 0,
                     0, 0, 0, 0,  0, 0, 0, 0, 0, 0};
  auto expected = makeRowVector({makeNullableFlatVector(
      std::vector<std::optional<int64_t>>{std::nullopt})});

  testSerialize(expected, data, 20);
  testDeserialize(data, 20, expected);
}

// The data result can be obtained by
// test("decimal serialize") {
//   val d1 = new
//   Decimal().set(BigDecimal("123456789012345678901234.57")).toPrecision(38, 2)
//   val row = InternalRow.apply(d1)
//   val unsafeRow = UnsafeProjection.create(Array[DataType](DecimalType(38,
//   2))).apply(row)
//   assert(unsafeRow.getDecimal(0, 38, 2) === d1)
//   unsafeRow.getBaseObject().asInstanceOf[Array[Byte]].foreach(b => print(b +
//   ", ")) print("\n")
// }
TEST_P(UnsafeRowSerializerTest, decimal) {
  // short decimal
  int8_t data[20] = {0, 0, 0,  16, 0,   0,   0, 0, 0, 0,
                     0, 0, 62, 28, -36, -33, 2, 0, 0, 0};
  auto expected =
      makeRowVector({makeConstant<int64_t>(12345678910, 1, DECIMAL(12, 2))});

  testSerialize(expected, data, 20);
  testDeserialize(data, 20, expected);

  // long decimal
  int8_t longData[36] = {0,  0,   0,   32,  0,   0,   0,   0, 0,  0,  0,  0,
                         11, 0,   0,   0,   16,  0,   0,   0, 10, 54, 76, -104,
                         34, 126, -86, 106, -36, -70, -63, 0, 0,  0,  0,  0};
  auto longExpected = makeRowVector({{makeConstant<int128_t>(
      HugeInt::build(
          669260, 10962463713375599297U), // 12345678901234567890123457
      1,
      DECIMAL(38, 2))}});

  testSerialize(longExpected, longData, 36);
  testDeserialize(longData, 36, longExpected);
}

TEST_P(UnsafeRowSerializerTest, manyRows) {
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

TEST_P(UnsafeRowSerializerTest, splitRow) {
  int8_t data[20] = {0, 0, 0,  16, 0,   0,   0, 0, 0, 0,
                     0, 0, 62, 28, -36, -33, 2, 0, 0, 0};
  auto expected =
      makeRowVector({makeFlatVector(std::vector<int64_t>{12345678910})});

  std::vector<std::string_view> buffers;
  const char* rawData = reinterpret_cast<const char*>(data);

  // Split input row into two buffers.
  buffers = {{rawData, 10}, {rawData + 10, 10}};
  testDeserialize(buffers, expected);

  // Split input row into many buffers.
  buffers = {
      {rawData, 4},
      {rawData + 4, 4},
      {rawData + 8, 4},
      {rawData + 12, 8},
  };
  testDeserialize(buffers, expected);

  // One byte at a time.
  buffers.clear();
  for (size_t i = 0; i < 20; i++) {
    buffers.push_back({rawData + i, 1});
  }
  testDeserialize(buffers, expected);
}

TEST_P(UnsafeRowSerializerTest, incompleteRow) {
  // The test data is for non-compression, and we don't know the compressed size
  // to construct header. If the row is incomplete, readBytes will fail.
  if (needCompression()) {
    return;
  }
  int8_t data[20] = {0, 0, 0,  16, 0,   0,   0, 0, 0, 0,
                     0, 0, 62, 28, -36, -33, 2, 0, 0, 0};
  auto expected =
      makeRowVector({makeFlatVector(std::vector<int64_t>{12345678910})});
  const char* rawData = reinterpret_cast<const char*>(data);

  std::vector<std::string_view> buffers;

  // Cut in the middle of the row.
  buffers = {{rawData, 10}};
  VELOX_ASSERT_RUNTIME_THROW(
      testDeserialize(buffers, expected), "Unable to read full serialized row");

  // Still incomplete row.
  buffers = {{rawData, 10}, {rawData, 5}};
  VELOX_ASSERT_RUNTIME_THROW(
      testDeserialize(buffers, expected), "Unable to read full serialized row");

  // Cut right after the row size.
  buffers = {{rawData, 4}};
  VELOX_ASSERT_RUNTIME_THROW(
      testDeserialize(buffers, expected), "Unable to read full serialized row");

  // Cut in the middle of the `size` integer.
  buffers = {{rawData, 2}};
  VELOX_ASSERT_RUNTIME_THROW(
      testDeserialize(buffers, expected),
      "(2 vs. 2) Reading past end of BufferInputStream");
}

TEST_P(UnsafeRowSerializerTest, types) {
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
       DECIMAL(20, 2),
       ROW({VARCHAR(), INTEGER(), DECIMAL(20, 3)}),
       ARRAY(INTEGER()),
       ARRAY(DECIMAL(20, 2)),
       ARRAY(INTEGER()),
       MAP(DECIMAL(20, 3), DECIMAL(20, 3)),
       MAP(VARCHAR(), ARRAY(INTEGER()))});

  VectorFuzzer::Options opts;
  opts.vectorSize = 5;
  opts.nullRatio = 0.1;
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

  auto data = fuzzer.fuzzInputRow(rowType);
  testRoundTrip(data);
}

TEST_P(UnsafeRowSerializerTest, date) {
  auto rowVector = makeRowVector({
      makeFlatVector<int32_t>({0, 1}, DATE()),
  });

  testRoundTrip(rowVector);
}

TEST_P(UnsafeRowSerializerTest, unknown) {
  // UNKNOWN type.
  auto rowVector = makeRowVector({
      BaseVector::createNullConstant(UNKNOWN(), 10, pool()),
  });

  testRoundTrip(rowVector);

  // ARRAY(UNKNOWN) type.
  rowVector = makeRowVector({
      makeArrayVector(
          {0, 3, 10, 15},
          BaseVector::createNullConstant(UNKNOWN(), 30, pool())),
  });

  testRoundTrip(rowVector);

  // MAP(BIGINT, UNKNOWN) type.
  rowVector = makeRowVector({
      makeMapVector(
          {0, 3, 7},
          makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7, 8, 9}),
          BaseVector::createNullConstant(UNKNOWN(), 9, pool())),
  });

  testRoundTrip(rowVector);

  // Mix of INTEGER, UNKNOWN and DOUBLE.
  rowVector = makeRowVector({
      makeNullableFlatVector<int32_t>({1, std::nullopt, 3, 4, 5}),
      BaseVector::createNullConstant(UNKNOWN(), 5, pool()),
      makeNullableFlatVector<double>(
          {1.1, 2.2, std::nullopt, 4.4, std::nullopt}),
  });
}

TEST_P(UnsafeRowSerializerTest, decimalVector) {
  auto rowVectorDecimal = makeRowVector({makeFlatVector<int128_t>(
      {
          0,
          123,
          DecimalUtil::kLongDecimalMin,
          DecimalUtil::kLongDecimalMax,
          HugeInt::build(
              669260, 10962463713375599297U), // 12345678901234567890123457
      },
      DECIMAL(20, 2))});
  testRoundTrip(rowVectorDecimal);

  auto rowVectorArray = makeRowVector({makeArrayVector(
      {0},
      makeConstant<int128_t>(
          HugeInt::build(
              669260, 10962463713375599297U), // 12345678901234567890123457
          1,
          DECIMAL(20, 2)))});

  testRoundTrip(rowVectorArray);
}

TEST_P(UnsafeRowSerializerTest, multiPage) {
  auto input =
      makeRowVector({makeFlatVector(std::vector<int64_t>{12345678910, 123})});
  std::ostringstream out;
  serialize(input, &out);
  serialize(input, &out);
  auto expected = makeRowVector({makeFlatVector(
      std::vector<int64_t>{12345678910, 123, 12345678910, 123})});
  auto rowType = std::dynamic_pointer_cast<const RowType>(input->type());
  auto deserialized = deserialize(rowType, {out.str()});
  test::assertEqualVectors(deserialized, expected);
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    UnsafeRowSerializerTest,
    UnsafeRowSerializerTest,
    testing::ValuesIn(UnsafeRowSerializerTest::getTestParams()));
