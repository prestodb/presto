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
#include "velox/vector/tests/VectorTestBase.h"

using namespace facebook::velox;

class UnsafeRowSerializerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = memory::getDefaultScopedMemoryPool();
    serde_ = std::make_unique<serializer::spark::UnsafeRowVectorSerde>();
  }

  void serialize(RowVectorPtr rowVector, std::ostream* output) {
    auto numRows = rowVector->size();

    std::vector<IndexRange> rows(numRows);
    for (int i = 0; i < numRows; i++) {
      rows[i] = IndexRange{i, 1};
    }

    auto arena =
        std::make_unique<StreamArena>(memory::MappedMemory::getInstance());
    auto rowType = std::dynamic_pointer_cast<const RowType>(rowVector->type());
    auto serializer = serde_->createSerializer(rowType, numRows, arena.get());

    serializer->append(rowVector, folly::Range(rows.data(), numRows));
    OStreamOutputStream out(output);
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

  void testRoundTrip(RowVectorPtr rowVector) {
    std::ostringstream out;
    serialize(rowVector, &out);

    auto rowType = std::dynamic_pointer_cast<const RowType>(rowVector->type());
    auto deserialized = deserialize(rowType, out.str());
    test::assertEqualVectors(deserialized, rowVector);
  }

  std::unique_ptr<memory::MemoryPool> pool_;
  std::unique_ptr<VectorSerde> serde_;
};

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
  // Spark uses microseconds to store timestamp
  opts.useMicrosecondPrecisionTimestamp = true;
  opts.containerLength = 10;

  auto seed = folly::Random::rand32();

  LOG(ERROR) << "Seed: " << seed;
  SCOPED_TRACE(fmt::format("seed: {}", seed));
  VectorFuzzer fuzzer(opts, pool_.get(), seed);

  auto data = fuzzer.fuzzRow(rowType);
  testRoundTrip(data);
}
