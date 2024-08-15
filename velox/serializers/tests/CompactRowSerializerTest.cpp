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
#include "velox/serializers/CompactRowSerializer.h"
#include <gtest/gtest.h>
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::serializer {
namespace {

class CompactRowSerializerTest : public ::testing::Test,
                                 public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool();
    serde_ = std::make_unique<serializer::CompactRowVectorSerde>();
  }

  void serialize(RowVectorPtr rowVector, std::ostream* output) {
    auto numRows = rowVector->size();

    std::vector<IndexRange> rows(numRows);
    for (int i = 0; i < numRows; i++) {
      rows[i] = IndexRange{i, 1};
    }

    auto arena = std::make_unique<StreamArena>(pool_.get());
    auto rowType = asRowType(rowVector->type());
    auto serializer =
        serde_->createIterativeSerializer(rowType, numRows, arena.get());

    Scratch scratch;
    serializer->append(rowVector, folly::Range(rows.data(), numRows), scratch);
    auto size = serializer->maxSerializedSize();
    OStreamOutputStream out(output);
    serializer->flush(&out);
    ASSERT_EQ(size, output->tellp());
  }

  std::unique_ptr<ByteInputStream> toByteStream(
      const std::string_view& input,
      size_t pageSize = 32) {
    auto rawBytes = reinterpret_cast<uint8_t*>(const_cast<char*>(input.data()));
    size_t offset = 0;
    std::vector<ByteRange> ranges;

    // Split the input buffer into many different pages.
    while (offset < input.length()) {
      ranges.push_back({
          rawBytes + offset,
          std::min<int32_t>(pageSize, input.length() - offset),
          0,
      });
      offset += pageSize;
    }

    return std::make_unique<BufferInputStream>(std::move(ranges));
  }

  RowVectorPtr deserialize(
      const RowTypePtr& rowType,
      const std::string_view& input) {
    auto byteStream = toByteStream(input);

    RowVectorPtr result;
    serde_->deserialize(byteStream.get(), pool_.get(), rowType, &result);
    return result;
  }

  void testRoundTrip(RowVectorPtr rowVector) {
    std::ostringstream out;
    serialize(rowVector, &out);

    auto rowType = asRowType(rowVector->type());
    auto deserialized = deserialize(rowType, out.str());
    test::assertEqualVectors(deserialized, rowVector);
  }

  std::shared_ptr<memory::MemoryPool> pool_;
  std::unique_ptr<VectorSerde> serde_;
};

TEST_F(CompactRowSerializerTest, fuzz) {
  auto rowType = ROW({
      BOOLEAN(),
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
      MAP(VARCHAR(), INTEGER()),
      MAP(VARCHAR(), ARRAY(INTEGER())),
  });

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

} // namespace
} // namespace facebook::velox::serializer
