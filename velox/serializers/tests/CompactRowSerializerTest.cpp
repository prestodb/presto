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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/row/CompactRow.h"
#include "velox/serializers/RowSerializer.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::serializer {
namespace {

struct TestParam {
  common::CompressionKind compressionKind;
  bool appendRow;
  bool microBatchDeserialize;

  TestParam(
      common::CompressionKind _compressionKind,
      bool _appendRow,
      bool _microBatchDeserialize)
      : compressionKind(_compressionKind),
        appendRow(_appendRow),
        microBatchDeserialize(_microBatchDeserialize) {}
};

class CompactRowSerializerTest : public ::testing::Test,
                                 public velox::test::VectorTestBase,
                                 public testing::WithParamInterface<TestParam> {
 public:
  static std::vector<TestParam> getTestParams() {
    static std::vector<TestParam> testParams = {
        {common::CompressionKind::CompressionKind_NONE, false, false},
        {common::CompressionKind::CompressionKind_NONE, false, true},
        {common::CompressionKind::CompressionKind_ZLIB, true, false},
        {common::CompressionKind::CompressionKind_ZLIB, true, true},
        {common::CompressionKind::CompressionKind_SNAPPY, false, false},
        {common::CompressionKind::CompressionKind_SNAPPY, false, true},
        {common::CompressionKind::CompressionKind_ZSTD, true, false},
        {common::CompressionKind::CompressionKind_ZSTD, true, true},
        {common::CompressionKind::CompressionKind_LZ4, false, false},
        {common::CompressionKind::CompressionKind_LZ4, false, true},
        {common::CompressionKind::CompressionKind_GZIP, true, false},
        {common::CompressionKind::CompressionKind_GZIP, true, true}};
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
    serializer::CompactRowVectorSerde::registerVectorSerde();
    serializer::CompactRowVectorSerde::registerNamedVectorSerde();
    ASSERT_EQ(getVectorSerde()->kind(), VectorSerde::Kind::kCompactRow);
    ASSERT_EQ(
        getNamedVectorSerde(VectorSerde::Kind::kCompactRow)->kind(),
        VectorSerde::Kind::kCompactRow);
    appendRow_ = GetParam().appendRow;
    compressionKind_ = GetParam().compressionKind;
    microBatchDeserialize_ = GetParam().microBatchDeserialize;
    options_ = std::make_unique<VectorSerde::Options>(compressionKind_, 0.8);
  }

  void TearDown() override {
    deregisterVectorSerde();
    deregisterNamedVectorSerde(VectorSerde::Kind::kCompactRow);
  }

  void serialize(RowVectorPtr rowVector, std::ostream* output) {
    const auto numRows = rowVector->size();

    // Serialize with different range size.
    std::vector<IndexRange> ranges;
    vector_size_t offset = 0;
    vector_size_t rangeSize = 1;
    std::unique_ptr<row::CompactRow> compactRow;
    if (appendRow_) {
      compactRow = std::make_unique<row::CompactRow>(rowVector);
    }
    while (offset < numRows) {
      auto size = std::min<vector_size_t>(rangeSize, numRows - offset);
      ranges.push_back(IndexRange{offset, size});
      offset += size;
      rangeSize = checkedMultiply<vector_size_t>(rangeSize, 2);
    }

    auto arena = std::make_unique<StreamArena>(pool_.get());
    auto rowType = asRowType(rowVector->type());
    auto serializer = getVectorSerde()->createIterativeSerializer(
        rowType, numRows, arena.get(), options_.get());

    Scratch scratch;
    if (appendRow_) {
      std::vector<vector_size_t> serializedRowSizes(numRows);
      std::vector<vector_size_t*> serializedRowSizesPtr(numRows);
      for (auto i = 0; i < numRows; ++i) {
        serializedRowSizesPtr[i] = &serializedRowSizes[i];
      }
      for (const auto& range : ranges) {
        std::vector<vector_size_t> rows(range.size);
        for (auto i = 0; i < range.size; ++i) {
          rows[i] = range.begin + i;
        }
        getVectorSerde()->estimateSerializedSize(
            compactRow.get(), rows, serializedRowSizesPtr.data());
        serializer->append(
            *compactRow,
            folly::Range(rows.data(), rows.size()),
            serializedRowSizes);
      }
    } else {
      serializer->append(
          rowVector, folly::Range(ranges.data(), ranges.size()), scratch);
    }
    auto size = serializer->maxSerializedSize();
    OStreamOutputStream out(output);
    serializer->flush(&out);
    if (!needCompression()) {
      ASSERT_EQ(size, output->tellp());
    } else {
      ASSERT_GT(size, output->tellp());
    }
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

  RowVectorPtr concatenateRowVectors(
      const std::vector<RowVectorPtr>& rowVectors,
      velox::memory::MemoryPool* pool) {
    if (rowVectors.empty()) {
      return nullptr;
    }

    // Ensure all RowVectors have the same schema.
    auto rowType = rowVectors.front()->type();
    for (const auto& rowVector : rowVectors) {
      VELOX_CHECK(
          rowVector->type()->equivalent(*rowType),
          "RowVectors must have the same schema");
    }

    // Calculate total size.
    vector_size_t totalSize = 0;
    for (const auto& rowVector : rowVectors) {
      totalSize += rowVector->size();
    }

    // Create nulls buffer if any input has nulls
    BufferPtr nulls = nullptr;
    for (const auto& rowVector : rowVectors) {
      if (rowVector->nulls()) {
        nulls = AlignedBuffer::allocate<bool>(totalSize, pool, bits::kNotNull);
        break;
      }
    }

    // Concatenate child vectors.
    std::vector<VectorPtr> concatenatedChildren;
    for (size_t i = 0; i < rowType->size(); ++i) {
      std::vector<VectorPtr> childVectors;
      for (const auto& rowVector : rowVectors) {
        childVectors.push_back(rowVector->childAt(i));
      }
      concatenatedChildren.push_back(
          BaseVector::create(rowType->childAt(i), totalSize, pool));
      vector_size_t offset = 0;
      for (const auto& childVector : childVectors) {
        concatenatedChildren.back()->copy(
            childVector.get(), offset, 0, childVector->size());
        offset += childVector->size();
      }
    }

    // Copy nulls if needed
    if (nulls != nullptr) {
      auto rawNulls = nulls->asMutable<uint64_t>();
      vector_size_t offset = 0;
      for (const auto& rowVector : rowVectors) {
        if (rowVector->nulls()) {
          bits::copyBits(
              rowVector->nulls()->as<uint64_t>(),
              0,
              rawNulls,
              offset,
              rowVector->size());
        }
        offset += rowVector->size();
      }
    }

    return std::make_shared<RowVector>(
        pool, rowType, nulls, totalSize, std::move(concatenatedChildren));
  }

  RowVectorPtr deserialize(
      const RowTypePtr& rowType,
      const std::string_view& input) {
    auto byteStream = toByteStream(input);
    RowVectorPtr result;
    if (microBatchDeserialize_) {
      static constexpr int32_t kBatchSize = 3;
      std::unique_ptr<RowIterator> rowIterator;
      std::vector<RowVectorPtr> results;
      while (!byteStream->atEnd() ||
             (rowIterator != nullptr && rowIterator->hasNext())) {
        results.emplace_back();
        dynamic_cast<CompactRowVectorSerde*>(getVectorSerde())
            ->deserialize(
                byteStream.get(),
                rowIterator,
                kBatchSize,
                rowType,
                &results.back(),
                pool_.get(),
                options_.get());
      }
      result = concatenateRowVectors(results, pool_.get());
    } else {
      getVectorSerde()->deserialize(
          byteStream.get(), pool_.get(), rowType, &result, options_.get());
    }
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

 private:
  bool needCompression() {
    return compressionKind_ != common::CompressionKind::CompressionKind_NONE;
  }

  common::CompressionKind compressionKind_;
  std::unique_ptr<VectorSerde::Options> options_;
  bool appendRow_;
  bool microBatchDeserialize_;
};

TEST_P(CompactRowSerializerTest, fuzz) {
  const auto rowType = ROW({
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

  const auto seed = folly::Random::rand32();

  LOG(ERROR) << "Seed: " << seed;
  SCOPED_TRACE(fmt::format("seed: {}", seed));
  VectorFuzzer fuzzer(opts, pool_.get(), seed);

  auto data = fuzzer.fuzzInputRow(rowType);
  testRoundTrip(data);
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    CompactRowSerializerTest,
    CompactRowSerializerTest,
    testing::ValuesIn(CompactRowSerializerTest::getTestParams()));
} // namespace
} // namespace facebook::velox::serializer
