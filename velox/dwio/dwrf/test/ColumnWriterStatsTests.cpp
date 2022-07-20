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

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "velox/common/base/Nulls.h"
#include "velox/dwio/common/MemoryInputStream.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/test/utils/BatchMaker.h"
#include "velox/dwio/dwrf/writer/FlushPolicy.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/dwio/type/fbhive/HiveTypeParser.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

using namespace ::testing;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::test;
using namespace facebook::velox::dwrf;
using namespace facebook::velox::dwio::type::fbhive;
using namespace facebook::velox;
using namespace facebook::velox::memory;
using folly::Random;

std::unique_ptr<RowReader> writeAndGetReader(
    MemoryPool& pool,
    const std::shared_ptr<const Type>& type,
    VectorPtr batch,
    const size_t repeat,
    const int32_t flatMapColId) {
  // write file to memory
  auto sink = std::make_unique<MemorySink>(pool, 200 * 1024 * 1024);
  auto sinkPtr = sink.get();

  auto config = std::make_shared<Config>();
  config->set(Config::ROW_INDEX_STRIDE, folly::to<uint32_t>(batch->size()));
  if (flatMapColId >= 0) {
    config->set(Config::FLATTEN_MAP, true);
    config->set(Config::MAP_FLAT_COLS, {folly::to<uint32_t>(flatMapColId)});
  }
  WriterOptions options;
  options.config = config;
  options.schema = type;
  options.flushPolicyFactory = [&]() {
    return std::make_unique<LambdaFlushPolicy>([]() {
      return false; // All batches are in one stripe.
    });
  };

  Writer writer{options, std::move(sink), pool};

  for (size_t i = 0; i < repeat; ++i) {
    writer.write(batch);
  }

  writer.close();

  auto input =
      std::make_unique<MemoryInputStream>(sinkPtr->getData(), sinkPtr->size());

  ReaderOptions readerOpts;
  RowReaderOptions rowReaderOpts;
  auto reader = std::make_unique<DwrfReader>(readerOpts, std::move(input));
  return reader->createRowReader(rowReaderOpts);
}

uint64_t computeCumulativeNodeSize(
    std::unordered_map<uint32_t, uint64_t>& nodeSizes,
    const TypeWithId& type) {
  auto totalSize = nodeSizes[type.id];
  for (auto i = 0; i < type.size(); i++) {
    totalSize += computeCumulativeNodeSize(nodeSizes, *type.childAt(i));
  }
  nodeSizes[type.id] = totalSize;
  return totalSize;
}

void verifyStats(
    DwrfRowReader& rowReader,
    const size_t repeat,
    const std::vector<size_t>& nodeSizePerStride,
    const bool hasFlatMapCol) {
  ASSERT_EQ(1, rowReader.getReader().getFooter().stripes_size())
      << "Only one stripe expected";

  ASSERT_EQ(true, rowReader.getReader().getFooter().has_rawdatasize())
      << "File raw data size does not exist";

  ASSERT_EQ(
      nodeSizePerStride.at(0) * repeat,
      rowReader.getReader().getFooter().rawdatasize())
      << "File raw data size does not match";

  // Verify File Column's raw Size.
  for (auto nodeId = 0; nodeId < nodeSizePerStride.size(); nodeId++) {
    ASSERT_EQ(
        nodeSizePerStride.at(nodeId) * repeat,
        rowReader.getReader().getColumnStatistics(nodeId)->getRawSize())
        << "RawSize does not match. Node " << nodeId << " "
        << rowReader.getReader().getColumnStatistics(nodeId)->toString();
  }

  bool preload = true;
  auto stripeInfo = rowReader.loadStripe(0, preload);

  // Verify Stripe content length + index length equals size of the column 0.
  auto totalStreamSize = stripeInfo.datalength() + stripeInfo.indexlength();
  auto node_0_Size = rowReader.getReader().getColumnStatistics(0)->getSize();

  ASSERT_EQ(node_0_Size, totalStreamSize) << "Total size does not match";

  // Compute Node Size and verify the File Footer Node Size matches.
  auto& stripeFooter = rowReader.getStripeFooter();
  std::unordered_map<uint32_t, uint64_t> nodeSizes;
  for (auto&& ss : stripeFooter.streams()) {
    nodeSizes[ss.node()] += ss.length();
  }

  computeCumulativeNodeSize(
      nodeSizes, *TypeWithId::create(rowReader.getReader().getSchema()));
  for (auto nodeId = 0;
       nodeId < rowReader.getReader().getFooter().statistics_size();
       nodeId++) {
    ASSERT_EQ(
        nodeSizes[nodeId],
        rowReader.getReader().getColumnStatistics(nodeId)->getSize().value())
        << "Size does not match. Node " << nodeId << " "
        << rowReader.getReader().getColumnStatistics(nodeId)->toString();
  }

  // Verify Stride Stats.
  StripeStreamsImpl streams{
      rowReader,
      rowReader.getColumnSelector(),
      rowReader.getRowReaderOptions(),
      stripeInfo.offset(),
      rowReader,
      0};
  streams.loadReadPlan();

  // FlatMap does not write the Stride statistics (RowIndex)
  // for Key and Value Streams. Key and Value RowIndex (Stride stats)
  // are only captured if there is physical stream. FlatMap does
  // not have physical streams, so do not verify the stride stats.
  const auto strideSize = hasFlatMapCol ? 2 : nodeSizePerStride.size();

  for (auto nodeId = 0; nodeId < strideSize; nodeId++) {
    auto si = EncodingKey(nodeId).forKind(proto::Stream::ROW_INDEX);
    auto rowIndex =
        ProtoUtils::readProto<proto::RowIndex>(streams.getStream(si, true));
    EXPECT_NE(rowIndex, nullptr);
    EXPECT_EQ(rowIndex->entry_size(), repeat) << " entry size mismatch";

    for (auto count = 0; count < rowIndex->entry_size(); count++) {
      auto stridStatistics = buildColumnStatisticsFromProto(
          rowIndex->entry(count).statistics(),
          dwrf::StatsContext(WriterVersion_CURRENT));
      // TODO, take in a lambda to verify the entire statistics instead of Just
      // the rawSize.
      EXPECT_EQ(nodeSizePerStride.at(nodeId), stridStatistics->getRawSize())
          << " raw size mismatch count:" << count
          << stridStatistics->toString();
    }
  }
}

using PopulateBatch =
    std::function<std::vector<size_t>(MemoryPool&, VectorPtr*, size_t)>;

void verifyTypeStats(
    MemoryPool& pool,
    const std::string& schema,
    PopulateBatch populateBatch,
    int32_t flatMapColId = -1) {
  HiveTypeParser parser;
  auto type = parser.parse(schema);

  constexpr size_t batchSize = 1000;
  VectorPtr childBatch;
  auto nodeSizePerStride = populateBatch(pool, &childBatch, batchSize);
  VectorPtr batch = std::make_shared<RowVector>(
      &pool, type, nullptr, batchSize, std::vector<VectorPtr>{childBatch}, 0);

  constexpr size_t repeat = 2;
  auto rowReaderPtr =
      writeAndGetReader(pool, type, std::move(batch), repeat, flatMapColId);
  auto& rowReader = dynamic_cast<DwrfRowReader&>(*rowReaderPtr);
  const bool hasFlatMapCol = flatMapColId >= 0;
  verifyStats(rowReader, repeat, nodeSizePerStride, hasFlatMapCol);
}

class ColumnWriterStatsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    scopedPool = getDefaultScopedMemoryPool();
  }
  std::unique_ptr<ScopedMemoryPool> scopedPool;
};

template <typename T>
VectorPtr makeFlatVector(
    MemoryPool& pool,
    BufferPtr nulls,
    size_t nullCount,
    size_t length,
    BufferPtr values) {
  auto flatVector = std::make_shared<FlatVector<T>>(
      &pool, nulls, length, values, std::vector<BufferPtr>());
  flatVector->setNullCount(nullCount);
  return flatVector;
}

TEST_F(ColumnWriterStatsTest, Bool) {
  auto populateBoolBatch = [](MemoryPool& pool,
                              VectorPtr* vector,
                              size_t size) {
    BufferPtr nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), &pool);
    auto* nullsPtr = nulls->asMutable<uint64_t>();
    size_t nullCount = 0;

    BufferPtr values = AlignedBuffer::allocate<bool>(size, &pool);
    auto valuesPtr = values->asMutable<char>();

    for (size_t i = 0; i < size; ++i) {
      bool isNull = i & 1;
      bits::setNull(nullsPtr, i, isNull);
      if (isNull) {
        ++nullCount;
      } else {
        bits::setBit(valuesPtr, i, static_cast<bool>(i & 2));
      }
    }

    *vector = makeFlatVector<bool>(pool, nulls, nullCount, size, values);
    return std::vector<size_t>{size, size};
  };
  verifyTypeStats(
      scopedPool->getPool(), "struct<bool_val:boolean>", populateBoolBatch);
}

TEST_F(ColumnWriterStatsTest, TinyInt) {
  auto populateTinyIntBatch = [](MemoryPool& pool,
                                 VectorPtr* vector,
                                 size_t size) {
    BufferPtr nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), &pool);
    auto* nullsPtr = nulls->asMutable<uint64_t>();
    size_t nullCount = 0;

    BufferPtr values = AlignedBuffer::allocate<int8_t>(size, &pool);
    auto valuesPtr = values->asMutable<int8_t>();

    for (auto i = 0; i < size; i++) {
      bool isNull = i & 1;
      bits::setNull(nullsPtr, i, isNull);
      if (isNull) {
        ++nullCount;
      } else {
        valuesPtr[i] = static_cast<int8_t>(i);
      }
    }

    *vector = makeFlatVector<int8_t>(pool, nulls, nullCount, size, values);
    return std::vector<size_t>{size, size};
  };
  verifyTypeStats(
      scopedPool->getPool(), "struct<byte_val:tinyint>", populateTinyIntBatch);
}

TEST_F(ColumnWriterStatsTest, SmallInt) {
  auto populateSmallIntBatch = [](MemoryPool& pool,
                                  VectorPtr* vector,
                                  size_t size) {
    BufferPtr nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), &pool);
    auto* nullsPtr = nulls->asMutable<uint64_t>();
    size_t nullCount = 0;

    BufferPtr values = AlignedBuffer::allocate<int16_t>(size, &pool);
    auto valuesPtr = values->asMutable<int16_t>();

    for (auto i = 0; i < size; i++) {
      bool isNull = i & 1;
      bits::setNull(nullsPtr, i, isNull);
      if (isNull) {
        ++nullCount;
      } else {
        valuesPtr[i] = static_cast<int16_t>(i);
      }
    }

    size_t totalSize =
        nullCount * NULL_SIZE + (size - nullCount) * sizeof(int16_t);
    *vector = makeFlatVector<int16_t>(pool, nulls, nullCount, size, values);
    return std::vector<size_t>{totalSize, totalSize};
  };
  verifyTypeStats(
      scopedPool->getPool(),
      "struct<small_val:smallint>",
      populateSmallIntBatch);
}

TEST_F(ColumnWriterStatsTest, Int) {
  auto populateIntBatch = [](MemoryPool& pool, VectorPtr* vector, size_t size) {
    BufferPtr nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), &pool);
    auto* nullsPtr = nulls->asMutable<uint64_t>();
    size_t nullCount = 0;

    BufferPtr values = AlignedBuffer::allocate<int32_t>(size, &pool);
    auto valuesPtr = values->asMutable<int32_t>();

    for (auto i = 0; i < size; i++) {
      bool isNull = i & 1;
      bits::setNull(nullsPtr, i, isNull);
      if (isNull) {
        ++nullCount;
      } else {
        valuesPtr[i] = static_cast<int32_t>(i);
      }
    }

    size_t totalSize =
        nullCount * NULL_SIZE + (size - nullCount) * sizeof(int32_t);
    *vector = makeFlatVector<int32_t>(pool, nulls, nullCount, size, values);
    return std::vector<size_t>{totalSize, totalSize};
  };
  verifyTypeStats(
      scopedPool->getPool(), "struct<int_val:int>", populateIntBatch);
}

TEST_F(ColumnWriterStatsTest, Long) {
  auto populateLongBatch = [](MemoryPool& pool,
                              VectorPtr* vector,
                              size_t size) {
    BufferPtr nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), &pool);
    auto* nullsPtr = nulls->asMutable<uint64_t>();
    size_t nullCount = 0;

    BufferPtr values = AlignedBuffer::allocate<int64_t>(size, &pool);
    auto valuesPtr = values->asMutable<int64_t>();

    for (auto i = 0; i < size; i++) {
      bool isNull = i & 1;
      bits::setNull(nullsPtr, i, isNull);
      if (isNull) {
        ++nullCount;
      } else {
        valuesPtr[i] = static_cast<int64_t>(i);
      }
    }

    size_t totalSize =
        nullCount * NULL_SIZE + (size - nullCount) * sizeof(int64_t);
    *vector = makeFlatVector<int64_t>(pool, nulls, nullCount, size, values);
    return std::vector<size_t>{totalSize, totalSize};
  };
  verifyTypeStats(
      scopedPool->getPool(), "struct<long_val:bigint>", populateLongBatch);
}

auto populateFloatBatch = [](MemoryPool& pool, VectorPtr* vector, size_t size) {
  BufferPtr nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), &pool);
  auto* nullsPtr = nulls->asMutable<uint64_t>();
  size_t nullCount = 0;

  BufferPtr values = AlignedBuffer::allocate<float>(size, &pool);
  auto valuesPtr = values->asMutable<float>();

  for (auto i = 0; i < size; i++) {
    bool isNull = i & 1;
    bits::setNull(nullsPtr, i, isNull);
    if (isNull) {
      ++nullCount;
    } else {
      valuesPtr[i] = static_cast<float>(i);
    }
  }

  size_t totalSize = nullCount * NULL_SIZE + (size - nullCount) * sizeof(float);
  *vector = makeFlatVector<float>(pool, nulls, nullCount, size, values);
  return std::vector<size_t>{totalSize, totalSize};
};

TEST_F(ColumnWriterStatsTest, Float) {
  verifyTypeStats(
      scopedPool->getPool(), "struct<float_val:float>", populateFloatBatch);
}

TEST_F(ColumnWriterStatsTest, Double) {
  auto populateDoubleBatch = [](MemoryPool& pool,
                                VectorPtr* vector,
                                size_t size) {
    BufferPtr nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), &pool);
    auto* nullsPtr = nulls->asMutable<uint64_t>();
    size_t nullCount = 0;

    BufferPtr values = AlignedBuffer::allocate<double>(size, &pool);
    auto valuesPtr = values->asMutable<double>();

    for (auto i = 0; i < size; i++) {
      bool isNull = i & 1;
      bits::setNull(nullsPtr, i, isNull);
      if (isNull) {
        ++nullCount;
      } else {
        valuesPtr[i] = static_cast<double>(i);
      }
    }

    size_t totalSize =
        nullCount * NULL_SIZE + (size - nullCount) * sizeof(double);
    *vector = makeFlatVector<double>(pool, nulls, nullCount, size, values);
    return std::vector<size_t>{totalSize, totalSize};
  };
  verifyTypeStats(
      scopedPool->getPool(), "struct<long_val:double>", populateDoubleBatch);
}

TEST(ColumnWriterStats, String) {
  auto scopedPool = getDefaultScopedMemoryPool();
  auto populateStringBatch =
      [](MemoryPool& pool, VectorPtr* vector, size_t size) {
        std::mt19937 gen{};
        *vector = BatchMaker::createVector<TypeKind::VARCHAR>(
            nullptr, size, pool, gen);
        auto flatVector = (*vector)->asFlatVector<StringView>();
        size_t totalSize = 0;
        for (auto i = 0; i < size; i++) {
          if (flatVector->isNullAt(i)) {
            totalSize += NULL_SIZE;
          } else {
            totalSize += flatVector->valueAt(i).size();
          }
        }
        return std::vector<size_t>{totalSize, totalSize};
      };
  verifyTypeStats(
      scopedPool->getPool(), "struct<string_val:string>", populateStringBatch);
}

TEST(ColumnWriterStats, Binary) {
  auto scopedPool = getDefaultScopedMemoryPool();
  auto populateBinaryBatch =
      [](MemoryPool& pool, VectorPtr* vector, size_t size) {
        std::mt19937 gen{};
        *vector = BatchMaker::createVector<TypeKind::VARBINARY>(
            nullptr, size, pool, gen);
        auto flatVector = (*vector)->asFlatVector<StringView>();
        size_t totalSize = 0;
        for (auto i = 0; i < size; i++) {
          if (flatVector->isNullAt(i)) {
            totalSize += NULL_SIZE;
          } else {
            totalSize += flatVector->valueAt(i).size();
          }
        }
        return std::vector<size_t>{totalSize, totalSize};
      };
  verifyTypeStats(
      scopedPool->getPool(), "struct<binary_val:binary>", populateBinaryBatch);
}

TEST_F(ColumnWriterStatsTest, Timestamp) {
  auto populateTimestampBatch = [](MemoryPool& pool,
                                   VectorPtr* vector,
                                   size_t size) {
    BufferPtr nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), &pool);
    auto* nullsPtr = nulls->asMutable<uint64_t>();
    size_t nullCount = 0;

    BufferPtr values = AlignedBuffer::allocate<Timestamp>(size, &pool);
    auto valuesPtr = values->asMutable<Timestamp>();

    for (auto i = 0; i < size; i++) {
      bool isNull = i & 1;
      bits::setNull(nullsPtr, i, isNull);
      if (isNull) {
        ++nullCount;
      } else {
        valuesPtr[i] = Timestamp(i, 0);
      }
    }

    size_t totalSize = nullCount * NULL_SIZE +
        (size - nullCount) * (sizeof(int64_t) + sizeof(int32_t));
    *vector = makeFlatVector<Timestamp>(pool, nulls, nullCount, size, values);
    return std::vector<size_t>{totalSize, totalSize};
  };
  verifyTypeStats(
      scopedPool->getPool(),
      "struct<timestamp_val:timestamp>",
      populateTimestampBatch);
}

TEST_F(ColumnWriterStatsTest, List) {
  auto populateListBatch = [](MemoryPool& pool,
                              VectorPtr* vector,
                              size_t size) {
    BufferPtr nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), &pool);
    auto* nullsPtr = nulls->asMutable<uint64_t>();
    size_t nullCount = 0;

    auto offsets = AlignedBuffer::allocate<vector_size_t>(size, &pool);
    auto* offsetsPtr = offsets->asMutable<vector_size_t>();

    auto lengths = AlignedBuffer::allocate<vector_size_t>(size, &pool);
    auto* lengthsPtr = lengths->asMutable<vector_size_t>();

    size_t childSize = 0;
    // Populate list offsets.
    for (auto i = 0; i < size; i++) {
      offsetsPtr[i] = childSize;
      bool isNull = i & 1;
      bits::setNull(nullsPtr, i, isNull);
      if (isNull) {
        ++nullCount;
      } else {
        lengthsPtr[i] = 2;
        childSize += 2;
      }
    }

    // Populate List's child batch.
    VectorPtr childVector;
    auto nodeSizePerStride = populateFloatBatch(pool, &childVector, childSize);
    *vector = std::make_shared<ArrayVector>(
        &pool,
        CppToType<Array<float>>::create(),
        nulls,
        size,
        offsets,
        lengths,
        childVector,
        nullCount);
    size_t totalSize = nullCount * NULL_SIZE + nodeSizePerStride.at(0);
    return std::vector<size_t>{totalSize, totalSize, nodeSizePerStride.at(0)};
  };
  verifyTypeStats(
      scopedPool->getPool(),
      "struct<array_val:array<float>>",
      populateListBatch);
}

TEST_F(ColumnWriterStatsTest, Map) {
  auto populateMapBatch = [](MemoryPool& pool, VectorPtr* vector, size_t size) {
    BufferPtr nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), &pool);
    auto* nullsPtr = nulls->asMutable<uint64_t>();
    size_t nullCount = 0;

    auto offsets = AlignedBuffer::allocate<vector_size_t>(size, &pool);
    auto* offsetsPtr = offsets->asMutable<vector_size_t>();

    auto lengths = AlignedBuffer::allocate<vector_size_t>(size, &pool);
    auto* lengthsPtr = lengths->asMutable<vector_size_t>();

    size_t childSize = 0;
    for (auto i = 0; i < size; i++) {
      offsetsPtr[i] = childSize;
      bool isNull = i & 1;
      bits::setNull(nullsPtr, i, isNull);
      if (isNull) {
        ++nullCount;
      } else {
        lengthsPtr[i] = 2;
        childSize += 2;
      }
    }

    auto keys = AlignedBuffer::allocate<int32_t>(childSize, &pool);
    auto* keysPtr = keys->asMutable<int32_t>();
    for (auto i = 0; i < childSize; i++) {
      keysPtr[i] = i & 3;
    }
    VectorPtr keyVector = makeFlatVector<int32_t>(pool, nullptr, 0, size, keys);

    VectorPtr valueVector;
    auto nodeSizePerStride = populateFloatBatch(pool, &valueVector, childSize);
    *vector = std::make_shared<MapVector>(
        &pool,
        CppToType<Map<int32_t, float>>::create(),
        nulls,
        size,
        offsets,
        lengths,
        keyVector,
        valueVector,
        nullCount);
    size_t keySize = childSize * sizeof(int32_t);
    size_t valueSize = nodeSizePerStride.at(0);
    size_t totalSize = nullCount * NULL_SIZE + keySize + valueSize;
    return std::vector<size_t>{totalSize, totalSize, keySize, valueSize};
  };
  verifyTypeStats(
      scopedPool->getPool(),
      "struct<map_val:map<int,float>>",
      populateMapBatch);
  const uint32_t FLAT_MAP_COL_ID = 0;
  verifyTypeStats(
      scopedPool->getPool(),
      "struct<map_val:map<int,float>>",
      populateMapBatch,
      FLAT_MAP_COL_ID);
}

TEST_F(ColumnWriterStatsTest, Struct) {
  auto populateStructBatch = [](MemoryPool& pool,
                                VectorPtr* vector,
                                size_t size) {
    BufferPtr nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), &pool);
    auto* nullsPtr = nulls->asMutable<uint64_t>();
    size_t nullCount = 0;

    for (auto i = 0; i < size; i++) {
      bool isNull = i & 1;
      bits::setNull(nullsPtr, i, isNull);
      if (isNull) {
        ++nullCount;
      }
    }

    // set 0 for node 0 and node 1 size and backfill it after computing child
    // node sizes.
    std::vector<size_t> nodeSizePerStride = {0, 0};
    std::vector<VectorPtr> children;
    for (auto child = 0; child < 2; child++) {
      VectorPtr childVector;
      populateFloatBatch(pool, &childVector, size);
      children.push_back(childVector);

      size_t floatBatchSize = 0;
      for (auto i = 0; i < size; i++) {
        // Count only float sizes, when struct is not null.
        if (!bits::isBitNull(nullsPtr, i)) {
          floatBatchSize +=
              childVector->isNullAt(i) ? NULL_SIZE : sizeof(float);
        }
      }
      nodeSizePerStride.push_back(floatBatchSize);
    }
    *vector = std::make_shared<RowVector>(
        &pool,
        CppToType<Row<float, float>>::create(),
        nulls,
        size,
        children,
        nullCount);
    nodeSizePerStride.at(0) =
        nullCount + nodeSizePerStride.at(2) + nodeSizePerStride.at(3);
    nodeSizePerStride.at(1) =
        nullCount + nodeSizePerStride.at(2) + nodeSizePerStride.at(3);

    return nodeSizePerStride;
  };
  verifyTypeStats(
      scopedPool->getPool(),
      "struct<struct_val:struct<a:float,b:float>>",
      populateStructBatch);
}
