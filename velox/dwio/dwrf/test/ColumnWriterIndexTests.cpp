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
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/dwrf/test/OrcTest.h"
#include "velox/dwio/dwrf/writer/ColumnWriter.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

using namespace ::testing;
using namespace facebook::velox;
using namespace facebook::velox::dwrf;
using namespace facebook::velox::dwio::common;

using facebook::velox::memory::MemoryPool;

namespace facebook::velox::dwrf {

namespace {
template <typename Integer>
Integer generateDistinctValues(size_t i, size_t /* unused */) {
  return i;
}

template <typename Integer>
Integer generateSomewhatRandomData(size_t i, size_t size) {
  int64_t val = size % (i + 1);
  return i % 2 ? val : -val;
}

std::string alphabeticRoundRobin(size_t index, size_t size) {
  char element = 'a' + index % 26;
  return std::string(size % (index + 1), element);
}

bool someNulls(size_t i, size_t size) {
  return (size % (i + 1)) % 2;
}

bool neverAbandonDict(size_t /* unused */, size_t /* unused */) {
  return false;
}

std::function<bool(size_t, size_t)> abandonNthWrite(size_t n) {
  return [n](size_t stripeCount, size_t pageCount) {
    return stripeCount == 0 && pageCount == n;
  };
}

bool abandonEveryWrite(size_t /* unused */, size_t /* unused */) {
  return true;
}

template <typename T>
VectorPtr prepBatchImpl(
    size_t size,
    MemoryPool* pool,
    std::function<T(size_t, size_t)> genData,
    std::function<bool(size_t, size_t)> genNulls) {
  BufferPtr nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), pool);
  auto* nullsPtr = nulls->asMutable<uint64_t>();
  size_t nullCount = 0;

  BufferPtr values = AlignedBuffer::allocate<T>(size, pool);
  auto valuesPtr = values->asMutableRange<T>();

  for (size_t i = 0; i < size; ++i) {
    bool isPresent = genNulls(i, size);
    bits::setNull(nullsPtr, i, !isPresent);

    if (!isPresent) {
      nullCount++;
      // valuesPtr[i] = 0;
    } else {
      valuesPtr[i] = genData(i, size);
    }
  }

  auto batch = std::make_shared<FlatVector<T>>(
      pool, nulls, size, values, std::vector<BufferPtr>());
  batch->setNullCount(nullCount);
  return batch;
}

VectorPtr prepStringBatchImpl(
    size_t size,
    MemoryPool* pool,
    std::function<std::string(size_t, size_t)> genData,
    std::function<bool(size_t, size_t)> genNulls) {
  BufferPtr nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), pool);
  auto* nullsPtr = nulls->asMutable<uint64_t>();
  size_t nullCount = 0;

  BufferPtr values = AlignedBuffer::allocate<StringView>(size, pool);
  auto* valuesPtr = values->asMutable<StringView>();

  std::vector<BufferPtr> dataChunks;
  BufferPtr dataChunk = AlignedBuffer::allocate<char>(1024, pool);
  dataChunks.push_back(dataChunk);
  auto* dataChunkPtr = dataChunk->asMutable<char>();
  size_t offset = 0;

  // Prepare input
  for (size_t i = 0; i < size; ++i) {
    bool isPresent = genNulls(i, size);
    bits::setNull(nullsPtr, i, !isPresent);

    if (isPresent) {
      auto val = genData(i, size);
      if (offset + val.size() > dataChunk->capacity()) {
        dataChunk = AlignedBuffer::allocate<char>(1024, pool);
        dataChunks.push_back(dataChunk);
        dataChunkPtr = dataChunk->asMutable<char>();
        offset = 0;
      }

      memcpy(dataChunkPtr + offset, val.data(), val.size());
      valuesPtr[i] = StringView(dataChunkPtr + offset, val.size());
      offset += val.size();
    } else {
      nullCount++;
    }
  }

  auto batch = std::make_shared<FlatVector<StringView>>(
      pool, nulls, size, values, std::move(dataChunks));
  batch->setNullCount(nullCount);
  return batch;
}

void validateBasicStats(const VectorPtr& batch, const ColumnStatistics& stats) {
  ASSERT_EQ(batch->getNullCount().value() > 0, stats.hasNull().value());
  ASSERT_EQ(
      batch->size() - batch->getNullCount().value(), stats.getNumberOfValues());
}

template <typename INTEGER>
void validateIntStats(const VectorPtr& batch, const ColumnStatistics& stats) {
  int64_t min = std::numeric_limits<int64_t>::max();
  int64_t max = std::numeric_limits<int64_t>::min();
  int64_t sum = 0;
  auto* converted = batch->as<FlatVector<INTEGER>>();
  for (size_t i = 0; i < converted->size(); ++i) {
    if (!converted->isNullAt(i)) {
      auto val = converted->valueAt(i);
      if (val < min) {
        min = val;
      }
      if (val > max) {
        max = val;
      }
      sum += val;
    }
  }
  auto& intStats = dynamic_cast<const IntegerColumnStatistics&>(stats);
  ASSERT_EQ(min, intStats.getMinimum());
  ASSERT_EQ(max, intStats.getMaximum());
  ASSERT_EQ(sum, intStats.getSum());
}

void validateBooleanStats(
    const VectorPtr& batch,
    const ColumnStatistics& stats) {
  uint64_t trueCount = 0;
  auto* converted = batch->as<FlatVector<bool>>();
  for (size_t i = 0; i < converted->size(); ++i) {
    if (!converted->isNullAt(i)) {
      if (converted->valueAt(i)) {
        ++trueCount;
      }
    }
  }
  auto& boolStats = dynamic_cast<const BooleanColumnStatistics&>(stats);
  ASSERT_EQ(trueCount, boolStats.getTrueCount());
}

void validateStringStats(
    const VectorPtr& batch,
    const ColumnStatistics& stats) {
  auto* converted = batch->as<FlatVector<StringView>>();
  std::string min;
  std::string max;
  int64_t length = 0;
  bool first = true;
  for (size_t i = 0; i < converted->size(); ++i) {
    if (!converted->isNullAt(i)) {
      auto val = converted->valueAt(i).str();
      if (first) {
        min = val;
        max = val;
        first = false;
      } else {
        if (val < min) {
          min = val;
        }
        if (val > max) {
          max = val;
        }
      }
      length += val.size();
    }
  }
  auto& strStats = dynamic_cast<const StringColumnStatistics&>(stats);
  ASSERT_EQ(min, strStats.getMinimum());
  ASSERT_EQ(max, strStats.getMaximum());
  ASSERT_EQ(length, strStats.getTotalLength());
}

template <typename FLOAT>
void validateDoubleStats(
    const VectorPtr& batch,
    const ColumnStatistics& stats) {
  double min = std::numeric_limits<double>::infinity();
  double max = -std::numeric_limits<double>::infinity();
  double sum = 0;
  auto* converted = batch->as<FlatVector<FLOAT>>();
  for (size_t i = 0; i < converted->size(); ++i) {
    if (!converted->isNullAt(i)) {
      auto val = converted->valueAt(i);
      if (val < min) {
        min = val;
      }
      if (val > max) {
        max = val;
      }
      sum += val;
    }
  }
  auto& doubleStats = dynamic_cast<const DoubleColumnStatistics&>(stats);
  ASSERT_EQ(min, doubleStats.getMinimum());
  ASSERT_EQ(max, doubleStats.getMaximum());
  ASSERT_EQ(sum, doubleStats.getSum());
}

void validateBinaryStats(
    const VectorPtr& batch,
    const ColumnStatistics& stats) {
  auto* converted = batch->as<FlatVector<StringView>>();
  int64_t length = 0;
  for (size_t i = 0; i < converted->size(); ++i) {
    if (!converted->isNullAt(i)) {
      length += converted->valueAt(i).size();
    }
  }
  auto& binStats = dynamic_cast<const BinaryColumnStatistics&>(stats);
  ASSERT_EQ(length, binStats.getTotalLength());
}

} // namespace

template <typename Type>
class WriterEncodingIndexTest2 {
 public:
  WriterEncodingIndexTest2()
      : config_{std::make_shared<Config>()},
        scopedPool_{facebook::velox::memory::getDefaultScopedMemoryPool()} {}

  virtual ~WriterEncodingIndexTest2() = default;

  void runTest(
      size_t pageCount,
      size_t recordPositionCount,
      size_t backfillPositionCount,
      size_t stripeCount,
      bool isRoot = false) {
    runTest(
        pageCount,
        std::vector<size_t>{recordPositionCount},
        std::vector<size_t>{backfillPositionCount},
        stripeCount,
        isRoot);
  }

  void runTest(
      size_t pageCount,
      const std::vector<size_t>& recordPositionCount,
      const std::vector<size_t>& backfillPositionCount,
      size_t stripeCount,
      bool isRoot = false,
      size_t flatMapOffset = 0) {
    auto isFlatMap = isRoot && flatMapOffset > 0;
    ASSERT_EQ(recordPositionCount.size(), backfillPositionCount.size());
    WriterContext context{config_, velox::memory::getDefaultScopedMemoryPool()};
    std::vector<StrictMock<MockIndexBuilder>*> mocks;
    for (auto i = 0; i < recordPositionCount.size(); ++i) {
      mocks.push_back(new StrictMock<MockIndexBuilder>());
    }
    // Hack: for flat map, value node is destructed after each stripe and hence
    // index build is also destructed. So reset pos when all the pointers are
    // used, and meanwhile, create new index builder after every flush
    size_t pos = 0;
    context.indexBuilderFactory_ =
        [&](auto stream) -> std::unique_ptr<IndexBuilder> {
      if (pos < recordPositionCount.size()) {
        auto ptr = std::unique_ptr<StrictMock<MockIndexBuilder>>(mocks[pos++]);
        if (isFlatMap && pos == mocks.size()) {
          pos = flatMapOffset + 1;
        }
        return ptr;
      } else {
        return std::make_unique<IndexBuilder>(std::move(stream));
      }
    };
    auto type = CppToType<Type>::create();
    auto typeWithId = TypeWithId::create(type, isRoot ? 0 : 1);
    auto batch = prepBatch(1000, &scopedPool_->getPool(), isRoot);

    // Creating a stream calls recordPosition.
    for (auto n = 0; n < mocks.size(); ++n) {
      EXPECT_CALL(*mocks.at(n), add(0, -1)).Times(recordPositionCount[n]);
    }
    auto columnWriter = BaseColumnWriter::create(context, *typeWithId);

    // Indices are captured the same way for all stripes in the derived tests.
    for (size_t j = 0; j != stripeCount; ++j) {
      for (size_t i = 0; i != pageCount; ++i) {
        columnWriter->write(batch, Ranges::of(0, 1000));
        for (auto n = 0; n < mocks.size(); ++n) {
          EXPECT_CALL(*mocks.at(n), addEntry(_))
              .WillOnce(Invoke([&, k = n](const StatisticsBuilder& builder) {
                auto stats = builder.build();
                if (k == 0) {
                  validateStats(batch, *stats);
                }
              }));
          // RecordPosition is invoked on call to createIndexEntry
          EXPECT_CALL(*mocks.at(n), add(_, -1)).Times(recordPositionCount[n]);
        }
        columnWriter->createIndexEntry();
      }

      for (auto n = 0; n < mocks.size(); ++n) {
        if (backfillPositionCount[n]) {
          // Backfill starting positions for starting stride.
          // All Inherited tests have starting position as 0.
          EXPECT_CALL(*mocks.at(n), add(0, 0)).Times(backfillPositionCount[n]);
          for (size_t i = 1; i != pageCount + 1; ++i) {
            // Backfilling all but PRESENT positions for all subsequent strides.
            EXPECT_CALL(*mocks.at(n), add(_, i))
                .Times(backfillPositionCount[n]);
          }
        }
        EXPECT_CALL(*mocks.at(n), flush());
        if (!(isFlatMap && n > flatMapOffset)) {
          // Recording positions for the new stripe.
          EXPECT_CALL(*mocks.at(n), add(0, -1)).Times(recordPositionCount[n]);
        }
      }
      proto::StripeFooter stripeFooter;
      columnWriter->flush(
          [&stripeFooter](uint32_t /* unused */) -> proto::ColumnEncoding& {
            return *stripeFooter.add_encoding();
          });

      // Simulate continue writing to next stripe, so internally buffered data
      // is cleared
      context.nextStripe();
      columnWriter->reset();

      // create new index builders
      if (isFlatMap && j < stripeCount - 1) {
        EXPECT_EQ(pos, flatMapOffset + 1);
        for (auto i = flatMapOffset + 1; i < recordPositionCount.size(); ++i) {
          mocks[i] = new StrictMock<MockIndexBuilder>();
          // Recording positions for the new stripe.
          EXPECT_CALL(*mocks.at(i), add(0, -1)).Times(recordPositionCount[i]);
        }
      }
    }
  }

 protected:
  virtual VectorPtr prepBatch(size_t size, MemoryPool* pool) = 0;

  virtual VectorPtr prepBatch(size_t size, MemoryPool* pool, bool isRoot) {
    return prepBatch(size, pool);
  }

  virtual void validateStats(
      const VectorPtr& batch,
      const ColumnStatistics& stats) {
    validateBasicStats(batch, stats);
  }

  std::shared_ptr<Config> config_;
  std::unique_ptr<facebook::velox::memory::ScopedMemoryPool> scopedPool_;
};

class TimestampWriterIndexTest : public testing::Test,
                                 public WriterEncodingIndexTest2<Timestamp> {
 public:
  TimestampWriterIndexTest() : WriterEncodingIndexTest2<Timestamp>() {}

 protected:
  VectorPtr prepBatch(size_t size, MemoryPool* pool) override {
    return prepBatchImpl<Timestamp>(
        size,
        &scopedPool_->getPool(),
        [](size_t i, size_t /*unused*/) -> Timestamp {
          return Timestamp(i, i);
        },
        someNulls);
  }

  // timestamp column only has basic stats, so validation method in base class
  // is sufficient
};

TEST_F(TimestampWriterIndexTest, TestIndex) {
  // Present Stream has 4. seconds and nanos are Ints with 3 each.
  // 4+3+3 = 10. There is no backfill.
  this->runTest(2, 10, 0, 0);
  this->runTest(2, 10, 0, 1);
  this->runTest(2, 10, 0, 100);
}

template <typename Integer>
class IntegerColumnWriterDictionaryEncodingIndexTest
    : public testing::Test,
      public WriterEncodingIndexTest2<Integer> {
 public:
  explicit IntegerColumnWriterDictionaryEncodingIndexTest()
      : WriterEncodingIndexTest2<Integer>() {
    this->config_->set(Config::DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD, 1.0f);
  }

 protected:
  VectorPtr prepBatch(size_t size, MemoryPool* pool) override {
    return prepBatchImpl<Integer>(
        size, pool, generateSomewhatRandomData<Integer>, someNulls);
  }

  void validateStats(const VectorPtr& batch, const ColumnStatistics& stats)
      override {
    validateBasicStats(batch, stats);
    validateIntStats<Integer>(batch, stats);
  }
};

using DictionaryTypes = ::testing::Types<int16_t, int32_t, int64_t>;
VELOX_TYPED_TEST_SUITE(
    IntegerColumnWriterDictionaryEncodingIndexTest,
    DictionaryTypes);

TYPED_TEST(IntegerColumnWriterDictionaryEncodingIndexTest, WriteAllStreams) {
  // Present stream uses 4 positions.
  // compressed size, uncompressed size, ByteRLE numLiterals,
  // Boolean 8-bitsRemaining
  constexpr size_t RECORD_POSITION_COUNT = 4;

  // Data Stream 3 Positions
  // In Dictionary 4 positions (Similar to Present Stream)
  constexpr size_t BACKFILL_POSITION_COUNT = 7;

  this->runTest(1, RECORD_POSITION_COUNT, BACKFILL_POSITION_COUNT, 0);

  this->runTest(1, RECORD_POSITION_COUNT, BACKFILL_POSITION_COUNT, 1);
  this->runTest(1, RECORD_POSITION_COUNT, BACKFILL_POSITION_COUNT, 100);
}

TYPED_TEST(IntegerColumnWriterDictionaryEncodingIndexTest, OmitInDictStream) {
  // Present stream uses 4 positions.
  // compressed size, uncompressed size, ByteRLE numLiterals,
  // Boolean 8-bitsRemaining
  constexpr size_t RECORD_POSITION_COUNT = 4;

  // Data Stream 3 Positions
  constexpr size_t BACKFILL_POSITION_COUNT = 3;

  // Writing the batch twice so that all values are in dictionary.
  this->runTest(2, RECORD_POSITION_COUNT, BACKFILL_POSITION_COUNT, 0);
  this->runTest(2, RECORD_POSITION_COUNT, BACKFILL_POSITION_COUNT, 1);
  this->runTest(2, RECORD_POSITION_COUNT, BACKFILL_POSITION_COUNT, 100);
}

class BoolColumnWriterEncodingIndexTest
    : public testing::Test,
      public WriterEncodingIndexTest2<bool> {
 public:
  explicit BoolColumnWriterEncodingIndexTest()
      : WriterEncodingIndexTest2<bool>() {}

 protected:
  VectorPtr prepBatch(size_t size, MemoryPool* pool) override {
    return prepBatchImpl<bool>(
        size,
        pool,
        [](size_t i, size_t /*unused*/) -> bool { return true; },
        [](size_t i, size_t /*unused*/) -> bool { return i % 3 == 0; });
  }

  void validateStats(const VectorPtr& batch, const ColumnStatistics& stats)
      override {
    validateBasicStats(batch, stats);
    validateBooleanStats(batch, stats);
  }
};

TEST_F(BoolColumnWriterEncodingIndexTest, TestIndex) {
  // Boolean Stream uses one Stream for Presence and Other Stream for data
  // Each Stream has compressed size, uncompressed size, ByteRLE numLiterals,
  // Boolean 8-bitsRemaining , so 4+4 =8. It has no back fill streams.
  this->runTest(2, 8, 0, 0);
  this->runTest(2, 8, 0, 1);
  this->runTest(2, 8, 0, 100);
}

class ByteColumnWriterEncodingIndexTest
    : public testing::Test,
      public WriterEncodingIndexTest2<int8_t> {
 public:
  explicit ByteColumnWriterEncodingIndexTest()
      : WriterEncodingIndexTest2<int8_t>() {}

 protected:
  VectorPtr prepBatch(size_t size, MemoryPool* pool) override {
    int8_t value = 0;
    return prepBatchImpl<int8_t>(
        size,
        pool,
        [&value](size_t i, size_t /*unused*/) -> int8_t { return value++; },
        [](size_t i, size_t /*unused*/) -> bool { return i % INT8_MAX == 0; });
  }

  void validateStats(const VectorPtr& batch, const ColumnStatistics& stats)
      override {
    validateBasicStats(batch, stats);
    validateIntStats<int8_t>(batch, stats);
  }
};

TEST_F(ByteColumnWriterEncodingIndexTest, TestIndex) {
  // Byte Stream has presence Stream and data stream.
  // Present Stream has compressed size, uncompressed size, ByteRLE numLiterals,
  // Boolean 8-bitsRemaining (4)
  // Data stream has compressed size, uncompressed size, ByteRLE numLiterals (3)
  // so total Streams(4+3) = 7. It has no back fill streams.

  this->runTest(2, 7, 0, 0);
  this->runTest(2, 7, 0, 1);
  this->runTest(2, 7, 0, 100);
}

class BinaryColumnWriterEncodingIndexTest
    : public testing::Test,
      public WriterEncodingIndexTest2<Varbinary> {
 public:
  BinaryColumnWriterEncodingIndexTest() = default;

 protected:
  VectorPtr prepBatch(size_t size, MemoryPool* pool) override {
    BufferPtr nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), pool);
    auto* nullsPtr = nulls->asMutable<uint64_t>();
    size_t nullCount = 0;

    BufferPtr values = AlignedBuffer::allocate<StringView>(size, pool);
    auto* valuesPtr = values->asMutable<StringView>();

    auto data = AlignedBuffer::allocate<char>(100, pool);
    auto dataPtr = data->asMutable<char>();
    std::memset(dataPtr, 'a', 100);

    size_t value = 0;
    size_t nullEvery = 10;
    for (size_t i = 0; i < size; ++i) {
      if (i % nullEvery != 0) {
        bits::clearNull(nullsPtr, i);
        valuesPtr[i] = StringView(dataPtr, value++);
        value %= 50;
      } else {
        bits::setNull(nullsPtr, i);
        nullCount++;
      }
    }

    auto batch = std::make_shared<FlatVector<StringView>>(
        pool, nulls, size, values, std::vector<BufferPtr>{data});
    batch->setNullCount(nullCount);
    return batch;
  }

  void validateStats(const VectorPtr& batch, const ColumnStatistics& stats)
      override {
    validateBasicStats(batch, stats);
    validateBinaryStats(batch, stats);
  }
};

TEST_F(BinaryColumnWriterEncodingIndexTest, TestIndex) {
  // Binary Stream has present, data and length.
  // Present Stream has compressed size, uncompressed size, ByteRLE numLiterals,
  // Boolean 8-bitsRemaining (4)
  // Data stream has compressed size, uncompressed size (2)
  // Length stream has compressed size, uncompressed size, RLE numLiterals (3)
  // so total Streams(4+2+3) = 9. It has no back fill streams.

  this->runTest(2, 9, 0, 0);
  this->runTest(2, 9, 0, 1);
  this->runTest(2, 9, 0, 100);
}

template <typename FLOAT>
class FloatColumnWriterEncodingIndexTest
    : public testing::Test,
      public WriterEncodingIndexTest2<FLOAT> {
 public:
  FloatColumnWriterEncodingIndexTest() = default;

 protected:
  VectorPtr prepBatch(size_t size, MemoryPool* pool) override {
    return prepBatchImpl<FLOAT>(
        size,
        pool,
        [](size_t i, size_t /*unused*/) -> float { return i; },
        [](size_t i, size_t /*unused*/) -> bool { return i % 30 != 0; });
  }

  void validateStats(const VectorPtr& batch, const ColumnStatistics& stats)
      override {
    validateBasicStats(batch, stats);
    validateDoubleStats<FLOAT>(batch, stats);
  }
};

using FloatTypes = ::testing::Types<float, double>;
VELOX_TYPED_TEST_SUITE(FloatColumnWriterEncodingIndexTest, FloatTypes);

TYPED_TEST(FloatColumnWriterEncodingIndexTest, TestIndex) {
  // Float Stream has present and data.
  // Present Stream has compressed size, uncompressed size, ByteRLE numLiterals,
  // Boolean 8-bitsRemaining (4)
  // Data stream has uncompressed size, ByteRLE numLiterals (2)
  // so total Streams(4+2) = 6. It has no back fill streams.

  this->runTest(2, 6, 0, 0);
  this->runTest(2, 6, 0, 1);
  this->runTest(2, 6, 0, 100);
}

class IntegerColumnWriterDirectEncodingIndexTest : public testing::Test {
 public:
  explicit IntegerColumnWriterDirectEncodingIndexTest(bool abandonDict = false)
      : pool_{memory::getDefaultScopedMemoryPool()},
        config_{std::make_shared<Config>()},
        abandonDict_{abandonDict} {
    config_->set(
        Config::DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD,
        abandonDict ? 1.0f : 0.0f);
  }

 protected:
  void testForAllTypes(
      size_t pageCount,
      size_t positionCount,
      size_t stripeCount,
      std::function<bool(size_t, size_t)> callAbandonDict = neverAbandonDict) {
    runTest<int16_t>(pageCount, positionCount, stripeCount, callAbandonDict);
    runTest<int32_t>(pageCount, positionCount, stripeCount, callAbandonDict);
    runTest<int64_t>(pageCount, positionCount, stripeCount, callAbandonDict);
  }

  template <typename Integer>
  VectorPtr prepBatch(
      size_t size,
      std::function<Integer(size_t, size_t)> genData,
      std::function<bool(size_t, size_t)> genNulls) {
    return prepBatchImpl<Integer>(size, pool_.get(), genData, genNulls);
  }

  template <typename Integer>
  void validateStats(const VectorPtr& batch, const ColumnStatistics& stats) {
    validateBasicStats(batch, stats);
    validateIntStats<Integer>(batch, stats);
  }

  template <typename Integer>
  void runTest(
      size_t pageCount,
      size_t positionCount,
      size_t stripeCount,
      std::function<bool(size_t, size_t)> callAbandonDict) {
    WriterContext context{config_, velox::memory::getDefaultScopedMemoryPool()};
    auto mockIndexBuilder = std::make_unique<StrictMock<MockIndexBuilder>>();
    auto mockIndexBuilderPtr = mockIndexBuilder.get();
    context.indexBuilderFactory_ = [&](auto /* unused */) {
      return std::move(mockIndexBuilder);
    };
    auto type = CppToType<Integer>::create();
    auto typeWithId = TypeWithId::create(type, 1);
    auto batch =
        prepBatch<Integer>(1000, generateDistinctValues<Integer>, someNulls);

    // ColumnWriter::recordPosition to capture PRESENT stream positions.
    // Compression + BufferedOutputStream + byteRLE + booleanRLE
    EXPECT_CALL(*mockIndexBuilderPtr, add(0, -1)).Times(4);
    auto columnWriter = BaseColumnWriter::create(context, *typeWithId);

    for (size_t j = 0; j != stripeCount; ++j) {
      // We need to convert from dictionary in the first stripe. This part calls
      // index builder in a very similar pattern to that of dictionary encoding.
      // When we need to abandon dictionary, however, we do need to backfill all
      // strides so far with direct encoding positions.
      if (j == 0) {
        size_t currentPage = 0;
        while (currentPage < pageCount) {
          if (abandonDict_) {
            if (callAbandonDict(j, currentPage)) {
              // Backfilling starting positions for all but PRESENT stream.
              EXPECT_CALL(*mockIndexBuilderPtr, add(0, 0))
                  .Times(positionCount - 4);
              for (size_t k = 1; k <= currentPage; ++k) {
                // Backfilling all but PRESENT positions for all subsequent
                // strides.
                EXPECT_CALL(*mockIndexBuilderPtr, add(_, k))
                    .Times(positionCount - 4);
              }
              columnWriter->tryAbandonDictionaries(true);
              break;
            }
          }
          columnWriter->write(batch, Ranges::of(0, 1000));
          EXPECT_CALL(*mockIndexBuilderPtr, addEntry(_))
              .WillOnce(Invoke([&](const StatisticsBuilder& builder) {
                auto stats = builder.build();
                validateStats<Integer>(batch, *stats);
              }));
          // PRESENT stream positions are always recorded as soon as we create
          // new entries.
          EXPECT_CALL(*mockIndexBuilderPtr, add(_, -1)).Times(4);
          columnWriter->createIndexEntry();
          ++currentPage;
        }

        // The rest of the strides are all written directly in direct encoding.
        for (size_t i = currentPage + 1; i < pageCount; ++i) {
          columnWriter->write(batch, Ranges::of(0, 1000));
          if (abandonDict_) {
            if (callAbandonDict(j, i)) {
              // These calls should essentially be no-ops.
              columnWriter->tryAbandonDictionaries(true);
            }
          }
          EXPECT_CALL(*mockIndexBuilderPtr, addEntry(_))
              .WillOnce(Invoke([&](const StatisticsBuilder& builder) {
                auto stats = builder.build();
                validateStats<Integer>(batch, *stats);
              }));
          // We can record all stream positions immediately now that we have
          // determined the encoding.
          EXPECT_CALL(*mockIndexBuilderPtr, add(_, -1)).Times(positionCount);
          columnWriter->createIndexEntry();
        }

        if (!abandonDict_) {
          // Backfilling starting positions for all but PRESENT stream.
          EXPECT_CALL(*mockIndexBuilderPtr, add(0, 0)).Times(positionCount - 4);
          for (size_t i = 1; i != pageCount + 1; ++i) {
            // Backfilling all but PRESENT positions for all subsequent strides.
            EXPECT_CALL(*mockIndexBuilderPtr, add(_, i))
                .Times(positionCount - 4);
          }
        }
        EXPECT_CALL(*mockIndexBuilderPtr, flush());
        // Recording starting positions for the new stripe right after flush for
        // *all* streams
        EXPECT_CALL(*mockIndexBuilderPtr, add(0, -1)).Times(positionCount);
        proto::StripeFooter stripeFooter;
        columnWriter->flush(
            [&stripeFooter](uint32_t /* unused */) -> proto::ColumnEncoding& {
              return *stripeFooter.add_encoding();
            });
      } else {
        for (size_t i = 0; i != pageCount; ++i) {
          columnWriter->write(batch, Ranges::of(0, 1000));
          if (abandonDict_) {
            if (callAbandonDict(j, i)) {
              // These calls should essentially be no-ops.
              columnWriter->tryAbandonDictionaries(true);
            }
          }
          EXPECT_CALL(*mockIndexBuilderPtr, addEntry(_))
              .WillOnce(Invoke([&](const StatisticsBuilder& builder) {
                auto stats = builder.build();
                validateStats<Integer>(batch, *stats);
              }));
          // We can record all stream positions immediately now that we have
          // determined the encoding.
          EXPECT_CALL(*mockIndexBuilderPtr, add(_, -1)).Times(positionCount);
          columnWriter->createIndexEntry();
        }
        EXPECT_CALL(*mockIndexBuilderPtr, flush());
        EXPECT_CALL(*mockIndexBuilderPtr, add(0, -1)).Times(positionCount);
        proto::StripeFooter stripeFooter;
        columnWriter->flush(
            [&stripeFooter](uint32_t /* unused */) -> proto::ColumnEncoding& {
              return *stripeFooter.add_encoding();
            });
      }

      // Simulate continue writing to next stripe, so internally buffered data
      // is cleared
      context.nextStripe();
      columnWriter->reset();
    }
  }

  std::unique_ptr<memory::ScopedMemoryPool> pool_;
  std::shared_ptr<Config> config_;
  bool abandonDict_;
};

TEST_F(IntegerColumnWriterDirectEncodingIndexTest, ConvertFromDictionary) {
  testForAllTypes(1, 6, 0);
  testForAllTypes(10, 6, 0);
  // 6 positions: {PRESENT, 4}, {DATA, 2}
  testForAllTypes(1, 6, 1);
  testForAllTypes(10, 6, 1);
}

TEST_F(IntegerColumnWriterDirectEncodingIndexTest, DirectWrites) {
  // Same 6 positions but recorded at different points in time.
  testForAllTypes(1, 6, 2);
  testForAllTypes(10, 6, 100);
}

class IntegerColumnWriterAbandonDictionaryIndexTest
    : public IntegerColumnWriterDirectEncodingIndexTest {
 public:
  explicit IntegerColumnWriterAbandonDictionaryIndexTest()
      : IntegerColumnWriterDirectEncodingIndexTest{true} {}
};

TEST_F(IntegerColumnWriterAbandonDictionaryIndexTest, AbandonDictionary) {
  testForAllTypes(1, 6, 0, abandonEveryWrite);
  testForAllTypes(10, 6, 0, abandonEveryWrite);
  // 6 positions: {PRESENT, 4}, {DATA, 2}
  testForAllTypes(1, 6, 5, abandonNthWrite(0));
  testForAllTypes(10, 6, 5, abandonNthWrite(7));
  testForAllTypes(1, 6, 5, abandonEveryWrite);
  testForAllTypes(10, 6, 5, abandonEveryWrite);
}

class StringColumnWriterDictionaryEncodingIndexTest : public testing::Test {
 public:
  explicit StringColumnWriterDictionaryEncodingIndexTest()
      : pool_{memory::getDefaultScopedMemoryPool()},
        config_{std::make_shared<Config>()} {
    config_->set(
        Config::STRING_STATS_LIMIT, std::numeric_limits<uint32_t>::max());
    config_->set(Config::ENTROPY_KEY_STRING_SIZE_THRESHOLD, 0.0f);
    config_->set(Config::DICTIONARY_STRING_KEY_SIZE_THRESHOLD, 1.0f);
  }

 protected:
  VectorPtr prepBatch(
      size_t size,
      MemoryPool* pool,
      std::function<std::string(size_t, size_t)> genData,
      std::function<bool(size_t, size_t)> genNulls) {
    return prepStringBatchImpl(size, pool, genData, genNulls);
  }

  void validateStats(const VectorPtr& batch, const ColumnStatistics& stats) {
    validateBasicStats(batch, stats);
    validateStringStats(batch, stats);
  }

  void runTest(size_t pageCount, size_t positionCount, size_t stripeCount) {
    WriterContext context{config_, velox::memory::getDefaultScopedMemoryPool()};
    auto mockIndexBuilder = std::make_unique<StrictMock<MockIndexBuilder>>();
    auto mockIndexBuilderPtr = mockIndexBuilder.get();
    context.indexBuilderFactory_ = [&](auto /* unused */) {
      return std::move(mockIndexBuilder);
    };
    auto type = CppToType<folly::StringPiece>::create();
    auto typeWithId = TypeWithId::create(type, 1);
    auto batch = prepBatch(1000, pool_.get(), alphabeticRoundRobin, someNulls);

    // ColumnWriter::recordPosition to capture PRESENT stream positions.
    // Compression + BufferedOutputStream + byteRLE + booleanRLE
    EXPECT_CALL(*mockIndexBuilderPtr, add(0, -1)).Times(4);
    auto columnWriter = BaseColumnWriter::create(context, *typeWithId);

    // Indices are captured the same way for all stripes when using dictionary
    // encoding.
    for (size_t j = 0; j != stripeCount; ++j) {
      for (size_t i = 0; i != pageCount; ++i) {
        columnWriter->write(batch, Ranges::of(0, 1000));
        EXPECT_CALL(*mockIndexBuilderPtr, addEntry(_))
            .WillOnce(Invoke([&](const StatisticsBuilder& builder) {
              auto stats = builder.build();
              validateStats(batch, *stats);
            }));
        // PRESENT stream positions are always recorded as soon as we create new
        // entries.
        EXPECT_CALL(*mockIndexBuilderPtr, add(_, -1)).Times(4);
        columnWriter->createIndexEntry();
      }
      // Backfill starting positions for all but PRESENT stream, and the stride
      // dictionary size for the first stride.
      EXPECT_CALL(*mockIndexBuilderPtr, add(_, 0)).Times(positionCount - 4);
      for (size_t i = 1; i != pageCount + 1; ++i) {
        // Backfilling all but PRESENT positions for all subsequent strides.
        EXPECT_CALL(*mockIndexBuilderPtr, add(_, i)).Times(positionCount - 4);
      }
      EXPECT_CALL(*mockIndexBuilderPtr, flush());
      // Recording PRESENT stream starting positions for the new stripe.
      EXPECT_CALL(*mockIndexBuilderPtr, add(0, -1)).Times(4);
      proto::StripeFooter stripeFooter;
      columnWriter->flush(
          [&stripeFooter](uint32_t /* unused */) -> proto::ColumnEncoding& {
            return *stripeFooter.add_encoding();
          });

      // Simulate continue writing to next stripe, so internally buffered data
      // is cleared
      context.nextStripe();
      columnWriter->reset();
    }
  }

  std::unique_ptr<memory::ScopedMemoryPool> pool_;
  std::shared_ptr<Config> config_;
};

TEST_F(StringColumnWriterDictionaryEncodingIndexTest, WriteAllStreams) {
  runTest(1, 17, 0);
  // 17 positions: {PRESENT, 4}, {STRIDE_DICT_DATA, 2}, {STRIDE_DICT_LENGTH, 3},
  // {stride dict size, 1}, {IN_DICTIONARY, 4}, {DATA, 3}
  runTest(1, 17, 1);
  runTest(1, 17, 100);
}

TEST_F(StringColumnWriterDictionaryEncodingIndexTest, OmitInDictStream) {
  runTest(2, 7, 0);
  // Writing the batch twice so that all values are in dictionary.
  // 7 positions: {PRESENT, 4}, {DATA, 3}
  runTest(2, 7, 1);
  runTest(2, 7, 100);
}

class StringColumnWriterDirectEncodingIndexTest : public testing::Test {
 public:
  explicit StringColumnWriterDirectEncodingIndexTest(bool abandonDict = false)
      : pool_{memory::getDefaultScopedMemoryPool()},
        config_{std::make_shared<Config>()},
        abandonDict_{abandonDict} {
    config_->set(
        Config::STRING_STATS_LIMIT, std::numeric_limits<uint32_t>::max());
    config_->set(Config::ENTROPY_KEY_STRING_SIZE_THRESHOLD, 0.0f);
    config_->set(Config::DICTIONARY_STRING_KEY_SIZE_THRESHOLD, 0.0f);
  }

 protected:
  VectorPtr prepBatch(
      size_t size,
      std::function<std::string(size_t, size_t)> genData,
      std::function<bool(size_t, size_t)> genNulls) {
    return prepStringBatchImpl(size, pool_.get(), genData, genNulls);
  }

  void validateStats(const VectorPtr& batch, const ColumnStatistics& stats) {
    validateBasicStats(batch, stats);
    validateStringStats(batch, stats);
  }

  void runTest(
      size_t pageCount,
      size_t positionCount,
      size_t stripeCount,
      std::function<bool(size_t, size_t)> callAbandonDict = neverAbandonDict) {
    WriterContext context{config_, velox::memory::getDefaultScopedMemoryPool()};
    auto mockIndexBuilder = std::make_unique<StrictMock<MockIndexBuilder>>();
    auto mockIndexBuilderPtr = mockIndexBuilder.get();
    context.indexBuilderFactory_ = [&](auto /* unused */) {
      return std::move(mockIndexBuilder);
    };
    auto type = CppToType<folly::StringPiece>::create();
    auto typeWithId = TypeWithId::create(type, 1);
    auto batch = prepBatch(1000, alphabeticRoundRobin, someNulls);

    // ColumnWriter::recordPosition to capture PRESENT stream positions.
    // Compression + BufferedOutputStream + byteRLE + booleanRLE
    EXPECT_CALL(*mockIndexBuilderPtr, add(0, -1)).Times(4);
    auto columnWriter = BaseColumnWriter::create(context, *typeWithId);

    for (size_t j = 0; j != stripeCount; ++j) {
      // We need to convert from dictionary in the first stripe. This part calls
      // index builder in a very similar pattern to that of dictionary encoding.
      // When we need to abandon dictionary, however, we do need to backfill all
      // strides so far with direct encoding positions.
      if (j == 0) {
        size_t currentPage = 0;
        while (currentPage < pageCount) {
          if (abandonDict_) {
            if (callAbandonDict(j, currentPage)) {
              // Backfilling starting positions for all but PRESENT stream.
              EXPECT_CALL(*mockIndexBuilderPtr, add(0, 0))
                  .Times(positionCount - 4);
              for (size_t k = 1; k <= currentPage; ++k) {
                // Backfilling all but PRESENT positions for all subsequent
                // strides.
                EXPECT_CALL(*mockIndexBuilderPtr, add(_, k))
                    .Times(positionCount - 4);
              }
              columnWriter->tryAbandonDictionaries(true);
              break;
            }
          }
          columnWriter->write(batch, Ranges::of(0, 1000));
          EXPECT_CALL(*mockIndexBuilderPtr, addEntry(_))
              .WillOnce(Invoke([&](const StatisticsBuilder& builder) {
                auto stats = builder.build();
                validateStats(batch, *stats);
              }));
          // PRESENT stream positions are always recorded as soon as we create
          // new entries.
          EXPECT_CALL(*mockIndexBuilderPtr, add(_, -1)).Times(4);
          columnWriter->createIndexEntry();
          ++currentPage;
        }

        // The rest of the strides are all written directly in direct encoding.
        for (size_t i = currentPage + 1; i < pageCount; ++i) {
          columnWriter->write(batch, Ranges::of(0, 1000));
          if (abandonDict_) {
            if (callAbandonDict(j, i)) {
              // These calls should essentially be no-ops.
              columnWriter->tryAbandonDictionaries(true);
            }
          }
          EXPECT_CALL(*mockIndexBuilderPtr, addEntry(_))
              .WillOnce(Invoke([&](const StatisticsBuilder& builder) {
                auto stats = builder.build();
                validateStats(batch, *stats);
              }));
          // We can record all stream positions immediately now that we have
          // determined the encoding.
          EXPECT_CALL(*mockIndexBuilderPtr, add(_, -1)).Times(positionCount);
          columnWriter->createIndexEntry();
        }

        if (!abandonDict_) {
          // Backfilling starting positions for all but PRESENT stream.
          EXPECT_CALL(*mockIndexBuilderPtr, add(0, 0)).Times(positionCount - 4);
          for (size_t i = 1; i != pageCount + 1; ++i) {
            // Backfilling all but PRESENT positions for all subsequent strides.
            EXPECT_CALL(*mockIndexBuilderPtr, add(_, i))
                .Times(positionCount - 4);
          }
        }
        EXPECT_CALL(*mockIndexBuilderPtr, flush());
        // Recording starting positions for the new stripe right after flush for
        // *all* streams
        EXPECT_CALL(*mockIndexBuilderPtr, add(0, -1)).Times(positionCount);
        proto::StripeFooter stripeFooter;
        columnWriter->flush(
            [&stripeFooter](uint32_t /* unused */) -> proto::ColumnEncoding& {
              return *stripeFooter.add_encoding();
            });
      } else {
        for (size_t i = 0; i != pageCount; ++i) {
          columnWriter->write(batch, Ranges::of(0, 1000));
          if (abandonDict_) {
            if (callAbandonDict(j, i)) {
              // These calls should essentially be no-ops.
              columnWriter->tryAbandonDictionaries(true);
            }
          }
          EXPECT_CALL(*mockIndexBuilderPtr, addEntry(_))
              .WillOnce(Invoke([&](const StatisticsBuilder& builder) {
                auto stats = builder.build();
                validateStats(batch, *stats);
              }));
          // We can record all stream positions immediately now that we have
          // determined the encoding.
          EXPECT_CALL(*mockIndexBuilderPtr, add(_, -1)).Times(positionCount);
          columnWriter->createIndexEntry();
        }
        EXPECT_CALL(*mockIndexBuilderPtr, flush());
        EXPECT_CALL(*mockIndexBuilderPtr, add(0, -1)).Times(positionCount);
        proto::StripeFooter stripeFooter;
        columnWriter->flush(
            [&stripeFooter](uint32_t /* unused */) -> proto::ColumnEncoding& {
              return *stripeFooter.add_encoding();
            });
      }

      // Simulate continue writing to next stripe, so internally buffered data
      // is cleared
      context.nextStripe();
      columnWriter->reset();
    }
  }

  std::unique_ptr<memory::ScopedMemoryPool> pool_;
  std::shared_ptr<Config> config_;
  bool abandonDict_;
};

TEST_F(StringColumnWriterDirectEncodingIndexTest, ConvertFromDictionary) {
  runTest(1, 9, 0);
  runTest(10, 9, 0);
  // 9 positions: {PRESENT, 4}, {DATA, 2}, {DATA_LENGTH, 3}
  runTest(1, 9, 1);
  runTest(10, 9, 1);
}

TEST_F(StringColumnWriterDirectEncodingIndexTest, DirectWrites) {
  // Same 9 positions but recorded at different points in time.
  runTest(1, 9, 2);
  runTest(10, 9, 100);
}

class StringColumnWriterAbandonDictionaryIndexTest
    : public StringColumnWriterDirectEncodingIndexTest {
 public:
  explicit StringColumnWriterAbandonDictionaryIndexTest()
      : StringColumnWriterDirectEncodingIndexTest{true} {}
};

TEST_F(StringColumnWriterAbandonDictionaryIndexTest, AbandonDictionary) {
  runTest(1, 9, 0, abandonEveryWrite);
  runTest(10, 9, 0, abandonEveryWrite);
  // 9 positions: {PRESENT, 4}, {DATA, 2}, {DATA_LENGTH, 3}
  runTest(1, 9, 5, abandonNthWrite(0));
  runTest(10, 9, 5, abandonNthWrite(7));
  runTest(1, 9, 5, abandonEveryWrite);
  runTest(10, 9, 5, abandonEveryWrite);
}

class ListColumnWriterEncodingIndexTest
    : public testing::Test,
      public WriterEncodingIndexTest2<Array<float>> {
 public:
  ListColumnWriterEncodingIndexTest() = default;

 protected:
  VectorPtr prepBatch(size_t size, MemoryPool* pool) override {
    auto nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), pool);
    auto* nullsPtr = nulls->asMutable<uint64_t>();
    size_t nullCount = 0;

    auto offsets = AlignedBuffer::allocate<vector_size_t>(size, pool);
    auto* offsetsPtr = offsets->asMutable<vector_size_t>();

    auto lengths = AlignedBuffer::allocate<vector_size_t>(size, pool);
    auto* lengthsPtr = lengths->asMutable<vector_size_t>();

    size_t value = 0;
    size_t nullEvery = 10;
    // set up list
    auto offset = 0;
    for (size_t i = 0; i < size; i++) {
      if (i % nullEvery == 0) {
        bits::setNull(nullsPtr, i);
        nullCount++;
      } else {
        bits::clearNull(nullsPtr, i);
        offsetsPtr[i] = offset;
        lengthsPtr[i] = value++;
        offset += lengthsPtr[i];
        value %= 50;
      }
    }
    offsetsPtr[size] = offset;

    auto noNulls = [](size_t i, size_t /*unused*/) -> bool { return false; };

    return std::make_shared<ArrayVector>(
        pool,
        CppToType<Array<float>>::create(),
        nulls,
        size,
        offsets,
        lengths,
        prepBatchImpl<float>(
            offset,
            pool,
            [](size_t i, size_t /*unused*/) -> float { return i; },
            noNulls),
        nullCount);
  }
};

TEST_F(ListColumnWriterEncodingIndexTest, TestIndex) {
  // List Stream has present and length.
  // Present Stream has compressed size, uncompressed size, ByteRLE numLiterals,
  // Boolean 8-bitsRemaining (4)
  // Length stream has compressed size, uncompressed size, RLE numLiterals (3)
  // so total Streams(4+3) = 7. It has no back fill streams.

  this->runTest(2, 7, 0, 0);
  this->runTest(2, 7, 0, 1);
  this->runTest(2, 7, 0, 100);
}

class MapColumnWriterEncodingIndexTest
    : public testing::Test,
      public WriterEncodingIndexTest2<Map<float, float>> {
 public:
  MapColumnWriterEncodingIndexTest() = default;

 protected:
  VectorPtr prepBatch(size_t size, MemoryPool* pool) override {
    auto nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), pool);
    auto* nullsPtr = nulls->asMutable<uint64_t>();
    size_t nullCount = 0;

    auto offsets = AlignedBuffer::allocate<vector_size_t>(size, pool);
    auto* offsetsPtr = offsets->asMutable<vector_size_t>();

    auto lengths = AlignedBuffer::allocate<vector_size_t>(size, pool);
    auto* lengthsPtr = lengths->asMutable<vector_size_t>();

    size_t value = 0;
    size_t nullEvery = 10;
    // set up list
    auto offset = 0;
    for (size_t i = 0; i < size; i++) {
      if (i % nullEvery == 0) {
        bits::setNull(nullsPtr, i);
        nullCount++;
      } else {
        bits::clearNull(nullsPtr, i);
        offsetsPtr[i] = offset;
        lengthsPtr[i] = value++;
        offset += lengthsPtr[i];
        value %= 50;
      }
    }
    offsetsPtr[size] = offset;

    auto noNulls = [](size_t i, size_t /*unused*/) -> bool { return false; };

    return std::make_shared<MapVector>(
        pool,
        CppToType<Map<float, float>>::create(),
        nulls,
        size,
        offsets,
        lengths,
        prepBatchImpl<float>(
            offset,
            pool,
            [](size_t i, size_t /*unused*/) -> float { return i; },
            noNulls),
        prepBatchImpl<float>(
            offset,
            pool,
            [](size_t i, size_t /*unused*/) -> float { return i; },
            noNulls),
        nullCount);
  }
};

TEST_F(MapColumnWriterEncodingIndexTest, TestIndex) {
  // Map Stream has present and length.
  // Present Stream has compressed size, uncompressed size, ByteRLE numLiterals,
  // Boolean 8-bitsRemaining (4)
  // Length stream has compressed size, uncompressed size, RLE numLiterals (3)
  // so total Streams(4+3) = 7. It has no back fill streams.

  this->runTest(2, 7, 0, 0);
  this->runTest(2, 7, 0, 1);
  this->runTest(2, 7, 0, 100);
}

class FlatMapColumnWriterEncodingIndexTest
    : public testing::Test,
      public WriterEncodingIndexTest2<Row<Map<int32_t, Map<int32_t, float>>>> {
 public:
  FlatMapColumnWriterEncodingIndexTest() = default;

 protected:
  VectorPtr prepBatch(size_t size, MemoryPool* pool) override {
    auto offsets = AlignedBuffer::allocate<vector_size_t>(size, pool);
    auto* offsetsPtr = offsets->asMutable<vector_size_t>();
    auto offsets2 = AlignedBuffer::allocate<vector_size_t>(size, pool);
    auto* offsets2Ptr = offsets2->asMutable<vector_size_t>();

    auto lengths = AlignedBuffer::allocate<vector_size_t>(size, pool);
    auto* lengthsPtr = lengths->asMutable<vector_size_t>();
    auto lengths2 = AlignedBuffer::allocate<vector_size_t>(size, pool);
    auto* lengths2Ptr = lengths2->asMutable<vector_size_t>();

    for (size_t i = 0; i < size; i++) {
      offsetsPtr[i] = i;
      offsets2Ptr[i] = i;
      lengthsPtr[i] = 1;
      lengths2Ptr[i] = 1;
    }

    auto noNulls = [](size_t /*unused*/, size_t /*unused*/) -> bool {
      return true;
    };
    auto inner = std::make_shared<MapVector>(
        pool,
        MAP(INTEGER(), REAL()),
        nullptr,
        size,
        offsets2,
        lengths2,
        prepBatchImpl<int32_t>(
            size,
            pool,
            [](size_t i, size_t /*unused*/) -> int32_t { return i; },
            noNulls),
        prepBatchImpl<float>(
            size,
            pool,
            [](size_t i, size_t /*unused*/) -> float { return i; },
            noNulls),
        0);

    auto outer = std::make_shared<MapVector>(
        pool,
        MAP(INTEGER(), MAP(INTEGER(), REAL())),
        nullptr,
        size,
        offsets,
        lengths,
        prepBatchImpl<int32_t>(
            size,
            pool,
            [](size_t i, size_t /*unused*/) -> int32_t { return i % 2; },
            noNulls),
        inner,
        0);

    std::vector<VectorPtr> children{outer};
    return std::make_shared<RowVector>(
        pool,
        ROW({MAP(INTEGER(), MAP(INTEGER(), REAL()))}),
        nullptr,
        size,
        children,
        0);
  }
};

TEST_F(FlatMapColumnWriterEncodingIndexTest, TestIndex) {
  config_->set(Config::FLATTEN_MAP, true);
  config_->set(Config::MAP_FLAT_COLS, {0});

  // #0 is root, 0
  // #1 is a flat map, present 4 = 4
  // #2 is regular map, in_map 4 + present 4 + length 3 = 11
  // #3 is int direct: present 4 + data 2 = 6
  // #4 is float: present 4 + data 2 = 6

  // empty flat map doesn't create value writer
  this->runTest(2, {0, 4}, {0, 0}, 0, true, 1);
  this->runTest(
      2, {0, 4, 11, 6, 6, 11, 6, 6}, {0, 0, 0, 0, 0, 0, 0, 0}, 1, true, 1);
  this->runTest(
      2, {0, 4, 11, 6, 6, 11, 6, 6}, {0, 0, 0, 0, 0, 0, 0, 0}, 100, true, 1);
}

class StructColumnWriterEncodingIndexTest
    : public testing::Test,
      public WriterEncodingIndexTest2<Row<float, float>> {
 public:
  StructColumnWriterEncodingIndexTest() = default;

 protected:
  VectorPtr prepBatch(size_t size, MemoryPool* pool) override {
    return prepBatch(size, pool, false);
  }

  VectorPtr prepBatch(size_t size, MemoryPool* pool, bool isRoot) override {
    auto nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), pool);
    auto* nullsPtr = nulls->asMutable<uint64_t>();
    size_t nullCount = 0;

    size_t nullEvery = 10;
    for (size_t i = 0; i < size; i++) {
      bool isNull = !isRoot && (i % nullEvery == 0);
      bits::setNull(nullsPtr, i, isNull);
      if (isNull) {
        nullCount++;
      }
    }

    auto noNulls = [](size_t i, size_t /*unused*/) -> bool { return false; };

    std::vector<VectorPtr> children(2);
    for (size_t i = 0; i < 2; i++) {
      children[i] = prepBatchImpl<float>(
          size,
          pool,
          [](size_t i, size_t /*unused*/) -> float { return i; },
          noNulls);
    }

    return std::make_shared<RowVector>(
        pool,
        CppToType<Row<float, float>>::create(),
        nulls,
        size,
        children,
        nullCount);
  }
};

TEST_F(StructColumnWriterEncodingIndexTest, TestIndex) {
  // Struct Stream has present stream
  // Present Stream has compressed size, uncompressed size, ByteRLE numLiterals,
  // Boolean 8-bitsRemaining (4)
  this->runTest(2, 4, 0, 0, false);
  this->runTest(2, 4, 0, 1, false);
  this->runTest(2, 4, 0, 100, false);

  // For root, we don't have present stream, so 0
  this->runTest(2, 0, 0, 0, true);
  this->runTest(2, 0, 0, 1, true);
  this->runTest(2, 0, 0, 100, true);
}

} // namespace facebook::velox::dwrf
