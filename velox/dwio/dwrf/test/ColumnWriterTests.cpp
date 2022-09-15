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
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <optional>
#include <vector>
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/IntDecoder.h"
#include "velox/dwio/common/MemoryInputStream.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/dwio/common/exception/Exception.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/dwio/common/tests/utils/MapBuilder.h"
#include "velox/dwio/dwrf/common/DecoderUtil.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/type/Type.h"
#include "velox/vector/DictionaryVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

using namespace ::testing;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwrf;
using namespace facebook::velox::test;
using namespace facebook::velox;
using namespace facebook::velox::memory;
using folly::Random;

namespace facebook::velox::dwrf {

class MockStrideIndexProvider : public StrideIndexProvider {
 public:
  MOCK_CONST_METHOD0(getStrideIndex, uint64_t());
};

class MockStreamInformation : public StreamInformation {
 public:
  explicit MockStreamInformation(const DwrfStreamIdentifier& streamIdentifier)
      : streamIdentifier_{streamIdentifier} {}

  StreamKind getKind() const override {
    return streamIdentifier_.kind();
  }

  uint32_t getNode() const override {
    return streamIdentifier_.encodingKey().node;
  }

  uint32_t getSequence() const override {
    return streamIdentifier_.encodingKey().sequence;
  }

  MOCK_CONST_METHOD0(getOffset, uint64_t());
  MOCK_CONST_METHOD0(getLength, uint64_t());
  MOCK_CONST_METHOD0(getUseVInts, bool());
  MOCK_CONST_METHOD0(valid, bool());

 private:
  const DwrfStreamIdentifier& streamIdentifier_;
};

class TestStripeStreams : public StripeStreamsBase {
 public:
  TestStripeStreams(
      WriterContext& context,
      const proto::StripeFooter& footer,
      const std::shared_ptr<const RowType>& rowType,
      bool returnFlatVector = false,
      std::unordered_map<uint32_t, std::vector<std::string>>
          structReaderContext = {})
      : StripeStreamsBase{&memory::getProcessDefaultMemoryManager().getRoot()},
        context_{context},
        footer_{footer},
        selector_{rowType} {
    options_.setReturnFlatVector(returnFlatVector);
    if (!structReaderContext.empty()) {
      options_.setFlatmapNodeIdsAsStruct(structReaderContext);
    }
  }

  std::unique_ptr<SeekableInputStream> getStream(
      const DwrfStreamIdentifier& si,
      bool throwIfNotFound) const override {
    const DataBufferHolder* stream = nullptr;
    if (context_.hasStream(si)) {
      stream = std::addressof(context_.getStream(si));
    }
    if (!stream || stream->isSuppressed()) {
      if (throwIfNotFound) {
        DWIO_RAISE(fmt::format(
            "stream (node = {}, seq = {}, column = {}, kind = {}) not found",
            si.encodingKey().node,
            si.encodingKey().sequence,
            si.column(),
            si.kind()));
      } else {
        return nullptr;
      }
    }

    auto buf = std::make_unique<DataBuffer<char>>(
        context_.getMemoryPool(MemoryUsageCategory::GENERAL), 0);
    stream->spill(*buf);
    auto compressed =
        std::make_unique<SeekableArrayInputStream>(buf->data(), buf->size());
    buffers_.push_back(std::move(buf));

    return createDecompressor(
        context_.compression,
        std::move(compressed),
        context_.compressionBlockSize,
        getMemoryPool(),
        si.toString());
  }

  const proto::ColumnEncoding& getEncoding(
      const EncodingKey& ek) const override {
    for (auto& enc : footer_.encoding()) {
      if (ek.node == enc.node() && ek.sequence == enc.sequence()) {
        return enc;
      }
    }
    DWIO_RAISE("encoding not found");
  }

  uint32_t visitStreamsOfNode(
      uint32_t node,
      std::function<void(const StreamInformation&)> visitor) const override {
    uint32_t count = 0;
    context_.iterateUnSuppressedStreams([&](auto& pair) {
      if (pair.first.encodingKey().node == node) {
        visitor(MockStreamInformation(pair.first));
        ++count;
      }
    });

    return count;
  }

  const ColumnSelector& getColumnSelector() const override {
    return selector_;
  }

  const RowReaderOptions& getRowReaderOptions() const override {
    return options_;
  }

  bool getUseVInts(const DwrfStreamIdentifier& streamId) const override {
    DWIO_ENSURE(
        context_.hasStream(streamId),
        fmt::format("Stream not found: {}", streamId.toString()));
    return context_.getConfig(Config::USE_VINTS);
  }

  const StrideIndexProvider& getStrideIndexProvider() const override {
    return mockStrideIndexProvider_;
  }

  const StrictMock<MockStrideIndexProvider>& getMockStrideIndexProvider() {
    return mockStrideIndexProvider_;
  }

  uint32_t rowsPerRowGroup() const override {
    VELOX_UNSUPPORTED();
  }

 private:
  WriterContext& context_;
  const proto::StripeFooter& footer_;
  ColumnSelector selector_;
  RowReaderOptions options_;
  mutable std::vector<std::unique_ptr<DataBuffer<char>>> buffers_;
  StrictMock<MockStrideIndexProvider> mockStrideIndexProvider_;
};

constexpr uint32_t ITERATIONS = 100'000;

template <typename T>
VectorPtr populateBatch(
    std::vector<std::optional<T>> const& data,
    MemoryPool* pool) {
  BufferPtr values = AlignedBuffer::allocate<T>(data.size(), pool);
  auto valuesPtr = values->asMutableRange<T>();

  BufferPtr nulls =
      AlignedBuffer::allocate<char>(bits::nbytes(data.size()), pool);
  auto* nullsPtr = nulls->asMutable<uint64_t>();
  size_t index = 0;
  size_t nullCount = 0;
  for (auto val : data) {
    if (val) {
      valuesPtr[index] = val.value();
      bits::clearNull(nullsPtr, index);
    } else {
      bits::setNull(nullsPtr, index);
      ++nullCount;
    }
    ++index;
  }

  auto batch = std::make_shared<FlatVector<T>>(
      pool, nulls, data.size(), values, std::vector<BufferPtr>{});
  batch->setNullCount(nullCount);
  return batch;
}

template <typename T>
void verifyValue(
    const std::shared_ptr<FlatVector<T>>& fv,
    size_t index,
    T value,
    const uint32_t seed,
    bool* failed) {
  *failed = value != fv->valueAt(index);
  ASSERT_EQ(value, fv->valueAt(index))
      << "value mismatch at " << index << " with seed " << seed;
}

template <>
void verifyValue(
    const std::shared_ptr<FlatVector<Timestamp>>& fv,
    size_t index,
    Timestamp timestamp,
    const uint32_t seed,
    bool* failed) {
  *failed = true;
  auto v = fv->valueAt(index);
  if (timestamp.getNanos() > 0 && timestamp.getSeconds() == -1) {
    // This value should be corrupted by Java Writer. Ensure that
    // same behavior happens in the CPP as well. Look into
    // the TimestampColumnWriter comment on the reason behind this.
    ASSERT_EQ(v.getSeconds(), 0) << "Unexpected seconds" << v.getSeconds()
                                 << " with index, seed " << index << seed;
  } else {
    ASSERT_EQ(v.getSeconds(), timestamp.getSeconds())
        << "Seconds mismatch with index, seed " << index << seed;
  }

  ASSERT_EQ(v.getNanos(), timestamp.getNanos())
      << "Nanos mismatch with index, seed " << index << seed;
  *failed = false;
}

template <typename T>
void verifyBatch(
    std::vector<std::optional<T>> const& data,
    const VectorPtr& out,
    const std::optional<vector_size_t>& nullCount,
    const uint32_t seed) {
  auto size = data.size();
  ASSERT_EQ(out->size(), size) << "Batch size mismatch with seed " << seed;
  ASSERT_EQ(nullCount, out->getNullCount())
      << "nullCount mismatch with seed " << seed;

  auto outFv = std::dynamic_pointer_cast<FlatVector<T>>(out);
  size_t index = 0;
  for (auto val : data) {
    bool failed = false;
    if (val) {
      ASSERT_FALSE(out->isNullAt(index))
          << "null mismatch with index, seed " << index << seed;

      if constexpr (std::is_floating_point<T>::value) {
        // for floating point nan != nan
        if (std::isnan(val.value())) {
          ASSERT_TRUE(std::isnan(outFv->rawValues()[index]))
              << "nan mismatch with seed " << seed;
        } else {
          verifyValue(outFv, index, val.value(), seed, &failed);
        }
      } else {
        verifyValue(outFv, index, val.value(), seed, &failed);
      }
    } else {
      ASSERT_TRUE(out->isNullAt(index)) << "null mismatch with seed " << seed;
    }
    ++index;
    if (failed) {
      break;
    }
  }
}

template <typename T>
void testDataTypeWriter(
    const TypePtr& type,
    std::vector<std::optional<T>>& data,
    const uint32_t sequence = 0) {
  // Generate a seed and randomly shuffle the data
  uint32_t seed = Random::rand32();
  std::shuffle(data.begin(), data.end(), std::default_random_engine(seed));

  auto config = std::make_shared<Config>();
  auto scopedPool = getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  WriterContext context{config, getDefaultScopedMemoryPool()};
  auto rowType = ROW({type});
  auto dataTypeWithId = TypeWithId::create(type, 1);

  // write
  auto writer = BaseColumnWriter::create(context, *dataTypeWithId, sequence);
  auto size = data.size();
  auto batch = populateBatch(data, &pool);
  const size_t stripeCount = 2;
  const size_t strideCount = 3;

  for (auto stripeI = 0; stripeI < stripeCount; ++stripeI) {
    proto::StripeFooter sf;
    for (auto strideI = 0; strideI < strideCount; ++strideI) {
      writer->write(batch, Ranges::of(0, size));
      writer->createIndexEntry();
    }
    writer->flush([&sf](uint32_t /* unused */) -> proto::ColumnEncoding& {
      return *sf.add_encoding();
    });

    TestStripeStreams streams(context, sf, rowType);
    auto typeWithId = TypeWithId::create(rowType);
    auto reqType = typeWithId->childAt(0);
    auto reader = ColumnReader::build(
        reqType, reqType, streams, FlatMapContext{sequence, nullptr});
    VectorPtr out;
    for (auto strideI = 0; strideI < strideCount; ++strideI) {
      reader->next(size, out);
      verifyBatch(data, out, batch->getNullCount(), seed);
    }
    // Reader API requires the caller to read the Stripe for number of
    // values and iterate only until that number.
    // It does not support hasNext/next protocol.
    // Use a bigger number like 50, as some values may be bit packed.
    EXPECT_THROW({ reader->next(50, out); }, exception::LoggedException);

    context.nextStripe();
    writer->reset();
  }
}

TEST(ColumnWriterTests, LowMemoryModeConfig) {
  auto dataTypeWithId = TypeWithId::create(std::make_shared<VarcharType>(), 1);
  auto config = std::make_shared<Config>();
  WriterContext context{
      config, facebook::velox::memory::getDefaultScopedMemoryPool()};
  auto writer = BaseColumnWriter::create(context, *dataTypeWithId);
  EXPECT_TRUE(writer->useDictionaryEncoding());
}

TEST(ColumnWriterTests, TestBooleanWriter) {
  std::vector<std::optional<bool>> data;
  for (auto i = 0; i < ITERATIONS; ++i) {
    bool value = (bool)(Random::rand32() & 1);
    data.emplace_back(value);
  }
  testDataTypeWriter(BOOLEAN(), data);

  // Test writer with non-zero sequence
  testDataTypeWriter(BOOLEAN(), data, 3);
}

TEST(ColumnWriterTests, TestNullBooleanWriter) {
  std::vector<std::optional<bool>> data;
  for (auto i = 0; i < ITERATIONS; ++i) {
    data.emplace_back();
  }
  testDataTypeWriter(BOOLEAN(), data);
}

TEST(ColumnWriterTests, TestTimestampEpochWriter) {
  std::vector<std::optional<Timestamp>> data;
  // This value will be corrupted. verified in verifyValue method.
  data.emplace_back(Timestamp(-1, 1));
  data.emplace_back(Timestamp(-1, MAX_NANOS));

  // The following values should not be corrupted.
  data.emplace_back(Timestamp(-1, 0));
  data.emplace_back(Timestamp(0, 0));
  data.emplace_back(Timestamp(0, 1));
  data.emplace_back(Timestamp(0, MAX_NANOS));
  testDataTypeWriter(TIMESTAMP(), data);
}

TEST(ColumnWriterTests, TestTimestampWriter) {
  std::vector<std::optional<Timestamp>> data;
  for (int64_t i = 0; i < ITERATIONS; ++i) {
    Timestamp ts(i, i);
    data.emplace_back(ts);
  }
  testDataTypeWriter(TIMESTAMP(), data);

  // Test writer with non-zero sequence
  testDataTypeWriter(TIMESTAMP(), data, 6);
}

TEST(ColumnWriterTests, TestTimestampBoundaryValuesWriter) {
  std::vector<std::optional<Timestamp>> data;
  for (int64_t i = 0; i < ITERATIONS; ++i) {
    if (i & 1) {
      Timestamp ts(INT64_MAX, MAX_NANOS);
      data.emplace_back(ts);
    } else {
      Timestamp ts(MIN_SECONDS, MAX_NANOS);
      data.emplace_back(ts);
    }
    data.emplace_back();
  }
  testDataTypeWriter(TIMESTAMP(), data);
}

TEST(ColumnWriterTests, TestTimestampMixedWriter) {
  std::vector<std::optional<Timestamp>> data;
  for (int64_t i = 0; i < ITERATIONS; ++i) {
    int64_t seconds = static_cast<int64_t>(Random::rand64());
    if (seconds < MIN_SECONDS) {
      seconds = MIN_SECONDS;
    }
    int64_t nanos = Random::rand32(0, MAX_NANOS + 1);
    Timestamp ts(seconds, nanos);
    data.emplace_back(ts);
    // Add null value
    data.emplace_back();
  }
  testDataTypeWriter(TIMESTAMP(), data);
}

void verifyInvalidTimestamp(int64_t seconds, int64_t nanos) {
  std::vector<std::optional<Timestamp>> data;
  for (int64_t i = 1; i < ITERATIONS; ++i) {
    Timestamp ts(i, i);
    data.emplace_back(ts);
  }
  Timestamp ts(seconds, nanos);
  data.emplace_back(ts);
  EXPECT_THROW(
      testDataTypeWriter(TIMESTAMP(), data), exception::LoggedException);
}

TEST(ColumnWriterTests, TestTimestampInvalidWriter) {
  // Nanos invalid range.
  verifyInvalidTimestamp(ITERATIONS, UINT64_MAX);
  verifyInvalidTimestamp(ITERATIONS, MAX_NANOS + 1);

  // Seconds invalid range.
  verifyInvalidTimestamp(INT64_MIN, 0);
  verifyInvalidTimestamp(MIN_SECONDS - 1, MAX_NANOS);
}

TEST(ColumnWriterTests, TestTimestampNullWriter) {
  std::vector<std::optional<Timestamp>> data;
  for (int64_t i = 0; i < ITERATIONS; ++i) {
    data.emplace_back();
  }
  testDataTypeWriter(TIMESTAMP(), data);
}

TEST(ColumnWriterTests, TestBooleanMixedWriter) {
  std::vector<std::optional<bool>> data;
  for (auto i = 0; i < ITERATIONS; ++i) {
    bool value = (bool)(Random::rand32() & 1);
    data.emplace_back(value);
    data.emplace_back();
  }
  testDataTypeWriter(BOOLEAN(), data);
}

TEST(ColumnWriterTests, TestAllBytesWriter) {
  std::vector<std::optional<int8_t>> data;
  for (int16_t i = INT8_MIN; i <= INT8_MAX; ++i) {
    data.emplace_back(i);
  }
  for (int16_t i = INT8_MAX; i >= INT8_MIN; --i) {
    data.emplace_back(i);
  }
  testDataTypeWriter(TINYINT(), data);
}

TEST(ColumnWriterTests, TestRepeatedValuesByteWriter) {
  std::vector<std::optional<int8_t>> data;
  for (auto i = 0; i < ITERATIONS; ++i) {
    data.emplace_back(INT8_MIN);
  }
  testDataTypeWriter(TINYINT(), data);
}

TEST(ColumnWriterTests, TestOnlyNullByteWriter) {
  std::vector<std::optional<int8_t>> data;
  for (auto i = 0; i <= ITERATIONS; ++i) {
    data.emplace_back();
  }
  testDataTypeWriter(TINYINT(), data);
}

TEST(ColumnWriterTests, TestByteNullAndExtremeValueMixed) {
  std::vector<std::optional<int8_t>> data;
  for (auto i = 0; i < ITERATIONS; ++i) {
    data.emplace_back(INT8_MIN);
    data.emplace_back();
    data.emplace_back(INT8_MAX);
  }
  testDataTypeWriter(TINYINT(), data);
}

template <typename T>
void generateSampleData(std::vector<std::optional<T>>& data) {
  const size_t size = 100;
  for (size_t i = 0; i < size; ++i) {
    if (i != 20 && i != 40) {
      data.emplace_back(i);
    } else {
      data.emplace_back();
    }
    ASSERT_EQ(i != 20 && i != 40, data.at(i).has_value());
  }
}

TEST(ColumnWriterTests, TestByteWriter) {
  std::vector<std::optional<int8_t>> data;
  generateSampleData(data);
  testDataTypeWriter(TINYINT(), data);

  // Test writer with non-zero sequence
  testDataTypeWriter(TINYINT(), data, 5);
}

TEST(ColumnWriterTests, TestShortWriter) {
  std::vector<std::optional<int16_t>> data;
  generateSampleData(data);
  testDataTypeWriter(SMALLINT(), data);

  // Test writer with non-zero sequence
  testDataTypeWriter(SMALLINT(), data, 23);
}

TEST(ColumnWriterTests, TestIntWriter) {
  std::vector<std::optional<int32_t>> data;
  generateSampleData(data);
  testDataTypeWriter(INTEGER(), data);

  // Test writer with non-zero sequence
  testDataTypeWriter(INTEGER(), data, 1);
}

TEST(ColumnWriterTests, TestLongWriter) {
  std::vector<std::optional<int64_t>> data;
  generateSampleData(data);
  testDataTypeWriter(BIGINT(), data);

  // Test writer with non-zero sequence
  testDataTypeWriter(BIGINT(), data, 42);
}

TEST(ColumnWriterTests, TestBinaryWriter) {
  std::vector<std::optional<StringView>> data;
  const size_t size = 100;
  for (size_t i = 0; i < size; ++i) {
    if (i != 20 && i != 40) {
      data.emplace_back(folly::to<std::string>(i));
    } else {
      data.emplace_back();
    }
    ASSERT_EQ(i != 20 && i != 40, data.at(i).has_value());
  }
  testDataTypeWriter(VARBINARY(), data);

  // Test writer with non-zero sequence
  testDataTypeWriter(VARBINARY(), data, 42);
}

TEST(ColumnWriterTests, TestBinaryWriterAllNulls) {
  std::vector<std::optional<StringView>> data{100};
  testDataTypeWriter(VARBINARY(), data);
}

template <typename T>
struct ValueOf {
  static std::string get(const VectorPtr& batch, const uint32_t offset) {
    auto scalarBatch = std::dynamic_pointer_cast<FlatVector<T>>(batch);
    return std::to_string(scalarBatch->valueAt(offset));
  }
};

template <>
struct ValueOf<bool> {
  static std::string get(const VectorPtr& batch, const uint32_t offset) {
    auto scalarBatch = std::dynamic_pointer_cast<FlatVector<bool>>(batch);
    return folly::to<std::string>(scalarBatch->valueAt(offset));
  }
};

template <>
struct ValueOf<StringView> {
  static std::string get(const VectorPtr& batch, const uint32_t offset) {
    auto scalarBatch = std::dynamic_pointer_cast<FlatVector<StringView>>(batch);
    return scalarBatch->valueAt(offset).str();
  }
};

template <typename keyT, typename valueT>
struct ValueOf<Map<keyT, valueT>> {
  static std::string get(const VectorPtr& batch, const uint32_t offset) {
    auto mapBatch = std::dynamic_pointer_cast<MapVector>(batch);
    return folly::to<std::string>(
        "map at ",
        offset,
        " child: ",
        mapBatch->offsetAt(offset),
        ":",
        mapBatch->sizeAt(offset));
  }
};

template <typename elemT>
struct ValueOf<Array<elemT>> {
  static std::string get(const VectorPtr& batch, const uint32_t offset) {
    auto arrayBatch = std::dynamic_pointer_cast<ArrayVector>(batch);
    return folly::to<std::string>(
        "array at ",
        offset,
        " child: ",
        arrayBatch->offsetAt(offset),
        ":",
        arrayBatch->sizeAt(offset));
  }
};

template <typename... T>
struct ValueOf<Row<T...>> {
  static std::string get(const VectorPtr& /* batch */, const uint32_t offset) {
    return folly::to<std::string>("row at ", offset);
  }
};

template <typename T>
std::string getNullCountStr(const T& vector) {
  return vector.getNullCount().has_value()
      ? std::to_string(vector.getNullCount().value())
      : "none";
}

template <typename TKEY, typename TVALUE>
void printMap(const std::string& title, const VectorPtr& batch) {
  auto mv = std::dynamic_pointer_cast<MapVector>(batch);
  if (!mv) {
    VLOG(3) << "To be implemented for encoded vector";
    return;
  }

  VLOG(3) << "*******" << title << "*******";
  VLOG(3) << "Size: " << mv->size() << ", Null count: " << getNullCountStr(*mv);
  for (int32_t i = 0; i <= mv->size(); ++i) {
    VLOG(3) << "Offset[" << i << "]: " << mv->offsetAt(i)
            << (i < mv->size() && mv->isNullAt(i) ? " (null)" : "");
  }

  auto keys = mv->mapKeys();
  auto values = mv->mapValues();

  VLOG(3) << "Keys Size: " << keys->size()
          << ", Keys Null count: " << getNullCountStr(*keys);
  VLOG(3) << "Values Size: " << values->size()
          << ", Values Null count: " << getNullCountStr(*values);

  for (int32_t i = 0; i < keys->size(); ++i) {
    VLOG(3) << "[" << i << "]: " << ValueOf<TKEY>::get(keys, i) << " -> "
            << (values->isNullAt(i) ? "null" : ValueOf<TVALUE>::get(values, i));
  }
}

void printRow(const std::string& title, const VectorPtr& batch) {
  auto row = std::dynamic_pointer_cast<RowVector>(batch);
  if (!row) {
    VLOG(3) << "To be implemented for encoded vector";
    return;
  }

  VLOG(3) << "*******" << title << "*******";
  VLOG(3) << "Size: " << row->size()
          << ", Null count: " << getNullCountStr(*row);
  for (int i = 0; i < row->size(); i++) {
    VLOG(3) << "[" << i << "]: " << row->toString(i);
  }
}

VectorPtr
wrapInDictionary(const VectorPtr& batch, size_t stride, MemoryPool& pool) {
  VectorPtr ret = batch;
  // Wrap key if least significant bit is 1
  if (stride & 0x01) {
    auto map = batch->as<MapVector>();
    auto keys = map->mapKeys();
    auto size = keys->size();

    auto indices = AlignedBuffer::allocate<vector_size_t>(size, &pool);
    auto rawIndices = indices->asMutable<vector_size_t>();
    for (auto i = 0; i < size; ++i) {
      rawIndices[i] = i;
    }

    ret = std::make_shared<MapVector>(
        map->pool(),
        map->type(),
        map->nulls(),
        map->size(),
        map->offsets(),
        map->sizes(),
        BaseVector::wrapInDictionary(nullptr, indices, size, keys),
        map->mapValues());
  }

  // Wrap map if 2nd least significant bit is 1
  if (stride & 0x02) {
    auto size = ret->size();

    auto indices = AlignedBuffer::allocate<vector_size_t>(size, &pool);
    auto rawIndices = indices->asMutable<vector_size_t>();
    for (auto i = 0; i < size; ++i) {
      rawIndices[i] = i;
    }

    ret = BaseVector::wrapInDictionary(nullptr, indices, size, ret);
  }

  return ret;
}

VectorPtr wrapInDictionaryRow(const VectorPtr& batch, MemoryPool& pool) {
  auto row = batch->as<RowVector>();

  auto size = row->size();
  auto indices = AlignedBuffer::allocate<vector_size_t>(size, &pool);
  auto rawIndices = indices->asMutable<vector_size_t>();
  for (auto i = 0; i < size; i++) {
    rawIndices[i] = i;
  }

  return BaseVector::wrapInDictionary(nullptr, indices, size, batch);
}

template <typename T>
void getUniqueKeys(
    std::vector<T>& uniqueKeys,
    const std::vector<VectorPtr>& batches) {
  std::unordered_set<T> seenKeys;

  for (auto batch : batches) {
    auto map = std::dynamic_pointer_cast<MapVector>(batch);
    ASSERT_TRUE(map);
    auto keys = map->mapKeys();
    auto flatKeys = std::dynamic_pointer_cast<FlatVector<T>>(keys);
    ASSERT_TRUE(flatKeys);
    for (vector_size_t i = 0; i < flatKeys->size(); i++) {
      ASSERT_TRUE(!flatKeys->isNullAt(i));
      seenKeys.insert(flatKeys->valueAt(i));
    }
  }

  uniqueKeys.clear();
  uniqueKeys.insert(uniqueKeys.end(), seenKeys.cbegin(), seenKeys.cend());
}

template <typename TKEY, typename TVALUE>
void mapToStruct(
    MemoryPool& pool,
    std::vector<VectorPtr>& batches,
    const std::vector<TKEY>& uniqueKeys) {
  std::unordered_map<TKEY, int> keyColIndex;
  for (auto i = 0; i < uniqueKeys.size(); i++) {
    keyColIndex[uniqueKeys[i]] = i; // lookup from key -> column#
  }

  for (size_t i = 0; i < batches.size(); i++) {
    auto origBatch = batches[i];
    std::vector<VectorPtr> childrenVectors(uniqueKeys.size());
    // initialize children of batch size filled with nulls
    VectorMaker maker{&pool};
    for (auto column = 0; column < uniqueKeys.size(); column++) {
      childrenVectors[column] =
          maker.allNullFlatVector<TVALUE>(origBatch->size());
      // only flat for scalar types
      // create function to handle nested complex types
    }
    batches[i] = maker.rowVector(childrenVectors);
    auto batchStruct = std::dynamic_pointer_cast<RowVector>(batches[i]);

    auto mapBatch = std::dynamic_pointer_cast<MapVector>(origBatch);
    ASSERT_TRUE(mapBatch);

    auto keys = mapBatch->mapKeys();
    auto flatKeys = std::dynamic_pointer_cast<FlatVector<TKEY>>(keys);
    ASSERT_TRUE(flatKeys);
    auto values = mapBatch->mapValues();
    auto flatValues = std::dynamic_pointer_cast<FlatVector<TVALUE>>(values);
    ASSERT_TRUE(flatValues);

    auto offsets = mapBatch->offsets()->as<vector_size_t>();
    auto sizes = mapBatch->sizes()->as<vector_size_t>();

    // for each row in current batch
    for (vector_size_t row = 0; row < mapBatch->size(); row++) {
      // for each key in row (single map)
      for (vector_size_t index = offsets[row],
                         endOffset = offsets[row] + sizes[row];
           index < endOffset;
           index++) {
        ASSERT_FALSE(flatKeys->isNullAt(index));
        // set value in correct row
        auto key = flatKeys->valueAt(index);
        auto element = std::dynamic_pointer_cast<FlatVector<TVALUE>>(
            batchStruct->childAt(keyColIndex[key]));
        ASSERT_TRUE(element);
        element->set(row, flatValues->valueAt(index));
      }
    }
  }
}

template <typename TKEY, typename TVALUE>
void testMapWriter(
    MemoryPool& pool,
    const std::vector<VectorPtr>& batches,
    bool useFlatMap,
    bool disableDictionaryEncoding,
    bool testEncoded,
    bool printMaps = true,
    bool useStruct = false) {
  const auto rowType = CppToType<Row<Map<TKEY, TVALUE>>>::create();
  const auto dataType = rowType->childAt(0);
  const auto rowTypeWithId = TypeWithId::create(rowType);
  const auto dataTypeWithId = rowTypeWithId->childAt(0);
  const auto writerSchema = TypeWithId::create(rowType);
  const auto writerDataTypeWithId = writerSchema->childAt(0);

  VLOG(2) << "Testing map writer " << dataType->toString() << " using "
          << (useFlatMap ? "Flat Map" : "Regular Map")
          << (useFlatMap && useStruct ? " - Struct" : "");

  const auto config = std::make_shared<Config>();
  auto* pBatches = &batches;
  std::vector<VectorPtr> structs;
  std::unordered_map<uint32_t, std::vector<std::string>> structReaderContext;
  if (useFlatMap) {
    if (useStruct) {
      structs = batches;
      pBatches = &structs;
      std::vector<TKEY> uniqueKeys;
      ASSERT_NO_FATAL_FAILURE(getUniqueKeys<TKEY>(uniqueKeys, batches));
      ASSERT_NO_FATAL_FAILURE(
          (mapToStruct<TKEY, TVALUE>(pool, structs, uniqueKeys)));

      std::vector<std::string> uniqueKeysString;
      uniqueKeysString.reserve(uniqueKeys.size());
      std::transform(
          uniqueKeys.cbegin(),
          uniqueKeys.cend(),
          std::back_inserter(uniqueKeysString),
          [](const auto& e) { return folly::to<std::string>(e); });
      ASSERT_EQ(writerDataTypeWithId->column, 0);
      config->set(Config::MAP_FLAT_COLS_STRUCT_KEYS, {uniqueKeysString});
      structReaderContext[writerDataTypeWithId->id] = uniqueKeysString;
    }

    config->set(Config::FLATTEN_MAP, true);
    config->set(Config::MAP_FLAT_COLS, {writerDataTypeWithId->column});
    config->set(
        Config::MAP_FLAT_DISABLE_DICT_ENCODING, disableDictionaryEncoding);

    // expect that if we pass useStruct true with useFlatMap false, it will fail
  }

  WriterContext context{config, getDefaultScopedMemoryPool()};
  const auto writer = BaseColumnWriter::create(context, *writerDataTypeWithId);
  // For writing flat map with encoded input, we'd like to test all 4
  // combinations.
  size_t strideCount = testEncoded ? 4 : 2;

  // Each batch represents an input for a separate stripe
  for (auto batch : *pBatches) {
    auto isStruct = useFlatMap && useStruct;
    if (printMaps) {
      if (isStruct) {
        printRow("Input", batch);
      } else {
        printMap<TKEY, TVALUE>("Input", batch);
      }
    }

    proto::StripeFooter sf;
    std::vector<VectorPtr> writtenBatches;

    // Write map/row
    for (auto strideI = 0; strideI < strideCount; ++strideI) {
      auto toWrite = batch;
      if (testEncoded) {
        if (isStruct) {
          toWrite = wrapInDictionaryRow(toWrite, pool);
        } else {
          toWrite = wrapInDictionary(toWrite, strideI, pool);
        }
      }
      writer->write(toWrite, Ranges::of(0, toWrite->size()));
      writer->createIndexEntry();
      writtenBatches.push_back(toWrite);
    }

    writer->flush([&sf](uint32_t /* unused */) -> proto::ColumnEncoding& {
      return *sf.add_encoding();
    });

    auto validate = [&](bool returnFlatVector = false) {
      TestStripeStreams streams(
          context, sf, rowType, returnFlatVector, structReaderContext);
      const auto reader =
          ColumnReader::build(dataTypeWithId, dataTypeWithId, streams);
      VectorPtr out;

      // Read map/row
      for (auto& writtenBatch : writtenBatches) {
        reader->next(writtenBatch->size(), out);
        ASSERT_EQ(out->size(), writtenBatch->size()) << "Batch size mismatch";

        if (printMaps) {
          if (isStruct) {
            printRow("Result", batch);
          } else {
            printMap<TKEY, TVALUE>("Result", out);
          }
        }

        for (int32_t i = 0; i < writtenBatch->size(); ++i) {
          ASSERT_TRUE(writtenBatch->equalValueAt(out.get(), i, i))
              << "Row mismatch at index " << i;
        }
      }

      // Reader API requires the caller to read the Stripe for number of
      // values and iterate only until that number.
      // It does not support hasNext/next protocol.
      // Use a bigger number like 50, as some values may be bit packed.
      EXPECT_THROW({ reader->next(50, out); }, exception::LoggedException);
    };

    ASSERT_NO_FATAL_FAILURE(validate());
    if (useFlatMap) {
      ASSERT_NO_FATAL_FAILURE(validate(true));
    }

    context.nextStripe();

    auto valueNodeId = dataTypeWithId->childAt(1)->id;
    auto streamCount = 0;
    context.iterateUnSuppressedStreams([&](auto& pair) {
      if (pair.first.encodingKey().node == valueNodeId) {
        ++streamCount;
      }
    });

    ASSERT_GT(streamCount, 0) << "Expecting to find at least one value stream";

    writer->reset();

    streamCount = 0;
    context.iterateUnSuppressedStreams([&](auto& pair) {
      if (pair.first.encodingKey().node == valueNodeId) {
        ++streamCount;
      }
    });

    if (useFlatMap) {
      ASSERT_EQ(streamCount, 0)
          << "Expecting all flat map value streams to be disposed";
    } else {
      ASSERT_GT(streamCount, 0)
          << "Expecting to find at least one regular map value stream";
    }
  }
}

template <typename TVALUE>
void testMapWriterRow(
    MemoryPool& pool,
    const std::vector<VectorPtr>& batches,
    bool disableDictionaryEncoding,
    bool testEncoded,
    bool printInput = true) {
  const auto rowType = CppToType<Row<Map<int32_t, TVALUE>>>::create();
  const auto dataType = rowType->childAt(0);
  const auto rowTypeWithId = TypeWithId::create(rowType);
  const auto dataTypeWithId = rowTypeWithId->childAt(0);
  const auto writerSchema = TypeWithId::create(rowType);
  const auto writerDataTypeWithId = writerSchema->childAt(0);

  VLOG(2) << "Testing map writer struct input " << dataType->toString();

  const auto config = std::make_shared<Config>();
  std::unordered_map<uint32_t, std::vector<std::string>> structReaderContext;
  ASSERT_TRUE(batches.size() > 0);
  auto row = std::dynamic_pointer_cast<RowVector>(batches[0]);
  ASSERT_TRUE(row);

  // defaulting to int32_t keys
  std::vector<std::string> uniqueKeysString;
  uniqueKeysString.reserve(row->childrenSize());
  for (auto i = 0; i < row->childrenSize(); i++) {
    uniqueKeysString.push_back(folly::to<std::string>(i));
  }

  ASSERT_EQ(writerDataTypeWithId->column, 0);
  config->set(Config::MAP_FLAT_COLS_STRUCT_KEYS, {uniqueKeysString});
  structReaderContext[writerDataTypeWithId->id] = uniqueKeysString;

  config->set(Config::FLATTEN_MAP, true);
  config->set(Config::MAP_FLAT_COLS, {writerDataTypeWithId->column});
  config->set(
      Config::MAP_FLAT_DISABLE_DICT_ENCODING, disableDictionaryEncoding);

  WriterContext context{config, getDefaultScopedMemoryPool()};
  const auto writer = BaseColumnWriter::create(context, *writerDataTypeWithId);

  // Each batch represents an input for a separate stripe
  for (auto batch : batches) {
    if (printInput) {
      printRow("Input", batch);
    }

    proto::StripeFooter sf;
    std::vector<VectorPtr> writtenBatches;

    // Write map/row
    auto toWrite = batch;
    if (testEncoded) {
      toWrite = wrapInDictionaryRow(toWrite, pool);
    }
    writer->write(toWrite, Ranges::of(0, toWrite->size()));
    writer->createIndexEntry();
    writtenBatches.push_back(toWrite);

    writer->flush([&sf](uint32_t /* unused */) -> proto::ColumnEncoding& {
      return *sf.add_encoding();
    });

    auto validate = [&](bool returnFlatVector = false) {
      TestStripeStreams streams(
          context, sf, rowType, returnFlatVector, structReaderContext);
      const auto reader =
          ColumnReader::build(dataTypeWithId, dataTypeWithId, streams);
      VectorPtr out;

      // Read map/row
      for (auto& writtenBatch : writtenBatches) {
        reader->next(writtenBatch->size(), out);
        ASSERT_EQ(out->size(), writtenBatch->size()) << "Batch size mismatch";

        if (printInput) {
          printRow("Result", batch);
        }

        for (int32_t i = 0; i < writtenBatch->size(); ++i) {
          ASSERT_TRUE(writtenBatch->equalValueAt(out.get(), i, i))
              << "Row mismatch at index " << i;
        }
      }

      // Reader API requires the caller to read the Stripe for number of
      // values and iterate only until that number.
      // It does not support hasNext/next protocol.
      // Use a bigger number like 50, as some values may be bit packed.
      EXPECT_THROW({ reader->next(50, out); }, exception::LoggedException);
    };

    ASSERT_NO_FATAL_FAILURE(validate());
    ASSERT_NO_FATAL_FAILURE(validate(true));

    context.nextStripe();

    auto valueNodeId = dataTypeWithId->childAt(1)->id;
    auto streamCount = 0;
    context.iterateUnSuppressedStreams([&](auto& pair) {
      if (pair.first.encodingKey().node == valueNodeId) {
        ++streamCount;
      }
    });

    ASSERT_GT(streamCount, 0) << "Expecting to find at least one value stream";

    writer->reset();

    streamCount = 0;
    context.iterateUnSuppressedStreams([&](auto& pair) {
      if (pair.first.encodingKey().node == valueNodeId) {
        ++streamCount;
      }
    });

    ASSERT_EQ(streamCount, 0)
        << "Expecting all flat map value streams to be disposed";
  }
}

template <typename TVALUE>
void testMapWriterRowImpl() {
  auto type = CppToType<Row<TVALUE, TVALUE>>::create();

  std::unique_ptr<ScopedMemoryPool> scopedPool = getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  auto batch = BatchMaker::createVector<TypeKind::ROW>(type, 10, pool);

  std::vector<VectorPtr> batches{batch, batch};

  testMapWriterRow<TVALUE>(pool, batches, true, false);
  testMapWriterRow<TVALUE>(pool, batches, true, true);
}

TEST(ColumnWriterTests, TestMapWriterNestedRow) {
  testMapWriterRowImpl<bool>();
  testMapWriterRowImpl<Array<int32_t>>();
  testMapWriterRowImpl<Array<bool>>();
  testMapWriterRowImpl<Array<StringView>>();
  testMapWriterRowImpl<Map<int32_t, bool>>();
  testMapWriterRowImpl<Map<int32_t, int32_t>>();
  testMapWriterRowImpl<Map<int32_t, StringView>>();
  testMapWriterRowImpl<Map<int32_t, Array<int32_t>>>();
  testMapWriterRowImpl<Map<int32_t, Map<int32_t, StringView>>>();
  testMapWriterRowImpl<Map<int32_t, Row<int32_t, bool, StringView>>>();
  testMapWriterRowImpl<Row<int32_t, bool, StringView>>();
}

template <typename TKEY, typename TVALUE>
void testMapWriter(
    MemoryPool& pool,
    const VectorPtr& batch,
    bool useFlatMap,
    bool printMaps = true,
    bool useStruct = false) {
  std::vector<VectorPtr> batches{batch, batch};
  testMapWriter<TKEY, TVALUE>(
      pool, batches, useFlatMap, true, false, printMaps, useStruct);
  if (useFlatMap) {
    testMapWriter<TKEY, TVALUE>(
        pool, batches, useFlatMap, false, false, printMaps, useStruct);
    testMapWriter<TKEY, TVALUE>(
        pool, batches, useFlatMap, true, true, printMaps, useStruct);
  }
}

template <typename T>
void testMapWriterNumericKey(bool useFlatMap, bool useStruct = false) {
  using b = MapBuilder<T, T>;

  std::unique_ptr<ScopedMemoryPool> scopedPool = getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  auto batch = b::create(
      pool,
      {typename b::row{
           typename b::pair{std::numeric_limits<T>::max(), 3},
           typename b::pair{2, std::numeric_limits<T>::max()}},
       typename b::row{
           typename b::pair{2, 5},
           typename b::pair{
               std::numeric_limits<T>::min(), std::numeric_limits<T>::min()}}});

  testMapWriter<T, T>(pool, batch, useFlatMap, true, useStruct);
}

TEST(ColumnWriterTests, TestMapWriterFloatKey) {
  testMapWriterNumericKey<float>(/* useFlatMap */ false);

  EXPECT_THROW(
      { testMapWriterNumericKey<float>(/* useFlatMap */ true); },
      exception::LoggedException);

  EXPECT_THROW(
      {
        testMapWriterNumericKey<float>(
            /* useFlatMap */ true, /* useStruct */ true);
      },
      exception::LoggedException);
}

TEST(ColumnWriterTests, TestMapWriterInt64Key) {
  testMapWriterNumericKey<int64_t>(/* useFlatMap */ false);
  testMapWriterNumericKey<int64_t>(/* useFlatMap */ true);
  testMapWriterNumericKey<int64_t>(/* useFlatMap */ true, /* useStruct */ true);
}

TEST(ColumnWriterTests, TestMapWriterInt32Key) {
  testMapWriterNumericKey<int32_t>(/* useFlatMap */ false);
  testMapWriterNumericKey<int32_t>(/* useFlatMap */ true);
  testMapWriterNumericKey<int32_t>(/* useFlatMap */ true, /* useStruct */ true);
}

TEST(ColumnWriterTests, TestMapWriterInt16Key) {
  testMapWriterNumericKey<int16_t>(/* useFlatMap */ false);
  testMapWriterNumericKey<int16_t>(/* useFlatMap */ true);
  testMapWriterNumericKey<int16_t>(/* useFlatMap */ true, /* useStruct */ true);
}

TEST(ColumnWriterTests, TestMapWriterInt8Key) {
  testMapWriterNumericKey<int8_t>(/* useFlatMap */ false);
  testMapWriterNumericKey<int8_t>(/* useFlatMap */ true);
  testMapWriterNumericKey<int8_t>(/* useFlatMap */ true, /* useStruct */ true);
}

TEST(ColumnWriterTests, TestMapWriterStringKey) {
  using keyType = StringView;
  using valueType = StringView;
  using b = MapBuilder<keyType, valueType>;

  std::unique_ptr<ScopedMemoryPool> scopedPool = getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  auto batch = b::create(
      pool,
      {b::row{b::pair{"1", "3"}, b::pair{"2", "2"}},
       b::row{b::pair{"2", "5"}, b::pair{"3", "8"}}});

  testMapWriter<keyType, valueType>(pool, batch, /* useFlatMap */ false);
  testMapWriter<keyType, valueType>(pool, batch, /* useFlatMap */ true);
  testMapWriter<keyType, valueType>(
      pool, batch, /* useFlatMap */ true, true, /* useStruct */ true);
}

TEST(ColumnWriterTests, TestMapWriterDifferentNumericKeyValue) {
  using keyType = float;
  using valueType = int32_t;
  using b = MapBuilder<keyType, valueType>;

  std::unique_ptr<ScopedMemoryPool> scopedPool = getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  auto batch = b::create(
      pool,
      {b::row{b::pair{1, 3}, b::pair{2, 2}},
       b::row{b::pair{2, 5}, b::pair{3, 8}}});

  testMapWriter<keyType, valueType>(pool, batch, /* useFlatMap */ false);
}

TEST(ColumnWriterTests, TestMapWriterDifferentKeyValue) {
  using keyType = float;
  using valueType = StringView;
  using b = MapBuilder<keyType, valueType>;

  std::unique_ptr<ScopedMemoryPool> scopedPool = getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  auto batch = b::create(
      pool,
      {b::row{b::pair{1, "3"}, b::pair{2, "2"}},
       b::row{b::pair{2, "5"}, b::pair{3, "8"}}});

  testMapWriter<keyType, valueType>(pool, batch, /* useFlatMap */ false);
}

TEST(ColumnWriterTests, TestMapWriterMixedBatchTypeHandling) {
  using keyType = int32_t;
  using valueType = int32_t;
  using b = MapBuilder<keyType, valueType>;

  using valueType2 = StringView;
  using b2 = MapBuilder<keyType, valueType2>;

  std::unique_ptr<ScopedMemoryPool> scopedPool = getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  auto batch1 = b::create(
      pool,
      {b::row{b::pair{1, 3}, b::pair{2, 2}},
       b::row{b::pair{5, 5}, b::pair{3, 4}, b::pair{2, 5}}});

  auto batch2 = b2::create(
      pool,
      {b2::row{b2::pair{8, "3"}, b2::pair{6, "2"}},
       b2::row{b2::pair{20, "5"}, b2::pair{2, "4"}, b2::pair{63, "5"}}});

  std::vector<VectorPtr> batches{batch1, batch2};
  // Test type cast assertion in the direct encoding case.
  // TODO(T91654228): Check and throw for non-homogeneous batch types
  // when dictionary encoding is enabled.
  EXPECT_THROW(
      (testMapWriter<keyType, valueType>(
          pool,
          batches,
          /* useFlatMap */ true,
          true,
          false)),
      exception::LoggedException);
}

TEST(ColumnWriterTests, TestMapWriterBinaryKey) {
  using keyType = StringView;
  using valueType = int32_t;
  using b = MapBuilder<keyType, valueType>;

  std::unique_ptr<ScopedMemoryPool> scopedPool = getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  auto batch = b::create(
      pool,
      {b::row{b::pair{"1", 3}, b::pair{"2", 2}},
       b::row{b::pair{"2", 5}, b::pair{"3", 8}}});

  testMapWriter<keyType, valueType>(pool, batch, /* useFlatMap */ false);
  testMapWriter<keyType, valueType>(pool, batch, /* useFlatMap */ true);
  testMapWriter<keyType, valueType>(
      pool, batch, /* useFlatMap */ true, true, /* useStruct */ true);
}

template <typename keyType, typename valueType>
void testMapWriterImpl() {
  auto type = CppToType<Map<keyType, valueType>>::create();

  std::unique_ptr<ScopedMemoryPool> scopedPool = getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  auto batch = BatchMaker::createVector<TypeKind::MAP>(type, 100, pool);

  testMapWriter<keyType, valueType>(pool, batch, /* useFlatMap */ false);
  testMapWriter<keyType, valueType>(pool, batch, /* useFlatMap */ true);
  // testMapWriter<keyType, valueType>(
  //     pool, batch, /* useFlatMap */ true, true, /* useStruct */ true);
}

TEST(ColumnWriterTests, TestMapWriterNestedMap) {
  testMapWriterImpl<int32_t, bool>();
  testMapWriterImpl<int32_t, Array<int32_t>>();
  testMapWriterImpl<int32_t, Array<bool>>();
  testMapWriterImpl<int32_t, Array<StringView>>();
  testMapWriterImpl<int32_t, Map<int32_t, bool>>();
  testMapWriterImpl<int32_t, Map<int32_t, int32_t>>();
  testMapWriterImpl<int32_t, Map<int32_t, StringView>>();
  testMapWriterImpl<int32_t, Map<int32_t, Array<int32_t>>>();
  testMapWriterImpl<int32_t, Map<int32_t, Map<int32_t, StringView>>>();
  testMapWriterImpl<int32_t, Map<int32_t, Row<int32_t, bool, StringView>>>();
  testMapWriterImpl<int32_t, Row<int32_t, bool, StringView>>();
}

TEST(ColumnWriterTests, TestMapWriterDifferentStripeBatches) {
  using keyType = int32_t;
  using valueType = int32_t;
  using b = MapBuilder<keyType, valueType>;

  std::unique_ptr<ScopedMemoryPool> scopedPool = getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  auto batch1 = b::create(
      pool,
      {b::row{b::pair{1, 3}, b::pair{2, 2}},
       b::row{b::pair{5, 5}, b::pair{3, 4}, b::pair{2, 5}}});

  auto batch2 = b::create(
      pool,
      {b::row{b::pair{8, 3}, b::pair{6, 2}},
       b::row{b::pair{20, 5}, b::pair{2, 4}, b::pair{63, 5}}});

  std::vector<VectorPtr> batches{batch1, batch2};

  testMapWriter<keyType, valueType>(
      pool,
      batches,
      /* useFlatMap */ false,
      false,
      false);
  testMapWriter<keyType, valueType>(
      pool,
      batches,
      /* useFlatMap */ true,
      false,
      false);
}

TEST(ColumnWriterTests, TestMapWriterNullValues) {
  using keyType = int32_t;
  using valueType = int32_t;
  using b = MapBuilder<keyType, valueType>;

  std::unique_ptr<ScopedMemoryPool> scopedPool = getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  auto batch = b::create(
      pool,
      {b::row{b::pair{1, std::nullopt}, b::pair{2, 2}},
       b::row{b::pair{5, 5}, b::pair{3, std::nullopt}, b::pair{2, 5}}});

  testMapWriter<keyType, valueType>(pool, batch, /* useFlatMap */ false);
  testMapWriter<keyType, valueType>(pool, batch, /* useFlatMap */ true);
}

TEST(ColumnWriterTests, TestMapWriterNullRows) {
  using keyType = int32_t;
  using valueType = int32_t;
  using b = MapBuilder<keyType, valueType>;

  std::unique_ptr<ScopedMemoryPool> scopedPool = getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  auto batch = b::create(
      pool,
      {std::nullopt,
       b::row{b::pair{1, 3}, b::pair{2, 2}},
       std::nullopt,
       b::row{b::pair{2, 5}},
       std::nullopt});

  testMapWriter<keyType, valueType>(pool, batch, /* useFlatMap */ false);
  testMapWriter<keyType, valueType>(pool, batch, /* useFlatMap */ true);
}

TEST(ColumnWriterTests, TestMapWriterDuplicateKeys) {
  using keyType = int32_t;
  using valueType = int32_t;
  using b = MapBuilder<keyType, valueType>;

  std::unique_ptr<ScopedMemoryPool> scopedPool = getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  auto batch = b::create(
      pool,
      {
          b::row{b::pair{1, 3}, b::pair{1, 2}},
      });

  // Default map writer doesn't throw on duplicate keys
  // TODO: Is there a way to easily detect duplicate keys in MapColumnWriter?
  testMapWriter<keyType, valueType>(pool, batch, /* useFlatMap */ false);
  EXPECT_THROW(
      (testMapWriter<keyType, valueType>(pool, batch, true)),
      exception::LoggedException);
}

TEST(ColumnWriterTests, TestMapWriterBigBatch) {
  using keyType = int32_t;
  using valueType = float;
  using b = MapBuilder<keyType, valueType>;

  const auto size = 1000;
  const auto maxDictionarySize = 50;
  const auto nullEvery = 10;

  std::unique_ptr<ScopedMemoryPool> scopedPool = getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  b::rows rows;
  for (int32_t i = 0; i < size; ++i) {
    if ((i % nullEvery) == 0) {
      rows.push_back(std::nullopt);
    }
    const auto rowCount = Random::rand32(maxDictionarySize) + 1;
    const auto randomStart = Random::rand32(10);
    b::row row;
    for (int32_t j = 0; j < rowCount; j++) {
      row.push_back(b::pair{randomStart + j, Random::randDouble(0, 50)});
    }

    rows.push_back(row);
  }

  auto batch = b::create(pool, rows);
  testMapWriter<keyType, valueType>(
      pool,
      batch,
      /* useFlatMap */ false);
  testMapWriter<keyType, valueType>(
      pool,
      batch,
      /* useFlatMap */ true);
}

TEST(ColumnWriterTests, TestStructKeysConfigSerializationDeserialization) {
  const std::vector<std::vector<std::string>> columns{
      {"1.45", "hi, you;", "29102819", "1e-4"},
      {"291", "world"},
      {},
      {"one", ", more", "\"two'three$"}};
  const auto config = std::make_shared<Config>();
  config->set<decltype(columns)>(Config::MAP_FLAT_COLS_STRUCT_KEYS, columns);
  EXPECT_EQ(config->get(Config::MAP_FLAT_COLS_STRUCT_KEYS), columns);
}

std::unique_ptr<DwrfReader> getDwrfReader(
    MemoryPool& pool,
    const std::shared_ptr<const RowType> type,
    const VectorPtr& batch,
    bool useFlatMap) {
  auto config = std::make_shared<Config>();
  if (useFlatMap) {
    config->set(Config::FLATTEN_MAP, true);
    config->set(Config::MAP_FLAT_COLS, {0});
  }

  auto sink = std::make_unique<MemorySink>(pool, 2 * 1024 * 1024);
  auto sinkPtr = sink.get();

  WriterOptions options;
  options.config = config;
  options.schema = type;
  options.flushPolicyFactory = [&]() {
    return std::make_unique<LambdaFlushPolicy>([]() {
      return true; // Flushes every batch.
    });
  };
  Writer writer{options, std::move(sink), pool};
  writer.write(batch);
  writer.close();

  auto input =
      std::make_unique<MemoryInputStream>(sinkPtr->getData(), sinkPtr->size());

  ReaderOptions readerOpts;
  RowReaderOptions rowReaderOpts;
  return std::make_unique<DwrfReader>(readerOpts, std::move(input));
}

void removeSizeFromStats(std::string& input) {
  // FlatMap and Map, Serializes the data differently. Size is the sum of
  // all Serialized Stream and hence they don't match. Remove the Size
  // and value from the String used for comparison.
  auto firstPos = input.find(" Size:");
  ASSERT_NE(firstPos, std::string::npos) << " Size not found in " << input;
  auto endPos = input.find(",", firstPos);
  ASSERT_NE(endPos, std::string::npos) << " , not found after Size " << input;
  input.erase(firstPos, (endPos + 1) - firstPos);
}

void testMapWriterStats(const std::shared_ptr<const RowType> type) {
  std::unique_ptr<ScopedMemoryPool> scopedPool = getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  auto batch = BatchMaker::createBatch(type, 10, pool);
  auto mapReader = getDwrfReader(pool, type, batch, false);
  auto flatMapReader = getDwrfReader(pool, type, batch, true);
  ASSERT_EQ(
      mapReader->getFooter().statisticsSize(),
      flatMapReader->getFooter().statisticsSize());

  for (int32_t i = 0; i < mapReader->getFooter().statisticsSize(); ++i) {
    LOG(INFO) << "Stats " << i
              << "     map: " << mapReader->columnStatistics(i)->toString();
    LOG(INFO) << "Stats " << i
              << " flatmap: " << flatMapReader->columnStatistics(i)->toString();
    auto mapReaderStatString = mapReader->columnStatistics(i)->toString();
    auto flatMapReaderStatString =
        flatMapReader->columnStatistics(i)->toString();

    removeSizeFromStats(mapReaderStatString);
    removeSizeFromStats(flatMapReaderStatString);
    ASSERT_EQ(mapReaderStatString, flatMapReaderStatString);
  }
}

TEST(ColumnWriterTests, TestMapWriterCompareStatsBinaryKey) {
  using keyType = Varbinary;
  // We create a complex map with complex value structure to test that value
  // aggregation work well in flat maps
  const auto type = CppToType<Row<
      Map<keyType, Row<int32_t, Row<std::string, Array<int16_t>>>>>>::create();

  testMapWriterStats(type);
}

TEST(ColumnWriterTests, TestMapWriterCompareStatsStringKey) {
  using keyType = std::string;
  // We create a complex map with complex value structure to test that value
  // aggregation work well in flat maps
  const auto type = CppToType<Row<
      Map<keyType, Row<int32_t, Row<std::string, Array<int16_t>>>>>>::create();

  testMapWriterStats(type);
}

TEST(ColumnWriterTests, TestMapWriterCompareStatsInt8Key) {
  using keyType = int8_t;

  // We create a complex map with complex value structure to test that value
  // aggregation work well in flat maps
  const auto type = CppToType<Row<
      Map<keyType, Row<int32_t, Row<std::string, Array<int16_t>>>>>>::create();

  testMapWriterStats(type);
}

TEST(ColumnWriterTests, TestMapWriterCompareStatsInt16Key) {
  using keyType = int16_t;

  // We create a complex map with complex value structure to test that value
  // aggregation work well in flat maps
  const auto type = CppToType<Row<
      Map<keyType, Row<int32_t, Row<std::string, Array<int16_t>>>>>>::create();

  testMapWriterStats(type);
}

TEST(ColumnWriterTests, TestMapWriterCompareStatsInt32Key) {
  using keyType = int32_t;

  // We create a complex map with complex value structure to test that value
  // aggregation work well in flat maps
  const auto type = CppToType<Row<
      Map<keyType, Row<int32_t, Row<std::string, Array<int16_t>>>>>>::create();

  testMapWriterStats(type);
}

TEST(ColumnWriterTests, TestMapWriterCompareStatsInt64Key) {
  using keyType = int64_t;

  // We create a complex map with complex value structure to test that value
  // aggregation work well in flat maps
  const auto type = CppToType<Row<
      Map<keyType, Row<int32_t, Row<std::string, Array<int16_t>>>>>>::create();

  testMapWriterStats(type);
}

template <typename type>
void testFractionalWrite(const TypePtr& t) {
  std::vector<std::optional<type>> data;
  generateSampleData(data);
  testDataTypeWriter(t, data);
}

TEST(ColumnWriterTests, TestFloatWriter) {
  testFractionalWrite<float>(REAL());
}

TEST(ColumnWriterTests, TestDoubleWriter) {
  testFractionalWrite<double>(DOUBLE());
}

template <typename type>
void testFractionalInfinityWrite(const TypePtr& t) {
  std::vector<std::optional<type>> data;
  type infinity = std::numeric_limits<type>::infinity();
  for (auto i = 0; i < ITERATIONS; ++i) {
    data.emplace_back(infinity);
  }
  testDataTypeWriter(t, data);
}

TEST(ColumnWriterTests, TestFloatInfinityWriter) {
  testFractionalInfinityWrite<float>(REAL());
}

TEST(ColumnWriterTests, TestDoubleInfinityWriter) {
  testFractionalInfinityWrite<double>(DOUBLE());
}

template <typename type>
void testFractionalNegativeInfinityWrite(const TypePtr& t) {
  std::vector<std::optional<type>> data;
  type negativeInfinity = -std::numeric_limits<type>::infinity();
  for (auto i = 0; i < ITERATIONS; ++i) {
    data.emplace_back(negativeInfinity);
  }
  testDataTypeWriter(t, data);
}

TEST(ColumnWriterTests, TestFloatNegativeInfinityWriter) {
  testFractionalNegativeInfinityWrite<float>(REAL());
}

TEST(ColumnWriterTests, TestDoubleNegativeInfinityWriter) {
  testFractionalNegativeInfinityWrite<double>(DOUBLE());
}

template <typename type>
void testFractionalNaNWrite(const TypePtr& t) {
  std::vector<std::optional<type>> data;
  type nan = -std::numeric_limits<type>::quiet_NaN();
  for (auto i = 0; i < ITERATIONS; ++i) {
    data.emplace_back(nan);
  }
  testDataTypeWriter(t, data);
}

TEST(ColumnWriterTests, TestFloatNanWriter) {
  testFractionalNaNWrite<float>(REAL());
}

TEST(ColumnWriterTests, TestDoubleNanWriter) {
  testFractionalNaNWrite<double>(DOUBLE());
}

template <typename type>
void testFractionalNullWrite(const TypePtr& t) {
  std::vector<std::optional<type>> data;
  for (auto i = 0; i < ITERATIONS; ++i) {
    data.emplace_back();
  }
  testDataTypeWriter(t, data);
}

TEST(ColumnWriterTests, TestFloatAllNullWriter) {
  testFractionalNullWrite<float>(REAL());
}

TEST(ColumnWriterTests, TestDoubleAllNullWriter) {
  testFractionalNullWrite<double>(DOUBLE());
}

template <typename type>
void testFractionalMixedWrite(const TypePtr& t) {
  std::vector<std::optional<type>> data;
  type nan = -std::numeric_limits<type>::quiet_NaN();
  type negativeInfinity = -std::numeric_limits<type>::infinity();
  type infinity = std::numeric_limits<type>::infinity();
  type max = std::numeric_limits<type>::max();
  type min = std::numeric_limits<type>::lowest();
  for (auto i = 0; i < 10'000; ++i) {
    data.emplace_back(nan);
    data.emplace_back();
    data.emplace_back(negativeInfinity);
    data.emplace_back(infinity);
    data.emplace_back(i);
    data.emplace_back(min);
    data.emplace_back(max);
  }
  testDataTypeWriter(t, data);
}

TEST(ColumnWriterTests, TestFloatMixedWriter) {
  testFractionalMixedWrite<float>(REAL());
}

TEST(ColumnWriterTests, TestDoubleMixedWriter) {
  testFractionalMixedWrite<double>(DOUBLE());
}

// This generation method is skewed due to double precision. However, this
// does not really matter - it would generate enough distinct values across
// the range for reasonable sizes. Tests for sizes bigger than the positive
// numeric range don't require a precise behavior for dinstinct value
// generation anyway since it overflows the dictionary.
int64_t generateRangeWithCustomLimits(
    size_t i,
    size_t size,
    int64_t lowest,
    int64_t max) {
  // Explicitly include the numeric limits.
  if (i == 0) {
    return lowest;
  }
  if (i == size - 1) {
    return max;
  }
  auto interval = -2 * folly::to<double>(lowest) / size;
  // Generate the range such that we have similar amounts of values generated
  // for each exponent.
  double center = size % 2 ? -0.5 : interval / 2 - 0.5;
  double value = center + (i - size / 2) * interval;
  // Return a guard-railed value with the numeric limits.
  // NOTE: There can be a more compact way to write this if we cast i and size
  // to signed types, but it's not worth the effort enforcing the assumptions.
  if (i < size / 2) {
    auto distance = (size / 2 - i) * interval;
    return std::max(center - distance, folly::to<double>(lowest));
  } else {
    auto distance = (i - size / 2) * interval;
    return std::min(center + distance + 1, -folly::to<double>(lowest)) - 1;
  }
}

template <typename Integer>
Integer generateRangeWithLimits(size_t i, size_t size) {
  auto lowest = std::numeric_limits<Integer>::lowest();
  auto max = std::numeric_limits<Integer>::max();
  return generateRangeWithCustomLimits(i, size, lowest, max);
}

int64_t generateSomewhatRandomPositiveData(
    size_t i,
    size_t size,
    int64_t /* unused */,
    int64_t /* unused */) {
  return size % (i + 1);
}

int64_t generateSomewhatRandomData(
    size_t i,
    size_t size,
    int64_t /* unused */,
    int64_t /* unused */) {
  int64_t val = size % (i + 1);
  return i % 2 ? val : -val;
}

template <typename Integer>
Integer generateSomewhatRandomDataTyped(size_t i, size_t size) {
  int64_t val = size % (i + 1);
  return i % 2 ? val : -val;
}

// This really is a placeholder method. In most practical cases, the PRESENT
// stream would just be empty if there are no nulls in the stripe.
bool noNulls(size_t /* unused */, size_t /* unused */) {
  return true;
}

bool noNullsWithStride(
    size_t /* unused */,
    size_t /* unused */,
    size_t /* unused */) {
  return true;
}

bool allNulls(size_t /* unused */, size_t /* unused */) {
  return false;
}

bool allNullsWithStride(
    size_t /* unused */,
    size_t /* unused */,
    size_t /* unused */) {
  return false;
}

bool someNulls(size_t i, size_t size) {
  return (size % (i + 1)) % 2;
}

bool someNullsWithStride(size_t /* unused */, size_t i, size_t size) {
  return (size % (i + 1)) % 2;
}

std::function<void(ColumnWriter&, size_t, size_t)> checkAbandonDict(
    const std::function<bool(size_t, size_t)>& callTryAbandonDict,
    bool force,
    const std::function<bool(size_t, size_t)>& abandonDictSuccess) {
  return [callTryAbandonDict, force, abandonDictSuccess](
             ColumnWriter& columnWriter, size_t stripeCount, size_t repCount) {
    if (callTryAbandonDict(stripeCount, repCount)) {
      EXPECT_EQ(
          abandonDictSuccess(stripeCount, repCount),
          columnWriter.tryAbandonDictionaries(force));
    }
  };
}

void noPostProcessing(
    ColumnWriter& /* unused */,
    size_t /* unused */,
    size_t /* unused */) {}

bool neverAbandonDict(size_t /* unused */, size_t /* unused */) {
  return false;
}

std::function<bool(size_t, size_t)> abandonNthWriteForStripe(
    size_t stripeIndex,
    size_t n) {
  return [stripeIndex, n](size_t stripeCount, size_t repCount) {
    return stripeCount == stripeIndex && repCount == n;
  };
}

bool abandonEveryWrite(size_t /* unused */, size_t /* unused */) {
  return true;
}

std::function<bool(size_t, size_t)> successAtNthWriteForStripe(
    size_t stripeIndex,
    size_t n) {
  return [stripeIndex, n](size_t stripeCount, size_t repCount) {
    return stripeCount == stripeIndex && repCount == n;
  };
}

bool noSuccess(size_t /* unused */, size_t /* unused */) {
  return false;
}

// A type erasure runner for the different test types.
struct TestRunner {
  template <typename T>
  /* implicit */ TestRunner(const T& test)
      : test_{std::make_shared<Test<T>>(test)} {}

  void runTest() const {
    test_->runTest();
  }

  struct TestInterface {
    virtual ~TestInterface() = default;
    virtual void runTest() const = 0;
  };

  template <typename T>
  struct Test : public TestInterface {
    explicit Test(const T& test) : test_{test} {}
    void runTest() const override {
      test_.runTest();
    }

   private:
    T test_;
  };

 private:
  std::shared_ptr<const TestInterface> test_;
};

template <typename Integer>
struct IntegerColumnWriterTypedTestCase {
  const size_t size;
  const bool writeDirect;
  const float dictionaryWriteThreshold;
  const size_t finalDictionarySize;
  const std::function<Integer(size_t, size_t)> genData;
  const bool hasNulls;
  const std::function<bool(size_t, size_t)> genNulls;
  const std::function<void(ColumnWriter&, size_t, size_t)> postProcess;
  const size_t repetitionCount;
  const size_t flushCount;

  IntegerColumnWriterTypedTestCase(
      size_t size,
      bool writeDirect,
      float dictionaryWriteThreshold,
      size_t finalDictionarySize,
      const std::function<Integer(size_t, size_t)>& genData,
      bool hasNulls,
      const std::function<bool(size_t, size_t)>& genNulls,
      const std::function<void(ColumnWriter&, size_t, size_t)>& postProcess,
      size_t repetitionCount = 1,
      size_t flushCount = 1)
      : size{size},
        writeDirect{writeDirect},
        dictionaryWriteThreshold{dictionaryWriteThreshold},
        finalDictionarySize{finalDictionarySize},
        genData{genData},
        hasNulls{hasNulls},
        genNulls{genNulls},
        postProcess{postProcess},
        repetitionCount{repetitionCount},
        flushCount{flushCount} {}

  virtual ~IntegerColumnWriterTypedTestCase() = default;

  void runTest() const {
    auto type = CppToType<Integer>::create();
    auto typeWithId = TypeWithId::create(type, 1);
    std::unique_ptr<ScopedMemoryPool> scopedPool = getDefaultScopedMemoryPool();
    auto& pool = *scopedPool;

    // Prepare input
    BufferPtr nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), &pool);
    auto* nullsPtr = nulls->asMutable<uint64_t>();
    size_t nullCount = 0;

    BufferPtr values = AlignedBuffer::allocate<Integer>(size, &pool);
    auto* valuesPtr = values->asMutable<Integer>();

    for (size_t i = 0; i != size; ++i) {
      bool isPresent = genNulls(i, size);
      if (hasNulls && !isPresent) {
        bits::setNull(nullsPtr, i);
        valuesPtr[i] = 0;
        nullCount++;
      } else {
        bits::clearNull(nullsPtr, i);
        valuesPtr[i] = genData(i, size);
      }
    }

    // Randomly shuffle the input, based on a seed to verify more
    // combinations.
    uint32_t seed = Random::rand32();

    std::mt19937 gen;
    gen.seed(seed);
    for (size_t i = 0; i != size; ++i) {
      size_t j = Random::rand32(i, size, gen);
      if (i != j) {
        std::swap(valuesPtr[i], valuesPtr[j]);
        auto tmp = bits::isBitNull(nullsPtr, i);
        bits::setNull(nullsPtr, i, bits::isBitNull(nullsPtr, j));
        bits::setNull(nullsPtr, j, tmp);
      }
    }

    auto batch = std::make_shared<FlatVector<Integer>>(
        &pool, nulls, size, values, std::vector<BufferPtr>());
    batch->setNullCount(nullCount);

    // Set up writer.
    auto config = std::make_shared<Config>();
    config->set(
        Config::DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD,
        dictionaryWriteThreshold);
    WriterContext context{config, getDefaultScopedMemoryPool()};
    // Register root node.
    auto columnWriter = BaseColumnWriter::create(context, *typeWithId);

    for (size_t i = 0; i != flushCount; ++i) {
      proto::StripeFooter stripeFooter;
      for (size_t j = 0; j != repetitionCount; ++j) {
        columnWriter->write(batch, Ranges::of(0, batch->size()));
        postProcess(*columnWriter, i, j);
        columnWriter->createIndexEntry();
      }
      // We only flush once per stripe.
      columnWriter->flush(
          [&stripeFooter](uint32_t /* unused */) -> proto::ColumnEncoding& {
            return *stripeFooter.add_encoding();
          });

      // Read and verify.
      const size_t nodeId = 1;
      auto rowType = ROW({{"integral_column", type}});
      TestStripeStreams streams(context, stripeFooter, rowType);
      EncodingKey key{nodeId};
      const auto& encoding = streams.getEncoding(key);
      if (writeDirect) {
        ASSERT_EQ(
            proto::ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT,
            encoding.kind());
      } else {
        ASSERT_EQ(
            proto::ColumnEncoding_Kind::ColumnEncoding_Kind_DICTIONARY,
            encoding.kind());
        ASSERT_EQ(finalDictionarySize, encoding.dictionarysize());
      }

      auto reqType = TypeWithId::create(rowType)->childAt(0);
      auto columnReader = ColumnReader::build(reqType, reqType, streams);

      for (size_t j = 0; j != repetitionCount; ++j) {
        // TODO Make reuse work
        VectorPtr resultBatch;
        columnReader->next(size, resultBatch);
        auto resultIv =
            std::dynamic_pointer_cast<FlatVector<Integer>>(resultBatch);
        std::vector<Integer> resultVec{
            resultIv->rawValues(), resultIv->rawValues() + resultIv->size()};
        ASSERT_EQ(batch->size(), resultBatch->size());
        ASSERT_EQ(batch->getNullCount(), resultBatch->getNullCount());
        if (!batch->getNullCount().has_value() ||
            batch->getNullCount().value() > 0) {
          // Normalizing the null values so that we can leverage gtest
          // matchers later for better failure dumps.
          for (size_t k = 0; k != batch->size(); ++k) {
            if (batch->isNullAt(k)) {
              resultVec[k] = 0;
            }
          }
          for (size_t k = 0; k < batch->size(); k++) {
            EXPECT_EQ(batch->isNullAt(k), resultBatch->isNullAt(k));
          }
        }
        EXPECT_THAT(
            resultVec,
            ElementsAreArray(batch->rawValues(), resultBatch->size()));
      }
      context.nextStripe();
      columnWriter->reset();
    }
  }
};

template <typename Integer>
struct TypedDictionaryEncodingTestCase
    : public IntegerColumnWriterTypedTestCase<Integer> {
  TypedDictionaryEncodingTestCase(
      size_t size,
      float dictionaryWriteThreshold,
      size_t finalDictionarySize,
      const std::function<Integer(size_t, size_t)>& genData,
      size_t repetitionCount = 1,
      size_t flushCount = 1)
      : IntegerColumnWriterTypedTestCase<Integer>{
            size,
            false,
            dictionaryWriteThreshold,
            finalDictionarySize,
            genData,
            false,
            noNulls,
            noPostProcessing,
            repetitionCount,
            flushCount} {}
};

// Even as the dictionary size threshold allows, we might not end up writing
// with dictionary encoding.
template <typename Integer>
struct TypedDirectEncodingTestCase
    : public IntegerColumnWriterTypedTestCase<Integer> {
  TypedDirectEncodingTestCase(
      size_t size,
      float dictionaryWriteThreshold,
      const std::function<Integer(size_t, size_t)>& genData,
      size_t repetitionCount = 1,
      size_t flushCount = 1)
      : IntegerColumnWriterTypedTestCase<Integer>{
            size,
            true,
            dictionaryWriteThreshold,
            0,
            genData,
            false,
            noNulls,
            noPostProcessing,
            repetitionCount,
            flushCount} {}
};

struct IntegerColumnWriterUniversalTestCase {
  IntegerColumnWriterTypedTestCase<int16_t> int16tTestCase;
  IntegerColumnWriterTypedTestCase<int32_t> int32tTestCase;
  IntegerColumnWriterTypedTestCase<int64_t> int64tTestCase;

  IntegerColumnWriterUniversalTestCase(
      size_t size,
      bool writeDirect,
      float dictionaryWriteThreshold,
      size_t finalDictionarySize,
      const std::function<int64_t(size_t, size_t, int64_t, int64_t)>& genData,
      bool hasNulls,
      const std::function<bool(size_t, size_t)>& genNulls,
      const std::function<void(ColumnWriter&, size_t, size_t)>& postProcess,
      size_t repetitionCount = 1,
      size_t flushCount = 1)
      : int16tTestCase{size, writeDirect, dictionaryWriteThreshold, finalDictionarySize, coerceGenData<int16_t>(genData), hasNulls, genNulls, postProcess, repetitionCount, flushCount},
        int32tTestCase{
            size,
            writeDirect,
            dictionaryWriteThreshold,
            finalDictionarySize,
            coerceGenData<int32_t>(genData),
            hasNulls,
            genNulls,
            postProcess,
            repetitionCount,
            flushCount},
        int64tTestCase{
            size,
            writeDirect,
            dictionaryWriteThreshold,
            finalDictionarySize,
            coerceGenData<int64_t>(genData),
            hasNulls,
            genNulls,
            postProcess,
            repetitionCount,
            flushCount} {}

  virtual ~IntegerColumnWriterUniversalTestCase() = default;

  template <typename Integer>
  std::function<Integer(size_t, size_t)> coerceGenData(
      const std::function<int64_t(size_t, size_t, int64_t, int64_t)>& genData) {
    return [=](size_t i, size_t size) -> Integer {
      return genData(
          i,
          size,
          std::numeric_limits<Integer>::lowest(),
          std::numeric_limits<Integer>::max());
    };
  }

  void runTest() const {
    int16tTestCase.runTest();
    int32tTestCase.runTest();
    int64tTestCase.runTest();
  }
};

// Test cases that would end up writing with dictionary encoding
struct IntegerColumnWriterDictionaryEncodingUniversalTestCase
    : public IntegerColumnWriterUniversalTestCase {
  IntegerColumnWriterDictionaryEncodingUniversalTestCase(
      size_t size,
      float dictionaryWriteThreshold,
      size_t finalDictionarySize,
      const std::function<int64_t(size_t, size_t, int64_t, int64_t)>& genData,
      bool hasNulls,
      const std::function<bool(size_t, size_t)>& genNulls,
      size_t repetitionCount = 1,
      size_t flushCount = 1)
      : IntegerColumnWriterUniversalTestCase{
            size,
            false,
            dictionaryWriteThreshold,
            finalDictionarySize,
            genData,
            hasNulls,
            genNulls,
            noPostProcessing,
            repetitionCount,
            flushCount} {}
};

// Universal test cases that would end up writing with direct encoding.
struct IntegerColumnWriterDirectEncodingUniversalTestCase
    : public IntegerColumnWriterUniversalTestCase {
  IntegerColumnWriterDirectEncodingUniversalTestCase(
      size_t size,
      float dictionaryWriteThreshold,
      const std::function<int64_t(size_t, size_t, int64_t, int64_t)>& genData,
      bool hasNulls,
      const std::function<bool(size_t, size_t)>& genNulls,
      const std::function<void(ColumnWriter&, size_t, size_t)>& postProcess,
      size_t repetitionCount = 1,
      size_t flushCount = 1)
      : IntegerColumnWriterUniversalTestCase{
            size,
            true,
            dictionaryWriteThreshold,
            0,
            genData,
            hasNulls,
            genNulls,
            postProcess,
            repetitionCount,
            flushCount} {}
};

TEST(ColumnWriterTests, IntegerTypeDictionaryEncodingWrites) {
  struct TestCase
      : public IntegerColumnWriterDictionaryEncodingUniversalTestCase {
    TestCase(
        size_t size,
        size_t finalDictionarySize,
        const std::function<int64_t(size_t, size_t, int64_t, int64_t)>& genData,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : IntegerColumnWriterDictionaryEncodingUniversalTestCase{
              size,
              1.0,
              finalDictionarySize,
              genData,
              false,
              noNulls,
              repetitionCount,
              flushCount} {}
  };

  std::vector<TestRunner> testCases{
      // No values in dictionary.
      TestCase{1000, 0, generateRangeWithCustomLimits},
      // Mixture of values in dictionary vs not.
      TestCase{1000, 201, generateSomewhatRandomData},
      // Test repeated writes. All values are in dictionary.
      TestCase{1000, 617, generateSomewhatRandomData, 20},
      // Test repeated writes and flushes with mixture of values in
      // dictionary.
      // Tests independence of dictionary encoding writes across stripes. (In
      // other words, whether the dictionary is cleared across stripes.)
      TestCase{1000, 201, generateSomewhatRandomData, 1, 10},
      // Test repeated writes and flushes. All values are in dictionary.
      TestCase{1000, 617, generateSomewhatRandomData, 2, 10},
  };
  for (const auto& testCase : testCases) {
    testCase.runTest();
  }
}

TEST(ColumnWriterTests, IntegerTypeDictionaryEncodingWritesWithNulls) {
  struct DictionaryEncodingTestCase
      : public IntegerColumnWriterDictionaryEncodingUniversalTestCase {
    DictionaryEncodingTestCase(
        size_t size,
        size_t finalDictionarySize,
        const std::function<int64_t(size_t, size_t, int64_t, int64_t)>& genData,
        const std::function<bool(size_t, size_t)>& genNulls,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : IntegerColumnWriterDictionaryEncodingUniversalTestCase{
              size,
              1.0,
              finalDictionarySize,
              genData,
              true,
              genNulls,
              repetitionCount,
              flushCount} {}
  };

  // Even as the dictionary size threshold allows, we might not end up writing
  // with dictionary encoding.
  struct DirectEncodingTestCase
      : public IntegerColumnWriterDirectEncodingUniversalTestCase {
    DirectEncodingTestCase(
        size_t size,
        const std::function<int64_t(size_t, size_t, int64_t, int64_t)>& genData,
        const std::function<bool(size_t, size_t)>& genNulls,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : IntegerColumnWriterDirectEncodingUniversalTestCase{
              size,
              1.0,
              genData,
              true,
              genNulls,
              noPostProcessing,
              repetitionCount,
              flushCount} {}
  };

  std::vector<TestRunner> testCases{
      // When all values are null, we don't try to write dictionary at all.
      DirectEncodingTestCase{1000, generateRangeWithCustomLimits, allNulls},
      // No values in dictionary.
      DictionaryEncodingTestCase{
          1000, 0, generateRangeWithCustomLimits, someNulls},
      // Mixture of values in dictionary vs not.
      DictionaryEncodingTestCase{
          1000, 62, generateSomewhatRandomData, someNulls},
      // Test repeated writes. All values are in dictionary.
      DictionaryEncodingTestCase{
          1000, 250, generateSomewhatRandomData, someNulls, 20},
      // Test repeated writes and flushes with mixture of values in
      // dictionary.
      // Tests independence of dictionary encoding writes across stripes. (In
      // other words, whether the dictionary is cleared across stripes.)
      DictionaryEncodingTestCase{
          1000, 62, generateSomewhatRandomData, someNulls, 1, 10},
      // Test repeated writes and flushes. All values are in dictionary.
      DictionaryEncodingTestCase{
          1000, 250, generateSomewhatRandomData, someNulls, 2, 10},
  };
  for (const auto& testCase : testCases) {
    testCase.runTest();
  }
}

TEST(ColumnWriterTests, IntegerTypeDictionaryEncodingHugeWrites) {
  struct TestCase
      : public IntegerColumnWriterDictionaryEncodingUniversalTestCase {
    TestCase(
        size_t size,
        size_t finalDictionarySize,
        const std::function<int64_t(size_t, size_t, int64_t, int64_t)>& genData,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : IntegerColumnWriterDictionaryEncodingUniversalTestCase{
              size,
              1.0,
              finalDictionarySize,
              genData,
              false,
              noNulls,
              repetitionCount,
              flushCount} {}
  };

  std::vector<TestRunner> testCases{
      // We hit each distinct value in int16_t more than once.
      TypedDictionaryEncodingTestCase<int16_t>{
          1000000,
          1.0,
          -2 * std::numeric_limits<int16_t>::lowest(),
          generateRangeWithLimits<int16_t>},
      TypedDictionaryEncodingTestCase<int32_t>{
          1000000, 1.0, 0, generateRangeWithLimits<int32_t>},
      TypedDictionaryEncodingTestCase<int64_t>{
          1000000, 1.0, 0, generateRangeWithLimits<int64_t>},
      // Mixture of values in dictionary vs not. Out of range for int16_t.
      TypedDictionaryEncodingTestCase<int32_t>{
          1000000, 1.0, 201668, generateSomewhatRandomDataTyped<int32_t>},
      TypedDictionaryEncodingTestCase<int64_t>{
          1000000, 1.0, 201668, generateSomewhatRandomDataTyped<int64_t>},
  };

  for (const auto& testCase : testCases) {
    testCase.runTest();
  }
}

// Split test to avoid sandcastle timeouts.
TEST(ColumnWriterTests, IntegerTypeDictionaryEncodingHugeRepeatedWrites) {
  struct TestCase
      : public IntegerColumnWriterDictionaryEncodingUniversalTestCase {
    TestCase(
        size_t size,
        size_t finalDictionarySize,
        const std::function<int64_t(size_t, size_t, int64_t, int64_t)>& genData,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : IntegerColumnWriterDictionaryEncodingUniversalTestCase{
              size,
              1.0,
              finalDictionarySize,
              genData,
              false,
              noNulls,
              repetitionCount,
              flushCount} {}
  };

  std::vector<TestRunner> testCases{
      // Test repeated writes. All values are in dictionary.
      TestCase{1000, 617, generateSomewhatRandomData, 1000},
      // Test repeated writes and flushes with mixture of values in
      // dictionary.
      // Tests independence of dictionary encoding writes across stripes. (In
      // other words, whether the dictionary is cleared across stripes.)
      TestCase{1000, 201, generateSomewhatRandomData, 1, 1000},
      // Test repeated writes and flushes. All values are in dictionary.
      TestCase{1000, 617, generateSomewhatRandomData, 2, 500},
  };

  for (const auto& testCase : testCases) {
    testCase.runTest();
  }
}

TEST(ColumnWriterTests, IntegerTypeDirectEncodingWrites) {
  struct TestCase : public IntegerColumnWriterDirectEncodingUniversalTestCase {
    TestCase(
        size_t size,
        const std::function<int64_t(size_t, size_t, int64_t, int64_t)>& genData,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : IntegerColumnWriterDirectEncodingUniversalTestCase{
              size,
              0.0,
              genData,
              false,
              noNulls,
              noPostProcessing,
              repetitionCount,
              flushCount} {}
  };

  std::vector<TestRunner> testCases{
      TestCase{1000, generateRangeWithCustomLimits},
      TestCase{1000, generateSomewhatRandomData},
      TestCase{1000, generateSomewhatRandomData, 20},
      // Note, writes from the second stripe on uses direct encoding from the
      // get-go.
      // TODO: reinforce that the subsequent writes go through the right code
      // path when we have more granular memory tracking.
      TestCase{1000, generateSomewhatRandomData, 1, 10},
      TestCase{1000, generateSomewhatRandomData, 2, 10},
  };
  for (const auto& testCase : testCases) {
    testCase.runTest();
  }
}

TEST(ColumnWriterTests, IntegerTypeDirectEncodingWritesWithNulls) {
  struct TestCase : public IntegerColumnWriterDirectEncodingUniversalTestCase {
    TestCase(
        size_t size,
        const std::function<int64_t(size_t, size_t, int64_t, int64_t)>& genData,
        const std::function<bool(size_t, size_t)>& genNulls,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : IntegerColumnWriterDirectEncodingUniversalTestCase{
              size,
              0.0,
              genData,
              true,
              genNulls,
              noPostProcessing,
              repetitionCount,
              flushCount} {}
  };

  std::vector<TestRunner> testCases{
      TestCase{1000, generateRangeWithCustomLimits, allNulls},
      TestCase{1000, generateRangeWithCustomLimits, someNulls},
      TestCase{1000, generateSomewhatRandomData, someNulls},
      TestCase{1000, generateSomewhatRandomData, someNulls, 20},
      // Note, writes from the second stripe on uses direct encoding from the
      // get-go.
      // TODO: reinforce that the subsequent writes go through the right code
      // path when we have more granular memory tracking.
      TestCase{1000, generateSomewhatRandomData, someNulls, 1, 10},
      TestCase{1000, generateSomewhatRandomData, someNulls, 2, 10},
  };
  for (const auto& testCase : testCases) {
    testCase.runTest();
  }
}

TEST(ColumnWriterTests, IntegerTypeDirectEncodingHugeWrites) {
  struct TestCase : public IntegerColumnWriterDirectEncodingUniversalTestCase {
    TestCase(
        size_t size,
        const std::function<int64_t(size_t, size_t, int64_t, int64_t)>& genData,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : IntegerColumnWriterDirectEncodingUniversalTestCase{
              size,
              0.0,
              genData,
              false,
              noNulls,
              noPostProcessing,
              repetitionCount,
              flushCount} {}
  };

  std::vector<TestRunner> testCases{
      // Everything is straight forward for direct encoding writes, so no
      // special casing needed for types.
      TestCase{1000000, generateRangeWithCustomLimits},
      TestCase{1000000, generateSomewhatRandomData},
  };
  for (const auto& testCase : testCases) {
    testCase.runTest();
  }
}

// Split test to avoid sandcastle timeouts.
TEST(ColumnWriterTests, IntegerTypeDirectEncodingHugeRepeatedWrites) {
  struct TestCase : public IntegerColumnWriterDirectEncodingUniversalTestCase {
    TestCase(
        size_t size,
        const std::function<int64_t(size_t, size_t, int64_t, int64_t)>& genData,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : IntegerColumnWriterDirectEncodingUniversalTestCase{
              size,
              0.0,
              genData,
              false,
              noNulls,
              noPostProcessing,
              repetitionCount,
              flushCount} {}
  };

  std::vector<TestRunner> testCases{
      // Test repeated writes.
      TestCase{1000, generateSomewhatRandomData, 1000},
      // Note, writes from the second stripe on uses direct encoding from the
      // get-go.
      // TODO: reinforce that the subsequent writes go through the right code
      // path when we have more granular memory tracking.
      TestCase{1000, generateSomewhatRandomData, 1, 1000},
      TestCase{1000, generateSomewhatRandomData, 2, 500},
  };
  for (const auto& testCase : testCases) {
    testCase.runTest();
  }
}

TEST(ColumnWriterTests, IntegerTypeDictionaryWriteThreshold) {
  struct DictionaryEncodingTestCase
      : public IntegerColumnWriterDictionaryEncodingUniversalTestCase {
    DictionaryEncodingTestCase(
        size_t size,
        float dictionarySizeThreshold,
        size_t finalDictionarySize,
        const std::function<int64_t(size_t, size_t, int64_t, int64_t)>& genData,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : IntegerColumnWriterDictionaryEncodingUniversalTestCase{
              size,
              dictionarySizeThreshold,
              finalDictionarySize,
              genData,
              false,
              noNulls,
              repetitionCount,
              flushCount} {}
  };

  struct DirectEncodingTestCase
      : public IntegerColumnWriterDirectEncodingUniversalTestCase {
    DirectEncodingTestCase(
        size_t size,
        float dictionarySizeThreshold,
        const std::function<int64_t(size_t, size_t, int64_t, int64_t)>& genData,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : IntegerColumnWriterDirectEncodingUniversalTestCase{
              size,
              dictionarySizeThreshold,
              genData,
              false,
              noNulls,
              noPostProcessing,
              repetitionCount,
              flushCount} {}
  };

  struct DirectEncodingTestCaseAllNulls
      : public IntegerColumnWriterDirectEncodingUniversalTestCase {
    DirectEncodingTestCaseAllNulls(
        size_t size,
        float dictionarySizeThreshold,
        const std::function<int64_t(size_t, size_t, int64_t, int64_t)>& genData,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : IntegerColumnWriterDirectEncodingUniversalTestCase{
              size,
              dictionarySizeThreshold,
              genData,
              true,
              allNulls,
              noPostProcessing,
              repetitionCount,
              flushCount} {}
  };

  std::vector<TestRunner> testCases{
      // All nulls should use direct encoding regardless.
      DirectEncodingTestCaseAllNulls{1000, 1.0, generateRangeWithCustomLimits},
      // No value in dictionary.
      DirectEncodingTestCase{1000, 0.75, generateRangeWithCustomLimits},
      DictionaryEncodingTestCase{1000, 1.0, 0, generateRangeWithCustomLimits},
      // Dict size is exactly 50.
      DictionaryEncodingTestCase{
          100, 0.5, 24, generateSomewhatRandomPositiveData},
      // Fails with a threshold that is the slightest amount under possible.
      DirectEncodingTestCase{
          100,
          0.5 - std::numeric_limits<float>::epsilon(),
          generateSomewhatRandomPositiveData},
      // All values are in dictionary in repeated writes.
      DirectEncodingTestCase{
          1000,
          0.5 - std::numeric_limits<float>::epsilon(),
          generateRangeWithCustomLimits,
          2},
      DictionaryEncodingTestCase{
          1000,
          0.5 - std::numeric_limits<float>::epsilon(),
          1000,
          generateRangeWithCustomLimits,
          4},
  };
  for (const auto& testCase : testCases) {
    testCase.runTest();
  }
}

TEST(ColumnWriterTests, IntegerColumnWriterAbandonDictionaries) {
  struct TestCase : public IntegerColumnWriterUniversalTestCase {
    TestCase(
        size_t size,
        bool writeDirect,
        size_t finalDictionarySize,
        const std::function<int64_t(size_t, size_t, int64_t, int64_t)>& genData,
        const std::function<void(ColumnWriter&, size_t, size_t)>& postProcess,
        size_t repetitionCount,
        size_t flushCount)
        : IntegerColumnWriterUniversalTestCase{
              size,
              writeDirect,
              1.0f,
              finalDictionarySize,
              genData,
              false,
              noNulls,
              postProcess,
              repetitionCount,
              flushCount} {}
  };

  struct DirectEncodingTestCase : public TestCase {
    DirectEncodingTestCase(
        size_t size,
        const std::function<int64_t(size_t, size_t, int64_t, int64_t)>& genData,
        const std::function<void(ColumnWriter&, size_t, size_t)>& postProcess,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : TestCase(
              size,
              /* writeDirect */ true,
              /* finalDictionarySize */ 0,
              genData,
              postProcess,
              repetitionCount,
              flushCount) {}
  };

  struct DictionaryEncodingTestCase : public TestCase {
    DictionaryEncodingTestCase(
        size_t size,
        size_t finalDictionarySize,
        const std::function<int64_t(size_t, size_t, int64_t, int64_t)>& genData,
        const std::function<void(ColumnWriter&, size_t, size_t)>& postProcess,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : TestCase(
              size,
              /* writeDirect */ false,
              finalDictionarySize,
              genData,
              postProcess,
              repetitionCount,
              flushCount) {}
  };

  std::vector<TestRunner> testCases{
      DirectEncodingTestCase{
          1000,
          generateRangeWithCustomLimits,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 0),
              /* force */ true,
              successAtNthWriteForStripe(0, 0)),
          10},
      DirectEncodingTestCase{
          1000,
          generateRangeWithCustomLimits,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 0),
              /* force */ true,
              successAtNthWriteForStripe(0, 0)),
          1,
          10},
      // Test abandoning dictionary in multi-write scenarios. For now we could
      // only abandon dictionary starting from the first stripe, because we
      // could not switch up encoding of a writer once determined.
      DirectEncodingTestCase{
          1000,
          generateRangeWithCustomLimits,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 0),
              /* force */ true,
              successAtNthWriteForStripe(0, 0)),
          2,
          5},
      DirectEncodingTestCase{
          1000,
          generateRangeWithCustomLimits,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 1),
              /* force */ true,
              successAtNthWriteForStripe(0, 1)),
          2,
          5},
      // We shouldn't be able to switch encodings beyond the first stripe.
      DictionaryEncodingTestCase{
          1000,
          /* finalDictionarySize */ 1000,
          generateRangeWithCustomLimits,
          checkAbandonDict(
              abandonNthWriteForStripe(1, 0), /* force */ true, noSuccess),
          2,
          5},
      // Abandoning at every write to make sure subsequent abandon dict calls
      // are safe.
      DirectEncodingTestCase{
          1000,
          generateRangeWithCustomLimits,
          checkAbandonDict(
              abandonEveryWrite,
              /* force */ true,
              successAtNthWriteForStripe(0, 0)),
          10},
      DirectEncodingTestCase{
          1000,
          generateRangeWithCustomLimits,
          checkAbandonDict(
              abandonEveryWrite,
              /* force */ true,
              successAtNthWriteForStripe(0, 0)),
          1,
          10},
  };

  for (const auto& testCase : testCases) {
    testCase.runTest();
  }
}

TEST(ColumnWriterTests, IntegerColumnWriterAbandonDictionariesWithNulls) {
  struct TestCase : public IntegerColumnWriterUniversalTestCase {
    TestCase(
        size_t size,
        bool writeDirect,
        size_t finalDictionarySize,
        const std::function<int64_t(size_t, size_t, int64_t, int64_t)>& genData,
        const std::function<void(ColumnWriter&, size_t, size_t)>& postProcess,
        size_t repetitionCount,
        size_t flushCount)
        : IntegerColumnWriterUniversalTestCase{
              size,
              writeDirect,
              1.0f,
              finalDictionarySize,
              genData,
              true,
              someNulls,
              postProcess,
              repetitionCount,
              flushCount} {}
  };

  struct DirectEncodingTestCase : public TestCase {
    DirectEncodingTestCase(
        size_t size,
        const std::function<int64_t(size_t, size_t, int64_t, int64_t)>& genData,
        const std::function<void(ColumnWriter&, size_t, size_t)>& postProcess,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : TestCase(
              size,
              /* writeDirect */ true,
              /* finalDictionarySize */ 0,
              genData,
              postProcess,
              repetitionCount,
              flushCount) {}
  };

  struct DictionaryEncodingTestCase : public TestCase {
    DictionaryEncodingTestCase(
        size_t size,
        size_t finalDictionarySize,
        const std::function<int64_t(size_t, size_t, int64_t, int64_t)>& genData,
        const std::function<void(ColumnWriter&, size_t, size_t)>& postProcess,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : TestCase(
              size,
              /* writeDirect */ false,
              finalDictionarySize,
              genData,
              postProcess,
              repetitionCount,
              flushCount) {}
  };

  std::vector<TestRunner> testCases{
      DirectEncodingTestCase{
          1000,
          generateRangeWithCustomLimits,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 0),
              /* force */ true,
              successAtNthWriteForStripe(0, 0)),
          10},
      DirectEncodingTestCase{
          1000,
          generateRangeWithCustomLimits,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 0),
              /* force */ true,
              successAtNthWriteForStripe(0, 0)),
          1,
          10},
      // Test abandoning dictionary in multi-write scenarios. For now we could
      // only abandon dictionary starting from the first stripe, because we
      // could not switch up encoding of a writer once determined.
      DirectEncodingTestCase{
          1000,
          generateRangeWithCustomLimits,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 0),
              /* force */ true,
              successAtNthWriteForStripe(0, 0)),
          2,
          5},
      DirectEncodingTestCase{
          1000,
          generateRangeWithCustomLimits,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 1),
              /* force */ true,
              successAtNthWriteForStripe(0, 1)),
          2,
          5},
      // We shouldn't be able to switch encodings beyond the first stripe.
      DictionaryEncodingTestCase{
          1000,
          /* finalDictionarySize */ 341,
          generateRangeWithCustomLimits,
          checkAbandonDict(
              abandonNthWriteForStripe(1, 0), /* force */ true, noSuccess),
          2,
          5},
      // Abandoning at every write to make sure subsequent abandon dict calls
      // are safe.
      DirectEncodingTestCase{
          1000,
          generateRangeWithCustomLimits,
          checkAbandonDict(
              abandonEveryWrite,
              /* force */ true,
              successAtNthWriteForStripe(0, 0)),
          10},
      DirectEncodingTestCase{
          1000,
          generateRangeWithCustomLimits,
          checkAbandonDict(
              abandonEveryWrite,
              /* force */ true,
              successAtNthWriteForStripe(0, 0)),
          1,
          10},
  };

  for (const auto& testCase : testCases) {
    testCase.runTest();
  }
}

TEST(ColumnWriterTests, IntegerColumnWriterAbandonLowValueDictionaries) {
  struct TestCase : public IntegerColumnWriterUniversalTestCase {
    TestCase(
        size_t size,
        bool writeDirect,
        float dictionaryWriteThreshold,
        size_t finalDictionarySize,
        const std::function<int64_t(size_t, size_t, int64_t, int64_t)>& genData,
        const std::function<void(ColumnWriter&, size_t, size_t)>& postProcess,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : IntegerColumnWriterUniversalTestCase{
              size,
              writeDirect,
              dictionaryWriteThreshold,
              finalDictionarySize,
              genData,
              false,
              noNulls,
              postProcess,
              repetitionCount,
              flushCount} {}
  };

  struct DirectEncodingTestCase : public TestCase {
    DirectEncodingTestCase(
        size_t size,
        float dictionaryWriteThreshold,
        const std::function<int64_t(size_t, size_t, int64_t, int64_t)>& genData,
        const std::function<void(ColumnWriter&, size_t, size_t)>& postProcess,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : TestCase(
              size,
              /* writeDirect */ true,
              dictionaryWriteThreshold,
              /* finalDictionarySize */ 0,
              genData,
              postProcess,
              repetitionCount,
              flushCount) {}
  };

  struct DictionaryEncodingTestCase : public TestCase {
    DictionaryEncodingTestCase(
        size_t size,
        float dictionaryWriteThreshold,
        size_t finalDictionarySize,
        const std::function<int64_t(size_t, size_t, int64_t, int64_t)>& genData,
        const std::function<void(ColumnWriter&, size_t, size_t)>& postProcess,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : TestCase(
              size,
              /* writeDirect */ false,
              dictionaryWriteThreshold,
              finalDictionarySize,
              genData,
              postProcess,
              repetitionCount,
              flushCount) {}
  };

  std::vector<TestRunner> testCases{
      DictionaryEncodingTestCase{
          1000,
          /* dictionaryWriteThreshold */ 1.0f,
          /* finalDictionarySize */ 1000,
          generateRangeWithCustomLimits,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 0),
              /* force */ false,
              noSuccess),
          /* repCount */ 10},
      // finalDictionarySize == 0 because in each stripe
      // each value only appeared once.
      DictionaryEncodingTestCase{
          1000,
          /* dictionaryWriteThreshold */ 1.0f,
          /* finalDictionarySize */ 0,
          generateRangeWithCustomLimits,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 0),
              /* force */ false,
              noSuccess),
          /* repCount */ 1,
          /* stripeCount */ 10},
      DirectEncodingTestCase{
          1000,
          /* dictionaryWriteThreshold */ 0.4f,
          generateRangeWithCustomLimits,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 1),
              /* force */ false,
              successAtNthWriteForStripe(0, 1)),
          /* repCount */ 10},
      DictionaryEncodingTestCase{
          1000,
          /* dictionaryWriteThreshold */ 0.4f,
          /* finalDictionarySize */ 1000,
          generateRangeWithCustomLimits,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 2),
              /* force */ false,
              noSuccess),
          /* repCount */ 10},
      // Test abandoning dictionary in multi-write scenarios. For now we could
      // only abandon dictionary starting from the first stripe, because we
      // could not switch up encoding of a writer once determined.
      DictionaryEncodingTestCase{
          1000,
          /* dictionaryWriteThreshold */ 1.0f,
          /* finalDictionarySize */ 1000,
          generateRangeWithCustomLimits,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 1),
              /* force */ false,
              noSuccess),
          /* repCount */ 5,
          /* stripeCount*/ 2},
      // We shouldn't be able to switch encodings beyond the first stripe.
      DictionaryEncodingTestCase{
          1000,
          /* dictionaryWriteThreshold */ 1.0f,
          /* finalDictionarySize */ 1000,
          generateRangeWithCustomLimits,
          checkAbandonDict(
              abandonNthWriteForStripe(1, 0), /* force */ false, noSuccess),
          /* repCount */ 5,
          /* stripeCount*/ 2},
      DirectEncodingTestCase{
          1000,
          /* dictionaryWriteThreshold */ 0.1f,
          generateRangeWithCustomLimits,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 1),
              /* force */ false,
              successAtNthWriteForStripe(0, 1)),
          /* repCount */ 5,
          /* stripeCount*/ 2},
      // Return false because we were already using direct encoding.
      DirectEncodingTestCase{
          1000,
          /* dictionaryWriteThreshold */ 0.1f,
          generateRangeWithCustomLimits,
          checkAbandonDict(
              abandonNthWriteForStripe(1, 0), /* force */ false, noSuccess),
          /* repCount */ 5,
          /* stripeCount*/ 2},
      // Abandoning at every write to make sure subsequent abandon dict calls
      // are safe.
      DictionaryEncodingTestCase{
          1000,
          /* dictionaryWriteThreshold */ 1.0f,
          /* finalDictionarySize */ 1000,
          generateRangeWithCustomLimits,
          checkAbandonDict(
              abandonEveryWrite,
              /* force */ false,
              noSuccess),
          /* repCount */ 10},
      // finalDictionarySize == 0 because in each stripe
      // each value only appeared once.
      DictionaryEncodingTestCase{
          1000,
          /* dictionaryWriteThreshold */ 1.0f,
          /* finalDictionarySize */ 0,
          generateRangeWithCustomLimits,
          checkAbandonDict(
              abandonEveryWrite,
              /* force */ false,
              noSuccess),
          /* repCount */ 1,
          /* stripeCount*/ 10},
  };

  for (const auto& testCase : testCases) {
    testCase.runTest();
  }
}

template <typename Integer>
void testIntegerDictionaryEncodableWriterConstructor() {
  auto type = CppToType<Integer>::create();
  auto typeWithId = TypeWithId::create(type, 1);
  std::unique_ptr<ScopedMemoryPool> scopedPool = getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;

  // Write input
  auto config = std::make_shared<Config>();
  float slightlyOver = 1.0f + std::numeric_limits<float>::epsilon() * 2;
  {
    config->set(Config::DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD, slightlyOver);
    WriterContext context{config, getDefaultScopedMemoryPool()};
    EXPECT_ANY_THROW(BaseColumnWriter::create(context, *typeWithId));
  }
  float slightlyUnder = -std::numeric_limits<float>::epsilon();
  {
    config->set(Config::DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD, slightlyUnder);
    WriterContext context{config, getDefaultScopedMemoryPool()};
    EXPECT_ANY_THROW(BaseColumnWriter::create(context, *typeWithId));
  }
}

TEST(ColumnWriterTests, IntegerDictionaryDictionaryEncodableWriterCtor) {
  testIntegerDictionaryEncodableWriterConstructor<int16_t>();
  testIntegerDictionaryEncodableWriterConstructor<int32_t>();
  testIntegerDictionaryEncodableWriterConstructor<int64_t>();
}

std::string
generateSomewhatRandomStringData(size_t /*unused*/, size_t i, size_t size) {
  return folly::to<std::string>(generateSomewhatRandomData(i, size, 0, 0));
}

std::string generateStringRange(size_t /*unused*/, size_t i, size_t size) {
  return folly::to<std::string>(generateRangeWithLimits<int64_t>(i, size));
}

struct StringColumnWriterTestCase {
  const size_t size;
  const bool writeDirect;
  const float dictionaryKeyEfficiencyThreshold;
  const float entropyKeyEfficiencyThreshold;
  const size_t finalDictionarySize;
  const std::function<std::string(size_t, size_t, size_t)> genData;
  const bool hasNulls;
  const std::function<bool(size_t, size_t, size_t)> genNulls;
  const std::function<void(ColumnWriter&, size_t, size_t)> postProcess;
  const size_t repetitionCount;
  const size_t flushCount;
  const std::shared_ptr<const Type> type;

  StringColumnWriterTestCase(
      size_t size,
      bool writeDirect,
      float dictionaryKeyEfficiencyThreshold,
      float entropyKeyEfficiencyThreshold,
      size_t finalDictionarySize,
      const std::function<std::string(size_t, size_t, size_t)>& genData,
      bool hasNulls,
      const std::function<bool(size_t, size_t, size_t)>& genNulls,
      const std::function<void(ColumnWriter&, size_t, size_t)>& postProcess,
      size_t repetitionCount = 1,
      size_t flushCount = 1)
      : size{size},
        writeDirect{writeDirect},
        dictionaryKeyEfficiencyThreshold{dictionaryKeyEfficiencyThreshold},
        entropyKeyEfficiencyThreshold{entropyKeyEfficiencyThreshold},
        finalDictionarySize{finalDictionarySize},
        genData{genData},
        hasNulls{hasNulls},
        genNulls{genNulls},
        postProcess{postProcess},
        repetitionCount{repetitionCount},
        flushCount{flushCount},
        type{CppToType<folly::StringPiece>::create()} {}

  virtual ~StringColumnWriterTestCase() = default;

  FlatVectorPtr<StringView> generateStringSlice(
      size_t strideIndex,
      MemoryPool* pool) const {
    auto nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), pool);
    auto* nullsPtr = nulls->asMutable<uint64_t>();

    auto values = AlignedBuffer::allocate<StringView>(size, pool);
    auto* valuesPtr = values->asMutable<StringView>();

    std::vector<BufferPtr> dataChunks;

    BufferPtr dataChunk = AlignedBuffer::allocate<char>(1024, pool);
    dataChunks.push_back(dataChunk);
    auto* dataChunkPtr = dataChunk->asMutable<char>();

    size_t nullCount = 0;
    size_t offset = 0;
    for (size_t i = 0; i != size; ++i) {
      bool isPresent = genNulls(strideIndex, i, size);
      bits::setNull(nullsPtr, i, !isPresent);

      if (isPresent) {
        auto val = genData(strideIndex, i, size);
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

    auto stringVector = std::make_shared<FlatVector<StringView>>(
        pool, nulls, size, values, std::move(dataChunks));
    stringVector->setNullCount(nullCount);
    return stringVector;
  }

  void runTest() const {
    std::unique_ptr<ScopedMemoryPool> scopedPool = getDefaultScopedMemoryPool();
    auto& pool = *scopedPool;

    // Set up writer.
    auto config = std::make_shared<Config>();
    config->set(
        Config::ENTROPY_KEY_STRING_SIZE_THRESHOLD,
        entropyKeyEfficiencyThreshold);
    config->set(
        Config::DICTIONARY_STRING_KEY_SIZE_THRESHOLD,
        dictionaryKeyEfficiencyThreshold);
    WriterContext context{config, getDefaultScopedMemoryPool()};
    // Register root node.
    auto typeWithId = TypeWithId::create(type, 1);
    auto columnWriter = BaseColumnWriter::create(context, *typeWithId);

    // Prepare input
    std::vector<VectorPtr> batches;

    for (size_t j = 0; j != repetitionCount; j++) {
      batches.emplace_back(generateStringSlice(j, &pool));
    }

    for (size_t i = 0; i != flushCount; ++i) {
      proto::StripeFooter stripeFooter;
      // Write Stride
      for (size_t j = 0; j != repetitionCount; ++j) {
        // TODO: break the batch into multiple strides.
        columnWriter->write(batches[j], Ranges::of(0, size));
        postProcess(*columnWriter, i, j);
        columnWriter->createIndexEntry();
      }

      // Flush when all strides are written (once per stripe).
      columnWriter->flush(
          [&stripeFooter](uint32_t /* unused */) -> proto::ColumnEncoding& {
            return *stripeFooter.add_encoding();
          });

      // Read and verify.
      const size_t nodeId = 1;
      auto rowType = ROW({{"string_column", type}});
      TestStripeStreams streams(context, stripeFooter, rowType);
      EncodingKey key{nodeId};
      const auto& encoding = streams.getEncoding(key);
      if (writeDirect) {
        ASSERT_EQ(
            proto::ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT,
            encoding.kind());
      } else {
        ASSERT_EQ(
            proto::ColumnEncoding_Kind::ColumnEncoding_Kind_DICTIONARY,
            encoding.kind());
        ASSERT_EQ(finalDictionarySize, encoding.dictionarysize());
      }

      auto reqType = TypeWithId::create(rowType)->childAt(0);
      auto columnReader = ColumnReader::build(reqType, reqType, streams);

      for (size_t j = 0; j != repetitionCount; ++j) {
        if (!writeDirect) {
          EXPECT_CALL(streams.getMockStrideIndexProvider(), getStrideIndex())
              .Times(::testing::AtMost(1)) // Stride dictionary is optional
              .WillOnce(Return(j));
        }

        // TODO Make reuse work
        VectorPtr resultBatch;
        columnReader->next(size, resultBatch);

        auto batch = batches[j];
        ASSERT_EQ(batch->size(), resultBatch->size());

        auto sv = std::dynamic_pointer_cast<FlatVector<StringView>>(batch);
        ASSERT_TRUE(sv);
        auto resultSv =
            std::dynamic_pointer_cast<SimpleVector<StringView>>(resultBatch);
        ASSERT_TRUE(resultSv);
        ASSERT_EQ(sv->getNullCount(), resultSv->getNullCount());
        for (size_t k = 0; k < sv->size(); k++) {
          EXPECT_EQ(sv->isNullAt(k), resultSv->isNullAt(k));
        }

        for (size_t k = 0; k != sv->size(); ++k) {
          if (!sv->isNullAt(k)) {
            EXPECT_EQ(sv->valueAt(k), resultSv->valueAt(k)) << folly::sformat(
                "Mismatch on {}-th element. \nExpected: {}\n Actual: {}",
                k,
                sv->valueAt(k),
                resultSv->valueAt(k));
          }
        }
      }
      context.nextStripe();
      columnWriter->reset();
    }
  }
};

struct StringDictionaryEncodingTestCase : public StringColumnWriterTestCase {
  explicit StringDictionaryEncodingTestCase(
      size_t size,
      size_t finalDictionarySize,
      const std::function<std::string(size_t, size_t, size_t)>& genData,
      bool hasNull,
      const std::function<bool(size_t, size_t, size_t)>& genNulls,
      size_t repetitionCount = 1,
      size_t flushCount = 1)
      : StringColumnWriterTestCase{
            size,
            false,
            1.0f,
            0.0f,
            finalDictionarySize,
            genData,
            hasNull,
            genNulls,
            noPostProcessing,
            repetitionCount,
            flushCount} {}
};

struct StringDirectEncodingTestCase : public StringColumnWriterTestCase {
  explicit StringDirectEncodingTestCase(
      size_t size,
      const std::function<std::string(size_t, size_t, size_t)>& genData,
      bool hasNull,
      const std::function<bool(size_t, size_t, size_t)>& genNulls,
      size_t repetitionCount = 1,
      size_t flushCount = 1)
      : StringColumnWriterTestCase{
            size,
            true,
            0.0f,
            1.0f,
            0,
            genData,
            hasNull,
            genNulls,
            noPostProcessing,
            repetitionCount,
            flushCount} {}
};

TEST(ColumnWriterTests, StringDictionaryEncodingWrite) {
  struct TestCase : public StringDictionaryEncodingTestCase {
    explicit TestCase(
        size_t size,
        size_t finalDictionarySize,
        const std::function<std::string(size_t, size_t, size_t)>& genData,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : StringDictionaryEncodingTestCase{
              size,
              finalDictionarySize,
              genData,
              false,
              noNullsWithStride,
              repetitionCount,
              flushCount} {}
  };
  std::vector<TestRunner> testCases{
      // No values in dictionary.
      TestCase{1000, 0, generateStringRange},
      // Mixture of values in dictionary vs not.
      TestCase{1000, 201, generateSomewhatRandomStringData},
      // Test repeated writes. All values are in dictionary.
      TestCase{1000, 617, generateSomewhatRandomStringData, 20},
      // Test repeated writes and flushes with mixture of values in
      // dictionary.
      // Tests independence of dictionary encoding writes across stripes. (In
      // other words, whether the dictionary is cleared across stripes.)
      TestCase{1000, 201, generateSomewhatRandomStringData, 1, 10},
      // Test repeated writes and flushes. All values are in dictionary.
      TestCase{1000, 617, generateSomewhatRandomStringData, 2, 10},
  };
  for (const auto& testCase : testCases) {
    testCase.runTest();
  }
}

std::string genStrideData_scheme1(
    size_t strideIndex,
    size_t strideOffset,
    size_t /*unused*/) {
  // 1st stride contains - "0", "a",  "a", "a",...
  // 2nd stride contains - "1", "b",  "b", "b",...
  // The 1st string will block the inDictStream optimization and exercise
  // the related code path.
  char ch = static_cast<char>(
      (strideOffset == 0) ? '0' + strideIndex : 'a' + strideIndex);
  return std::string(1, ch);
}

std::string genStrideData_scheme2(
    size_t strideIndex,
    size_t strideOffset,
    size_t /*unused*/) {
  // 1st stride contains - "start",  "a", "a",...
  // 2nd stride contains - "b",  "b", "b",...
  if (strideOffset == 0 && strideIndex == 0) {
    // If all the keys are in dictionary, inDictStream is skipped and the code
    // path is skipped. So introducing a non repeated string here
    // intentionally.
    return std::string("start");
  }
  char ch = static_cast<char>('a' + strideIndex);
  return std::string(1, ch);
}

bool genNulls_ForStride2(
    size_t strideIndex,
    size_t /*unused*/,
    size_t /*unused*/) {
  // Except for Stride 2, the rest strides will contain nulls.
  return strideIndex == 2;
}

TEST(ColumnWriterTests, StrideStringWithSomeDataNotInDictionary) {
  struct TestCase : public StringDictionaryEncodingTestCase {
    explicit TestCase(
        size_t size,
        size_t finalDictionarySize,
        const std::function<std::string(size_t, size_t, size_t)>& genData,
        bool hasNulls,
        const std::function<bool(size_t, size_t, size_t)>& genNulls,
        size_t repetitionCount,
        size_t flushCount)
        : StringDictionaryEncodingTestCase{
              size,
              finalDictionarySize,
              genData,
              hasNulls,
              genNulls,
              repetitionCount,
              flushCount} {}
  };

  std::vector<TestRunner> testCases{
      TestCase{1000, 2, genStrideData_scheme1, false, noNullsWithStride, 2, 5},

      TestCase{1000, 2, genStrideData_scheme2, false, noNullsWithStride, 2, 3},

      TestCase{
          1000, 1, genStrideData_scheme1, true, genNulls_ForStride2, 3, 1}};
  for (const auto& testCase : testCases) {
    testCase.runTest();
  }
}

TEST(ColumnWriterTests, StringDictionaryEncodingWritesWithNulls) {
  struct DictionaryEncodingTestCase : public StringDictionaryEncodingTestCase {
    DictionaryEncodingTestCase(
        size_t size,
        size_t finalDictionarySize,
        const std::function<std::string(size_t, size_t, size_t)>& genData,
        const std::function<bool(size_t, size_t, size_t)>& genNulls,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : StringDictionaryEncodingTestCase{
              size,
              finalDictionarySize,
              genData,
              true,
              genNulls,
              repetitionCount,
              flushCount} {}
  };

  // Even as the dictionary size threshold allows, we might not end up writing
  // with dictionary encoding.
  struct DirectEncodingTestCase : public StringDirectEncodingTestCase {
    DirectEncodingTestCase(
        size_t size,
        const std::function<std::string(size_t, size_t, size_t)>& genData,
        const std::function<bool(size_t, size_t, size_t)>& genNulls,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : StringDirectEncodingTestCase{
              size,
              genData,
              true,
              genNulls,
              repetitionCount,
              flushCount} {}
  };

  std::vector<TestRunner> testCases{
      // When all values are null, we don't try to write dictionary at all.
      DirectEncodingTestCase{1000, generateStringRange, allNullsWithStride},
      // No values in dictionary.
      DictionaryEncodingTestCase{
          100, 0, generateStringRange, someNullsWithStride},
      // Mixture of values in dictionary vs not.
      DictionaryEncodingTestCase{
          1000, 62, generateSomewhatRandomStringData, someNullsWithStride},
      // Test repeated writes. All values are in dictionary.
      DictionaryEncodingTestCase{
          1000, 250, generateSomewhatRandomStringData, someNullsWithStride, 20},
      // Test repeated writes and flushes with mixture of values in
      // dictionary.
      // Tests independence of dictionary encoding writes across stripes. (In
      // other words, whether the dictionary is cleared across stripes.)
      DictionaryEncodingTestCase{
          1000,
          62,
          generateSomewhatRandomStringData,
          someNullsWithStride,
          1,
          10},
      // Test repeated writes and flushes. All values are in dictionary.
      DictionaryEncodingTestCase{
          1000,
          250,
          generateSomewhatRandomStringData,
          someNullsWithStride,
          2,
          10},
  };
  for (const auto& testCase : testCases) {
    testCase.runTest();
  }
}

TEST(ColumnWriterTests, StringDirectEncodingWrites) {
  struct TestCase : public StringDirectEncodingTestCase {
    TestCase(
        size_t size,
        const std::function<std::string(size_t, size_t, size_t)>& genData,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : StringDirectEncodingTestCase{
              size,
              genData,
              false,
              noNullsWithStride,
              repetitionCount,
              flushCount} {}
  };

  std::vector<TestRunner> testCases{
      TestCase{1000, generateStringRange},
      TestCase{1000, generateSomewhatRandomStringData},
      TestCase{1000, generateSomewhatRandomStringData, 20},
      // Note, writes from the second stripe on uses direct encoding from the
      // get-go.
      // TODO: reinforce that the subsequent writes go through the right code
      // path when we have more granular memory tracking.
      TestCase{1000, generateSomewhatRandomStringData, 1, 10},
      TestCase{1000, generateSomewhatRandomStringData, 2, 10},
  };
  for (const auto& testCase : testCases) {
    testCase.runTest();
  }
}

TEST(ColumnWriterTests, StringDirectEncodingWritesWithNulls) {
  struct TestCase : public StringDirectEncodingTestCase {
    TestCase(
        size_t size,
        const std::function<std::string(size_t, size_t, size_t)>& genData,
        const std::function<bool(size_t, size_t, size_t)>& genNulls,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : StringDirectEncodingTestCase{
              size,
              genData,
              true,
              genNulls,
              repetitionCount,
              flushCount} {}
  };

  std::vector<TestRunner> testCases{
      TestCase{1000, generateStringRange, allNullsWithStride},
      TestCase{1000, generateStringRange, someNullsWithStride},
      TestCase{1000, generateSomewhatRandomStringData, someNullsWithStride},
      TestCase{1000, generateSomewhatRandomStringData, someNullsWithStride, 20},
      // Note, writes from the second stripe on uses direct encoding from the
      // get-go.
      // TODO: reinforce that the subsequent writes go through the right code
      // path when we have more granular memory tracking.
      TestCase{
          1000, generateSomewhatRandomStringData, someNullsWithStride, 1, 10},
      TestCase{
          1000, generateSomewhatRandomStringData, someNullsWithStride, 2, 10},
  };
  for (const auto& testCase : testCases) {
    testCase.runTest();
  }
}

TEST(ColumnWriterTests, StringColumnWriterAbandonDictionaries) {
  struct TestCase : public StringColumnWriterTestCase {
    TestCase(
        size_t size,
        bool writeDirect,
        size_t finalDictionarySize,
        const std::function<std::string(size_t, size_t, size_t)>& genData,
        const std::function<void(ColumnWriter&, size_t, size_t)>& postProcess,
        size_t repetitionCount,
        size_t flushCount)
        : StringColumnWriterTestCase{
              size,
              writeDirect,
              1.0f,
              0.0f,
              finalDictionarySize,
              genData,
              false,
              noNullsWithStride,
              postProcess,
              repetitionCount,
              flushCount} {}
  };

  struct DirectEncodingTestCase : public TestCase {
    DirectEncodingTestCase(
        size_t size,
        const std::function<std::string(size_t, size_t, size_t)>& genData,
        const std::function<void(ColumnWriter&, size_t, size_t)>& postProcess,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : TestCase(
              size,
              /* writeDirect */ true,
              /* finalDictionarySize */ 0,
              genData,
              postProcess,
              repetitionCount,
              flushCount) {}
  };

  struct DictionaryEncodingTestCase : public TestCase {
    DictionaryEncodingTestCase(
        size_t size,
        size_t finalDictionarySize,
        const std::function<std::string(size_t, size_t, size_t)>& genData,
        const std::function<void(ColumnWriter&, size_t, size_t)>& postProcess,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : TestCase(
              size,
              /* writeDirect */ false,
              finalDictionarySize,
              genData,
              postProcess,
              repetitionCount,
              flushCount) {}
  };

  std::vector<TestRunner> testCases{
      DirectEncodingTestCase{
          1000,
          generateStringRange,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 0),
              /* force */ true,
              successAtNthWriteForStripe(0, 0)),
          10},
      DirectEncodingTestCase{
          1000,
          generateStringRange,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 0),
              /* force */ true,
              successAtNthWriteForStripe(0, 0)),
          1,
          10},
      // Test abandoning dictionary in multi-write scenarios. For now we could
      // only abandon dictionary starting from the first stripe, because we
      // could not switch up encoding of a writer once determined.
      DirectEncodingTestCase{
          1000,
          generateStringRange,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 0),
              /* force */ true,
              successAtNthWriteForStripe(0, 0)),
          2,
          5},
      DirectEncodingTestCase{
          1000,
          generateStringRange,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 1),
              /* force */ true,
              successAtNthWriteForStripe(0, 1)),
          2,
          5},
      // We shouldn't be able to switch encodings beyond the first stripe.
      DictionaryEncodingTestCase{
          1000,
          /* finalDictionarySize */ 1000,
          generateStringRange,
          checkAbandonDict(
              abandonNthWriteForStripe(1, 0), /* force */ true, noSuccess),
          2,
          5},
      // Abandoning at every write to make sure subsequent abandon dict calls
      // are safe.
      DirectEncodingTestCase{
          1000,
          generateStringRange,
          checkAbandonDict(
              abandonEveryWrite,
              /* force */ true,
              successAtNthWriteForStripe(0, 0)),
          10},
      DirectEncodingTestCase{
          1000,
          generateStringRange,
          checkAbandonDict(
              abandonEveryWrite,
              /* force */ true,
              successAtNthWriteForStripe(0, 0)),
          1,
          10},
  };

  for (const auto& testCase : testCases) {
    testCase.runTest();
  }
}

// TODO: how about all nulls?
TEST(ColumnWriterTests, StringColumnWriterAbandonDictionariesWithNulls) {
  struct TestCase : public StringColumnWriterTestCase {
    TestCase(
        size_t size,
        bool writeDirect,
        size_t finalDictionarySize,
        const std::function<std::string(size_t, size_t, size_t)>& genData,
        const std::function<void(ColumnWriter&, size_t, size_t)>& postProcess,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : StringColumnWriterTestCase{
              size,
              writeDirect,
              1.0f,
              0.0f,
              finalDictionarySize,
              genData,
              true,
              someNullsWithStride,
              postProcess,
              repetitionCount,
              flushCount} {}
  };

  struct DirectEncodingTestCase : public TestCase {
    DirectEncodingTestCase(
        size_t size,
        const std::function<std::string(size_t, size_t, size_t)>& genData,
        const std::function<void(ColumnWriter&, size_t, size_t)>& postProcess,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : TestCase(
              size,
              /* writeDirect */ true,
              /* finalDictionarySize */ 0,
              genData,
              postProcess,
              repetitionCount,
              flushCount) {}
  };

  struct DictionaryEncodingTestCase : public TestCase {
    DictionaryEncodingTestCase(
        size_t size,
        size_t finalDictionarySize,
        const std::function<std::string(size_t, size_t, size_t)>& genData,
        const std::function<void(ColumnWriter&, size_t, size_t)>& postProcess,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : TestCase(
              size,
              /* writeDirect */ false,
              finalDictionarySize,
              genData,
              postProcess,
              repetitionCount,
              flushCount) {}
  };

  std::vector<TestRunner> testCases{
      DirectEncodingTestCase{
          1000,
          generateStringRange,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 0),
              /* force */ true,
              successAtNthWriteForStripe(0, 0)),
          10},
      DirectEncodingTestCase{
          1000,
          generateStringRange,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 0),
              /* force */ true,
              successAtNthWriteForStripe(0, 0)),
          1,
          10},
      // Test abandoning dictionary in multi-write scenarios. For now we could
      // only abandon dictionary starting from the first stripe, because we
      // could not switch up encoding of a writer once determined.
      DirectEncodingTestCase{
          1000,
          generateStringRange,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 0),
              /* force */ true,
              successAtNthWriteForStripe(0, 0)),
          2,
          5},
      DirectEncodingTestCase{
          1000,
          generateStringRange,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 1),
              /* force */ true,
              successAtNthWriteForStripe(0, 1)),
          2,
          5},
      // We shouldn't be able to switch encodings beyond the first stripe.
      DictionaryEncodingTestCase{
          1000,
          /* finalDictionarySize */ 341,
          generateStringRange,
          checkAbandonDict(
              abandonNthWriteForStripe(1, 0), /* force */ true, noSuccess),
          2,
          5},
      // Abandoning at every write to make sure subsequent abandon dict calls
      // are safe.
      DirectEncodingTestCase{
          1000,
          generateStringRange,
          checkAbandonDict(
              abandonEveryWrite,
              /* force */ true,
              successAtNthWriteForStripe(0, 0)),
          10},
      DirectEncodingTestCase{
          1000,
          generateStringRange,
          checkAbandonDict(
              abandonEveryWrite,
              /* force */ true,
              successAtNthWriteForStripe(0, 0)),
          1,
          10},
  };

  for (const auto& testCase : testCases) {
    testCase.runTest();
  }
}

TEST(ColumnWriterTests, StringColumnWriterAbandonLowValueDictionaries) {
  struct TestCase : public StringColumnWriterTestCase {
    TestCase(
        size_t size,
        bool writeDirect,
        float dictionaryKeyEfficiencyThreshold,
        size_t finalDictionarySize,
        const std::function<std::string(size_t, size_t, size_t)>& genData,
        const std::function<void(ColumnWriter&, size_t, size_t)>& postProcess,
        size_t repetitionCount,
        size_t flushCount)
        : StringColumnWriterTestCase{
              size,
              writeDirect,
              dictionaryKeyEfficiencyThreshold,
              /* entropyKeyEfficiencyThreshold */ 0.0f,
              finalDictionarySize,
              genData,
              false,
              noNullsWithStride,
              postProcess,
              repetitionCount,
              flushCount} {}
  };

  struct DirectEncodingTestCase : public TestCase {
    DirectEncodingTestCase(
        size_t size,
        float dictionaryKeyEfficiencyThreshold,
        const std::function<std::string(size_t, size_t, size_t)>& genData,
        const std::function<void(ColumnWriter&, size_t, size_t)>& postProcess,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : TestCase(
              size,
              /* writeDirect */ true,
              dictionaryKeyEfficiencyThreshold,
              /* finalDictionarySize */ 0,
              genData,
              postProcess,
              repetitionCount,
              flushCount) {}
  };

  struct DictionaryEncodingTestCase : public TestCase {
    DictionaryEncodingTestCase(
        size_t size,
        float dictionaryKeyEfficiencyThreshold,
        size_t finalDictionarySize,
        const std::function<std::string(size_t, size_t, size_t)>& genData,
        const std::function<void(ColumnWriter&, size_t, size_t)>& postProcess,
        size_t repetitionCount = 1,
        size_t flushCount = 1)
        : TestCase(
              size,
              /* writeDirect */ false,
              dictionaryKeyEfficiencyThreshold,
              finalDictionarySize,
              genData,
              postProcess,
              repetitionCount,
              flushCount) {}
  };

  std::vector<TestRunner> testCases{
      DictionaryEncodingTestCase{
          1000,
          /* dictionaryKeyEfficiencyThreshold */ 1.0f,
          /* finalDictionarySize */ 1000,
          generateStringRange,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 0),
              /* force */ false,
              noSuccess),
          /* repCount */ 10},
      // FinalDictionarySize == 0 because in each stripe
      // each value only appeared once.
      DictionaryEncodingTestCase{
          1000,
          /* dictionaryKeyEfficiencyThreshold */ 1.0f,
          /* finalDictionarySize */ 0,
          generateStringRange,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 0),
              /* force */ false,
              noSuccess),
          /* repCount */ 1,
          /* stripeCount */ 10},
      DirectEncodingTestCase{
          1000,
          /* dictionaryKeyEfficiencyThreshold */ 0.4f,
          generateStringRange,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 1),
              /* force */ false,
              successAtNthWriteForStripe(0, 1)),
          /* repCount */ 10},
      DictionaryEncodingTestCase{
          1000,
          /* dictionaryKeyEfficiencyThreshold */ 0.4f,
          /* finalDictionarySize */ 1000,
          generateStringRange,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 2),
              /* force */ false,
              noSuccess),
          /* repCount */ 10},
      // Test abandoning dictionary in multi-write scenarios. For now we could
      // only abandon dictionary starting from the first stripe, because we
      // could not switch up encoding of a writer once determined.
      DictionaryEncodingTestCase{
          1000,
          /* dictionaryKeyEfficiencyThreshold */ 1.0f,
          /* finalDictionarySize */ 1000,
          generateStringRange,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 1),
              /* force */ false,
              noSuccess),
          /* repCount */ 5,
          /* stripeCount*/ 2},
      // We shouldn't be able to switch encodings beyond the first stripe.
      DictionaryEncodingTestCase{
          1000,
          /* dictionaryKeyEfficiencyThreshold */ 1.0f,
          /* finalDictionarySize */ 1000,
          generateStringRange,
          checkAbandonDict(
              abandonNthWriteForStripe(1, 0), /* force */ false, noSuccess),
          /* repCount */ 5,
          /* stripeCount*/ 2},
      DirectEncodingTestCase{
          1000,
          /* dictionaryKeyEfficiencyThreshold */ 0.1f,
          generateStringRange,
          checkAbandonDict(
              abandonNthWriteForStripe(0, 1),
              /* force */ false,
              successAtNthWriteForStripe(0, 1)),
          /* repCount */ 5,
          /* stripeCount*/ 2},
      // Return false because we were already using direct encoding.
      DirectEncodingTestCase{
          1000,
          /* dictionaryKeyEfficiencyThreshold */ 0.1f,
          generateStringRange,
          checkAbandonDict(
              abandonNthWriteForStripe(1, 0), /* force */ false, noSuccess),
          /* repCount */ 5,
          /* stripeCount*/ 2},
      // Abandoning at every write to make sure subsequent abandon dict calls
      // are safe.
      DictionaryEncodingTestCase{
          1000,
          /* dictionaryKeyEfficiencyThreshold */ 1.0f,
          /* finalDictionarySize */ 1000,
          generateStringRange,
          checkAbandonDict(
              abandonEveryWrite,
              /* force */ false,
              noSuccess),
          /* repCount */ 10},
      // FinalDictionarySize == 0 because in each stripe
      // each value only appeared once.
      DictionaryEncodingTestCase{
          1000,
          /* dictionaryKeyEfficiencyThreshold */ 1.0f,
          /* finalDictionarySize */ 0,
          generateStringRange,
          checkAbandonDict(
              abandonEveryWrite,
              /* force */ false,
              noSuccess),
          /* repCount */ 1,
          /* stripeCount*/ 10},
  };

  for (const auto& testCase : testCases) {
    testCase.runTest();
  }
}

TEST(ColumnWriterTests, IntDictWriterDirectValueOverflow) {
  auto config = std::make_shared<Config>();
  auto scopedPool = getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  WriterContext context{config, getDefaultScopedMemoryPool()};
  auto type = std::make_shared<const IntegerType>();
  auto typeWithId = TypeWithId::create(type, 1);

  // write
  constexpr size_t size = 100;
  std::vector<std::optional<int32_t>> data;
  for (auto i = 0; i < size; ++i) {
    data.push_back((i == 0 ? -1 : 1));
  }
  auto vector = populateBatch<int32_t>(data, &pool);

  auto writer = BaseColumnWriter::create(context, *typeWithId, 0);
  writer->write(vector, Ranges::of(0, size));
  writer->createIndexEntry();
  proto::StripeFooter sf;
  writer->flush([&sf](auto /* unused */) -> proto::ColumnEncoding& {
    return *sf.add_encoding();
  });
  auto& enc = sf.encoding(0);
  ASSERT_EQ(enc.kind(), proto::ColumnEncoding_Kind_DICTIONARY);

  // get data stream
  TestStripeStreams streams(context, sf, ROW({"foo"}, {type}));
  DwrfStreamIdentifier si{1, 0, 0, proto::Stream_Kind_DATA};
  auto stream = streams.getStream(si, true);

  // read it as long
  auto decoder = createRleDecoder<false>(
      std::move(stream), RleVersion_1, pool, streams.getUseVInts(si), 8);
  std::array<int64_t, size> actual;
  decoder->next(actual.data(), size, nullptr);
  for (auto i = 0; i < size; ++i) {
    ASSERT_EQ(actual[i], i == 0 ? -1 : 0);
  }
}

TEST(ColumnWriterTests, ShortDictWriterDictValueOverflow) {
  auto config = std::make_shared<Config>();
  auto scopedPool = getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  WriterContext context{config, getDefaultScopedMemoryPool()};
  auto type = std::make_shared<const SmallintType>();
  auto typeWithId = TypeWithId::create(type, 1);

  // write
  constexpr size_t repeat = 3;
  constexpr size_t count = 1 << 16;
  constexpr size_t size = repeat * count;
  std::vector<std::optional<int16_t>> data;
  for (auto i = 0; i < repeat; ++i) {
    int16_t val = std::numeric_limits<int16_t>::min();
    for (auto j = 0; j < count; ++j) {
      data.push_back(val++);
    }
  }
  auto vector = populateBatch<int16_t>(data, &pool);

  auto writer = BaseColumnWriter::create(context, *typeWithId, 0);
  writer->write(vector, Ranges::of(0, size));
  writer->createIndexEntry();
  proto::StripeFooter sf;
  writer->flush([&sf](auto /* unused */) -> proto::ColumnEncoding& {
    return *sf.add_encoding();
  });
  auto& enc = sf.encoding(0);
  ASSERT_EQ(enc.kind(), proto::ColumnEncoding_Kind_DICTIONARY);

  // get data stream
  TestStripeStreams streams(context, sf, ROW({"foo"}, {type}));
  DwrfStreamIdentifier si{1, 0, 0, proto::Stream_Kind_DATA};
  auto stream = streams.getStream(si, true);

  // read it as long
  auto decoder = createRleDecoder<false>(
      std::move(stream), RleVersion_1, pool, streams.getUseVInts(si), 8);
  std::array<int64_t, size> actual;
  decoder->next(actual.data(), size, nullptr);
  for (auto i = 0; i < size; ++i) {
    ASSERT_GE(actual[i], 0);
  }
}

TEST(ColumnWriterTests, RemovePresentStream) {
  auto config = std::make_shared<Config>();
  auto scopedPool = getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();

  std::vector<std::optional<int32_t>> data;
  auto size = 100;
  for (auto i = 0; i < size; ++i) {
    data.push_back(i);
  }
  auto vector = populateBatch<int32_t>(data, &pool);
  WriterContext context{config, getDefaultScopedMemoryPool()};
  auto type = std::make_shared<const IntegerType>();
  auto typeWithId = TypeWithId::create(type, 1);

  // write
  auto writer = BaseColumnWriter::create(context, *typeWithId, 0);

  writer->write(vector, Ranges::of(0, size));
  writer->createIndexEntry();
  proto::StripeFooter sf;
  writer->flush([&sf](auto /* unused */) -> proto::ColumnEncoding& {
    return *sf.add_encoding();
  });

  // get data stream
  TestStripeStreams streams(context, sf, ROW({"foo"}, {type}));
  DwrfStreamIdentifier si{1, 0, 0, proto::Stream_Kind_PRESENT};
  ASSERT_EQ(streams.getStream(si, false), nullptr);
}

TEST(ColumnWriterTests, ColumnIdInStream) {
  auto config = std::make_shared<Config>();
  auto scopedPool = getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();

  std::vector<std::optional<int32_t>> data;
  auto size = 100;
  for (auto i = 0; i < size; ++i) {
    data.push_back(i);
  }
  auto vector = populateBatch<int32_t>(data, &pool);
  WriterContext context{config, getDefaultScopedMemoryPool()};
  auto type = std::make_shared<const IntegerType>();
  const uint32_t kNodeId = 4;
  const uint32_t kColumnId = 2;
  auto typeWithId = std::make_shared<const TypeWithId>(
      type,
      std::vector<std::shared_ptr<const TypeWithId>>{},
      /* id */ kNodeId,
      /* maxId */ kNodeId,
      /* column */ kColumnId);

  // write
  auto writer = BaseColumnWriter::create(context, *typeWithId, 0);

  writer->write(vector, Ranges::of(0, size));
  writer->createIndexEntry();
  proto::StripeFooter sf;
  writer->flush([&sf](auto /* unused */) -> proto::ColumnEncoding& {
    return *sf.add_encoding();
  });

  // get data stream
  TestStripeStreams streams(context, sf, ROW({"foo"}, {type}));
  DwrfStreamIdentifier si{
      kNodeId, /* sequence */ 0, kColumnId, proto::Stream_Kind_DATA};
  ASSERT_NE(streams.getStream(si, false), nullptr);
}

template <typename T>
struct DictColumnWriterTestCase {
  DictColumnWriterTestCase(size_t size, bool writeDirect, const TypePtr& type)
      : size_(size), writeDirect_(writeDirect), type_(type) {}
  const size_t size_;
  const bool writeDirect_;
  TypePtr type_;

  BufferPtr randomIndices(vector_size_t size) {
    BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(size, &pool_);
    auto rawIndices = indices->asMutable<vector_size_t>();
    for (int32_t i = 0; i < size; i++) {
      rawIndices[i] = folly::Random::rand32(size);
    }
    return indices;
  }

  /**
   * Used to generate dictionary data for the non-complex columns
   * @param size the number of rows
   * @param valueAt the lambda generating values
   * @param isNullAt the lambda generating nulls
   * @return generated data as flat vector
   */
  FlatVectorPtr<T> makeFlatVector(
      vector_size_t size,
      std::function<T(vector_size_t /*index*/)> valueAt,
      std::function<bool(vector_size_t /*index*/)> isNullAt = nullptr) {
    auto vector = std::dynamic_pointer_cast<FlatVector<T>>(
        BaseVector::create(type_, size, &pool_));
    for (int32_t i = 0; i < size; ++i) {
      if (isNullAt && isNullAt(i)) {
        vector->setNull(i, true);
      } else {
        vector->set(i, valueAt(i));
      }
    }
    return vector;
  }

  /**
   * Used to only generate dictionary complex vector tests
   * The data is generated randomly while the nulls are
   * generated using a lambda.
   * @param rowType the complex row type
   * @param count the number of rows in the generated column
   * @param isNullAt the bool lambda for generating nulls
   * @return generated data as Row, Array or Map vectors.
   */
  VectorPtr makeComplexVectors(
      const std::shared_ptr<const RowType> rowType,
      int32_t count,
      std::function<bool(vector_size_t /*index*/)> isNullAt) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType, count, pool_, isNullAt));
    // Batch is returned as a RowVector and so we grab its first column
    return vector->childAt(0);
  }

  /**
   * Used to generate dicitionary data for the non-complex columns
   * @param size the size the generated data (before dictionary)
   * @param valueAt the lmbda generating values
   * @param isNullAt the lambda generating nulls
   * @param complexRowType only set if the data wil be complex (Row, Array or
   * Map)
   * @return
   */
  VectorPtr createDictionaryBatch(
      size_t size,
      std::function<T(vector_size_t /*index*/)> valueAt,
      std::function<bool(vector_size_t /*index*/)> isNullAt,
      const std::shared_ptr<const RowType> complexRowType = nullptr) {
    BufferPtr indices = randomIndices(size);
    VectorPtr dictionaryVector;

    VectorPtr flatVector;
    if (complexRowType == nullptr) {
      flatVector = makeFlatVector(size, valueAt, isNullAt);
    } else {
      flatVector = makeComplexVectors(complexRowType, size, isNullAt);
    }

    auto wrappedVector = BaseVector::wrapInDictionary(
        BufferPtr(nullptr), indices, size, flatVector);
    EXPECT_EQ(wrappedVector->encoding(), VectorEncoding::Simple::DICTIONARY);
    return wrappedVector;
  }

  void runTest(
      std::function<T(vector_size_t /*index*/)> valueAt,
      std::function<bool(vector_size_t /*index*/)> isNullAt = nullptr) {
    auto config = std::make_shared<Config>();
    auto typeWithId = TypeWithId::create(type_, 1);
    auto rowType = ROW({type_});

    WriterContext context{config, getDefaultScopedMemoryPool()};

    // complexVectorType will be nullptr if the vector is not complex.
    bool isComplexType = std::dynamic_pointer_cast<const RowType>(type_) ||
        std::dynamic_pointer_cast<const MapType>(type_) ||
        std::dynamic_pointer_cast<const ArrayType>(type_);

    auto complexVectorType = isComplexType ? rowType : nullptr;
    auto batch =
        createDictionaryBatch(size_, valueAt, isNullAt, complexVectorType);

    const auto writer = BaseColumnWriter::create(context, *typeWithId);

    // Testing write direct paths
    if (writeDirect_) {
      writer->tryAbandonDictionaries(true);
    }
    writer->write(batch, Ranges::of(0, batch->size()));
    writer->createIndexEntry();

    proto::StripeFooter sf;
    writer->flush([&sf](uint32_t /* unused */) -> proto::ColumnEncoding& {
      return *sf.add_encoding();
    });

    // Reading the vector out
    TestStripeStreams streams(context, sf, rowType);
    EXPECT_CALL(streams.getMockStrideIndexProvider(), getStrideIndex())
        .WillRepeatedly(Return(0));
    auto rowTypeWithId = TypeWithId::create(rowType);
    auto reqType = rowTypeWithId->childAt(0);
    auto reader = ColumnReader::build(reqType, reqType, streams);
    VectorPtr out;
    reader->next(batch->size(), out);
    compareResults(batch, out);

    context.nextStripe();
    writer->reset();
  }

  void compareResults(VectorPtr& writeVector, VectorPtr& readVector) {
    ASSERT_EQ(readVector->size(), writeVector->size());
    for (int32_t i = 0; i < writeVector->size(); ++i) {
      ASSERT_TRUE(readVector->equalValueAt(writeVector.get(), i, i))
          << "at index " << i;
    }
  }

  std::unique_ptr<ScopedMemoryPool> scopedPool_ = getDefaultScopedMemoryPool();
  MemoryPool& pool_ = *scopedPool_;
};

std::function<bool(vector_size_t /*index*/)> randomNulls(int32_t n) {
  return
      [n](vector_size_t /*index*/) { return folly::Random::rand32() % n == 0; };
}

template <typename T>
void testDictionary(
    const TypePtr& type,
    std::function<bool(vector_size_t)> isNullAt = nullptr,
    std::function<T(vector_size_t)> valueAt = nullptr) {
  constexpr int32_t vectorSize = 200;

  // Tests for null/non null data with direct or dict write
  DictColumnWriterTestCase<T>(vectorSize, true, type)
      .runTest(valueAt, isNullAt);

  DictColumnWriterTestCase<T>(vectorSize, false, type)
      .runTest(valueAt, isNullAt);

  // Tests for non null data with direct or dict write
  DictColumnWriterTestCase<T>(vectorSize, true, type).runTest(valueAt, [](int) {
    return false;
  });

  DictColumnWriterTestCase<T>(vectorSize, false, type)
      .runTest(valueAt, [](int) { return false; });
}

TEST(ColumnWriterTests, ColumnWriterDictionarySimple) {
  testDictionary<Timestamp>(TIMESTAMP(), randomNulls(11), [](vector_size_t i) {
    return Timestamp(i * 5, i * 2);
  });

  testDictionary<int64_t>(
      BIGINT(), randomNulls(5), [](vector_size_t i) { return i % 5; });

  testDictionary<int32_t>(
      INTEGER(), randomNulls(9), [](vector_size_t i) { return i % 5; });

  testDictionary<int16_t>(
      SMALLINT(), randomNulls(11), [](vector_size_t i) { return i % 5; });

  testDictionary<int8_t>(
      TINYINT(), randomNulls(13), [](vector_size_t i) { return i % 5; });

  testDictionary<float>(
      REAL(), randomNulls(7), [](vector_size_t i) { return (i % 3) * 0.2; });

  testDictionary<double>(
      DOUBLE(), randomNulls(9), [](vector_size_t i) { return (i % 3) * 0.2; });

  testDictionary<bool>(
      BOOLEAN(), randomNulls(11), [](vector_size_t i) { return i % 2 == 0; });

  testDictionary<StringView>(VARCHAR(), randomNulls(9), [](vector_size_t i) {
    return StringView(std::string("str") + std::to_string(i % 3));
  });

  testDictionary<StringView>(VARBINARY(), randomNulls(9), [](vector_size_t i) {
    return StringView(std::string("binary") + std::to_string(i % 3));
  });
};

TEST(ColumnWriterTests, rowDictionary) {
  // For complex data valueAt lambda is not set as the data is generated
  // randomly

  // Row tests
  testDictionary<Row<int32_t>>(ROW({INTEGER()}), randomNulls(5));

  testDictionary<Row<StringView, int32_t>>(
      ROW({VARCHAR(), INTEGER()}), randomNulls(11));

  testDictionary<Row<Row<StringView, int32_t>>>(
      ROW({ROW({VARCHAR(), INTEGER()})}), randomNulls(11));

  testDictionary<Row<int32_t, double, StringView>>(
      ROW({INTEGER(), DOUBLE(), VARCHAR()}), randomNulls(5));

  testDictionary<Row<int32_t, StringView, double, StringView>>(
      ROW({INTEGER(), VARCHAR(), DOUBLE(), VARCHAR()}), randomNulls(5));

  testDictionary<Row<Array<StringView>, StringView>>(
      ROW({ARRAY(VARCHAR()), VARCHAR()}), randomNulls(11));

  testDictionary<
      Row<Map<int32_t, double>,
          Array<Map<int32_t, Row<int32_t, double>>>,
          Row<int32_t, StringView>>>(
      ROW(
          {MAP(INTEGER(), DOUBLE()),
           ARRAY(MAP(INTEGER(), ROW({INTEGER(), DOUBLE()}))),
           ROW({INTEGER(), VARCHAR()})}),
      randomNulls(11));
}

TEST(ColumnWriterTests, arrayDictionary) {
  // Array tests
  testDictionary<Array<float>>(ARRAY(REAL()), randomNulls(7));

  testDictionary<
      Row<Array<int32_t>, Row<StringView, Array<Map<StringView, StringView>>>>>(
      ROW(
          {ARRAY(INTEGER()),
           ROW({VARCHAR(), ARRAY(MAP(VARCHAR(), VARCHAR()))})}),
      randomNulls(11));

  testDictionary<
      Array<Map<int32_t, Array<Map<int8_t, Row<StringView, Array<double>>>>>>>(
      ARRAY(MAP(
          INTEGER(), ARRAY(MAP(TINYINT(), ROW({VARCHAR(), ARRAY(DOUBLE())}))))),
      randomNulls(7));
}

TEST(ColumnWriterTests, mapDictionary) {
  // Map tests
  testDictionary<Map<int32_t, double>>(
      MAP(INTEGER(), DOUBLE()), randomNulls(7));

  testDictionary<Map<StringView, StringView>>(
      MAP(VARCHAR(), VARCHAR()), randomNulls(13));

  testDictionary<
      Map<StringView,
          Map<int32_t, Array<Row<int32_t, int32_t, Array<double>>>>>>(
      MAP(VARCHAR(),
          MAP(INTEGER(), ARRAY(ROW({INTEGER(), INTEGER(), ARRAY(DOUBLE())})))),
      randomNulls(9));

  testDictionary<Map<int32_t, Map<StringView, Map<StringView, int8_t>>>>(
      MAP(INTEGER(), MAP(VARCHAR(), MAP(VARCHAR(), TINYINT()))),
      randomNulls(3));
}

} // namespace facebook::velox::dwrf
