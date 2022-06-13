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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/common/Adaptor.h"
#include "velox/dwio/common/exception/Exceptions.h"
#include "velox/dwio/dwrf/common/wrap/dwrf-proto-wrapper.h"
#include "velox/dwio/dwrf/reader/ColumnReader.h"
#include "velox/dwio/dwrf/reader/SelectiveColumnReader.h"
#include "velox/dwio/dwrf/test/OrcTest.h"
#include "velox/dwio/type/fbhive/HiveTypeParser.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DictionaryVector.h"
#include "velox/vector/FlatVector.h"

#include <folly/Random.h>
#include <folly/String.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace ::testing;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox;
using namespace facebook::velox::dwrf;
using namespace facebook::velox::test;
using facebook::velox::dwio::type::fbhive::HiveTypeParser;

class TestStrideIndexProvider : public StrideIndexProvider {
 public:
  explicit TestStrideIndexProvider(uint32_t stride) noexcept
      : rowIndexStride(stride), rowCount(0) {}
  ~TestStrideIndexProvider() noexcept override {}

  uint64_t getStrideIndex() const override {
    return rowCount / rowIndexStride;
  }

  void addRow(uint64_t rowCount) {
    this->rowCount += rowCount;
  }

 private:
  uint32_t rowIndexStride;
  uint64_t rowCount;
};

void makeFieldSpecs(
    const std::string& pathPrefix,
    int32_t level,
    const std::shared_ptr<const RowType>& type,
    common::ScanSpec* spec) {
  for (auto i = 0; i < type->size(); ++i) {
    std::string path =
        level == 0 ? type->nameOf(i) : pathPrefix + "." + type->nameOf(i);
    common::Subfield subfield(path);
    common::ScanSpec* fieldSpec = spec->getOrCreateChild(subfield);
    fieldSpec->setProjectOut(true);
    if (level == 0) {
      fieldSpec->setChannel(i);
    }
    auto fieldType = type->childAt(i);
    if (fieldType->kind() == TypeKind::ROW) {
      makeFieldSpecs(
          path,
          level + 1,
          std::static_pointer_cast<const RowType>(fieldType),
          spec);
    }
  }
}

class SelectiveColumnReaderBuilder {
 public:
  std::unique_ptr<ColumnReader> build(
      const std::shared_ptr<const Type>& requestedType,
      MockStripeStreams& stripe,
      std::vector<uint64_t> nodes = {},
      const std::shared_ptr<const Type>& dataType = nullptr) {
    const auto& rowType =
        std::dynamic_pointer_cast<const RowType>(requestedType);
    ColumnSelector cs(rowType, nodes, true);
    auto options = RowReaderOptions();
    EXPECT_CALL(stripe, getColumnSelectorProxy())
        .WillRepeatedly(testing::Return(&cs));
    EXPECT_CALL(stripe, getRowReaderOptionsProxy())
        .WillRepeatedly(testing::Return(&options));
    auto dataTypeWithId =
        TypeWithId::create(dataType ? dataType : requestedType);

    scanSpec_ = std::make_unique<common::ScanSpec>("root");
    makeFieldSpecs("", 0, rowType, scanSpec_.get());

    return SelectiveColumnReader::build(
        cs.getSchemaWithId(),
        dataTypeWithId,
        stripe,
        scanSpec_.get(),
        FlatMapContext::nonFlatMapContext());
  }

 private:
  std::unique_ptr<common::ScanSpec> scanSpec_;
};

bool isNotNull(tm* timeptr) {
  return timeptr != nullptr;
}

std::unique_ptr<ColumnReader> buildColumnReader(
    const std::shared_ptr<const Type>& requestedType,
    MockStripeStreams& stripe,
    std::vector<uint64_t> nodes = {},
    bool returnFlatVector = false,
    const std::shared_ptr<const Type>& dataType = nullptr) {
  const std::shared_ptr<const RowType>& rowType =
      std::dynamic_pointer_cast<const RowType>(requestedType);
  ColumnSelector cs(rowType, nodes, true);
  auto options = RowReaderOptions();
  options.setReturnFlatVector(returnFlatVector);
  EXPECT_CALL(stripe, getColumnSelectorProxy())
      .WillRepeatedly(testing::Return(&cs));
  EXPECT_CALL(stripe, getRowReaderOptionsProxy())
      .WillRepeatedly(testing::Return(&options));
  auto dataTypeWithId = TypeWithId::create(dataType ? dataType : requestedType);
  return ColumnReader::build(cs.getSchemaWithId(), dataTypeWithId, stripe);
}

struct StringReaderTestParams {
  const bool useSelectiveReader;
  const bool returnFlatVector;
  const bool expectMemoryReuse;

  std::string toString() const {
    std::ostringstream out;
    out << (useSelectiveReader ? "selective" : "") << "_"
        << (returnFlatVector ? "as_flat" : "") << "_"
        << (expectMemoryReuse ? "reuse" : "");
    return out.str();
  }
};

class StringReaderTests
    : public ::testing::TestWithParam<StringReaderTestParams> {
 protected:
  StringReaderTests()
      : expectMemoryReuse_{GetParam().expectMemoryReuse},
        returnFlatVector_{GetParam().returnFlatVector} {}

  std::unique_ptr<ColumnReader> buildReader(
      const std::shared_ptr<const Type>& requestedType,
      MockStripeStreams& stripe,
      std::vector<uint64_t> nodes = {},
      const std::shared_ptr<const Type>& dataType = nullptr) {
    if (useSelectiveReader()) {
      return builder_.build(requestedType, stripe, nodes, dataType);
    } else {
      return buildColumnReader(
          requestedType, stripe, nodes, returnFlatVector_, dataType);
    }
  }

  VectorPtr newBatch(const TypePtr& rowType) const {
    return useSelectiveReader()
        ? BaseVector::create(rowType, 0, &streams.getMemoryPool())
        : nullptr;
  }

  vector_size_t getNullCount(const VectorPtr& vector) const {
    if (useSelectiveReader()) {
      return BaseVector::countNulls(vector->nulls(), vector->size());
    } else {
      return vector->getNullCount().value();
    }
  }

  // TODO Rename to streams_
  MockStripeStreams streams;
  const bool expectMemoryReuse_;
  const bool returnFlatVector_;

 private:
  bool useSelectiveReader() const {
    return GetParam().useSelectiveReader;
  }

  SelectiveColumnReaderBuilder builder_;
};

void skip(std::unique_ptr<ColumnReader>& reader, int32_t skipSize = 0) {
  if (skipSize > 0) {
    reader->skip(skipSize);
    // TODO Fix SelectiveColumnReader::skip
    auto selectiveReader = dynamic_cast<SelectiveColumnReader*>(reader.get());
    if (selectiveReader) {
      selectiveReader->setReadOffset(selectiveReader->readOffset() + skipSize);
    }
  }
}

void skipAndRead(
    std::unique_ptr<ColumnReader>& reader,
    VectorPtr& batch,
    int32_t readSize = 2,
    int32_t skipSize = 0,
    int32_t nullCount = 0) {
  skip(reader, skipSize);

  reader->next(readSize, batch);
  ASSERT_EQ(readSize, batch->size());
  ASSERT_EQ(nullCount, BaseVector::countNulls(batch->nulls(), batch->size()));
  if (batch->getNullCount().has_value()) {
    ASSERT_EQ(nullCount, batch->getNullCount().value());
  }
}

template <typename T, typename F>
std::shared_ptr<T> getOnlyChild(const std::shared_ptr<F>& batch) {
  auto rowVector = std::dynamic_pointer_cast<RowVector>(batch);
  EXPECT_TRUE(rowVector.get() != nullptr)
      << "Vector is not a struct: " << typeid(F).name();
  auto child = std::dynamic_pointer_cast<T>(rowVector->loadedChildAt(0));
  EXPECT_TRUE(child.get() != nullptr)
      << "Child vector type doesn't match " << typeid(T).name();
  return child;
}

template <typename T, typename F>
std::shared_ptr<T> getChild(std::shared_ptr<F>& batch, size_t index) {
  auto child = std::dynamic_pointer_cast<T>(
      std::dynamic_pointer_cast<RowVector>(batch)->loadedChildAt(index));
  EXPECT_TRUE(child.get() != nullptr);
  return child;
}

struct ReaderTestParams {
  const bool useSelectiveReader;
  const bool expectMemoryReuse;

  std::string toString() const {
    std::ostringstream out;
    out << (useSelectiveReader ? "selective" : "") << "_"
        << (expectMemoryReuse ? "reuse" : "");
    return out.str();
  }
};

class TestColumnReader : public testing::TestWithParam<ReaderTestParams> {
 protected:
  TestColumnReader() : expectMemoryReuse_{GetParam().expectMemoryReuse} {}

  std::unique_ptr<ColumnReader> buildReader(
      const std::shared_ptr<const Type>& requestedType,
      std::vector<uint64_t> nodes = {},
      bool returnFlatVector = false,
      const std::shared_ptr<const Type>& dataType = nullptr) {
    if (useSelectiveReader()) {
      return builder_.build(requestedType, streams, nodes, dataType);
    } else {
      return buildColumnReader(
          requestedType, streams, nodes, returnFlatVector, dataType);
    }
  }

  VectorPtr newBatch(const TypePtr& rowType) const {
    return useSelectiveReader()
        ? BaseVector::create(rowType, 0, &streams.getMemoryPool())
        : nullptr;
  }

  vector_size_t getNullCount(const VectorPtr& vector) const {
    return getNullCount(vector.get());
  }

  vector_size_t getNullCount(BaseVector* vector) const {
    if (useSelectiveReader()) {
      return BaseVector::countNulls(vector->nulls(), vector->size());
    } else {
      return vector->getNullCount().value();
    }
  }

  bool useSelectiveReader() const {
    return GetParam().useSelectiveReader;
  }

  const bool expectMemoryReuse_;
  // TODO Rename to streams_
  MockStripeStreams streams;

 private:
  SelectiveColumnReaderBuilder builder_;
};

TEST_P(TestColumnReader, testBooleanWithNulls) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  // alternate 4 non-null and 4 null via [0xf0 for x in range(512 / 8)]
  const unsigned char buffer1[] = {0x3d, 0xf0};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  // [0x0f for x in range(256 / 8)]
  const unsigned char buffer2[] = {0x1d, 0x0f};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer2, VELOX_ARRAY_SIZE(buffer2))));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<col0:boolean>");

  auto reader = buildReader(rowType);
  VectorPtr batch = newBatch(rowType);
  skipAndRead(reader, batch, /* read */ 512);

  auto boolBatch = getOnlyChild<FlatVector<bool>>(batch);
  ASSERT_EQ(512, boolBatch->size());
  ASSERT_EQ(256, getNullCount(boolBatch));
  uint32_t next = 0;
  for (size_t i = 0; i < batch->size(); ++i) {
    if (i & 4) {
      EXPECT_TRUE(boolBatch->isNullAt(i)) << "Wrong value at " << i;
    } else {
      EXPECT_FALSE(boolBatch->isNullAt(i)) << "Wrong value at " << i;
      EXPECT_EQ((next++ & 4) != 0, boolBatch->valueAt(i))
          << "Wrong value at " << i;
    }
  }
}

TEST_P(TestColumnReader, testBooleanWithNullsFractional) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  // alternate 4 non-null and 4 null via [0xf0 for x in range(512 / 8)]
  std::array<const unsigned char, 2> buffer1 = {0x3d, 0xf0};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(buffer1.data(), buffer1.size())));

  // [0x0f for x in range(256 / 8)]
  std::array<const unsigned char, 2> buffer2 = {0x1d, 0x0f};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(buffer2.data(), buffer2.size())));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<col0:boolean>");

  auto reader = buildReader(rowType);
  VectorPtr batch = newBatch(rowType);

  skipAndRead(reader, batch, /* read */ 7);

  auto boolBatch = getOnlyChild<FlatVector<bool>>(batch);
  ASSERT_EQ(7, boolBatch->size());
  ASSERT_EQ(3, getNullCount(boolBatch));
  uint32_t next = 0;
  for (size_t i = 0; i < batch->size(); ++i) {
    if (i & 4) {
      EXPECT_TRUE(boolBatch->isNullAt(i)) << "Wrong value at " << i;
    } else {
      EXPECT_FALSE(boolBatch->isNullAt(i)) << "Wrong value at " << i;
      EXPECT_EQ((next++ & 4) != 0, boolBatch->valueAt(i))
          << "Wrong value at " << i;
    }
  }

  auto boolBatchPtr = boolBatch.get();
  if (expectMemoryReuse_) {
    boolBatch.reset();
  }
  skipAndRead(reader, batch, /* read */ 2);

  boolBatch = getOnlyChild<FlatVector<bool>>(batch);
  ASSERT_EQ(expectMemoryReuse_, boolBatch.get() == boolBatchPtr);

  ASSERT_EQ(2, boolBatch->size());
  ASSERT_EQ(1, getNullCount(boolBatch));
  EXPECT_EQ(0, boolBatch->valueAt(0));
  EXPECT_EQ(1, boolBatch->valueAt(1));
}

TEST_P(TestColumnReader, testBooleanSkipsWithNulls) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  // alternate 4 non-null and 4 null via [0xf0 for x in range(512 / 8)]
  const unsigned char buffer1[] = {0x3d, 0xf0};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));
  // [0x0f for x in range(128 / 8)]
  const unsigned char buffer2[] = {0x1d, 0x0f};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer2, VELOX_ARRAY_SIZE(buffer2))));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<col0:boolean>");

  auto reader = buildReader(rowType);
  VectorPtr batch = newBatch(rowType);
  skipAndRead(reader, batch, /* read */ 1);

  auto boolBatch = getOnlyChild<FlatVector<bool>>(batch);
  ASSERT_EQ(1, boolBatch->size());
  ASSERT_EQ(0, getNullCount(boolBatch));
  EXPECT_EQ(0, boolBatch->valueAt(0));

  auto boolBatchPtr = boolBatch.get();
  if (expectMemoryReuse_) {
    boolBatch.reset();
  }
  skipAndRead(reader, batch, /* read */ 5, /* skip */ 506);

  boolBatch = getOnlyChild<FlatVector<bool>>(batch);
  ASSERT_EQ(expectMemoryReuse_, boolBatch.get() == boolBatchPtr);

  ASSERT_EQ(5, boolBatch->size());
  ASSERT_EQ(4, getNullCount(boolBatch));
  EXPECT_EQ(1, boolBatch->valueAt(0));
  EXPECT_TRUE(boolBatch->isNullAt(1));
  EXPECT_TRUE(boolBatch->isNullAt(2));
  EXPECT_TRUE(boolBatch->isNullAt(3));
  EXPECT_TRUE(boolBatch->isNullAt(4));
}

TEST_P(TestColumnReader, testByteWithNulls) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  // alternate 4 non-null and 4 null via [0xf0 for x in range(512 / 8)]
  const unsigned char buffer1[] = {0x3d, 0xf0};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  // range(256)
  char buffer[258];
  buffer[0] = static_cast<char>(0x80);
  for (uint32_t i = 0; i < 128; ++i) {
    buffer[i + 1] = static_cast<char>(i);
  }
  buffer[129] = static_cast<char>(0x80);
  for (uint32_t i = 128; i < 256; ++i) {
    buffer[i + 2] = static_cast<char>(i);
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer, VELOX_ARRAY_SIZE(buffer))));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<col0:tinyint>");

  auto reader = buildReader(rowType);
  VectorPtr batch = newBatch(rowType);
  skipAndRead(reader, batch, /* read */ 512);

  auto byteBatch = getOnlyChild<FlatVector<int8_t>>(batch);
  ASSERT_EQ(512, byteBatch->size());
  ASSERT_LT(0, getNullCount(byteBatch));
  uint32_t next = 0;
  for (size_t i = 0; i < batch->size(); ++i) {
    if (i & 4) {
      EXPECT_TRUE(byteBatch->isNullAt(i)) << "Wrong value at " << i;
    } else {
      EXPECT_FALSE(byteBatch->isNullAt(i)) << "Wrong value at " << i;
      EXPECT_EQ(static_cast<char>(next++), byteBatch->valueAt(i))
          << "Wrong value at " << i;
    }
  }
}

TEST_P(TestColumnReader, testBatchReusedSizeEnsured) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // build up 3 rows of byte value
  const std::array<unsigned char, 2> buffer = {0x3d, 0xf0};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(buffer.data(), buffer.size())));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<col0:tinyint>");

  // somehow this batch was resized somewhere before reader fill data for it
  auto reader = buildReader(rowType);
  VectorPtr batch = newBatch(rowType);
  skipAndRead(reader, batch, /* read */ 7);

  auto byteBatch = getOnlyChild<FlatVector<int8_t>>(batch);
  ASSERT_EQ(7, byteBatch->size());
  ASSERT_EQ(0, getNullCount(byteBatch));

  // now, let's copy this batch's data out
  ASSERT_EQ(byteBatch->values()->size(), 7);
}

TEST_P(TestColumnReader, testByteSkipsWithNulls) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  // alternate 4 non-null and 4 null via [0xf0 for x in range(512 / 8)]
  const unsigned char buffer1[] = {0x3d, 0xf0};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  // range(256)
  char buffer[258];
  buffer[0] = static_cast<char>(0x80);
  for (uint32_t i = 0; i < 128; ++i) {
    buffer[i + 1] = static_cast<char>(i);
  }
  buffer[129] = static_cast<char>(0x80);
  for (uint32_t i = 128; i < 256; ++i) {
    buffer[i + 2] = static_cast<char>(i);
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer, VELOX_ARRAY_SIZE(buffer))));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<col0:tinyint>");

  auto reader = buildReader(rowType);
  VectorPtr batch = newBatch(rowType);
  skipAndRead(reader, batch, /* read */ 1);

  auto byteBatch = getOnlyChild<FlatVector<int8_t>>(batch);
  ASSERT_EQ(1, byteBatch->size());
  ASSERT_EQ(0, getNullCount(byteBatch));
  EXPECT_EQ(0, byteBatch->valueAt(0));

  auto byteBatchPtr = byteBatch.get();
  if (expectMemoryReuse_) {
    byteBatch.reset();
  }
  skipAndRead(reader, batch, /* read */ 5, /* skip */ 506);

  byteBatch = getOnlyChild<FlatVector<int8_t>>(batch);
  ASSERT_EQ(expectMemoryReuse_, byteBatch.get() == byteBatchPtr);

  ASSERT_EQ(5, byteBatch->size());
  ASSERT_EQ(4, getNullCount(byteBatch));
  EXPECT_EQ(static_cast<char>(-1), byteBatch->valueAt(0));
  EXPECT_TRUE(byteBatch->isNullAt(1));
  EXPECT_TRUE(byteBatch->isNullAt(2));
  EXPECT_TRUE(byteBatch->isNullAt(3));
  EXPECT_TRUE(byteBatch->isNullAt(4));
}

TEST_P(TestColumnReader, testIntegerWithNulls) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  const unsigned char buffer1[] = {0x16, 0xf0};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  char buffer2[1024];
  size_t size = writeRange(buffer2, 0, 100);
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(buffer2, size)));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<myInt:int>");

  auto reader = buildReader(rowType);
  VectorPtr batch = newBatch(rowType);
  skipAndRead(reader, batch, /* read */ 200);

  auto intBatch = getOnlyChild<FlatVector<int32_t>>(batch);
  ASSERT_EQ(200, intBatch->size());
  ASSERT_LT(0, getNullCount(intBatch));
  long next = 0;
  for (size_t i = 0; i < batch->size(); ++i) {
    if (i & 4) {
      EXPECT_TRUE(intBatch->isNullAt(i));
    } else {
      EXPECT_FALSE(intBatch->isNullAt(i));
      ASSERT_EQ(next++, intBatch->valueAt(i)) << "at index " << i;
    }
  }
}

TEST_P(TestColumnReader, testIntDictSkipNoNullsAllInDict) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  proto::ColumnEncoding dictEncoding;
  dictEncoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
  dictEncoding.set_dictionarysize(100);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));
  EXPECT_CALL(streams, getEncodingProxy(1))
      .WillRepeatedly(Return(&dictEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // even row points to dictionary.
  char data[1024];
  std::vector<uint64_t> v;
  data[0] = 0x9C; // rle literal, -100
  for (uint64_t i = 0; i < 100; ++i) {
    v.push_back(i);
  }
  size_t size = writeVuLongs(data + 1, v);
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(data, size + 1)));

  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_IN_DICTIONARY, false))
      .WillRepeatedly(Return(nullptr));

  EXPECT_CALL(streams, genMockDictDataSetter(1, 0))
      .WillRepeatedly(Return([&](BufferPtr& buffer, MemoryPool* pool) {
        buffer = useSelectiveReader() ? sequence<int32_t>(pool, 0, 100)
                                      : sequence<int64_t>(pool, 0, 100);
      }));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<myInt:int>");

  auto reader = buildReader(rowType);
  long offset = 0;
  VectorPtr batch = newBatch(rowType);
  FlatVectorPtr<int32_t> intBatch;
  for (int32_t round = 0; round < 2; ++round) {
    auto intBatchPtr = intBatch.get();
    if (expectMemoryReuse_) {
      intBatch.reset();
    }
    skipAndRead(reader, batch, /* read */ 40, /* skip */ 10);

    intBatch = getOnlyChild<FlatVector<int32_t>>(batch);
    if (round > 0) {
      ASSERT_EQ(expectMemoryReuse_, intBatch.get() == intBatchPtr);
    }

    ASSERT_EQ(40, intBatch->size());
    ASSERT_EQ(0, getNullCount(intBatch));
    offset += 10;
    for (size_t i = 0; i < batch->size(); ++i) {
      EXPECT_EQ(offset++, intBatch->valueAt(i));
    }
  }
}

TEST_P(TestColumnReader, testIntDictSkipWithNulls) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  proto::ColumnEncoding dictEncoding;
  dictEncoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
  dictEncoding.set_dictionarysize(50);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));
  EXPECT_CALL(streams, getEncodingProxy(1))
      .WillRepeatedly(Return(&dictEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  const unsigned char buffer1[] = {0x16, 0xaa};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  // even row points to dictionary.
  char buffer2[1024];
  std::vector<uint64_t> v;
  buffer2[0] = 0x9C; // rle literal, -100
  for (uint64_t i = 0; i < 100; ++i) {
    v.push_back(i % 2 == 0 ? i / 2 : i + 1000);
  }
  size_t size = writeVuLongs(buffer2 + 1, v);
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(buffer2, size + 1)));

  const unsigned char buffer3[] = {0x0a, 0xaa};
  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_IN_DICTIONARY, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer3, VELOX_ARRAY_SIZE(buffer3))));

  EXPECT_CALL(streams, genMockDictDataSetter(1, 0))
      .WillRepeatedly(Return([&](BufferPtr& buffer, MemoryPool* pool) {
        if (useSelectiveReader()) {
          buffer = AlignedBuffer::allocate<int32_t>(1024, pool);
          auto dataArray = buffer->asMutable<int32_t>();
          for (int64_t i = 0; i < 50; ++i) {
            dataArray[i] = i * 2;
          }
        } else {
          buffer = AlignedBuffer::allocate<int64_t>(1024, pool);
          auto dataArray = buffer->asMutable<int64_t>();
          for (int64_t i = 0; i < 50; ++i) {
            dataArray[i] = i * 2;
          }
        }
      }));
  // create the row type
  auto rowType = HiveTypeParser().parse("struct<myInt:int>");

  auto reader = buildReader(rowType);
  long next = 0;
  VectorPtr batch = newBatch(rowType);
  FlatVectorPtr<int32_t> intBatch;
  for (int32_t round = 0; round < 2; ++round) {
    auto intBatchPtr = intBatch.get();
    if (expectMemoryReuse_) {
      intBatch.reset();
    }
    skipAndRead(reader, batch, /* read */ 90, /* skip */ 10);
    next += 5;

    intBatch = getOnlyChild<FlatVector<int32_t>>(batch);
    if (round > 0) {
      ASSERT_EQ(expectMemoryReuse_, intBatch.get() == intBatchPtr);
    }
    ASSERT_EQ(90, intBatch->size());
    ASSERT_LT(0, getNullCount(intBatch));
    for (size_t i = 0; i < batch->size(); ++i) {
      if (i % 2 == 0) {
        EXPECT_FALSE(intBatch->isNullAt(i));
        EXPECT_EQ(next % 2 == 0 ? next : next + 1000, intBatch->valueAt(i));
        ++next;
      } else {
        EXPECT_TRUE(intBatch->isNullAt(i));
      }
    }
  }
}

TEST_P(TestColumnReader, testIntDictBoundary) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  constexpr size_t dictSize = std::numeric_limits<uint16_t>::max() + 1;
  proto::ColumnEncoding dictEncoding;
  dictEncoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
  dictEncoding.set_dictionarysize(dictSize);
  EXPECT_CALL(streams, getEncodingProxy(1))
      .WillRepeatedly(Return(&dictEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // even row points to dictionary.
  size_t size = 0;
  std::array<char, 4096> data;
  std::vector<uint64_t> v;
  size_t count = 0;
  while (count < dictSize) {
    size_t toWrite = std::min(dictSize - count, static_cast<size_t>(130));
    data[size++] = static_cast<char>(toWrite - 3); // rle run
    data[size++] = 0x1;
    size = writeVuLong(data.data(), size, count);
    count += toWrite;
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(data.data(), size)));

  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_IN_DICTIONARY, false))
      .WillRepeatedly(Return(nullptr));

  EXPECT_CALL(streams, genMockDictDataSetter(1, 0))
      .WillRepeatedly(Return([&](BufferPtr& buffer, MemoryPool* pool) {
        int64_t begin = std::numeric_limits<int16_t>::min();
        auto end =
            static_cast<int64_t>(std::numeric_limits<int16_t>::max()) + 1;
        buffer = useSelectiveReader() ? sequence<int16_t>(pool, begin, end)
                                      : sequence<int64_t>(pool, begin, end);
      }));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<foo:smallint>");

  auto reader = buildReader(rowType);
  VectorPtr batch = newBatch(rowType);

  skipAndRead(reader, batch, /* read */ dictSize);

  auto shortBatch = getOnlyChild<FlatVector<int16_t>>(batch);
  ASSERT_EQ(dictSize, shortBatch->size());
  ASSERT_EQ(0, getNullCount(shortBatch));
  int16_t val = std::numeric_limits<int16_t>::min();
  for (size_t i = 0; i < batch->size(); ++i) {
    EXPECT_EQ(val, shortBatch->valueAt(i));
    ++val;
  }
}

TEST_P(StringReaderTests, testDictionaryWithNulls) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(0))
      .WillRepeatedly(Return(&directEncoding));
  proto::ColumnEncoding dictionaryEncoding;
  dictionaryEncoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
  dictionaryEncoding.set_dictionarysize(2);
  EXPECT_CALL(streams, getEncodingProxy(1))
      .WillRepeatedly(Return(&dictionaryEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(1, _, _)).WillRepeatedly(Return(nullptr));
  const unsigned char buffer1[] = {0x19, 0xf0};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));
  const unsigned char buffer2[] = {0x2f, 0x00, 0x00, 0x2f, 0x00, 0x01};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer2, VELOX_ARRAY_SIZE(buffer2))));
  const unsigned char buffer3[] = {0x4f, 0x52, 0x43, 0x4f, 0x77, 0x65, 0x6e};
  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_DICTIONARY_DATA, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer3, VELOX_ARRAY_SIZE(buffer3))));
  const unsigned char buffer4[] = {0x02, 0x01, 0x03};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer4, VELOX_ARRAY_SIZE(buffer4))));

  TestStrideIndexProvider provider(10000);
  EXPECT_CALL(streams, getStrideIndexProviderProxy())
      .WillRepeatedly(Return(&provider));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<myString:string>");

  auto reader = buildReader(rowType, streams, {});
  VectorPtr batch = newBatch(rowType);
  skipAndRead(reader, batch, /* read */ 200);

  std::shared_ptr<SimpleVector<StringView>> stringBatch;
  if (returnFlatVector_) {
    stringBatch = getOnlyChild<FlatVector<StringView>>(batch);
  } else {
    stringBatch = getOnlyChild<DictionaryVector<StringView>>(batch);
  }
  ASSERT_EQ(200, stringBatch->size());
  ASSERT_EQ(100, getNullCount(stringBatch));
  for (size_t i = 0; i < batch->size(); ++i) {
    if (i & 4) {
      EXPECT_TRUE(stringBatch->isNullAt(i));
    } else {
      EXPECT_FALSE(stringBatch->isNullAt(i));
      const char* expected = i < 98 ? "ORC" : "Owen";
      ASSERT_EQ(strlen(expected), stringBatch->valueAt(i).size())
          << "Wrong length at " << i;
      for (size_t letter = 0; letter < strlen(expected); ++letter) {
        EXPECT_EQ(expected[letter], stringBatch->valueAt(i).data()[letter])
            << "Wrong contents at " << i << ", " << letter;
      }
    }
  }
}

TEST_P(StringReaderTests, testStringDictSkipNoNulls) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));
  proto::ColumnEncoding dictionaryEncoding;
  dictionaryEncoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
  dictionaryEncoding.set_dictionarysize(50);
  EXPECT_CALL(streams, getEncodingProxy(1))
      .WillRepeatedly(Return(&dictionaryEncoding));

  // set row index
  const int32_t rowIndexStride = 10;
  proto::RowIndex index;

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  char data[1024];
  data[0] = 0x9c; // -100
  size_t len = 1;
  size_t strideDictSize = 0;
  size_t strideDictSizeTotal = 0;
  char dict[2048];
  char strideDict[2048];
  size_t dictLen = 0;
  size_t strideDictLen = 0;
  std::vector<std::string> dictVals;
  std::vector<std::string> strideDictVals;
  proto::RowIndexEntry* entry = nullptr;
  for (uint32_t i = 0; i < 100; ++i) {
    if (i % rowIndexStride == 0) {
      // add stride dict size
      if (entry) {
        entry->add_positions(strideDictSize);
      }
      strideDictSizeTotal += strideDictSize;
      strideDictSize = 0;
      // capture indices
      entry = index.add_entry();
      entry->add_positions(strideDictLen);
      entry->add_positions(0);
      entry->add_positions(strideDictSizeTotal);
    }
    std::string val = folly::to<std::string>(folly::Random::rand32());
    auto valLen = val.length();
    if (i % 2 == 0) {
      len = writeVuLong(data, len, i / 2);
      dictVals.push_back(val);
      memcpy(dict + dictLen, val.c_str(), valLen);
      dictLen += valLen;
    } else {
      len = writeVuLong(data, len, strideDictSize++);
      strideDictVals.push_back(val);
      memcpy(strideDict + strideDictLen, val.c_str(), valLen);
      strideDictLen += valLen;
    }
  }
  entry->add_positions(strideDictSize);
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(data, len)));
  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_DICTIONARY_DATA, false))
      .WillRepeatedly(Return(new SeekableArrayInputStream(dict, dictLen)));
  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_STRIDE_DICTIONARY, true))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(strideDict, strideDictLen)));

  char dictLength[1024];
  char strideDictLength[1024];
  dictLength[0] = 0xce; // -50
  strideDictLength[0] = 0xce;
  dictLen = 1;
  strideDictLen = 1;
  for (uint32_t i = 0; i < 50; ++i) {
    dictLen = writeVuLong(dictLength, dictLen, dictVals[i].length());
    strideDictLen = writeVuLong(
        strideDictLength, strideDictLen, strideDictVals[i].length());
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, false))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(dictLength, dictLen)));
  EXPECT_CALL(
      streams,
      getStreamProxy(1, proto::Stream_Kind_STRIDE_DICTIONARY_LENGTH, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(strideDictLength, strideDictLen)));
  const unsigned char inDict[] = {0x0a, 0xaa};
  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_IN_DICTIONARY, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(inDict, VELOX_ARRAY_SIZE(inDict))));

  auto indexData = index.SerializePartialAsString();
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_ROW_INDEX, _))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(indexData.data(), indexData.size())));
  TestStrideIndexProvider provider(rowIndexStride);
  EXPECT_CALL(streams, getStrideIndexProviderProxy())
      .WillRepeatedly(Return(&provider));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<myString:string>");

  auto reader = buildReader(rowType, streams, {});
  uint64_t rowCount = 0;
  VectorPtr batch = newBatch(rowType);
  std::shared_ptr<SimpleVector<StringView>> stringBatch;
  VectorPtr rowVector;
  for (uint64_t toSkip = 0; toSkip < 10; ++toSkip) {
    skip(reader, toSkip);

    provider.addRow(toSkip);
    rowCount += toSkip;
    uint64_t rowsRead = rowIndexStride - toSkip;

    auto stringBatchPtr = stringBatch.get();
    if (expectMemoryReuse_) {
      stringBatch.reset();
    }
    reader->next(rowsRead, batch);
    ASSERT_EQ(rowsRead, batch->size());
    ASSERT_EQ(0, getNullCount(batch));

    if (returnFlatVector_) {
      stringBatch = getOnlyChild<FlatVector<StringView>>(batch);
    } else {
      stringBatch = getOnlyChild<DictionaryVector<StringView>>(batch);
    }
    if (toSkip > 0) {
      ASSERT_EQ(expectMemoryReuse_, stringBatch.get() == stringBatchPtr);
    }
    ASSERT_EQ(rowsRead, stringBatch->size());
    ASSERT_EQ(0, getNullCount(stringBatch));
    for (size_t i = 0; i < batch->size(); ++i) {
      EXPECT_EQ(
          rowCount % 2 == 0 ? dictVals[rowCount / 2]
                            : strideDictVals[rowCount / 2],
          std::string(
              stringBatch->valueAt(i).data(), stringBatch->valueAt(i).size()));
      ++rowCount;
    }
    provider.addRow(rowsRead);
  }
}

TEST_P(StringReaderTests, testStringDictSkipWithNulls) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));
  proto::ColumnEncoding dictionaryEncoding;
  dictionaryEncoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
  dictionaryEncoding.set_dictionarysize(50);
  EXPECT_CALL(streams, getEncodingProxy(1))
      .WillRepeatedly(Return(&dictionaryEncoding));

  // set row index
  const int32_t rowIndexStride = 10;
  proto::RowIndex index;

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  const unsigned char present[] = {0x16, 0xaa};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(present, VELOX_ARRAY_SIZE(present))));

  char data[1024];
  data[0] = 0x9c;
  size_t len = 1;
  size_t strideDictSize = 0;
  size_t strideDictSizeTotal = 0;
  char dict[2048];
  char strideDict[2048];
  size_t dictLen = 0;
  size_t strideDictLen = 0;
  std::vector<std::string> dictVals;
  std::vector<std::string> strideDictVals;
  proto::RowIndexEntry* entry = nullptr;
  for (uint32_t i = 0; i < 200; ++i) {
    if (i % rowIndexStride == 0) {
      // add stride dict size
      if (entry) {
        entry->add_positions(strideDictSize);
      }
      strideDictSizeTotal += strideDictSize;
      strideDictSize = 0;
      // capture indices
      entry = index.add_entry();
      for (uint32_t j = 0; j < 3; ++j) {
        entry->add_positions(0);
      }
      entry->add_positions(strideDictLen);
      entry->add_positions(0);
      entry->add_positions(strideDictSizeTotal);
    }
    if (i % 2 == 0) {
      std::string val = folly::to<std::string>(folly::Random::rand32());
      auto valLen = val.length();
      if (i % 4 == 0) {
        len = writeVuLong(data, len, i / 4);
        dictVals.push_back(val);
        memcpy(dict + dictLen, val.c_str(), valLen);
        dictLen += valLen;
      } else {
        len = writeVuLong(data, len, strideDictSize++);
        strideDictVals.push_back(val);
        memcpy(strideDict + strideDictLen, val.c_str(), valLen);
        strideDictLen += valLen;
      }
    }
  }
  entry->add_positions(strideDictSize);
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(data, len)));
  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_DICTIONARY_DATA, false))
      .WillRepeatedly(Return(new SeekableArrayInputStream(dict, dictLen)));
  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_STRIDE_DICTIONARY, true))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(strideDict, strideDictLen)));

  char dictLength[1024];
  char strideDictLength[1024];
  dictLength[0] = 0xce;
  strideDictLength[0] = 0xce;
  dictLen = 1;
  strideDictLen = 1;
  for (uint32_t i = 0; i < 50; ++i) {
    dictLen = writeVuLong(dictLength, dictLen, dictVals[i].length());
    strideDictLen = writeVuLong(
        strideDictLength, strideDictLen, strideDictVals[i].length());
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, false))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(dictLength, dictLen)));
  EXPECT_CALL(
      streams,
      getStreamProxy(1, proto::Stream_Kind_STRIDE_DICTIONARY_LENGTH, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(strideDictLength, strideDictLen)));
  const unsigned char inDict[] = {0x0a, 0xaa};
  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_IN_DICTIONARY, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(inDict, VELOX_ARRAY_SIZE(inDict))));

  auto indexData = index.SerializePartialAsString();
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_ROW_INDEX, _))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(indexData.data(), indexData.size())));
  TestStrideIndexProvider provider(rowIndexStride);
  EXPECT_CALL(streams, getStrideIndexProviderProxy())
      .WillRepeatedly(Return(&provider));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<myString:string>");

  auto reader = buildReader(rowType, streams, {});
  skip(reader, 50);
  provider.addRow(50);
  uint32_t rowCount = 0;
  VectorPtr batch = newBatch(rowType);
  std::shared_ptr<SimpleVector<StringView>> stringBatch;
  VectorPtr rowVector;
  while (rowCount < 150) {
    auto stringBatchPtr = stringBatch.get();
    if (expectMemoryReuse_) {
      stringBatch.reset();
    }
    reader->next(rowIndexStride, batch);
    ASSERT_EQ(rowIndexStride, batch->size());
    ASSERT_EQ(0, getNullCount(batch));

    if (returnFlatVector_) {
      stringBatch = getOnlyChild<FlatVector<StringView>>(batch);
    } else {
      stringBatch = getOnlyChild<DictionaryVector<StringView>>(batch);
    }
    if (rowCount > 0) {
      ASSERT_EQ(expectMemoryReuse_, stringBatch.get() == stringBatchPtr);
    }
    ASSERT_EQ(rowIndexStride, stringBatch->size());
    ASSERT_LT(0, getNullCount(stringBatch));
    for (size_t i = 0; i < batch->size(); ++i) {
      if (i % 2 == 0) {
        EXPECT_FALSE(stringBatch->isNullAt(i));
        size_t j = (i + rowCount + 50) / 2;
        size_t k = j / 2;
        EXPECT_EQ(
            j % 2 == 0 ? dictVals[k] : strideDictVals[k],
            std::string(
                stringBatch->valueAt(i).data(),
                stringBatch->valueAt(i).size()));
      } else {
        EXPECT_TRUE(stringBatch->isNullAt(i));
      }
    }
    rowCount += batch->size();
    provider.addRow(rowIndexStride);
  }
}

TEST_P(TestColumnReader, testSubstructsWithNulls) {
  if (useSelectiveReader()) {
    GTEST_SKIP() << "SelectiveColumnReader doesn't support nested structs yet";
  }

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  const unsigned char buffer1[] = {0x16, 0x0f};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  const unsigned char buffer2[] = {0x0a, 0x55};
  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer2, VELOX_ARRAY_SIZE(buffer2))));

  const unsigned char buffer3[] = {0x04, 0xf0};
  EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer3, VELOX_ARRAY_SIZE(buffer3))));

  char buffer4[256];
  size_t size = writeRange(buffer4, 0, 26);
  EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(buffer4, size)));

  // create the row type
  auto rowType =
      HiveTypeParser().parse("struct<col0:struct<col1:struct<col2:bigint>>>");

  auto reader = buildReader(rowType);

  VectorPtr batch = newBatch(rowType);
  skipAndRead(reader, batch, /* read */ 200);

  auto middle = getOnlyChild<RowVector>(batch);
  ASSERT_EQ(200, middle->size());
  ASSERT_LT(0, getNullCount(middle));

  auto inner = getOnlyChild<RowVector>(middle);
  ASSERT_EQ(200, inner->size());
  ASSERT_LT(0, getNullCount(inner));

  auto longs = getOnlyChild<FlatVector<int64_t>>(inner);
  ASSERT_EQ(200, longs->size());
  ASSERT_LT(0, getNullCount(longs));
  long middleCount = 0;
  long innerCount = 0;
  long longCount = 0;
  for (size_t i = 0; i < batch->size(); ++i) {
    if (i & 4) {
      EXPECT_FALSE(middle->isNullAt(i)) << "Wrong at " << i;
      if (middleCount++ & 1) {
        EXPECT_FALSE(inner->isNullAt(i)) << "Wrong at " << i;
        if (innerCount++ & 4) {
          EXPECT_TRUE(longs->isNullAt(i)) << "Wrong at " << i;
        } else {
          EXPECT_FALSE(longs->isNullAt(i)) << "Wrong at " << i;
          EXPECT_EQ(longCount++, longs->valueAt(i)) << "Wrong at " << i;
        }
      } else {
        EXPECT_TRUE(inner->isNullAt(i)) << "Wrong at " << i;
      }
    } else {
      EXPECT_TRUE(middle->isNullAt(i)) << "Wrong at " << i;
    }
  }
  EXPECT_EQ(100, middleCount);
  EXPECT_EQ(50, innerCount);
  EXPECT_EQ(26, longCount);
}

TEST_P(TestColumnReader, testSkipWithNulls) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));
  proto::ColumnEncoding dictionaryEncoding;
  dictionaryEncoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
  dictionaryEncoding.set_dictionarysize(100);
  EXPECT_CALL(streams, getEncodingProxy(2))
      .WillRepeatedly(Return(&dictionaryEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  const unsigned char buffer1[] = {
      0x03, 0x00, 0xff, 0x3f, 0x08, 0xff, 0xff, 0xfc, 0x03, 0x00};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));
  EXPECT_CALL(streams, getStreamProxy(2, _, _)).WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  TestStrideIndexProvider provider(10000);
  EXPECT_CALL(streams, getStrideIndexProviderProxy())
      .WillRepeatedly(Return(&provider));

  char buffer2[1024];
  size_t size = writeRange(buffer2, 0, 100);
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(buffer2, size)));
  const unsigned char buffer3[] = {0x61, 0x01, 0x00};
  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer3, VELOX_ARRAY_SIZE(buffer3))));

  // fill the dictionary with '00' to '99'
  char digits[200];
  for (int32_t i = 0; i < 10; ++i) {
    for (int32_t j = 0; j < 10; ++j) {
      digits[2 * (10 * i + j)] = static_cast<char>('0' + i);
      digits[2 * (10 * i + j) + 1] = static_cast<char>('0' + j);
    }
  }
  EXPECT_CALL(
      streams, getStreamProxy(2, proto::Stream_Kind_DICTIONARY_DATA, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(digits, VELOX_ARRAY_SIZE(digits))));
  const unsigned char buffer4[] = {0x61, 0x00, 0x02};
  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_LENGTH, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer4, VELOX_ARRAY_SIZE(buffer4))));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<myInt:int,myString:string>");

  auto reader = buildReader(rowType);
  VectorPtr batch = newBatch(rowType);
  skipAndRead(reader, batch, /* read */ 20);

  auto intBatch = getChild<SimpleVector<int32_t>>(batch, 0);
  ASSERT_EQ(20, intBatch->size());

  auto stringBatch = getChild<DictionaryVector<StringView>>(batch, 1);
  ASSERT_EQ(20, stringBatch->size());
  ASSERT_EQ(0, getNullCount(batch));
  ASSERT_EQ(20, getNullCount(intBatch));
  ASSERT_EQ(20, getNullCount(stringBatch));
  for (size_t i = 0; i < 20; ++i) {
    EXPECT_TRUE(intBatch->isNullAt(i)) << "Wrong at " << i;
    EXPECT_TRUE(stringBatch->isNullAt(i)) << "Wrong at " << i;
  }

  auto intBatchPtr = intBatch.get();
  auto stringBatchPtr = stringBatch.get();
  if (expectMemoryReuse_) {
    intBatch.reset();
    stringBatch.reset();
  }
  skipAndRead(reader, batch, /* read */ 100, /* skip */ 30);

  intBatch = getChild<SimpleVector<int32_t>>(batch, 0);
  stringBatch = getChild<DictionaryVector<StringView>>(batch, 1);
  ASSERT_EQ(expectMemoryReuse_, intBatch.get() == intBatchPtr);
  ASSERT_EQ(expectMemoryReuse_, stringBatch.get() == stringBatchPtr);

  ASSERT_EQ(0, getNullCount(intBatch));
  ASSERT_EQ(0, getNullCount(stringBatch));
  for (size_t i = 0; i < 10; ++i) {
    for (size_t j = 0; j < 10; ++j) {
      size_t k = 10 * i + j;
      EXPECT_FALSE(intBatch->isNullAt(k)) << "Wrong at " << i;
      ASSERT_EQ(2, stringBatch->valueAt(k).size()) << "Wrong at " << k;
      EXPECT_EQ('0' + static_cast<char>(i), stringBatch->valueAt(k).data()[0])
          << "Wrong at " << k;
      EXPECT_EQ('0' + static_cast<char>(j), stringBatch->valueAt(k).data()[1])
          << "Wrong at " << k;
    }
  }
  reader->skip(50);
}

TEST_P(StringReaderTests, testBinaryDirect) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  char blob[200];
  for (size_t i = 0; i < 10; ++i) {
    for (size_t j = 0; j < 10; ++j) {
      blob[2 * (10 * i + j)] = static_cast<char>(i);
      blob[2 * (10 * i + j) + 1] = static_cast<char>(j);
    }
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(blob, VELOX_ARRAY_SIZE(blob))));

  const unsigned char buffer[] = {0x61, 0x00, 0x02};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer, VELOX_ARRAY_SIZE(buffer))));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<col0:binary>");

  auto reader = buildReader(rowType, streams, {});

  VectorPtr batch = newBatch(rowType);
  FlatVectorPtr<StringView> strings;
  VectorPtr rowVector;
  for (size_t i = 0; i < 2; ++i) {
    auto stringsPtr = strings.get();
    if (expectMemoryReuse_) {
      strings.reset();
    }
    skipAndRead(reader, batch, /* read */ 50);

    strings = getOnlyChild<FlatVector<StringView>>(batch);
    if (i > 0) {
      ASSERT_EQ(expectMemoryReuse_, strings.get() == stringsPtr);
    }
    ASSERT_EQ(50, strings->size());
    ASSERT_EQ(0, getNullCount(strings));
    for (size_t j = 0; j < batch->size(); ++j) {
      ASSERT_EQ(2, strings->valueAt(j).size());
      EXPECT_EQ((50 * i + j) / 10, strings->valueAt(j).data()[0]);
      EXPECT_EQ((50 * i + j) % 10, strings->valueAt(j).data()[1]);
    }
  }
}

TEST_P(StringReaderTests, testBinaryDirectWithNulls) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  const unsigned char buffer1[] = {0x1d, 0xf0};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  char blob[256];
  for (size_t i = 0; i < 8; ++i) {
    for (size_t j = 0; j < 16; ++j) {
      blob[2 * (16 * i + j)] = static_cast<char>('A' + i);
      blob[2 * (16 * i + j) + 1] = static_cast<char>('A' + j);
    }
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(blob, VELOX_ARRAY_SIZE(blob))));

  const unsigned char buffer2[] = {0x7d, 0x00, 0x02};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer2, VELOX_ARRAY_SIZE(buffer2))));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<col0:binary>");

  auto reader = buildReader(rowType, streams, {});

  size_t next = 0;
  VectorPtr batch = newBatch(rowType);
  FlatVectorPtr<StringView> strings;
  VectorPtr rowVector;
  for (size_t i = 0; i < 2; ++i) {
    auto stringsPtr = strings.get();
    if (expectMemoryReuse_) {
      strings.reset();
    }
    skipAndRead(reader, batch, /* read */ 128);

    strings = getOnlyChild<FlatVector<StringView>>(batch);
    if (i > 0) {
      ASSERT_EQ(expectMemoryReuse_, strings.get() == stringsPtr);
    }
    ASSERT_EQ(128, strings->size());
    ASSERT_LT(0, getNullCount(strings));
    for (size_t j = 0; j < batch->size(); ++j) {
      ASSERT_EQ(((128 * i + j) & 4) != 0, strings->isNullAt(j));
      if (!strings->isNullAt(j)) {
        ASSERT_EQ(2, strings->valueAt(j).size());
        EXPECT_EQ(
            'A' + static_cast<char>(next / 16), strings->valueAt(j).data()[0]);
        EXPECT_EQ(
            'A' + static_cast<char>(next % 16), strings->valueAt(j).data()[1]);
        next += 1;
      }
    }
  }
}

TEST_P(TestColumnReader, testShortBlobError) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  char blob[100];
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(blob, VELOX_ARRAY_SIZE(blob))));

  const unsigned char buffer1[] = {0x61, 0x00, 0x02};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<col0:string>");

  auto reader = buildReader(rowType);

  VectorPtr batch = newBatch(rowType);
  try {
    reader->next(100, batch);
    batch->as<RowVector>()->childAt(0)->loadedVector();
    FAIL() << "Expected an error";
  } catch (const exception::LoggedException& e) {
    ASSERT_EQ("bad read in readFully", e.message());
  } catch (const VeloxRuntimeError& e) {
    ASSERT_EQ("Reading past end", e.message());
  }
}

TEST_P(StringReaderTests, testStringDirectShortBuffer) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  char blob[200];
  for (size_t i = 0; i < 10; ++i) {
    for (size_t j = 0; j < 10; ++j) {
      blob[2 * (10 * i + j)] = static_cast<char>(i);
      blob[2 * (10 * i + j) + 1] = static_cast<char>(j);
    }
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(blob, VELOX_ARRAY_SIZE(blob), 3)));

  const unsigned char buffer1[] = {0x61, 0x00, 0x02};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<col0:string>");

  auto reader = buildReader(rowType, streams, {});

  VectorPtr batch = newBatch(rowType);
  FlatVectorPtr<StringView> strings;
  VectorPtr rowVector;
  for (size_t i = 0; i < 4; ++i) {
    auto stringsPtr = strings.get();
    if (expectMemoryReuse_) {
      strings.reset();
    }
    skipAndRead(reader, batch, /* read */ 25);

    strings = getOnlyChild<FlatVector<StringView>>(batch);
    if (i > 0) {
      ASSERT_EQ(expectMemoryReuse_, strings.get() == stringsPtr);
    }
    ASSERT_EQ(25, strings->size());
    ASSERT_EQ(0, getNullCount(strings));
    for (size_t j = 0; j < batch->size(); ++j) {
      ASSERT_EQ(2, strings->valueAt(j).size());
      EXPECT_EQ((25 * i + j) / 10, strings->valueAt(j).data()[0]);
      EXPECT_EQ((25 * i + j) % 10, strings->valueAt(j).data()[1]);
    }
  }
}

TEST_P(StringReaderTests, testStringDirectShortBufferWithNulls) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  const unsigned char buffer1[] = {0x3d, 0xf0};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  char blob[512];
  for (size_t i = 0; i < 16; ++i) {
    for (size_t j = 0; j < 16; ++j) {
      blob[2 * (16 * i + j)] = static_cast<char>('A' + i);
      blob[2 * (16 * i + j) + 1] = static_cast<char>('A' + j);
    }
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(blob, VELOX_ARRAY_SIZE(blob), 30)));

  const unsigned char buffer2[] = {0x7d, 0x00, 0x02, 0x7d, 0x00, 0x02};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer2, VELOX_ARRAY_SIZE(buffer2))));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<col0:string>");

  auto reader = buildReader(rowType, streams, {});

  size_t next = 0;
  VectorPtr batch = newBatch(rowType);
  FlatVectorPtr<StringView> strings;
  VectorPtr rowVector;
  for (size_t i = 0; i < 8; ++i) {
    auto stringsPtr = strings.get();
    if (expectMemoryReuse_) {
      strings.reset();
    }
    skipAndRead(reader, batch, /* read */ 64);

    strings = getOnlyChild<FlatVector<StringView>>(batch);
    if (i > 0) {
      ASSERT_EQ(expectMemoryReuse_, strings.get() == stringsPtr);
    }

    ASSERT_EQ(64, strings->size());
    ASSERT_LT(0, getNullCount(strings));
    for (size_t j = 0; j < batch->size(); ++j) {
      ASSERT_EQ((j & 4) != 0, strings->isNullAt(j));
      if (!strings->isNullAt(j)) {
        ASSERT_EQ(2, strings->valueAt(j).size());
        EXPECT_EQ('A' + next / 16, strings->valueAt(j).data()[0]);
        EXPECT_EQ('A' + next % 16, strings->valueAt(j).data()[1]);
        next += 1;
      }
    }
  }
}

/**
 * Tests ORC-24.
 * Requires:
 *   * direct string encoding
 *   * a null value where the unused length crosses the streaming block
 *     and the actual value doesn't
 */
TEST_P(StringReaderTests, testStringDirectNullAcrossWindow) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  const unsigned char isNull[2] = {0xff, 0x7f};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(isNull, VELOX_ARRAY_SIZE(isNull))));

  const char blob[] = "abcdefg";
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(blob, VELOX_ARRAY_SIZE(blob), 4)));

  // [1] * 7
  const unsigned char lenData[] = {0x04, 0x00, 0x01};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(lenData, VELOX_ARRAY_SIZE(lenData))));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<col0:string>");

  auto reader = buildReader(rowType, streams, {});

  VectorPtr batch = newBatch(rowType);
  // This length value won't be overwritten because the value is null,
  // but it induces the problem.
  skipAndRead(reader, batch, /* read */ 6);
  FlatVectorPtr<StringView> strings;
  strings = getOnlyChild<FlatVector<StringView>>(batch);
  ASSERT_EQ(6, strings->size());
  ASSERT_EQ(1, getNullCount(strings));
  ASSERT_TRUE(strings->isNullAt(0));
  for (size_t j = 1; j < batch->size(); ++j) {
    ASSERT_FALSE(strings->isNullAt(j));
    ASSERT_EQ(1, strings->valueAt(j).size());
    ASSERT_EQ('a' + j - 1, strings->valueAt(j).data()[0])
        << "difference at " << j;

    skipAndRead(reader, batch, /* read */ 2);
    strings = getOnlyChild<FlatVector<StringView>>(batch);
    ASSERT_EQ(2, strings->size());
    ASSERT_EQ(0, getNullCount(strings));
    for (size_t j = 0; j < batch->size(); ++j) {
      ASSERT_EQ(1, strings->valueAt(j).size());
      ASSERT_EQ('f' + j, strings->valueAt(j).data()[0]);
    }
  }
}

void validateFlatVectorBatch(
    VectorPtr& batch,
    SimpleVectorPtr<StringView> strings,
    int offset) {
  for (size_t i = 0; i < batch->size(); ++i) {
    ASSERT_EQ(offset + i, strings->valueAt(i).size());
    for (size_t j = 0; j < strings->valueAt(i).size(); ++j) {
      EXPECT_EQ(static_cast<char>(j), strings->valueAt(i).data()[j]);
    }
  }
}

TEST_P(StringReaderTests, testStringDirectSkip) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // sum(0 to 1199)
  const size_t BLOB_SIZE = 719400;
  char blob[BLOB_SIZE];
  size_t posn = 0;
  for (size_t item = 0; item < 1200; ++item) {
    for (size_t ch = 0; ch < item; ++ch) {
      blob[posn++] = static_cast<char>(ch);
    }
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(blob, BLOB_SIZE, 200)));

  // the stream of 0 to 1199
  const unsigned char buffer1[] = {
      0x7f, 0x01, 0x00, 0x7f, 0x01, 0x82, 0x01, 0x7f, 0x01, 0x84,
      0x02, 0x7f, 0x01, 0x86, 0x03, 0x7f, 0x01, 0x88, 0x04, 0x7f,
      0x01, 0x8a, 0x05, 0x7f, 0x01, 0x8c, 0x06, 0x7f, 0x01, 0x8e,
      0x07, 0x7f, 0x01, 0x90, 0x08, 0x1b, 0x01, 0x92, 0x09};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<col0:string>");
  auto reader = buildReader(rowType, streams, {});

  VectorPtr batch = newBatch(rowType);
  skipAndRead(reader, batch, /* read */ 2);
  FlatVectorPtr<StringView> strings;

  strings = getOnlyChild<FlatVector<StringView>>(batch);
  ASSERT_EQ(2, strings->size());
  ASSERT_EQ(0, getNullCount(strings));
  validateFlatVectorBatch(batch, strings, 0);
  skipAndRead(reader, batch, /* read */ 2, /* skip */ 14);

  strings = getOnlyChild<FlatVector<StringView>>(batch);
  ASSERT_EQ(2, strings->size());
  ASSERT_EQ(0, getNullCount(strings));
  validateFlatVectorBatch(batch, strings, 16);
  skipAndRead(reader, batch, /* read */ 2, /* skip */ 1180);

  strings = getOnlyChild<FlatVector<StringView>>(batch);
  ASSERT_EQ(2, strings->size());
  ASSERT_EQ(0, getNullCount(strings));
  validateFlatVectorBatch(batch, strings, 1198);
}

TEST_P(StringReaderTests, testStringDirectSkipWithNulls) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  // alternate 4 non-null and 4 null via [0xf0 for x in range(2400 / 8)]
  const unsigned char buffer1[] = {0x7f, 0xf0, 0x7f, 0xf0, 0x25, 0xf0};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  // sum(range(1200))
  const size_t BLOB_SIZE = 719400;

  // each string is [x % 256 for x in range(r)]
  char blob[BLOB_SIZE];
  size_t posn = 0;
  for (size_t item = 0; item < 1200; ++item) {
    for (size_t ch = 0; ch < item; ++ch) {
      blob[posn++] = static_cast<char>(ch);
    }
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(blob, BLOB_SIZE, 200)));

  // range(1200)
  const unsigned char buffer2[] = {
      0x7f, 0x01, 0x00, 0x7f, 0x01, 0x82, 0x01, 0x7f, 0x01, 0x84,
      0x02, 0x7f, 0x01, 0x86, 0x03, 0x7f, 0x01, 0x88, 0x04, 0x7f,
      0x01, 0x8a, 0x05, 0x7f, 0x01, 0x8c, 0x06, 0x7f, 0x01, 0x8e,
      0x07, 0x7f, 0x01, 0x90, 0x08, 0x1b, 0x01, 0x92, 0x09};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer2, VELOX_ARRAY_SIZE(buffer2))));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<col0:string>");

  auto reader = buildReader(rowType, streams, {});

  VectorPtr batch = newBatch(rowType);
  SimpleVectorPtr<StringView> strings;
  skipAndRead(reader, batch, /* read */ 2);

  strings = getOnlyChild<FlatVector<StringView>>(batch);
  ASSERT_EQ(2, strings->size());
  ASSERT_EQ(0, getNullCount(strings));
  validateFlatVectorBatch(batch, strings, 0);

  skipAndRead(reader, batch, /* read */ 2, /* skip */ 30);
  strings = getOnlyChild<FlatVector<StringView>>(batch);
  ASSERT_EQ(2, strings->size());
  ASSERT_EQ(0, getNullCount(strings));
  validateFlatVectorBatch(batch, strings, 16);

  skipAndRead(reader, batch, /* read */ 2, /* skip */ 2364);
  strings = getOnlyChild<SimpleVector<StringView>>(batch);
  ASSERT_EQ(2, strings->size());
  ASSERT_LT(0, getNullCount(strings));
  for (size_t i = 0; i < batch->size(); ++i) {
    EXPECT_TRUE(strings->isNullAt(i));
  }
}

TEST_P(TestColumnReader, testList) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // [2 for x in range(600)]
  const unsigned char buffer1[] = {
      0x7f,
      0x00,
      0x02,
      0x7f,
      0x00,
      0x02,
      0x7f,
      0x00,
      0x02,
      0x7f,
      0x00,
      0x02,
      0x4d,
      0x00,
      0x02};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  // range(1200)
  char buffer2[8192];
  size_t size = writeRange(buffer2, 0, 1200);
  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(buffer2, size)));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<col0:array<bigint>>");

  auto reader = buildReader(rowType);

  VectorPtr batch = newBatch(rowType);
  skipAndRead(reader, batch, /* read */ 512);

  auto lists = getOnlyChild<ArrayVector>(batch);
  ASSERT_EQ(512, lists->size());
  ASSERT_EQ(0, getNullCount(lists));

  auto longs = lists->elements()->asFlatVector<int64_t>();
  ASSERT_EQ(1024, longs->size());
  ASSERT_EQ(0, getNullCount(longs));
  for (size_t i = 0; i < batch->size(); ++i) {
    EXPECT_EQ(2 * i, lists->offsetAt(i));
  }
  for (size_t i = 0; i < longs->size(); ++i) {
    EXPECT_EQ(i, longs->valueAt(i));
  }
}

TEST_P(TestColumnReader, testListPropagateNulls) {
  if (useSelectiveReader()) {
    GTEST_SKIP() << "SelectiveColumnReader doesn't support nested structs yet";
  }

  auto rowType =
      HiveTypeParser().parse("struct<col0:struct<col0_0:array<bigint>>>");

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // set getStream
  const unsigned char buffer[] = {0xff, 0x00};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer, VELOX_ARRAY_SIZE(buffer))));

  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(buffer, 0)));

  EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(buffer, 0)));

  // create the row type
  auto reader = buildReader(rowType);

  VectorPtr batch = newBatch(rowType);
  skipAndRead(reader, batch, /* read */ 8);

  auto structs = getOnlyChild<RowVector>(batch);
  ASSERT_EQ(8, structs->size());
  ASSERT_EQ(8, getNullCount(structs));

  auto lists = getOnlyChild<ArrayVector>(structs);
  ASSERT_EQ(8, lists->size());
  ASSERT_EQ(8, getNullCount(lists));

  auto longs = lists->elements();
  ASSERT_NE(longs, nullptr);
  ASSERT_EQ(0, longs->size());
}

TEST_P(TestColumnReader, testListWithNulls) {
  if (useSelectiveReader()) {
    GTEST_SKIP()
        << "SelectiveColumnReader doesn't have full support for arrays yet";
  }

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // [0xaa for x in range(2048/8)]
  const unsigned char buffer1[] = {0x7f, 0xaa, 0x7b, 0xaa};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // [1 for x in range(260)] +
  // [4 for x in range(260)] +
  // [0 for x in range(260)] +
  // [3 for x in range(243)] +
  // [19]
  const unsigned char buffer2[] = {0x7f, 0x00, 0x01, 0x7f, 0x00, 0x01, 0x7f,
                                   0x00, 0x04, 0x7f, 0x00, 0x04, 0x7f, 0x00,
                                   0x00, 0x7f, 0x00, 0x00, 0x7f, 0x00, 0x03,
                                   0x6e, 0x00, 0x03, 0xff, 0x13};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer2, VELOX_ARRAY_SIZE(buffer2))));

  // range(2048)
  char buffer3[8192];
  size_t size = writeRange(buffer3, 0, 2048);
  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(buffer3, size)));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<col0:array<bigint>>");

  auto reader = buildReader(rowType);

  VectorPtr batch = newBatch(rowType);
  skipAndRead(reader, batch, /* read */ 512);

  auto lists = getOnlyChild<ArrayVector>(batch);
  ASSERT_EQ(512, lists->size());
  ASSERT_LT(0, getNullCount(lists));

  auto longs =
      std::dynamic_pointer_cast<FlatVector<int64_t>>(lists->elements());
  ASSERT_EQ(256, longs->size());
  ASSERT_EQ(0, getNullCount(longs));
  for (size_t i = 0; i < batch->size(); ++i) {
    EXPECT_EQ(i % 2 != 0, lists->isNullAt(i)) << "Wrong value at " << i;
    EXPECT_EQ((i + 1) / 2, lists->offsetAt(i)) << "Wrong value at " << i;
  }

  for (size_t i = 0; i < longs->size(); ++i) {
    EXPECT_EQ(i, longs->valueAt(i));
  }

  auto listsPtr = lists.get();
  auto longsPtr = longs.get();
  if (expectMemoryReuse_) {
    lists.reset();
    longs.reset();
  }
  skipAndRead(reader, batch, /* read */ 512);
  lists = getOnlyChild<ArrayVector>(batch);
  longs = std::dynamic_pointer_cast<FlatVector<int64_t>>(lists->elements());
  ASSERT_EQ(expectMemoryReuse_, lists.get() == listsPtr);
  ASSERT_EQ(expectMemoryReuse_, longs.get() == longsPtr);

  ASSERT_EQ(512, lists->size());
  ASSERT_LT(0, getNullCount(lists));
  ASSERT_EQ(1012, longs->size());
  ASSERT_EQ(0, getNullCount(longs));
  for (size_t i = 0; i < batch->size(); ++i) {
    EXPECT_EQ(i % 2 != 0, lists->isNullAt(i)) << "Wrong value at " << i;
    if (i < 8) {
      EXPECT_EQ((i + 1) / 2, lists->offsetAt(i)) << "Wrong value at " << i;
    } else {
      EXPECT_EQ(4 * ((i + 1) / 2) - 12, lists->offsetAt(i))
          << "Wrong value at " << i;
    }
  }

  for (size_t i = 0; i < longs->size(); ++i) {
    EXPECT_EQ(256 + i, longs->valueAt(i));
  }

  listsPtr = lists.get();
  longsPtr = longs.get();
  if (expectMemoryReuse_) {
    lists.reset();
    longs.reset();
  }
  skipAndRead(reader, batch, /* read */ 512);
  lists = getOnlyChild<ArrayVector>(batch);
  longs = std::dynamic_pointer_cast<FlatVector<int64_t>>(lists->elements());
  ASSERT_EQ(expectMemoryReuse_, lists.get() == listsPtr);
  ASSERT_EQ(expectMemoryReuse_, longs.get() == longsPtr);

  ASSERT_EQ(512, lists->size());
  ASSERT_LT(0, getNullCount(lists));
  ASSERT_EQ(32, longs->size());
  ASSERT_EQ(0, getNullCount(longs));
  for (size_t i = 0; i < batch->size(); ++i) {
    EXPECT_EQ(i % 2 != 0, lists->isNullAt(i)) << "Wrong value at " << i;
    if (i < 16) {
      EXPECT_EQ(4 * ((i + 1) / 2), lists->offsetAt(i))
          << "Wrong value at " << i;
    } else {
      EXPECT_EQ(32, lists->offsetAt(i)) << "Wrong value at " << i;
    }
  }

  for (size_t i = 0; i < longs->size(); ++i) {
    EXPECT_EQ(1268 + i, longs->valueAt(i));
  }

  listsPtr = lists.get();
  longsPtr = longs.get();
  if (expectMemoryReuse_) {
    lists.reset();
    longs.reset();
  }
  skipAndRead(reader, batch, /* read */ 512);
  lists = getOnlyChild<ArrayVector>(batch);
  longs = std::dynamic_pointer_cast<FlatVector<int64_t>>(lists->elements());
  ASSERT_EQ(expectMemoryReuse_, lists.get() == listsPtr);
  ASSERT_EQ(expectMemoryReuse_, longs.get() == longsPtr);

  ASSERT_EQ(512, lists->size());
  ASSERT_LT(0, getNullCount(lists));
  ASSERT_EQ(748, longs->size());
  ASSERT_EQ(0, getNullCount(longs));
  for (size_t i = 0; i < batch->size(); ++i) {
    EXPECT_EQ(i % 2 != 0, lists->isNullAt(i)) << "Wrong value at " << i;
    if (i < 24) {
      EXPECT_EQ(0, lists->offsetAt(i)) << "Wrong value at " << i;
    } else if (i < 510) {
      EXPECT_EQ(3 * ((i - 23) / 2), lists->offsetAt(i))
          << "Wrong value at " << i;
    } else if (i < 511) {
      EXPECT_EQ(729, lists->offsetAt(i)) << "Wrong value at " << i;
    } else {
      EXPECT_EQ(748, lists->offsetAt(i)) << "Wrong value at " << i;
    }
  }

  for (size_t i = 0; i < longs->size(); ++i) {
    EXPECT_EQ(1300 + i, longs->valueAt(i));
  }
}

TEST_P(TestColumnReader, testListSkipWithNulls) {
  if (useSelectiveReader()) {
    GTEST_SKIP()
        << "SelectiveColumnReader doesn't have full support for arrays yet";
  }

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // [0xaa for x in range(2048/8)]
  const unsigned char buffer1[] = {0x7f, 0xaa, 0x7b, 0xaa};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // [1 for x in range(260)] +
  // [4 for x in range(260)] +
  // [0 for x in range(260)] +
  // [3 for x in range(243)] +
  // [19]
  const unsigned char buffer2[] = {0x7f, 0x00, 0x01, 0x7f, 0x00, 0x01, 0x7f,
                                   0x00, 0x04, 0x7f, 0x00, 0x04, 0x7f, 0x00,
                                   0x00, 0x7f, 0x00, 0x00, 0x7f, 0x00, 0x03,
                                   0x6e, 0x00, 0x03, 0xff, 0x13};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer2, VELOX_ARRAY_SIZE(buffer2))));

  // range(2048)
  char buffer3[8192];
  size_t size = writeRange(buffer3, 0, 2048);
  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(buffer3, size)));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<col0:array<bigint>>");

  auto reader = buildReader(rowType);

  VectorPtr batch = newBatch(rowType);

  skipAndRead(reader, batch, /* read */ 1);

  auto lists = getOnlyChild<ArrayVector>(batch);
  ASSERT_EQ(1, lists->size());
  ASSERT_EQ(0, getNullCount(lists));

  auto longs =
      std::dynamic_pointer_cast<FlatVector<int64_t>>(lists->elements());
  ASSERT_EQ(1, longs->size());
  ASSERT_EQ(0, getNullCount(longs));
  EXPECT_EQ(0, lists->offsetAt(0));
  EXPECT_EQ(0, longs->valueAt(0));

  auto listsPtr = lists.get();
  auto longsPtr = longs.get();
  if (expectMemoryReuse_) {
    lists.reset();
    longs.reset();
  }
  skipAndRead(reader, batch, /* read */ 1, /* skip */ 13);
  lists = getOnlyChild<ArrayVector>(batch);
  longs = std::dynamic_pointer_cast<FlatVector<int64_t>>(lists->elements());
  ASSERT_EQ(expectMemoryReuse_, lists.get() == listsPtr);
  ASSERT_EQ(expectMemoryReuse_, longs.get() == longsPtr);

  ASSERT_EQ(1, lists->size());
  ASSERT_EQ(0, getNullCount(lists));
  ASSERT_EQ(1, longs->size());
  ASSERT_EQ(0, getNullCount(longs));
  EXPECT_EQ(0, lists->offsetAt(0));
  EXPECT_EQ(7, longs->valueAt(0));

  listsPtr = lists.get();
  longsPtr = longs.get();
  if (expectMemoryReuse_) {
    lists.reset();
    longs.reset();
  }
  skipAndRead(reader, batch, /* read */ 2, /* skip */ 2031);
  lists = getOnlyChild<ArrayVector>(batch);
  longs = std::dynamic_pointer_cast<FlatVector<int64_t>>(lists->elements());
  ASSERT_EQ(expectMemoryReuse_, lists.get() == listsPtr);
  ASSERT_EQ(expectMemoryReuse_, longs.get() == longsPtr);

  ASSERT_EQ(2, lists->size());
  ASSERT_LT(0, getNullCount(lists));
  ASSERT_EQ(19, longs->size());
  ASSERT_EQ(0, getNullCount(longs));
  EXPECT_EQ(0, lists->offsetAt(0));
  EXPECT_EQ(19, lists->offsetAt(1));
  for (size_t i = 0; i < longs->size(); ++i) {
    EXPECT_EQ(2029 + i, longs->valueAt(i));
  }
}

TEST_P(TestColumnReader, testListSkipWithNullsNoData) {
  if (useSelectiveReader()) {
    GTEST_SKIP()
        << "SelectiveColumnReader doesn't have full support for arrays yet";
  }

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // [0xaa for x in range(2048/8)]
  const unsigned char buffer1[] = {0x7f, 0xaa, 0x7b, 0xaa};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // [1 for x in range(260)] +
  // [4 for x in range(260)] +
  // [0 for x in range(260)] +
  // [3 for x in range(243)] +
  // [19]
  const unsigned char buffer2[] = {0x7f, 0x00, 0x01, 0x7f, 0x00, 0x01, 0x7f,
                                   0x00, 0x04, 0x7f, 0x00, 0x04, 0x7f, 0x00,
                                   0x00, 0x7f, 0x00, 0x00, 0x7f, 0x00, 0x03,
                                   0x6e, 0x00, 0x03, 0xff, 0x13};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer2, VELOX_ARRAY_SIZE(buffer2))));

  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(nullptr));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<col0:array<bigint>>");
  // selected filter tree nodes list
  auto reader = buildReader(rowType, {0, 1});

  VectorPtr batch = newBatch(rowType);

  skipAndRead(reader, batch, /* read */ 1);

  auto lists = getOnlyChild<ArrayVector>(batch);
  ASSERT_EQ(1, lists->size());
  ASSERT_EQ(0, getNullCount(lists));
  EXPECT_EQ(0, lists->offsetAt(0));

  auto listsPtr = lists.get();
  if (expectMemoryReuse_) {
    lists.reset();
  }
  skipAndRead(reader, batch, /* read */ 1, /* skip */ 13);
  lists = getOnlyChild<ArrayVector>(batch);
  ASSERT_EQ(expectMemoryReuse_, lists.get() == listsPtr);

  ASSERT_EQ(1, lists->size());
  ASSERT_EQ(0, getNullCount(lists));
  EXPECT_EQ(0, lists->offsetAt(0));

  listsPtr = lists.get();
  if (expectMemoryReuse_) {
    lists.reset();
  }
  skipAndRead(reader, batch, /* read */ 2, /* skip */ 2031);
  lists = getOnlyChild<ArrayVector>(batch);
  ASSERT_EQ(expectMemoryReuse_, lists.get() == listsPtr);

  ASSERT_EQ(2, lists->size());
  ASSERT_LT(0, getNullCount(lists));
  EXPECT_EQ(0, lists->offsetAt(0));
  EXPECT_EQ(19, lists->offsetAt(1));
}

TEST_P(TestColumnReader, testListWithAllNulls) {
  if (useSelectiveReader()) {
    GTEST_SKIP()
        << "SelectiveColumnReader doesn't have full support for arrays yet";
  }

  auto rowType = HiveTypeParser().parse("struct<col0:array<bigint>>");

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // set getStream
  const unsigned char buffer[] = {0xff, 0x00};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer, VELOX_ARRAY_SIZE(buffer))));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(buffer, 0)));

  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(buffer, 0)));

  // create the row type
  auto reader = buildReader(rowType);
  VectorPtr batch = newBatch(rowType);
  skipAndRead(reader, batch, /* read */ 8);

  auto lists = getOnlyChild<ArrayVector>(batch);
  ASSERT_EQ(8, lists->size());
  ASSERT_EQ(8, getNullCount(lists));

  // Ensure that array vector is usable.
  ASSERT_NE(lists->elements(), nullptr);
  ASSERT_NE(lists->retainedSize(), 0);
}

TEST_P(TestColumnReader, testMap) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // [2 for x in range(600)]
  const unsigned char buffer1[] = {
      0x7f,
      0x00,
      0x02,
      0x7f,
      0x00,
      0x02,
      0x7f,
      0x00,
      0x02,
      0x7f,
      0x00,
      0x02,
      0x4d,
      0x00,
      0x02};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  // range(1200)
  char buffer2[8192];
  size_t size = writeRange(buffer2, 0, 1200);
  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(buffer2, size)));

  // range(8, 1208)
  char buffer3[8192];
  size = writeRange(buffer3, 8, 1208);
  EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(buffer3, size)));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<col0:map<bigint,bigint>>");

  auto reader = buildReader(rowType);

  VectorPtr batch = newBatch(rowType);
  skipAndRead(reader, batch, /* read */ 512);

  auto maps = getOnlyChild<MapVector>(batch);
  ASSERT_EQ(512, maps->size());
  ASSERT_EQ(0, getNullCount(maps));

  auto keys = maps->mapKeys()->asFlatVector<int64_t>();
  ASSERT_EQ(1024, keys->size());
  ASSERT_EQ(0, getNullCount(keys));

  auto elements = maps->mapValues()->asFlatVector<int64_t>();
  ASSERT_EQ(1024, elements->size());
  ASSERT_EQ(0, getNullCount(elements));
  for (size_t i = 0; i < batch->size(); ++i) {
    EXPECT_EQ(2 * i, maps->offsetAt(i));
  }
  for (size_t i = 0; i < keys->size(); ++i) {
    EXPECT_EQ(i, keys->valueAt(i));
    EXPECT_EQ(i + 8, elements->valueAt(i));
  }
}

TEST_P(TestColumnReader, testMapWithNulls) {
  if (useSelectiveReader()) {
    GTEST_SKIP()
        << "SelectiveColumnReader doesn't have full support for maps yet";
  }

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // [0xaa for x in range(2048/8)]
  const unsigned char buffer1[] = {0x7f, 0xaa, 0x7b, 0xaa};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // [0x55 for x in range(2048/8)]
  const unsigned char buffer2[] = {0x7f, 0x55, 0x7b, 0x55};
  EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer2, VELOX_ARRAY_SIZE(buffer2))));

  // [1 for x in range(260)] +
  // [4 for x in range(260)] +
  // [0 for x in range(260)] +
  // [3 for x in range(243)] +
  // [19]
  const unsigned char buffer3[] = {0x7f, 0x00, 0x01, 0x7f, 0x00, 0x01, 0x7f,
                                   0x00, 0x04, 0x7f, 0x00, 0x04, 0x7f, 0x00,
                                   0x00, 0x7f, 0x00, 0x00, 0x7f, 0x00, 0x03,
                                   0x6e, 0x00, 0x03, 0xff, 0x13};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer3, VELOX_ARRAY_SIZE(buffer3))));

  // range(2048)
  char buffer4[8192];
  size_t size = writeRange(buffer4, 0, 2048);
  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(buffer4, size)));

  // range(8, 1032)
  char buffer5[8192];
  size = writeRange(buffer5, 8, 1032);
  EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(buffer5, size)));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<col0:map<bigint,bigint>>");

  auto reader = buildReader(rowType);

  VectorPtr batch = newBatch(rowType);
  skipAndRead(reader, batch, /* read */ 512);

  auto maps = getOnlyChild<MapVector>(batch);
  ASSERT_EQ(512, maps->size());
  ASSERT_LT(0, getNullCount(maps));

  auto keys = std::dynamic_pointer_cast<FlatVector<int64_t>>(maps->mapKeys());
  ASSERT_EQ(256, keys->size());
  ASSERT_EQ(0, getNullCount(keys));

  auto elements =
      std::dynamic_pointer_cast<FlatVector<int64_t>>(maps->mapValues());
  ASSERT_EQ(256, elements->size());
  ASSERT_LT(0, getNullCount(elements));
  for (size_t i = 0; i < batch->size(); ++i) {
    EXPECT_EQ(i % 2 != 0, maps->isNullAt(i)) << "Wrong value at " << i;
    EXPECT_EQ((i + 1) / 2, maps->offsetAt(i)) << "Wrong value at " << i;
  }

  for (size_t i = 0; i < keys->size(); ++i) {
    EXPECT_EQ(i, keys->valueAt(i));
    EXPECT_EQ(i % 2 == 0, elements->isNullAt(i));
    if (!elements->isNullAt(i)) {
      EXPECT_EQ(i / 2 + 8, elements->valueAt(i));
    }
  }

  auto mapsPtr = maps.get();
  auto keysPtr = keys.get();
  auto elementsPtr = elements.get();
  if (expectMemoryReuse_) {
    maps.reset();
    keys.reset();
    elements.reset();
  }
  skipAndRead(reader, batch, /* read */ 512);
  maps = getOnlyChild<MapVector>(batch);
  keys = std::dynamic_pointer_cast<FlatVector<int64_t>>(maps->mapKeys());
  elements = std::dynamic_pointer_cast<FlatVector<int64_t>>(maps->mapValues());
  ASSERT_EQ(expectMemoryReuse_, maps.get() == mapsPtr);
  ASSERT_EQ(expectMemoryReuse_, keys.get() == keysPtr);
  ASSERT_EQ(expectMemoryReuse_, elements.get() == elementsPtr);

  ASSERT_EQ(512, maps->size());
  ASSERT_LT(0, getNullCount(maps));
  ASSERT_EQ(1012, keys->size());
  ASSERT_EQ(0, getNullCount(keys));
  ASSERT_EQ(1012, elements->size());
  ASSERT_LT(0, getNullCount(elements));
  for (size_t i = 0; i < batch->size(); ++i) {
    EXPECT_EQ(i % 2 != 0, maps->isNullAt(i)) << "Wrong value at " << i;
    if (i < 8) {
      EXPECT_EQ((i + 1) / 2, maps->offsetAt(i)) << "Wrong value at " << i;
    } else {
      EXPECT_EQ(4 * ((i + 1) / 2) - 12, maps->offsetAt(i))
          << "Wrong value at " << i;
    }
  }
  for (size_t i = 0; i < keys->size(); ++i) {
    EXPECT_EQ(256 + i, keys->valueAt(i));
    EXPECT_EQ(i % 2 == 0, elements->isNullAt(i));
    if (!elements->isNullAt(i)) {
      EXPECT_EQ(128 + 8 + i / 2, elements->valueAt(i));
    }
  }

  mapsPtr = maps.get();
  keysPtr = keys.get();
  elementsPtr = elements.get();
  if (expectMemoryReuse_) {
    maps.reset();
    keys.reset();
    elements.reset();
  }
  skipAndRead(reader, batch, /* read */ 512);
  maps = getOnlyChild<MapVector>(batch);
  keys = std::dynamic_pointer_cast<FlatVector<int64_t>>(maps->mapKeys());
  elements = std::dynamic_pointer_cast<FlatVector<int64_t>>(maps->mapValues());
  ASSERT_EQ(expectMemoryReuse_, maps.get() == mapsPtr);
  ASSERT_EQ(expectMemoryReuse_, keys.get() == keysPtr);
  ASSERT_EQ(expectMemoryReuse_, elements.get() == elementsPtr);

  ASSERT_EQ(512, maps->size());
  ASSERT_LT(0, getNullCount(maps));
  ASSERT_EQ(32, keys->size());
  ASSERT_EQ(0, getNullCount(keys));
  ASSERT_EQ(32, elements->size());
  ASSERT_LT(0, getNullCount(elements));
  for (size_t i = 0; i < batch->size(); ++i) {
    EXPECT_EQ(i % 2 != 0, maps->isNullAt(i)) << "Wrong value at " << i;
    if (i < 16) {
      EXPECT_EQ(4 * ((i + 1) / 2), maps->offsetAt(i)) << "Wrong value at " << i;
    } else {
      EXPECT_EQ(32, maps->offsetAt(i)) << "Wrong value at " << i;
    }
  }
  for (size_t i = 0; i < keys->size(); ++i) {
    EXPECT_EQ(1268 + i, keys->valueAt(i));
    EXPECT_EQ(i % 2 == 0, elements->isNullAt(i));
    if (!elements->isNullAt(i)) {
      EXPECT_EQ(634 + 8 + i / 2, elements->valueAt(i));
    }
  }

  mapsPtr = maps.get();
  keysPtr = keys.get();
  elementsPtr = elements.get();
  if (expectMemoryReuse_) {
    maps.reset();
    keys.reset();
    elements.reset();
  }
  skipAndRead(reader, batch, /* read */ 512);
  maps = getOnlyChild<MapVector>(batch);
  keys = std::dynamic_pointer_cast<FlatVector<int64_t>>(maps->mapKeys());
  elements = std::dynamic_pointer_cast<FlatVector<int64_t>>(maps->mapValues());
  ASSERT_EQ(expectMemoryReuse_, maps.get() == mapsPtr);
  ASSERT_EQ(expectMemoryReuse_, keys.get() == keysPtr);
  ASSERT_EQ(expectMemoryReuse_, elements.get() == elementsPtr);

  ASSERT_EQ(512, maps->size());
  ASSERT_LT(0, getNullCount(maps));
  ASSERT_EQ(748, keys->size());
  ASSERT_EQ(0, getNullCount(keys));
  ASSERT_EQ(748, elements->size());
  ASSERT_LT(0, getNullCount(elements));
  for (size_t i = 0; i < batch->size(); ++i) {
    EXPECT_EQ(i % 2 != 0, maps->isNullAt(i)) << "Wrong value at " << i;
    if (i < 24) {
      EXPECT_EQ(0, maps->offsetAt(i)) << "Wrong value at " << i;
    } else if (i < 510) {
      EXPECT_EQ(3 * ((i - 23) / 2), maps->offsetAt(i))
          << "Wrong value at " << i;
    } else if (i < 511) {
      EXPECT_EQ(729, maps->offsetAt(i)) << "Wrong value at " << i;
    } else {
      EXPECT_EQ(748, maps->offsetAt(i)) << "Wrong value at " << i;
    }
  }
  for (size_t i = 0; i < keys->size(); ++i) {
    EXPECT_EQ(1300 + i, keys->valueAt(i));
    EXPECT_EQ(i % 2 == 0, elements->isNullAt(i));
    if (!elements->isNullAt(i)) {
      EXPECT_EQ(650 + 8 + i / 2, elements->valueAt(i));
    }
  }
}

TEST_P(TestColumnReader, testMapSkipWithNulls) {
  if (useSelectiveReader()) {
    GTEST_SKIP()
        << "SelectiveColumnReader doesn't have full support for maps yet";
  }

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // [0xaa for x in range(2048/8)]
  const unsigned char buffer1[] = {0x7f, 0xaa, 0x7b, 0xaa};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  // [1 for x in range(260)] +
  // [4 for x in range(260)] +
  // [0 for x in range(260)] +
  // [3 for x in range(243)] +
  // [19]
  const unsigned char buffer2[] = {0x7f, 0x00, 0x01, 0x7f, 0x00, 0x01, 0x7f,
                                   0x00, 0x04, 0x7f, 0x00, 0x04, 0x7f, 0x00,
                                   0x00, 0x7f, 0x00, 0x00, 0x7f, 0x00, 0x03,
                                   0x6e, 0x00, 0x03, 0xff, 0x13};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer2, VELOX_ARRAY_SIZE(buffer2))));

  // range(2048)
  char buffer3[8192];
  size_t size = writeRange(buffer3, 0, 2048);
  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(buffer3, size)));

  // range(8, 2056)
  char buffer4[8192];
  size = writeRange(buffer4, 8, 2056);
  EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(buffer4, size)));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<col0:map<bigint,bigint>>");

  auto reader = buildReader(rowType);

  VectorPtr batch = newBatch(rowType);

  skipAndRead(reader, batch, /* read */ 1);

  auto maps = getOnlyChild<MapVector>(batch);
  ASSERT_EQ(1, maps->size());
  ASSERT_EQ(0, getNullCount(maps));

  auto keys = std::dynamic_pointer_cast<FlatVector<int64_t>>(maps->mapKeys());
  ASSERT_EQ(1, keys->size());
  ASSERT_EQ(0, getNullCount(keys));

  auto elements =
      std::dynamic_pointer_cast<FlatVector<int64_t>>(maps->mapValues());
  ASSERT_EQ(1, elements->size());
  ASSERT_EQ(0, getNullCount(elements));
  EXPECT_EQ(0, maps->offsetAt(0));
  EXPECT_EQ(0, keys->valueAt(0));
  EXPECT_EQ(8, elements->valueAt(0));

  auto mapsPtr = maps.get();
  auto keysPtr = keys.get();
  auto elementsPtr = elements.get();
  if (expectMemoryReuse_) {
    maps.reset();
    keys.reset();
    elements.reset();
  }
  skipAndRead(reader, batch, /* read */ 1, /* skip */ 13);
  maps = getOnlyChild<MapVector>(batch);
  keys = std::dynamic_pointer_cast<FlatVector<int64_t>>(maps->mapKeys());
  elements = std::dynamic_pointer_cast<FlatVector<int64_t>>(maps->mapValues());
  ASSERT_EQ(expectMemoryReuse_, maps.get() == mapsPtr);
  ASSERT_EQ(expectMemoryReuse_, keys.get() == keysPtr);
  ASSERT_EQ(expectMemoryReuse_, elements.get() == elementsPtr);

  ASSERT_EQ(1, maps->size());
  ASSERT_EQ(0, getNullCount(maps));
  ASSERT_EQ(1, keys->size());
  ASSERT_EQ(0, getNullCount(keys));
  ASSERT_EQ(1, elements->size());
  ASSERT_EQ(0, getNullCount(elements));
  EXPECT_EQ(0, maps->offsetAt(0));
  EXPECT_EQ(7, keys->valueAt(0));
  EXPECT_EQ(7 + 8, elements->valueAt(0));

  mapsPtr = maps.get();
  keysPtr = keys.get();
  elementsPtr = elements.get();
  if (expectMemoryReuse_) {
    maps.reset();
    keys.reset();
    elements.reset();
  }
  skipAndRead(reader, batch, /* read */ 2, /* skip */ 2031);
  maps = getOnlyChild<MapVector>(batch);
  keys = std::dynamic_pointer_cast<FlatVector<int64_t>>(maps->mapKeys());
  elements = std::dynamic_pointer_cast<FlatVector<int64_t>>(maps->mapValues());
  ASSERT_EQ(expectMemoryReuse_, maps.get() == mapsPtr);
  ASSERT_EQ(expectMemoryReuse_, keys.get() == keysPtr);
  ASSERT_EQ(expectMemoryReuse_, elements.get() == elementsPtr);

  ASSERT_EQ(2, maps->size());
  ASSERT_LT(0, getNullCount(maps));
  ASSERT_EQ(19, keys->size());
  ASSERT_EQ(0, getNullCount(keys));
  ASSERT_EQ(19, elements->size());
  ASSERT_EQ(0, getNullCount(elements));
  EXPECT_EQ(0, maps->offsetAt(0));
  EXPECT_EQ(19, maps->offsetAt(1));
  for (size_t i = 0; i < keys->size(); ++i) {
    EXPECT_EQ(2029 + i, keys->valueAt(i));
    EXPECT_EQ(2029 + 8 + i, elements->valueAt(i));
  }
}

TEST_P(TestColumnReader, testMapSkipWithNullsNoData) {
  if (useSelectiveReader()) {
    GTEST_SKIP()
        << "SelectiveColumnReader doesn't have full support for maps yet";
  }

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // [0xaa for x in range(2048/8)]
  const unsigned char buffer1[] = {0x7f, 0xaa, 0x7b, 0xaa};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  // [1 for x in range(260)] +
  // [4 for x in range(260)] +
  // [0 for x in range(260)] +
  // [3 for x in range(243)] +
  // [19]
  const unsigned char buffer2[] = {0x7f, 0x00, 0x01, 0x7f, 0x00, 0x01, 0x7f,
                                   0x00, 0x04, 0x7f, 0x00, 0x04, 0x7f, 0x00,
                                   0x00, 0x7f, 0x00, 0x00, 0x7f, 0x00, 0x03,
                                   0x6e, 0x00, 0x03, 0xff, 0x13};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer2, VELOX_ARRAY_SIZE(buffer2))));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<col0:map<bigint,bigint>>");
  auto reader = buildReader(rowType, {0, 1});

  VectorPtr batch = newBatch(rowType);

  skipAndRead(reader, batch, /* read */ 1);

  auto maps = getOnlyChild<MapVector>(batch);
  ASSERT_EQ(1, maps->size());
  ASSERT_EQ(0, getNullCount(maps));
  EXPECT_EQ(0, maps->offsetAt(0));

  auto mapsPtr = maps.get();
  if (expectMemoryReuse_) {
    maps.reset();
  }
  skipAndRead(reader, batch, /* read */ 1, /* skip */ 13);
  maps = getOnlyChild<MapVector>(batch);
  ASSERT_EQ(expectMemoryReuse_, maps.get() == mapsPtr);

  ASSERT_EQ(1, maps->size());
  ASSERT_EQ(0, getNullCount(maps));
  EXPECT_EQ(0, maps->offsetAt(0));

  mapsPtr = maps.get();
  if (expectMemoryReuse_) {
    maps.reset();
  }
  skipAndRead(reader, batch, /* read */ 2, /* skip */ 2031);
  maps = getOnlyChild<MapVector>(batch);
  ASSERT_EQ(expectMemoryReuse_, maps.get() == mapsPtr);

  ASSERT_EQ(2, maps->size());
  ASSERT_LT(0, getNullCount(maps));
  EXPECT_EQ(0, maps->offsetAt(0));
  EXPECT_EQ(19, maps->offsetAt(1));
}

TEST_P(TestColumnReader, testMapWithAllNulls) {
  if (useSelectiveReader()) {
    GTEST_SKIP()
        << "SelectiveColumnReader doesn't have full support for maps yet";
  }

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  const unsigned char buffer1[] = {0xff, 0x00};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(buffer1, 0)));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<col0:map<bigint,bigint>>");
  auto reader = buildReader(rowType, {0, 1});
  VectorPtr batch = newBatch(rowType);
  skipAndRead(reader, batch, /* read */ 8);

  auto maps = getOnlyChild<MapVector>(batch);
  ASSERT_EQ(8, maps->size());
  ASSERT_EQ(8, getNullCount(maps));

  ASSERT_NE(maps->mapKeys(), nullptr);
  ASSERT_NE(maps->mapValues(), nullptr);

  ASSERT_NE(maps->retainedSize(), 0);
}

TEST_P(TestColumnReader, testFloatBatchNotAligned) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  const float testValues[] = {1.0f, 2.5f, -100.125f};
  const unsigned char byteValues[] = {
      0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x20, 0x40, 0x00, 0x40, 0xc8, 0xc2};

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(
          byteValues,
          VELOX_ARRAY_SIZE(byteValues),
          VELOX_ARRAY_SIZE(byteValues) / 2)));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<myFloat:float>");

  auto reader = buildReader(rowType);

  VectorPtr batch = newBatch(rowType);
  skipAndRead(reader, batch, /* read */ 3);

  auto floatBatch = getOnlyChild<FlatVector<float>>(batch);
  ASSERT_EQ(3, floatBatch->size());
  ASSERT_EQ(0, getNullCount(floatBatch));

  for (size_t i = 0; i < batch->size(); ++i) {
    EXPECT_FLOAT_EQ(testValues[i], floatBatch->valueAt(i));
  }
}

TEST_P(TestColumnReader, testFloatWithNulls) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  // 13 non-nulls followed by 19 nulls
  const unsigned char buffer1[] = {0xfc, 0xff, 0xf8, 0x0, 0x0};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  const float test_vals[] = {
      1.0f,
      2.5f,
      -100.125f,
      10000.0f,
      1.234567E23f,
      -2.3456E-12f,
      std::numeric_limits<float>::infinity(),
      std::numeric_limits<float>::quiet_NaN(),
      -std::numeric_limits<float>::infinity(),
      3.4028235E38f,
      -3.4028235E38f,
      1.4e-45f,
      -1.4e-45f};
  const unsigned char buffer2[] = {
      0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x20, 0x40, 0x00, 0x40, 0xc8,
      0xc2, 0x00, 0x40, 0x1c, 0x46, 0xcf, 0x24, 0xd1, 0x65, 0x93, 0xe,
      0x25, 0xac, 0x0,  0x0,  0x80, 0x7f, 0x0,  0x0,  0xc0, 0x7f, 0x0,
      0x0,  0x80, 0xff, 0xff, 0xff, 0x7f, 0x7f, 0xff, 0xff, 0x7f, 0xff,
      0x1,  0x0,  0x0,  0x0,  0x1,  0x0,  0x0,  0x80};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer2, VELOX_ARRAY_SIZE(buffer2))));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<myFloat:float>");

  auto reader = buildReader(rowType);

  VectorPtr batch = newBatch(rowType);
  skipAndRead(reader, batch, /* read */ 32);

  auto floatBatch = getOnlyChild<FlatVector<float>>(batch);
  ASSERT_EQ(32, floatBatch->size());
  ASSERT_LT(0, getNullCount(floatBatch));

  for (size_t i = 0; i < batch->size(); ++i) {
    if (i > 12) {
      EXPECT_TRUE(floatBatch->isNullAt(i));
    } else if (i == 7) {
      EXPECT_FALSE(floatBatch->isNullAt(i));
      EXPECT_EQ(true, std::isnan(floatBatch->valueAt(i)));
    } else {
      EXPECT_FALSE(floatBatch->isNullAt(i));
      EXPECT_FLOAT_EQ(test_vals[i], floatBatch->valueAt(i));
    }
  }
}

TEST_P(TestColumnReader, testFloatSkipWithNulls) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  // 2 non-nulls, 2 nulls, 2 non-nulls, 2 nulls
  const unsigned char buffer1[] = {0xff, 0xcc};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  // 1, 2.5, -100.125, 10000
  const unsigned char buffer2[] = {
      0x00,
      0x00,
      0x80,
      0x3f,
      0x00,
      0x00,
      0x20,
      0x40,
      0x00,
      0x40,
      0xc8,
      0xc2,
      0x00,
      0x40,
      0x1c,
      0x46};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer2, VELOX_ARRAY_SIZE(buffer2))));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<myFloat:float>");

  auto reader = buildReader(rowType);

  VectorPtr batch = newBatch(rowType);

  float test_vals[] = {1.0, 2.5, -100.125, 10000.0};
  int32_t vals_ix = 0;

  skipAndRead(reader, batch, /* read */ 3);

  auto floatBatch = getOnlyChild<FlatVector<float>>(batch);
  ASSERT_EQ(3, floatBatch->size());
  ASSERT_LT(0, getNullCount(floatBatch));

  for (size_t i = 0; i < batch->size(); ++i) {
    if (i > 1) {
      EXPECT_TRUE(floatBatch->isNullAt(i));
    } else {
      EXPECT_FALSE(floatBatch->isNullAt(i));
      EXPECT_FLOAT_EQ(test_vals[vals_ix], floatBatch->valueAt(i));
      vals_ix++;
    }
  }

  auto floatBatchPtr = floatBatch.get();
  if (expectMemoryReuse_) {
    floatBatch.reset();
  }

  skipAndRead(reader, batch, /* read */ 4, /* skip */ 1);

  floatBatch = getOnlyChild<FlatVector<float>>(batch);
  ASSERT_EQ(expectMemoryReuse_, floatBatch.get() == floatBatchPtr);

  ASSERT_EQ(4, floatBatch->size());
  ASSERT_LT(0, getNullCount(floatBatch));
  for (size_t i = 0; i < batch->size(); ++i) {
    if (i > 1) {
      EXPECT_TRUE(floatBatch->isNullAt(i));
    } else {
      EXPECT_FALSE(floatBatch->isNullAt(i));
      EXPECT_FLOAT_EQ(test_vals[vals_ix], floatBatch->valueAt(i));
      vals_ix++;
    }
  }
}

TEST_P(TestColumnReader, testDoubleWithNulls) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  // 13 non-nulls followed by 19 nulls
  const unsigned char buffer1[] = {0xfc, 0xff, 0xf8, 0x0, 0x0};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  const double test_vals[] = {
      1.0,
      2.0,
      -2.0,
      100.0,
      1.23456789E32,
      -3.42234E-18,
      std::numeric_limits<double>::infinity(),
      std::numeric_limits<double>::quiet_NaN(),
      -std::numeric_limits<double>::infinity(),
      1.7976931348623157e308,
      -1.7976931348623157E308,
      4.9e-324,
      -4.9e-324};
  const unsigned char buffer2[] = {
      0x0,  0x0,  0x0,  0x0,  0x0,  0x0,  0xf0, 0x3f, 0x0,  0x0,  0x0,  0x0,
      0x0,  0x0,  0x0,  0x40, 0x0,  0x0,  0x0,  0x0,  0x0,  0x0,  0x0,  0xc0,
      0x0,  0x0,  0x0,  0x0,  0x0,  0x0,  0x59, 0x40, 0xe8, 0x38, 0x65, 0x99,
      0xf9, 0x58, 0x98, 0x46, 0xa1, 0x88, 0x41, 0x98, 0xc5, 0x90, 0x4f, 0xbc,
      0x0,  0x0,  0x0,  0x0,  0x0,  0x0,  0xf0, 0x7f, 0x0,  0x0,  0x0,  0x0,
      0x0,  0x0,  0xf8, 0x7f, 0x0,  0x0,  0x0,  0x0,  0x0,  0x0,  0xf0, 0xff,
      0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xef, 0x7f, 0xff, 0xff, 0xff, 0xff,
      0xff, 0xff, 0xef, 0xff, 0x1,  0x0,  0x0,  0x0,  0x0,  0x0,  0x0,  0x0,
      0x1,  0x0,  0x0,  0x0,  0x0,  0x0,  0x0,  0x80};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer2, VELOX_ARRAY_SIZE(buffer2))));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<myDouble:double>");

  auto reader = buildReader(rowType);

  VectorPtr batch = newBatch(rowType);
  skipAndRead(reader, batch, /* read */ 32);

  auto doubleBatch = getOnlyChild<FlatVector<double>>(batch);
  ASSERT_EQ(32, doubleBatch->size());
  ASSERT_LT(0, getNullCount(doubleBatch));

  for (size_t i = 0; i < batch->size(); ++i) {
    if (i > 12) {
      EXPECT_TRUE(doubleBatch->isNullAt(i)) << "Wrong value at " << i;
    } else if (i == 7) {
      EXPECT_FALSE(doubleBatch->isNullAt(i)) << "Wrong value at " << i;
      EXPECT_EQ(true, std::isnan(doubleBatch->valueAt(i)));
    } else {
      EXPECT_FALSE(doubleBatch->isNullAt(i)) << "Wrong value at " << i;
      EXPECT_DOUBLE_EQ(test_vals[i], doubleBatch->valueAt(i))
          << "Wrong value at " << i;
    }
  }
}

TEST_P(TestColumnReader, testDoubleSkipWithNulls) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  // 1 non-null, 5 nulls, 2 non-nulls
  const unsigned char buffer1[] = {0xff, 0x83};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  // 1, 2, -2
  const unsigned char buffer2[] = {
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xc0};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer2, VELOX_ARRAY_SIZE(buffer2))));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<myDouble:double>");

  auto reader = buildReader(rowType);

  VectorPtr batch = newBatch(rowType);

  double test_vals[] = {1.0, 2.0, -2.0};
  int32_t vals_ix = 0;

  skipAndRead(reader, batch, /* read */ 2);

  auto doubleBatch = getOnlyChild<FlatVector<double>>(batch);
  ASSERT_EQ(2, doubleBatch->size());
  ASSERT_LT(0, getNullCount(doubleBatch));

  for (size_t i = 0; i < batch->size(); ++i) {
    if (i > 0) {
      EXPECT_TRUE(doubleBatch->isNullAt(i));
    } else {
      EXPECT_FALSE(doubleBatch->isNullAt(i));
      EXPECT_DOUBLE_EQ(test_vals[vals_ix], doubleBatch->valueAt(i));
      vals_ix++;
    }
  }

  auto doubleBatchPtr = doubleBatch.get();
  if (expectMemoryReuse_) {
    doubleBatch.reset();
  }
  skipAndRead(reader, batch, /* read */ 3, /* skip */ 3);

  doubleBatch = getOnlyChild<FlatVector<double>>(batch);
  ASSERT_EQ(expectMemoryReuse_, doubleBatch.get() == doubleBatchPtr);

  ASSERT_EQ(3, doubleBatch->size());
  ASSERT_LT(0, getNullCount(doubleBatch));
  for (size_t i = 0; i < batch->size(); ++i) {
    if (i < 1) {
      EXPECT_TRUE(doubleBatch->isNullAt(i));
    } else {
      EXPECT_FALSE(doubleBatch->isNullAt(i));
      EXPECT_DOUBLE_EQ(test_vals[vals_ix], doubleBatch->valueAt(i));
      vals_ix++;
    }
  }
}

TEST_P(TestColumnReader, testTimestampSkipWithNulls) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  // 2 non-nulls, 2 nulls, 2 non-nulls, 2 nulls
  const unsigned char buffer1[] = {0xff, 0xcc};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  const unsigned char buffer2[] = {
      0xfc,
      0xbb,
      0xb5,
      0xbe,
      0x31,
      0xa1,
      0xee,
      0xe2,
      0x10,
      0xf8,
      0x92,
      0xee,
      0xf,
      0x92,
      0xa0,
      0xd4,
      0x30};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer2, VELOX_ARRAY_SIZE(buffer2))));

  const unsigned char buffer3[] = {0x1, 0x8, 0x5e};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_NANO_DATA, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer3, VELOX_ARRAY_SIZE(buffer3))));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<myTimestamp:timestamp>");

  auto reader = buildReader(rowType);

  VectorPtr batch = newBatch(rowType);

  const std::array<const char*, 4> expected{
      {"Fri May 10 17:40:50 2013\n",
       "Wed Jun 11 18:41:51 2014\n",
       "Sun Jul 12 19:42:52 2015\n",
       "Sat Aug 13 20:43:53 2016\n"}};
  const std::array<int64_t, 4> expected_nano{
      {110000000, 120000000, 130000000, 140000000}};
  int32_t vals_ix = 0;

  skipAndRead(reader, batch, /* read */ 3);

  auto tsBatch = getOnlyChild<FlatVector<Timestamp>>(batch);
  ASSERT_EQ(3, tsBatch->size());
  ASSERT_LT(0, getNullCount(tsBatch));

  for (size_t i = 0; i < batch->size(); ++i) {
    if (i > 1) {
      EXPECT_TRUE(tsBatch->isNullAt(i));
    } else {
      EXPECT_FALSE(tsBatch->isNullAt(i));
      time_t time = static_cast<time_t>(tsBatch->valueAt(i).getSeconds());
      tm timeStruct;
      ASSERT_PRED1(isNotNull, gmtime_r(&time, &timeStruct));
      char buffer[30];
      asctime_r(&timeStruct, buffer);
      EXPECT_STREQ(expected[vals_ix], buffer)
          << "Wrong value at " << vals_ix << ", " << i;
      EXPECT_EQ(expected_nano[vals_ix], tsBatch->valueAt(i).getNanos())
          << "Wrong value at " << vals_ix << ", " << i;
      vals_ix++;
    }
  }

  auto tsBatchPtr = tsBatch.get();
  if (expectMemoryReuse_) {
    tsBatch.reset();
  }
  skipAndRead(reader, batch, /* read */ 4, /* skip */ 1);

  tsBatch = getOnlyChild<FlatVector<Timestamp>>(batch);
  ASSERT_EQ(expectMemoryReuse_, tsBatch.get() == tsBatchPtr);
  ASSERT_EQ(4, tsBatch->size());
  ASSERT_LT(0, getNullCount(tsBatch));
  for (size_t i = 0; i < batch->size(); ++i) {
    if (i > 1) {
      EXPECT_TRUE(tsBatch->isNullAt(i));
    } else {
      EXPECT_FALSE(tsBatch->isNullAt(i));
      time_t time = static_cast<time_t>(tsBatch->valueAt(i).getSeconds());
      tm timeStruct;
      ASSERT_PRED1(isNotNull, gmtime_r(&time, &timeStruct));
      char buffer[30];
      asctime_r(&timeStruct, buffer);
      EXPECT_STREQ(expected[vals_ix], buffer);
      EXPECT_EQ(expected_nano[vals_ix], tsBatch->valueAt(i).getNanos());
      vals_ix++;
    }
  }
}

TEST_P(TestColumnReader, testTimestamp) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  const unsigned char buffer1[] = {
      0xf6, 0x9f, 0xf4, 0xc6, 0xbd, 0x03, 0xff, 0xec, 0xf3, 0xbc, 0x03,
      0xff, 0xb1, 0xf8, 0x84, 0x1b, 0x9d, 0x86, 0xd7, 0xfa, 0x1a, 0x9d,
      0xb8, 0xcd, 0xdc, 0x1a, 0x9d, 0xea, 0xc3, 0xbe, 0x1a, 0x9d, 0x9c,
      0xba, 0xa0, 0x1a, 0x9d, 0x88, 0xa6, 0x82, 0x1a, 0x9d, 0xba, 0x9c,
      0xe4, 0x19, 0x9d, 0xee, 0xe1, 0xcd, 0x18};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer1, VELOX_ARRAY_SIZE(buffer1))));

  const unsigned char buffer2[] = {
      0xf6, 0x00, 0xa8, 0xd1, 0xf9, 0xd6, 0x03, 0x00, 0x9e, 0x01, 0xec,
      0x76, 0xf4, 0x76, 0xfc, 0x76, 0x84, 0x77, 0x8c, 0x77, 0xfd, 0x0b};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_NANO_DATA, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(buffer2, VELOX_ARRAY_SIZE(buffer2))));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<myTimestamp:timestamp>");

  auto reader = buildReader(rowType);

  VectorPtr batch = newBatch(rowType);
  const std::array<const char*, 10> expected{
      {"Sun Mar 12 23:00:00 2000\n",
       "Mon Mar 20 20:00:00 2000\n",
       "Mon Jan  1 08:00:00 1900\n",
       "Sat May  5 20:34:56 1900\n",
       "Sun May  5 20:34:56 1901\n",
       "Mon May  5 20:34:56 1902\n",
       "Tue May  5 20:34:56 1903\n",
       "Thu May  5 20:34:56 1904\n",
       "Fri May  5 20:34:56 1905\n",
       "Thu May  5 20:34:56 1910\n"}};
  const std::array<int64_t, 10> expectedNano{
      {0,
       123456789,
       0,
       190000000,
       190100000,
       190200000,
       190300000,
       190400000,
       190500000,
       191000000}};

  skipAndRead(reader, batch, /* read */ 10);

  auto tsBatch = getOnlyChild<FlatVector<Timestamp>>(batch);
  ASSERT_EQ(10, tsBatch->size());
  ASSERT_EQ(0, getNullCount(tsBatch));

  for (size_t i = 0; i < batch->size(); ++i) {
    time_t time = static_cast<time_t>(tsBatch->valueAt(i).getSeconds());
    tm timeStruct;
    ASSERT_PRED1(isNotNull, gmtime_r(&time, &timeStruct));
    char buffer[30];
    asctime_r(&timeStruct, buffer);
    EXPECT_STREQ(expected[i], buffer) << "Wrong value at " << i;
    EXPECT_EQ(expectedNano[i], tsBatch->valueAt(i).getNanos());
  }
}

TEST_P(TestColumnReader, testLargeSkip) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // repeat 40 times [1, 60]
  unsigned char length[40 * 3];
  for (size_t i = 0; i < 40; ++i) {
    size_t pos = i * 3;
    length[pos] = 0x39;
    length[pos + 1] = 0x01;
    length[pos + 2] = 0x01;
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(length, VELOX_ARRAY_SIZE(length))));

  char data[1024 * 1024];
  size_t size = writeRange(data, 0, 73200);
  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(data, size)));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<col0:array<bigint>>");

  auto reader = buildReader(rowType);
  VectorPtr batch = newBatch(rowType);

  skipAndRead(reader, batch, /* read */ 100, /* skip */ 2100);

  auto lists = getOnlyChild<ArrayVector>(batch);
  ASSERT_EQ(100, lists->size());
  ASSERT_EQ(0, getNullCount(lists));
  // [1, 60], [1, 40]
  auto longs = lists->elements()->asFlatVector<int64_t>();
  ASSERT_EQ(2650, longs->size());
  ASSERT_EQ(0, getNullCount(longs));
  size_t next = 1;
  size_t offset = 0;
  for (size_t i = 0; i < batch->size(); ++i) {
    EXPECT_EQ(offset, lists->offsetAt(i));
    offset += next;
    next += 1;
    if (next > 60) {
      next = 1;
    }
  }
  next = 64050;
  for (size_t i = 0; i < longs->size(); ++i) {
    EXPECT_EQ(next++, longs->valueAt(i));
  }
}

void validateStringDirectBatches(
    std::vector<VectorPtr>& batches,
    const MockStripeStreams& streams) {
  for (size_t i = 0; i < batches.size(); ++i) {
    FlatVectorPtr<StringView> batch;
    batch = getOnlyChild<FlatVector<StringView>>(batches[i]);
    EXPECT_EQ(batch->size(), 1);

    // Hold a reference to the StringView item to keep it in scope.
    auto item = batch->valueAt(0);
    auto data = item.data();

    EXPECT_EQ(item.size(), i + 1);
    for (size_t j = 0; j < i + 1; ++j) {
      ASSERT_EQ('a' + i, data[j]) << "Wrong value at " << i << ", " << j;
    }
  }
}

TEST_P(StringReaderTests, testStringDirectUseBatchAfterClose) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // sum(range(1..26))
  const size_t BLOB_SIZE = 351;
  const size_t ROW_COUNT = 26;

  std::array<char, BLOB_SIZE> blob;
  size_t posn = 0;
  for (size_t len = 1; len <= ROW_COUNT; ++len) {
    for (size_t i = 0; i < len; ++i) {
      blob[posn++] = static_cast<char>('a' + len - 1);
    }
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(blob.data(), BLOB_SIZE)));

  // range(1..131)
  auto lengths = folly::make_array<char>(0x7f, 0x01, 0x01);
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(lengths.data(), lengths.size())));

  auto rowType = HiveTypeParser().parse("struct<myString:string>");
  auto reader = buildReader(rowType, streams, {});

  std::vector<VectorPtr> batches;
  for (size_t i = 0; i < ROW_COUNT; ++i) {
    VectorPtr batch = newBatch(rowType);
    reader->next(1, batch);
    batch->as<RowVector>()->childAt(0)->loadedVector();
    batches.push_back(batch);
  }

  validateStringDirectBatches(batches, streams);

  // make sure data can still be accessed after releasing column reader. This
  // will fail in asan mode if memory is recycled.
  reader.reset(nullptr);
  validateStringDirectBatches(batches, streams);
}

void validateStringDictBatches(
    std::vector<VectorPtr>& batches,
    bool returnFlatVector,
    MockStripeStreams& streams) {
  for (size_t i = 0; i < batches.size(); ++i) {
    std::shared_ptr<SimpleVector<StringView>> batch;
    if (returnFlatVector) {
      batch = getOnlyChild<FlatVector<StringView>>(batches[i]);
    } else {
      batch = getOnlyChild<DictionaryVector<StringView>>(batches[i]);
    }
    EXPECT_EQ(batch->size(), 2);
    auto len = batch->valueAt(0).size();
    EXPECT_EQ(len, i + 1);
    EXPECT_EQ(batch->valueAt(1).size(), i + 1);

    // Hold a reference to the StringView items to keep them in scope.
    auto item1 = batch->valueAt(0);
    auto data1 = item1.data();
    auto item2 = batch->valueAt(1);
    auto data2 = item2.data();

    for (size_t j = 0; j < len; ++j) {
      EXPECT_EQ('a' + i, data1[j]);
      EXPECT_EQ('A' + i, data2[j]);
    }
  }
}

TEST_P(StringReaderTests, testStringDictUseBatchAfterClose) {
  // sum(range(1..26))
  const size_t BLOB_SIZE = 351;
  const size_t DICT_SIZE = 26;

  // set getEncoding
  proto::ColumnEncoding dictEncoding;
  dictEncoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
  dictEncoding.set_dictionarysize(DICT_SIZE);
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(0))
      .WillRepeatedly(Return(&directEncoding));
  EXPECT_CALL(streams, getEncodingProxy(1))
      .WillRepeatedly(Return(&dictEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // dictionary content
  std::array<char, BLOB_SIZE> dict;
  std::array<char, BLOB_SIZE> strideDict;
  size_t posn = 0;
  for (size_t len = 1; len <= DICT_SIZE; ++len) {
    for (size_t i = 0; i < len; ++i) {
      dict[posn] = static_cast<char>('a' + len - 1);
      strideDict[posn++] = static_cast<char>('A' + len - 1);
    }
  }
  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_DICTIONARY_DATA, false))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(dict.data(), BLOB_SIZE)));
  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_STRIDE_DICTIONARY, true))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(strideDict.data(), BLOB_SIZE)));

  // length
  auto length = folly::make_array<char>(0x17, 0x01, 0x01);
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, false))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(length.data(), length.size())));
  EXPECT_CALL(
      streams,
      getStreamProxy(1, proto::Stream_Kind_STRIDE_DICTIONARY_LENGTH, true))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(length.data(), length.size())));

  // data
  std::array<char, 1024> data;
  data[0] = 0xcc;
  size_t dataSize = 1;
  for (size_t i = 0; i <= DICT_SIZE; ++i) {
    dataSize = writeVuLong(data.data(), dataSize, i);
    dataSize = writeVuLong(data.data(), dataSize, 0);
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(data.data(), dataSize)));

  // in_dict
  auto inDict = folly::make_array<char>(0x04, 0xaa);
  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_IN_DICTIONARY, false))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(inDict.data(), inDict.size())));

  // set row index
  proto::RowIndex index;
  size_t pos = 0;
  for (size_t i = 0; i < DICT_SIZE; ++i) {
    auto entry = index.add_entry();
    pos += i;
    entry->add_positions(pos);
    entry->add_positions(0);
    entry->add_positions(i);
    entry->add_positions(1);
  }

  const uint64_t BATCH_SIZE = 2;
  auto indexData = index.SerializePartialAsString();
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_ROW_INDEX, _))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(indexData.data(), indexData.size())));
  TestStrideIndexProvider provider(BATCH_SIZE);
  EXPECT_CALL(streams, getStrideIndexProviderProxy())
      .WillRepeatedly(Return(&provider));

  auto rowType = HiveTypeParser().parse("struct<myString:string>");
  auto reader = buildReader(rowType, streams, {});

  std::vector<VectorPtr> batches;
  for (size_t i = 0; i < DICT_SIZE; ++i) {
    VectorPtr batch = newBatch(rowType);
    reader->next(BATCH_SIZE, batch);
    for (auto& child : batch->as<RowVector>()->children()) {
      child->loadedVector();
    }
    provider.addRow(BATCH_SIZE);
    batches.push_back(batch);
  }
  validateStringDictBatches(batches, returnFlatVector_, streams);

  // make sure data can still be accessed after releasing column reader. This
  // will fail in asan mode if memory is recycled.
  reader.reset(nullptr);
  validateStringDictBatches(batches, returnFlatVector_, streams);
}

TEST_P(StringReaderTests, testStringDictStrideDictDoesntExist) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set row index
  const size_t rowIndexStride = 8;
  proto::RowIndex index;
  const size_t totalRowCount = rowIndexStride * 10;

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  std::array<char, 1024> data;
  data[0] = -(static_cast<int8_t>(totalRowCount));
  size_t len = 1;
  size_t strideDictSize = 0;
  size_t strideDictSizeTotal = 0;
  std::array<char, 2048> dict;
  std::array<char, 2048> strideDict;
  size_t dictLen = 0;
  size_t strideDictLen = 0;
  std::vector<std::string> dictVals;
  std::vector<std::string> strideDictVals;
  proto::RowIndexEntry* entry = nullptr;

  // only add stride dict for even strides
  auto inStride = [](size_t i) {
    return ((i / rowIndexStride) % 2 == 0) && (i % 2 == 0);
  };

  for (uint32_t i = 0; i < totalRowCount; ++i) {
    if (i % rowIndexStride == 0) {
      // add stride dict size
      if (entry) {
        entry->add_positions(strideDictSize);
      }
      strideDictSizeTotal += strideDictSize;
      strideDictSize = 0;
      // capture indices
      entry = index.add_entry();
      entry->add_positions(strideDictLen);
      entry->add_positions(0);
      entry->add_positions(strideDictSizeTotal);
    }
    std::string val = folly::to<std::string>(folly::Random::rand32());
    auto valLen = val.length();
    if (inStride(i)) {
      len = writeVuLong(data.data(), len, strideDictSize++);
      strideDictVals.push_back(val);
      memcpy(strideDict.data() + strideDictLen, val.c_str(), valLen);
      strideDictLen += valLen;
    } else {
      len = writeVuLong(data.data(), len, dictVals.size());
      dictVals.push_back(val);
      memcpy(dict.data() + dictLen, val.c_str(), valLen);
      dictLen += valLen;
    }
  }
  entry->add_positions(strideDictSize);

  proto::ColumnEncoding dictionaryEncoding;
  dictionaryEncoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
  dictionaryEncoding.set_dictionarysize(dictVals.size());
  EXPECT_CALL(streams, getEncodingProxy(1))
      .WillRepeatedly(Return(&dictionaryEncoding));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(data.data(), len)));
  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_DICTIONARY_DATA, false))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(dict.data(), dictLen)));
  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_STRIDE_DICTIONARY, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(strideDict.data(), strideDictLen)));

  std::array<char, 1024> dictLength;
  dictLength[0] = -(static_cast<int8_t>(dictVals.size()));
  dictLen = 1;
  for (size_t i = 0; i < dictVals.size(); ++i) {
    dictLen = writeVuLong(dictLength.data(), dictLen, dictVals[i].length());
  }
  std::array<char, 1024> strideDictLength;
  strideDictLength[0] = -(static_cast<int8_t>(strideDictVals.size()));
  strideDictLen = 1;
  for (size_t i = 0; i < strideDictVals.size(); ++i) {
    strideDictLen = writeVuLong(
        strideDictLength.data(), strideDictLen, strideDictVals[i].length());
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, false))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(dictLength.data(), dictLen)));
  EXPECT_CALL(
      streams,
      getStreamProxy(1, proto::Stream_Kind_STRIDE_DICTIONARY_LENGTH, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(
          strideDictLength.data(), strideDictLen)));

  auto inDict = folly::make_array<char>(
      0xf6, 0x55, 0xff, 0x55, 0xff, 0x55, 0xff, 0x55, 0xff, 0x55, 0xff);
  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_IN_DICTIONARY, false))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(inDict.data(), inDict.size())));

  auto indexData = index.SerializePartialAsString();
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_ROW_INDEX, _))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(indexData.data(), indexData.size())));
  TestStrideIndexProvider provider(rowIndexStride);
  EXPECT_CALL(streams, getStrideIndexProviderProxy())
      .WillRepeatedly(Return(&provider));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<myString:string>");

  auto reader = buildReader(rowType, streams, {});
  uint64_t rowCount = 0;
  size_t dictOffset = 0;
  size_t strideDictOffset = 0;
  VectorPtr batch = newBatch(rowType);
  std::shared_ptr<SimpleVector<StringView>> stringBatch;
  VectorPtr rowVector;
  while (rowCount < totalRowCount) {
    auto stringBatchPtr = stringBatch.get();
    if (expectMemoryReuse_) {
      stringBatch.reset();
    }
    reader->next(rowIndexStride, batch);
    ASSERT_EQ(rowIndexStride, batch->size());
    ASSERT_EQ(0, getNullCount(batch));
    if (returnFlatVector_) {
      stringBatch = getOnlyChild<FlatVector<StringView>>(batch);
    } else {
      stringBatch = getOnlyChild<DictionaryVector<StringView>>(batch);
    }
    if (rowCount > 0) {
      ASSERT_EQ(expectMemoryReuse_, stringBatch.get() == stringBatchPtr);
    }
    ASSERT_EQ(rowIndexStride, stringBatch->size());
    ASSERT_EQ(0, getNullCount(stringBatch));
    for (size_t i = 0; i < batch->size(); ++i) {
      EXPECT_EQ(
          inStride(rowCount) ? strideDictVals[strideDictOffset++]
                             : dictVals[dictOffset++],
          std::string(
              stringBatch->valueAt(i).data(), stringBatch->valueAt(i).size()));
      ++rowCount;
    }
    provider.addRow(rowIndexStride);
  }
}

TEST_P(StringReaderTests, testStringDictZeroLengthStrideDict) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set row index
  const size_t rowIndexStride = 8;
  proto::RowIndex index;
  const size_t totalRowCount = rowIndexStride * 10;

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  std::array<char, 1024> data;
  data[0] = -(static_cast<int8_t>(totalRowCount));
  size_t len = 1;
  size_t strideDictSize = 0;
  size_t strideDictSizeTotal = 0;
  std::array<char, 2048> dict;
  std::array<char, 2048> strideDict;
  size_t dictLen = 0;
  size_t strideDictLen = 0;
  std::vector<std::string> dictVals;
  std::vector<std::string> strideDictVals;
  proto::RowIndexEntry* entry = nullptr;

  // only add stride dict for even strides
  auto inStride = [](size_t i) {
    return ((i / rowIndexStride) % 2 == 0) && (i % 2 == 0);
  };

  for (uint32_t i = 0; i < totalRowCount; ++i) {
    if (i % rowIndexStride == 0) {
      // add stride dict size
      if (entry) {
        entry->add_positions(strideDictSize);
      }
      strideDictSizeTotal += strideDictSize;
      strideDictSize = 0;
      // capture indices
      entry = index.add_entry();
      entry->add_positions(strideDictLen);
      entry->add_positions(0);
      entry->add_positions(strideDictSizeTotal);
    }
    // zero length string
    std::string val;
    auto valLen = val.length();
    if (inStride(i)) {
      len = writeVuLong(data.data(), len, strideDictSize++);
      strideDictVals.push_back(val);
      memcpy(strideDict.data() + strideDictLen, val.c_str(), valLen);
      strideDictLen += valLen;
    } else {
      len = writeVuLong(data.data(), len, dictVals.size());
      dictVals.push_back(val);
      memcpy(dict.data() + dictLen, val.c_str(), valLen);
      dictLen += valLen;
    }
  }
  entry->add_positions(strideDictSize);

  proto::ColumnEncoding dictionaryEncoding;
  dictionaryEncoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
  dictionaryEncoding.set_dictionarysize(dictVals.size());
  EXPECT_CALL(streams, getEncodingProxy(1))
      .WillRepeatedly(Return(&dictionaryEncoding));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(data.data(), len)));
  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_DICTIONARY_DATA, false))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(dict.data(), dictLen)));
  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_STRIDE_DICTIONARY, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(strideDict.data(), strideDictLen)));

  std::array<char, 1024> dictLength;
  dictLength[0] = -(static_cast<int8_t>(dictVals.size()));
  dictLen = 1;
  for (size_t i = 0; i < dictVals.size(); ++i) {
    dictLen = writeVuLong(dictLength.data(), dictLen, dictVals[i].length());
  }
  std::array<char, 1024> strideDictLength;
  strideDictLength[0] = -(static_cast<int8_t>(strideDictVals.size()));
  strideDictLen = 1;
  for (size_t i = 0; i < strideDictVals.size(); ++i) {
    strideDictLen = writeVuLong(
        strideDictLength.data(), strideDictLen, strideDictVals[i].length());
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, false))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(dictLength.data(), dictLen)));
  EXPECT_CALL(
      streams,
      getStreamProxy(1, proto::Stream_Kind_STRIDE_DICTIONARY_LENGTH, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(
          strideDictLength.data(), strideDictLen)));

  auto inDict = folly::make_array<char>(
      0xf6, 0x55, 0xff, 0x55, 0xff, 0x55, 0xff, 0x55, 0xff, 0x55, 0xff);
  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_IN_DICTIONARY, false))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(inDict.data(), inDict.size())));

  auto indexData = index.SerializePartialAsString();
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_ROW_INDEX, _))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(indexData.data(), indexData.size())));
  TestStrideIndexProvider provider(rowIndexStride);
  EXPECT_CALL(streams, getStrideIndexProviderProxy())
      .WillRepeatedly(Return(&provider));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<myString:string>");
  auto reader = buildReader(rowType, streams, {});
  VectorPtr batch = newBatch(rowType);
  uint64_t rowCount = 0;
  size_t dictOffset = 0;
  size_t strideDictOffset = 0;
  while (rowCount < totalRowCount) {
    reader->next(rowIndexStride, batch);
    ASSERT_EQ(rowIndexStride, batch->size());
    ASSERT_EQ(0, getNullCount(batch));
    std::shared_ptr<SimpleVector<StringView>> stringBatch;
    if (returnFlatVector_) {
      stringBatch = getOnlyChild<FlatVector<StringView>>(batch);
    } else {
      stringBatch = getOnlyChild<DictionaryVector<StringView>>(batch);
    }
    ASSERT_EQ(rowIndexStride, stringBatch->size());
    ASSERT_EQ(0, getNullCount(stringBatch));
    for (size_t i = 0; i < batch->size(); ++i) {
      EXPECT_EQ(
          inStride(rowCount) ? strideDictVals[strideDictOffset++]
                             : dictVals[dictOffset++],
          std::string(
              stringBatch->valueAt(i).data(), stringBatch->valueAt(i).size()));
      ++rowCount;
    }
    provider.addRow(rowIndexStride);
  }
}

TEST_P(TestColumnReader, testPresentStreamChange) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // increasing values
  std::array<char, 1024> data;
  writeRange(data.data(), 0, 256);
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(data.data(), data.size())));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<a:int>");

  auto reader = buildReader(rowType);

  // simulate vector reuse
  auto pool = &streams.getMemoryPool();
  VectorPtr toReset = std::make_shared<FlatVector<int32_t>>(
      pool,
      AlignedBuffer::allocate<bool>(1, pool),
      1,
      AlignedBuffer::allocate<int32_t>(1, pool),
      std::vector<BufferPtr>());
  VectorPtr batch = std::make_shared<RowVector>(
      pool, rowType, nullptr, 1, std::vector<VectorPtr>{toReset}, 0);

  auto size = 100;
  reader->next(size, batch);
  ASSERT_EQ(size, batch->size());
  ASSERT_EQ(0, getNullCount(batch));

  auto ints = getOnlyChild<FlatVector<int32_t>>(batch);
  ASSERT_EQ(size, ints->size());
  ASSERT_EQ(0, getNullCount(ints));
  for (size_t i = 0; i < ints->size(); ++i) {
    EXPECT_FALSE(ints->isNullAt(i));
    EXPECT_EQ(i, ints->valueAt(i));
  }
}

TEST_P(TestColumnReader, testStructVectorTypeChange) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // increasing values
  std::array<char, 1024> data;
  writeRange(data.data(), 0, 256);
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(data.data(), data.size())));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<a:int>");

  auto reader = buildReader(rowType);

  // simulate vector reset by creating vector of wrong type
  auto pool = &streams.getMemoryPool();
  VectorPtr toReset = std::make_shared<FlatVector<float>>(
      pool,
      nullptr,
      1,
      AlignedBuffer::allocate<float>(1, pool),
      std::vector<BufferPtr>());
  VectorPtr batch = std::make_shared<RowVector>(
      pool, ROW({REAL()}), nullptr, 1, std::vector<VectorPtr>{toReset}, 0);

  auto size = 100;
  reader->next(size, batch);
  ASSERT_EQ(size, batch->size());
  ASSERT_EQ(0, getNullCount(batch));

  auto ints = getOnlyChild<FlatVector<int32_t>>(batch);
  ASSERT_EQ(size, ints->size());
  ASSERT_EQ(0, getNullCount(ints));
  for (size_t i = 0; i < ints->size(); ++i) {
    EXPECT_EQ(i, ints->valueAt(i));
  }
}

TEST_P(TestColumnReader, testListVectorTypeChange) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // length of 2s
  auto lengths = folly::make_array<char>(0x7f, 0x00, 0x02);
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(lengths.data(), lengths.size())));

  // increasing values
  std::array<char, 1024> data;
  writeRange(data.data(), 0, 256);
  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(data.data(), data.size())));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<a:array<int>>");

  auto reader = buildReader(rowType);

  // simulate batch reuse
  auto pool = &streams.getMemoryPool();
  VectorPtr elements = std::make_shared<FlatVector<int8_t>>(
      pool,
      nullptr,
      1,
      AlignedBuffer::allocate<int8_t>(1, pool),
      std::vector<BufferPtr>());
  VectorPtr list = std::make_shared<ArrayVector>(
      pool,
      ARRAY(TINYINT()),
      nullptr,
      1,
      AlignedBuffer::allocate<vector_size_t>(1, pool),
      AlignedBuffer::allocate<vector_size_t>(1, pool),
      elements,
      0);
  VectorPtr batch = std::make_shared<RowVector>(
      pool,
      ROW({ARRAY(TINYINT())}),
      nullptr,
      1,
      std::vector<VectorPtr>{list},
      0);

  auto size = 100;
  reader->next(size, batch);
  ASSERT_EQ(size, batch->size());
  ASSERT_EQ(0, getNullCount(batch));

  auto lists = getOnlyChild<ArrayVector>(batch);
  ASSERT_EQ(size, lists->size());
  ASSERT_EQ(0, getNullCount(lists));

  auto longs = lists->elements()->asFlatVector<int32_t>();
  ASSERT_EQ(size * 2, longs->size());
  ASSERT_EQ(0, getNullCount(longs));
  for (size_t i = 0; i < batch->size(); ++i) {
    EXPECT_EQ(2 * i, lists->offsetAt(i));
    EXPECT_EQ(2, lists->sizeAt(i));
  }
  for (size_t i = 0; i < longs->size(); ++i) {
    EXPECT_EQ(i, longs->valueAt(i));
  }
}

TEST_P(TestColumnReader, testMapVectorTypeChange) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // length of 2s
  auto lengths = folly::make_array<char>(0x7f, 0x00, 0x02);
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(lengths.data(), lengths.size())));

  // increasing values
  std::array<char, 1024> data;
  writeRange(data.data(), 0, 256);
  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(data.data(), data.size())));
  EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(data.data(), data.size())));

  // create the row type
  auto rowType = HiveTypeParser().parse("struct<a:map<int,int>>");

  auto reader = buildReader(rowType);

  // simulate batch reuse
  auto pool = &streams.getMemoryPool();
  VectorPtr toReset = std::make_shared<FlatVector<int8_t>>(
      pool,
      nullptr,
      1,
      AlignedBuffer::allocate<int8_t>(1, pool),
      std::vector<BufferPtr>());
  VectorPtr map = std::make_shared<MapVector>(
      pool,
      MAP(TINYINT(), TINYINT()),
      nullptr,
      1,
      AlignedBuffer::allocate<vector_size_t>(1, pool),
      AlignedBuffer::allocate<vector_size_t>(1, pool),
      toReset,
      toReset,
      0);
  VectorPtr batch = std::make_shared<RowVector>(
      pool,
      ROW({MAP(TINYINT(), TINYINT())}),
      nullptr,
      1,
      std::vector<VectorPtr>{map},
      0);

  auto size = 100;
  reader->next(size, batch);
  ASSERT_EQ(size, batch->size());
  ASSERT_EQ(0, getNullCount(batch));

  auto maps = getOnlyChild<MapVector>(batch);
  ASSERT_EQ(size, maps->size());
  ASSERT_EQ(0, getNullCount(maps));

  auto keys = maps->mapKeys()->asFlatVector<int32_t>();
  auto values = maps->mapValues()->asFlatVector<int32_t>();
  ASSERT_EQ(size * 2, keys->size());
  ASSERT_EQ(size * 2, values->size());
  ASSERT_EQ(0, getNullCount(keys));
  ASSERT_EQ(0, getNullCount(values));
  for (size_t i = 0; i < batch->size(); ++i) {
    EXPECT_EQ(2 * i, maps->offsetAt(i));
    EXPECT_EQ(2, maps->sizeAt(i));
  }
  for (size_t i = 0; i < keys->size(); ++i) {
    EXPECT_EQ(i, keys->valueAt(i));
    EXPECT_EQ(i, values->valueAt(i));
  }
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    TestColumnReaderNoReuse,
    TestColumnReader,
    ::testing::Values(ReaderTestParams{false, false}));

VELOX_INSTANTIATE_TEST_SUITE_P(
    TestColumnReaderReuse,
    TestColumnReader,
    ::testing::Values(ReaderTestParams{false, true}));

VELOX_INSTANTIATE_TEST_SUITE_P(
    TestSelectiveColumnReader,
    TestColumnReader,
    ::testing::Values(ReaderTestParams{true, false}));

VELOX_INSTANTIATE_TEST_SUITE_P(
    StringViewReaderNoReuse,
    StringReaderTests,
    ::testing::Values(StringReaderTestParams{false, false, false}),
    [](auto p) { return p.param.toString(); });

VELOX_INSTANTIATE_TEST_SUITE_P(
    StringViewReaderReuse,
    StringReaderTests,
    ::testing::Values(StringReaderTestParams{false, false, true}),
    [](auto p) { return p.param.toString(); });

VELOX_INSTANTIATE_TEST_SUITE_P(
    FlatStringViewReaderNoReuse,
    StringReaderTests,
    ::testing::Values(StringReaderTestParams{false, true, false}),
    [](auto p) { return p.param.toString(); });

VELOX_INSTANTIATE_TEST_SUITE_P(
    FlatStringViewReaderReuse,
    StringReaderTests,
    ::testing::Values(StringReaderTestParams{false, true, true}),
    [](auto p) { return p.param.toString(); });

VELOX_INSTANTIATE_TEST_SUITE_P(
    SelectiveStringViewReader,
    StringReaderTests,
    ::testing::Values(StringReaderTestParams{true, false, false}),
    [](auto p) { return p.param.toString(); });

class SchemaMismatchTest : public TestWithParam<bool> {
 protected:
  std::unique_ptr<ColumnReader> buildReader(
      SelectiveColumnReaderBuilder& builder,
      const std::shared_ptr<const Type>& requestedType,
      MockStripeStreams& stripe,
      std::vector<uint64_t> nodes = {},
      bool returnFlatVector = false,
      const std::shared_ptr<const Type>& dataType = nullptr) {
    if (useSelectiveReader()) {
      LOG(INFO) << "Using selective reader";
      return builder.build(requestedType, stripe, nodes, dataType);
    } else {
      LOG(INFO) << "Using normal reader";
      return buildColumnReader(
          requestedType, stripe, nodes, returnFlatVector, dataType);
    }
  }

  VectorPtr newBatch(const TypePtr& rowType) {
    return useSelectiveReader()
        ? BaseVector::create(rowType, 0, &streams.getMemoryPool())
        : nullptr;
  }

  // TODO Rename to streams_
  MockStripeStreams streams;

  bool useSelectiveReader() const {
    return GetParam();
  }

  template <typename From, typename To>
  void runTest(uint64_t size) {
    // create reader
    auto dataType = ROW({"c0"}, {CppToType<From>::create()});
    auto requestedType = ROW({"c0"}, {CppToType<To>::create()});
    auto mismatchReader =
        buildReader(builder_, requestedType, streams, {}, false, dataType);
    VectorPtr mismatchBatch = newBatch(requestedType);
    mismatchReader->next(size, mismatchBatch, nullptr);

    auto asIsReader = buildReader(builder2_, dataType, streams);
    VectorPtr asIsBatch = newBatch(dataType);
    asIsReader->next(size, asIsBatch, nullptr);

    ASSERT_EQ(asIsBatch->size(), mismatchBatch->size());
    auto mismatchField = getOnlyChild<SimpleVector<To>>(mismatchBatch);
    auto asIsField = getOnlyChild<SimpleVector<From>>(asIsBatch);
    for (auto i = 0; i < asIsBatch->size(); ++i) {
      auto isNull = asIsField->isNullAt(i);
      EXPECT_EQ(isNull, mismatchField->isNullAt(i));
      if (!isNull) {
        EXPECT_EQ(asIsField->valueAt(i), mismatchField->valueAt(i));
      }
    }
  }

  SelectiveColumnReaderBuilder builder_;
  SelectiveColumnReaderBuilder builder2_;
};

TEST_P(SchemaMismatchTest, testBoolean) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, _, _)).WillRepeatedly(Return(nullptr));
  // interleaved nulls, encoded as ByteRLE. 0x7f - 130 bytes (0x55 - 0101 0101)
  // repeated.
  auto nulls = folly::make_array<char>(0x7f, 0x55);
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(
          Invoke([&](auto /* unused */, auto /* unused */, auto /* unused */) {
            return new SeekableArrayInputStream(nulls.data(), nulls.size());
          }));
  auto data = folly::make_array<char>(0x7f, 0xaa);
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(
          Invoke([&](auto /* unused */, auto /* unused */, auto /* unused */) {
            return new SeekableArrayInputStream(data.data(), data.size());
          }));

  auto size = 100;
  runTest<bool, int8_t>(size);
  runTest<bool, int16_t>(size);
  runTest<bool, int32_t>(size);
  runTest<bool, int64_t>(size);
}

TEST_P(SchemaMismatchTest, testByte) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, _, _)).WillRepeatedly(Return(nullptr));
  // interleaved nulls, encoded as ByteRLE. 0x7f - 130 bytes (0x55 - 0101 0101)
  // repeated.
  auto nulls = folly::make_array<char>(0x7f, 0x55);
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(
          Invoke([&](auto /* unused */, auto /* unused */, auto /* unused */) {
            return new SeekableArrayInputStream(nulls.data(), nulls.size());
          }));

  constexpr auto round = 10;
  constexpr auto batch = 8;
  std::array<char, round*(batch + 1)> data;
  auto pos = 0;
  for (auto i = 0; i < round; ++i) {
    data[pos++] = 0xf8;
    for (auto j = 0; j < batch; ++j) {
      data[pos++] = (0x01 + j * 0x10);
    }
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(
          Invoke([&](auto /* unused */, auto /* unused */, auto /* unused */) {
            return new SeekableArrayInputStream(data.data(), data.size());
          }));

  auto size = 100;
  runTest<int8_t, int16_t>(size);
  runTest<int8_t, int32_t>(size);
  runTest<int8_t, int64_t>(size);
}

TEST_P(SchemaMismatchTest, testIntDirect) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, _, _)).WillRepeatedly(Return(nullptr));
  // interleaved nulls, encoded as ByteRLE. 0x7f - 130 bytes (0x55 - 0101 0101)
  // repeated.
  auto nulls = folly::make_array<char>(0x7f, 0x55);
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(
          Invoke([&](auto /* unused */, auto /* unused */, auto /* unused */) {
            return new SeekableArrayInputStream(nulls.data(), nulls.size());
          }));

  // increasing values
  std::array<char, 1024> data;
  writeRange(data.data(), 0, 256);
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(
          Invoke([&](auto /* unused */, auto /* unused */, auto /* unused */) {
            return new SeekableArrayInputStream(data.data(), data.size());
          }));

  auto size = 100;
  runTest<int16_t, int32_t>(size);
  runTest<int16_t, int64_t>(size);
  runTest<int32_t, int64_t>(size);
}

TEST_P(SchemaMismatchTest, testIntDict) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  int32_t sourceWidth = 0;
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));
  proto::ColumnEncoding dictEncoding;
  const auto dictSize = 100;
  dictEncoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
  dictEncoding.set_dictionarysize(dictSize);
  EXPECT_CALL(streams, getEncodingProxy(1))
      .WillRepeatedly(Return(&dictEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, _, _)).WillRepeatedly(Return(nullptr));
  // interleaved nulls, encoded as ByteRLE. 0x7f - 130 bytes (0x55 - 0101 0101)
  // repeated.
  auto nulls = folly::make_array<char>(0x7f, 0x55);
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(
          Invoke([&](auto /* unused */, auto /* unused */, auto /* unused */) {
            return new SeekableArrayInputStream(nulls.data(), nulls.size());
          }));

  std::array<char, 1024> data;
  std::vector<uint64_t> v;
  data[0] = 0x9c; // rle literal, -100
  for (uint64_t i = 0; i < dictSize; ++i) {
    v.push_back(i);
  }
  auto count = writeVuLongs(data.data() + 1, v);
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(
          Invoke([&](auto /* unused */, auto /* unused */, auto /* unused */) {
            return new SeekableArrayInputStream(data.data(), count + 1);
          }));

  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_IN_DICTIONARY, false))
      .WillRepeatedly(Return(nullptr));

  EXPECT_CALL(streams, genMockDictDataSetter(1, 0))
      .WillRepeatedly(Return([&](BufferPtr& buffer, MemoryPool* pool) {
        // Non-selective path always has int64 dictionary entries. Selective can
        // have 2 and 4 as well.
        if (!useSelectiveReader() || sourceWidth == 8) {
          buffer = sequence<int64_t>(pool, 0, dictSize);
        } else if (sourceWidth == 2) {
          buffer = sequence<int16_t>(pool, 0, dictSize);
        } else {
          buffer = sequence<int32_t>(pool, 0, dictSize);
        }
      }));

  auto size = 100;

  sourceWidth = 2;
  runTest<int16_t, int32_t>(size);
  runTest<int16_t, int64_t>(size);
  sourceWidth = 4;
  runTest<int32_t, int64_t>(size);
}

TEST_P(SchemaMismatchTest, testFloat) {
  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, _, _)).WillRepeatedly(Return(nullptr));
  // interleaved nulls, encoded as ByteRLE. 0x7f - 130 bytes (0x55 - 0101 0101)
  // repeated.
  auto nulls = folly::make_array<char>(0x7f, 0x55);
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(
          Invoke([&](auto /* unused */, auto /* unused */, auto /* unused */) {
            return new SeekableArrayInputStream(nulls.data(), nulls.size());
          }));
  constexpr auto size = 100;
  std::array<float, size> data;
  for (auto i = 0; i < size; ++i) {
    data[i] = folly::Random::randDouble(0, 100);
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(
          Invoke([&](auto /* unused */, auto /* unused */, auto /* unused */) {
            return new SeekableArrayInputStream(
                reinterpret_cast<char*>(data.data()),
                data.size() * sizeof(float));
          }));

  runTest<float, double>(size);
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    SchemaMismatch,
    SchemaMismatchTest,
    Values(false));

VELOX_INSTANTIATE_TEST_SUITE_P(
    SelectiveSchemaMismatch,
    SchemaMismatchTest,
    Values(true));
