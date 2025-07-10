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

#include <cuda_runtime.h> // @manual
#include <fmt/format.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "dwio/nimble/common/tests/NimbleFileWriter.h"
#include "dwio/nimble/encodings/EncodingLayout.h"
#include "dwio/nimble/encodings/EncodingLayoutCapture.h"
#include "dwio/nimble/encodings/EncodingSelectionPolicy.h"
#include "dwio/nimble/tablet/TabletReader.h"
#include "dwio/nimble/velox/VeloxWriterOptions.h"
#include "velox/common/file/File.h"
#include "velox/dwio/common/Statistics.h"
#include "velox/experimental/wave/dwio/ColumnReader.h"
#include "velox/experimental/wave/dwio/StructColumnReader.h"
#include "velox/experimental/wave/dwio/nimble/NimbleFileFormat.h"
#include "velox/experimental/wave/dwio/nimble/NimbleFormatData.h"
#include "velox/experimental/wave/dwio/nimble/SelectiveStructColumnReader.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::wave {
using namespace facebook::nimble;

namespace {
using namespace facebook::velox;

class NimbleReaderTest : public ::testing::Test,
                         public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::initializeMemoryManager(velox::memory::MemoryManagerOptions{});
  }

  void SetUp() override {
    if (int device; cudaGetDevice(&device) != cudaSuccess) {
      GTEST_SKIP() << "No CUDA detected, skipping all tests";
    }
    deviceArena_ = std::make_unique<GpuArena>(
        100000000, getDeviceAllocator(getDevice()), 400000000);
  }

  GpuArena& deviceArena() {
    return *deviceArena_;
  }

  memory::MemoryPool* leafPool() {
    return pool_.get();
  }

 private:
  std::unique_ptr<GpuArena> deviceArena_{nullptr};
};

#define ENCODING_TYPE_EXPECT_EQ(type1, type2) \
  EXPECT_EQ(static_cast<int>(type1), static_cast<int>(type2));

#define ENCODING_TYPE_ASSERT_EQ(type1, type2) \
  ASSERT_EQ(static_cast<int>(type1), static_cast<int>(type2));

void compareEncodingTree(
    NimbleEncoding& encoding,
    EncodingLayout const& layout) {
  SCOPED_TRACE(fmt::format(
      "Encoding: {}, #children: {}",
      toString(layout.encodingType()),
      encoding.childrenCount()));
  EXPECT_EQ(encoding.childrenCount(), layout.childrenCount());
  ENCODING_TYPE_EXPECT_EQ(encoding.encodingType(), layout.encodingType());
  for (int i = 0; i < encoding.childrenCount(); i++) {
    compareEncodingTree(*encoding.childAt(i), layout.child(i).value());
  }
}
} // namespace

TEST_F(NimbleReaderTest, rootTrivialEncoding) {
  using namespace facebook::nimble;

  auto c0 = makeRowVector({
      makeFlatVector<int16_t>(10, folly::identity),
  });
  auto pool = leafPool();

  // Configure writer options to enforce trivial encoding on column c0
  VeloxWriterOptions writerOptions;
  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Trivial, 1.0},
  };
  ManualEncodingSelectionPolicyFactory encodingFactory(readFactors);
  writerOptions.encodingSelectionPolicyFactory = [&](DataType dataType) {
    return encodingFactory.createPolicy(dataType);
  };

  auto file =
      facebook::nimble::test::createNimbleFile(*pool, c0, writerOptions);
  auto readFile = std::make_unique<InMemoryReadFile>(file);
  TabletReader tablet(*pool, std::move(readFile));
  auto streams = tablet.load(
      tablet.getStripeIdentifier(0), std::vector{static_cast<uint32_t>(1)});

  ASSERT_EQ(streams.size(), 1);
  ASSERT_NE(streams[0], nullptr);
  NimbleChunkedStream stream(*pool, streams[0]->getStream());

  ASSERT_TRUE(stream.hasNext());

  auto chunk = stream.nextChunk();
  auto encoding = NimbleChunk::parseEncodingFromChunk(chunk.chunkData());
  auto layout = EncodingLayoutCapture::capture(encoding->encodingData());

  ASSERT_EQ(layout.encodingType(), EncodingType::Trivial);

  EXPECT_EQ(encoding->numValues(), c0->childAt(0)->size());

  compareEncodingTree(*encoding, layout);
}

TEST_F(NimbleReaderTest, rootNullableEncoding) {
  using namespace facebook::nimble;

  const int nullFreq = 10;
  auto c0 = makeRowVector({
      makeFlatVector<int16_t>(100, folly::identity, nullEvery(nullFreq)),
  });
  auto pool = leafPool();

  VeloxWriterOptions writerOptions;
  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Nullable, 1.0},
  };
  ManualEncodingSelectionPolicyFactory encodingFactory(readFactors);
  writerOptions.encodingSelectionPolicyFactory = [&](DataType dataType) {
    return encodingFactory.createPolicy(dataType);
  };

  auto file =
      facebook::nimble::test::createNimbleFile(*pool, c0, writerOptions);
  auto readFile = std::make_unique<InMemoryReadFile>(file);
  TabletReader tablet(*pool, std::move(readFile));
  auto streams = tablet.load(
      tablet.getStripeIdentifier(0), std::vector{static_cast<uint32_t>(1)});

  ASSERT_EQ(streams.size(), 1);
  ASSERT_NE(streams[0], nullptr);
  NimbleChunkedStream stream(*pool, streams[0]->getStream());

  ASSERT_TRUE(stream.hasNext());

  auto chunk = stream.nextChunk();
  auto encoding = NimbleChunk::parseEncodingFromChunk(chunk.chunkData());

  ENCODING_TYPE_ASSERT_EQ(encoding->encodingType(), EncodingType::Nullable);

  EXPECT_EQ(encoding->numValues(), c0->childAt(0)->size());

  EXPECT_EQ(encoding->childrenCount(), 2);
  EXPECT_EQ(
      encoding->childAt(0)->numValues(),
      c0->size() - c0->size() / nullFreq); // non-nulls
  EXPECT_EQ(encoding->childAt(1)->numValues(), c0->size()); // nulls
}

TEST_F(NimbleReaderTest, rootDictionaryEncoding) {
  using namespace facebook::nimble;

  auto c0 = makeRowVector({
      makeFlatVector<int16_t>({3, 3, 4, 4, 5, 5, 6, 6, 7, 7}),
  });
  auto pool = leafPool();

  VeloxWriterOptions writerOptions;
  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Dictionary, 1.0},
  };
  ManualEncodingSelectionPolicyFactory encodingFactory(readFactors);
  writerOptions.encodingSelectionPolicyFactory = [&](DataType dataType) {
    return encodingFactory.createPolicy(dataType);
  };

  auto file =
      facebook::nimble::test::createNimbleFile(*pool, c0, writerOptions);
  auto readFile = std::make_unique<InMemoryReadFile>(file);
  TabletReader tablet(*pool, std::move(readFile));
  auto streams = tablet.load(
      tablet.getStripeIdentifier(0), std::vector{static_cast<uint32_t>(1)});

  ASSERT_EQ(streams.size(), 1);
  ASSERT_NE(streams[0], nullptr);
  NimbleChunkedStream stream(*pool, streams[0]->getStream());

  ASSERT_TRUE(stream.hasNext());

  auto chunk = stream.nextChunk();
  auto encoding = NimbleChunk::parseEncodingFromChunk(chunk.chunkData());
  auto layout = EncodingLayoutCapture::capture(encoding->encodingData());

  ASSERT_EQ(layout.encodingType(), EncodingType::Dictionary);

  EXPECT_EQ(encoding->numValues(), c0->childAt(0)->size());

  compareEncodingTree(*encoding, layout);
  EXPECT_EQ(encoding->childrenCount(), layout.childrenCount());
  EXPECT_EQ(encoding->childAt(0)->numValues(), 5); // unique values
  EXPECT_EQ(encoding->childAt(1)->numValues(), encoding->numValues());
}

TEST_F(NimbleReaderTest, rootRLEEncoding) {
  using namespace facebook::nimble;

  auto c0 = makeRowVector({
      makeFlatVector<int16_t>({1, 1, 1, 1, 2, 2, 2, 3, 3, 3}),
  });
  auto pool = leafPool();

  VeloxWriterOptions writerOptions;
  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::RLE, 1.0},
  };
  ManualEncodingSelectionPolicyFactory encodingFactory(readFactors);
  writerOptions.encodingSelectionPolicyFactory = [&](DataType dataType) {
    return encodingFactory.createPolicy(dataType);
  };

  auto file =
      facebook::nimble::test::createNimbleFile(*pool, c0, writerOptions);
  auto readFile = std::make_unique<InMemoryReadFile>(file);
  TabletReader tablet(*pool, std::move(readFile));
  auto streams = tablet.load(
      tablet.getStripeIdentifier(0), std::vector{static_cast<uint32_t>(1)});

  ASSERT_EQ(streams.size(), 1);
  ASSERT_NE(streams[0], nullptr);
  NimbleChunkedStream stream(*pool, streams[0]->getStream());

  ASSERT_TRUE(stream.hasNext());

  auto chunk = stream.nextChunk();
  auto encoding = NimbleChunk::parseEncodingFromChunk(chunk.chunkData());
  auto layout = EncodingLayoutCapture::capture(encoding->encodingData());

  ASSERT_EQ(layout.encodingType(), EncodingType::RLE);

  EXPECT_EQ(encoding->numValues(), c0->childAt(0)->size());

  compareEncodingTree(*encoding, layout);
  EXPECT_EQ(encoding->childAt(0)->numValues(), 3);
  EXPECT_EQ(encoding->childAt(1)->numValues(), 3);
}

TEST_F(NimbleReaderTest, decodePipeline) {
  using namespace facebook::nimble;

  auto c0 = makeRowVector({
      makeFlatVector<int16_t>({1, 1, 1, 1, 2, 2, 2, 2, 1, 1, 1, 1, 3, 3, 3, 3}),
  });
  auto pool = leafPool();

  VeloxWriterOptions writerOptions;
  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::RLE, 1.0},
  };
  ManualEncodingSelectionPolicyFactory encodingFactory(readFactors);
  writerOptions.encodingSelectionPolicyFactory = [&](DataType dataType) {
    return encodingFactory.createPolicy(dataType);
  };

  auto file =
      facebook::nimble::test::createNimbleFile(*pool, c0, writerOptions);
  auto readFile = std::make_unique<InMemoryReadFile>(file);
  TabletReader tablet(*pool, std::move(readFile));
  auto streams = tablet.load(
      tablet.getStripeIdentifier(0), std::vector{static_cast<uint32_t>(1)});

  ASSERT_EQ(streams.size(), 1);
  ASSERT_NE(streams[0], nullptr);
  NimbleChunkedStream stream(*pool, streams[0]->getStream());

  ASSERT_TRUE(stream.hasNext());

  auto chunk = stream.nextChunk();
  auto encoding = NimbleChunk::parseEncodingFromChunk(chunk.chunkData());
  auto layout = EncodingLayoutCapture::capture(encoding->encodingData());

  ASSERT_EQ(layout.encodingType(), EncodingType::RLE);
  ASSERT_EQ(encoding->childrenCount(), 2);

  NimbleChunkDecodePipeline pipeline(std::move(encoding));

  // rle(trivial(lengths), trivial(values))
  ASSERT_EQ(pipeline.size(), 3);
  auto encodingNode = pipeline.next();
  ENCODING_TYPE_EXPECT_EQ(encodingNode->encodingType(), EncodingType::Trivial);
  encodingNode = pipeline.next();
  ENCODING_TYPE_EXPECT_EQ(encodingNode->encodingType(), EncodingType::Trivial);
  // This encoding depends on the previous and therefore is not ready yet
  encodingNode = pipeline.next();
  EXPECT_EQ(encodingNode, nullptr);
  ENCODING_TYPE_EXPECT_EQ(pipeline[2].encodingType(), EncodingType::RLE);
  EXPECT_TRUE(pipeline[0].isDecoded());
  EXPECT_TRUE(pipeline[1].isDecoded());
  EXPECT_EQ(pipeline.finished(), false);
}

TEST_F(NimbleReaderTest, decodeTrivialSingleLevel) {
  using namespace facebook::nimble;
  auto pool = leafPool();

  auto c0 =
      makeFlatVector<int32_t>({1, 1, 1, 1, 2, 2, 2, 2, 1, 1, 1, 1, 3, 3, 3, 3});
  auto input = makeRowVector({c0});

  // Configure writer options to enforce trivial encoding on column c0
  VeloxWriterOptions writerOptions;
  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Trivial, 1.0},
  };
  ManualEncodingSelectionPolicyFactory encodingFactory(readFactors);
  writerOptions.encodingSelectionPolicyFactory = [&](DataType dataType) {
    return encodingFactory.createPolicy(dataType);
  };

  auto file =
      facebook::nimble::test::createNimbleFile(*pool, input, writerOptions);
  auto readFile = std::make_unique<InMemoryReadFile>(file);
  TabletReader tablet(*pool, std::move(readFile));
  auto streams = tablet.load(
      tablet.getStripeIdentifier(0), std::vector{static_cast<uint32_t>(1)});

  ASSERT_EQ(streams.size(), 1);
  ASSERT_NE(streams[0], nullptr);
  auto stream =
      std::make_unique<NimbleChunkedStream>(*pool, streams[0]->getStream());
  ASSERT_TRUE(stream->hasNext());

  std::vector<std::unique_ptr<NimbleChunkedStream>> streamVec;
  streamVec.emplace_back(std::move(stream));

  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());
  auto requestedType = input->type();
  auto fileType = dwio::common::TypeWithId::create(requestedType);
  std::shared_ptr<dwio::common::TypeWithId> fileTypeShared =
      std::move(fileType);

  NimbleStripe stripe(
      std::move(streamVec),
      fileTypeShared,
      c0->size()); // assume we know this size information from somewhere

  dwio::common::ColumnReaderStatistics stats;
  auto formatParams =
      std::make_unique<NimbleFormatParams>(*pool, stats, stripe);
  // std::vector<std::unique_ptr<common::Subfield::PathElement>> empty;
  // DefinesMap defines;
  auto reader = nimble::NimbleFormatReader::build(
      requestedType,
      fileTypeShared,
      *formatParams,
      *scanSpec,
      nullptr,
      // empty,
      // defines,
      true);

  io::IoStatistics ioStats;
  FileInfo fileInfo;
  auto arena = std::make_unique<GpuArena>(100000000, getAllocator(getDevice()));
  // auto deviceArena = std::make_unique<GpuArena>(
  //     100000000, getDeviceAllocator(getDevice()), 400000000);
  OperatorStateMap operandStateMap;
  InstructionStatus instructionStatus;
  auto waveStream = std::make_unique<WaveStream>(
      std::move(arena),
      deviceArena(),
      reinterpret_cast<nimble::SelectiveStructColumnReader*>(reader.get())
          ->getOperands(),
      &operandStateMap,
      instructionStatus,
      0);

  auto readStream = std::make_unique<ReadStream>(
      reinterpret_cast<StructColumnReader*>(reader.get()),
      *waveStream,
      &ioStats,
      fileInfo);
  RowSet rows(0, input->size());
  ReadStream::launch(std::move(readStream), 0, rows);
}
} // namespace facebook::velox::wave
