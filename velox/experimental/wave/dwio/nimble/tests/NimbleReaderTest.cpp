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
#include "velox/type/Filter.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::wave {
using namespace facebook::nimble;

namespace {
using namespace facebook::velox;

struct FilterSpec {
  std::string name;
  std::shared_ptr<common::Filter> filter;
  bool keepValues;
};

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

  // Helper function to write multiple vectors to nimble files and get back all
  // streamLoaders. A chunk vector group contains several chunked vectors
  // (mapping to chunked streams in Nimble) that share the same chunk
  // boundaries.
  std::vector<std::unique_ptr<StreamLoader>> writeToNimbleAndGetStreamLoaders(
      const std::vector<std::vector<VectorPtr>>& chunkVectorGroups,
      const std::vector<std::pair<EncodingType, float>>& readFactors,
      const CompressionOptions& compressionOptions) {
    using namespace facebook::nimble;
    auto pool = leafPool();

    // Configure writer options with the specified read factors and compression
    // options
    VeloxWriterOptions writerOptions;
    ManualEncodingSelectionPolicyFactory encodingFactory(
        readFactors, compressionOptions);
    writerOptions.encodingSelectionPolicyFactory = [&](DataType dataType) {
      return encodingFactory.createPolicy(dataType);
    };
    writerOptions.enableChunking = true;
    writerOptions.minStreamChunkRawSize = 0;
    writerOptions.flushPolicyFactory = [] {
      return std::make_unique<LambdaFlushPolicy>(
          [](const StripeProgress&) { return FlushDecision::Chunk; });
    };

    std::vector<std::unique_ptr<StreamLoader>> allStreamLoaders;

    for (const auto& chunkVectors : chunkVectorGroups) {
      if (chunkVectors.empty()) {
        continue;
      }

      auto file = facebook::nimble::test::createNimbleFile(
          *pool, chunkVectors, writerOptions, false);

      auto numChildren = chunkVectors[0]->as<RowVector>()->childrenSize();

      auto readFile = std::make_unique<InMemoryReadFile>(file);
      TabletReader tablet(*pool, std::move(readFile));
      auto stripeIdentifier = tablet.getStripeIdentifier(0);
      VELOX_CHECK_EQ(numChildren + 1, tablet.streamCount(stripeIdentifier));

      std::vector<uint32_t> streamIds;
      streamIds.resize(numChildren);
      std::iota(streamIds.begin(), streamIds.end(), 1);
      auto streamLoaders =
          tablet.load(stripeIdentifier, std::span<const uint32_t>(streamIds));

      for (auto& loader : streamLoaders) {
        allStreamLoaders.push_back(std::move(loader));
      }
    }

    return allStreamLoaders;
  }

  // Helper function to create input by combining chunk vector groups
  RowVectorPtr createInputFromChunkVectorGroups(
      const std::vector<std::vector<VectorPtr>>& chunkVectorGroups) {
    std::vector<RowVectorPtr> groupVectors;
    for (const auto& chunkVectors : chunkVectorGroups) {
      if (chunkVectors.empty()) {
        continue;
      }

      auto groupVector = std::dynamic_pointer_cast<RowVector>(chunkVectors[0]);
      for (int i = 1; i < chunkVectors.size(); i++) {
        groupVector->append(chunkVectors[i].get());
      }
      groupVectors.push_back(groupVector);
    }

    if (groupVectors.empty()) {
      return nullptr; // No valid groups
    }

    if (groupVectors.size() == 1) {
      // Only one group, use it directly
      return groupVectors[0];
    } else {
      // Multiple groups: combine them by adding columns
      std::vector<std::string> allNames;
      std::vector<TypePtr> allTypes;
      std::vector<VectorPtr> allChildren;

      // Collect all columns from all groups
      int32_t childId = 0;
      for (const auto& groupVector : groupVectors) {
        const auto& rowType = groupVector->type()->asRow();
        for (int i = 0; i < groupVector->childrenSize(); i++, childId++) {
          allNames.push_back("c" + std::to_string(childId));
          allTypes.push_back(rowType.childAt(i));
          allChildren.push_back(groupVector->childAt(i));
        }
      }

      auto combinedType = ROW(std::move(allNames), std::move(allTypes));
      return std::make_shared<RowVector>(
          groupVectors[0]->pool(),
          combinedType,
          BufferPtr(nullptr),
          groupVectors[0]->size(),
          std::move(allChildren));
    }
  }

  // Helper function to decode a vector with specified read factors and
  // compression options
  void decodeVectorAndCheck(
      const std::vector<std::vector<VectorPtr>>& chunkVectorGroups,
      const std::vector<std::pair<EncodingType, float>>& readFactors,
      const CompressionOptions& compressionOptions =
          {.compressionAcceptRatio = 0.0},
      const std::vector<FilterSpec>& filters = {},
      RowVectorPtr expectedOutput = nullptr) {
    using namespace facebook::nimble;
    auto pool = leafPool();

    auto streamLoaders = writeToNimbleAndGetStreamLoaders(
        chunkVectorGroups, readFactors, compressionOptions);

    auto input = createInputFromChunkVectorGroups(chunkVectorGroups);
    if (input == nullptr) {
      return; // No valid groups
    }

    std::vector<int32_t> nonNullOutputChildrenIds;
    std::vector<std::unique_ptr<NimbleChunkedStream>> chunkedStreams;
    for (int i = 0; i < streamLoaders.size(); i++) {
      auto& streamLoader = streamLoaders[i];
      if (streamLoader == nullptr) { // this is a null stream
        continue;
      }
      nonNullOutputChildrenIds.push_back(i);
      auto chunkedStream = std::make_unique<NimbleChunkedStream>(
          *pool, streamLoader->getStream());
      EXPECT_TRUE(chunkedStream->hasNext());
      chunkedStreams.emplace_back(std::move(chunkedStream));
    }

    if (chunkedStreams.empty()) { // only contain null streams
      return;
    }

    // Create non-null type and scan spec that only include non-null streams
    std::vector<std::string> nonNullNames;
    std::vector<TypePtr> nonNullTypes;
    const auto& inputRowType = input->type()->asRow();
    for (int32_t childId : nonNullOutputChildrenIds) {
      nonNullNames.push_back(inputRowType.nameOf(childId));
      nonNullTypes.push_back(inputRowType.childAt(childId));
    }
    auto nonNullRowType = ROW(std::move(nonNullNames), std::move(nonNullTypes));

    // Set up scan spec and format parameters
    auto scanSpec = std::make_shared<common::ScanSpec>("root");
    scanSpec->addAllChildFields(*nonNullRowType);

    // Apply filters to specific children if provided
    for (const auto& [childName, filter, keepValues] : filters) {
      auto* childSpec = scanSpec->childByName(childName);
      if (childSpec != nullptr) {
        childSpec->setFilter(filter);
        childSpec->setProjectOut(keepValues);
      }
    }
    auto requestedType = nonNullRowType;
    auto fileType = dwio::common::TypeWithId::create(requestedType);
    std::shared_ptr<dwio::common::TypeWithId> fileTypeShared =
        std::move(fileType);

    NimbleStripe stripe(
        std::move(chunkedStreams), fileTypeShared, input->size());

    dwio::common::ColumnReaderStatistics stats;
    auto formatParams =
        std::make_unique<NimbleFormatParams>(*pool, stats, stripe);
    auto reader = nimble::NimbleFormatReader::build(
        requestedType, fileTypeShared, *formatParams, *scanSpec, nullptr, true);

    // Set up wave stream and read stream
    io::IoStatistics ioStats;
    FileInfo fileInfo;
    auto arena =
        std::make_unique<GpuArena>(100000000, getAllocator(getDevice()));
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

    // Launch read stream and get output
    std::vector<int32_t> rowIds;
    rowIds.resize(input->size());
    std::iota(rowIds.begin(), rowIds.end(), 0);
    RowSet rows(rowIds.data(), rowIds.size());
    ReadStream::launch(std::move(readStream), 0, rows);

    std::vector<OperandId> operandIds;
    operandIds.resize(nonNullOutputChildrenIds.size());
    std::iota(operandIds.begin(), operandIds.end(), 1);
    folly::Range<const int32_t*> operandIdRange(
        operandIds.data(), operandIds.size());
    std::vector<VectorPtr> nonNullOutputChildren;
    nonNullOutputChildren.resize(nonNullOutputChildrenIds.size());
    auto outputNumValues = waveStream->getOutput(
        0, *pool, operandIdRange, nonNullOutputChildren.data());

    auto correct =
        expectedOutput == nullptr ? input.get() : expectedOutput.get();
    EXPECT_EQ(correct->size(), outputNumValues);

    auto output = std::make_shared<RowVector>(
        input->pool(),
        input->type(),
        BufferPtr(nullptr),
        input->size(),
        std::vector<VectorPtr>(input->childrenSize()));

    for (int i = 0; i < nonNullOutputChildrenIds.size(); i++) {
      output->childAt(nonNullOutputChildrenIds[i]) =
          std::move(nonNullOutputChildren[i]);
    }

    for (int i = 0; i < correct->childrenSize(); i++) {
      SCOPED_TRACE(fmt::format("i: {}", i));
      if (output->childAt(i) == nullptr) { // populate the null children
        output->childAt(i) = BaseVector::createNullConstant(
            correct->childAt(i)->type(), correct->size(), pool);
      }
      test::assertEqualVectors(correct->childAt(i), output->childAt(i));
    }
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

TEST_F(NimbleReaderTest, decodeTrivialSingleLevelInteger) {
  using namespace facebook::nimble;

  auto c0 =
      makeFlatVector<int32_t>({1, 1, 1, 1, 2, 2, 2, 2, 1, 1, 1, 1, 3, 3, 3, 3});
  auto input = makeRowVector({c0});

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Trivial, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  decodeVectorAndCheck({{input}}, readFactors, compressionOptions);
}

TEST_F(NimbleReaderTest, decodeTrivialSingleLevelFloat) {
  using namespace facebook::nimble;

  auto c0 = makeFlatVector<double>(1893, [](auto row) { return row * 1.1; });
  auto input = makeRowVector({c0});

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Trivial, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  decodeVectorAndCheck({{input}}, readFactors, compressionOptions);
}

TEST_F(NimbleReaderTest, TrivialWithCompressionShouldFail) {
  using namespace facebook::nimble;

  auto c0 = makeFlatVector<double>(17, [](auto row) { return row * 1.1; });
  auto input = makeRowVector({c0});

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Trivial, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 1.0};
  try {
    decodeVectorAndCheck({{input}}, readFactors, compressionOptions);
    FAIL() << "Expected exception for decoding trivial with compression";
  } catch (const VeloxRuntimeError& e) {
    EXPECT_NE(
        e.message().find("Trivial encoding does not support compression yet"),
        std::string::npos);
  } catch (...) {
    FAIL() << "unexpected exception thrown";
  }
}

TEST_F(NimbleReaderTest, loadMultiChunks) {
  using namespace facebook::nimble;

  auto pool = leafPool();

  auto chunk0 = makeRowVector({
      makeFlatVector<double>(17, folly::identity),
  });
  auto chunk1 = makeRowVector({
      makeFlatVector<double>(26, folly::identity),
  });
  auto chunk2 = makeRowVector({
      makeFlatVector<double>(34, folly::identity),
  });

  // Configure writer options to enforce trivial encoding on column c0
  VeloxWriterOptions options;
  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Trivial, 1.0},
  };
  options.enableChunking = true;
  options.minStreamChunkRawSize = 0;
  options.flushPolicyFactory = [] {
    return std::make_unique<LambdaFlushPolicy>(
        [](const StripeProgress&) { return FlushDecision::Chunk; });
  };
  ManualEncodingSelectionPolicyFactory encodingFactory(readFactors);
  options.encodingSelectionPolicyFactory = [&](DataType dataType) {
    return encodingFactory.createPolicy(dataType);
  };

  auto file = facebook::nimble::test::createNimbleFile(
      *pool,
      {chunk0, chunk1, chunk2},
      options,
      false /* make sure chunks are not flushed into individual stripes*/);
  auto readFile = std::make_unique<InMemoryReadFile>(file);
  TabletReader tablet(*pool, std::move(readFile));
  auto streams = tablet.load(
      tablet.getStripeIdentifier(0), std::vector{static_cast<uint32_t>(1)});

  ASSERT_EQ(streams.size(), 1);
  ASSERT_NE(streams[0], nullptr);
  NimbleChunkedStream stream(*pool, streams[0]->getStream());

  std::vector<RowVectorPtr> chunks{chunk0, chunk1, chunk2};
  for (int i = 0; i < 3; i++) {
    SCOPED_TRACE(fmt::format("i: {}", i));
    ASSERT_TRUE(stream.hasNext());
    auto chunk = stream.nextChunk();
    auto encoding = NimbleChunk::parseEncodingFromChunk(chunk.chunkData());
    auto layout = EncodingLayoutCapture::capture(encoding->encodingData());

    ASSERT_EQ(layout.encodingType(), EncodingType::Trivial);
    EXPECT_EQ(encoding->numValues(), chunks[i]->childAt(0)->size());
    compareEncodingTree(*encoding, layout);
  }
  ASSERT_FALSE(stream.hasNext());
}

TEST_F(NimbleReaderTest, decodeTrivialMultiChunksFloat) {
  using namespace facebook::nimble;

  auto chunk1 = makeRowVector(
      {makeFlatVector<double>(1893, [](auto row) { return row + 1.1; })});
  auto chunk2 = makeRowVector(
      {makeFlatVector<double>(267, [](auto row) { return row + 2.2; })});
  auto chunk3 = makeRowVector(
      {makeFlatVector<double>(449, [](auto row) { return row + 3.3; })});

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Trivial, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  decodeVectorAndCheck(
      {{chunk1, chunk2, chunk3}}, readFactors, compressionOptions);
}

TEST_F(NimbleReaderTest, decodeTrivialMultiChunksMultiStreams) {
  using namespace facebook::nimble;

  auto chunk1 = makeRowVector(
      {makeFlatVector<double>(513, [](auto row) { return row + 1.1; }),
       makeFlatVector<int32_t>(513, folly::identity),
       makeFlatVector<int64_t>(513, folly::identity)});
  auto chunk2 = makeRowVector(
      {makeFlatVector<double>(267, [](auto row) { return row + 2.2; }),
       makeFlatVector<int32_t>(267, folly::identity),
       makeFlatVector<int64_t>(267, folly::identity)});
  auto chunk3 = makeRowVector(
      {makeFlatVector<double>(449, [](auto row) { return row + 3.3; }),
       makeFlatVector<int32_t>(449, folly::identity),
       makeFlatVector<int64_t>(449, folly::identity)});

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Trivial, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  decodeVectorAndCheck(
      {{chunk1, chunk2, chunk3}}, readFactors, compressionOptions);
}

TEST_F(NimbleReaderTest, decodeNullableFloat) {
  using namespace facebook::nimble;

  auto c0 = makeFlatVector<double>(
      1025, [](auto row) { return row + 0.1; }, nullEvery(3));
  auto input = makeRowVector({c0});

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Nullable, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  decodeVectorAndCheck({{input}}, readFactors, compressionOptions);
}

TEST_F(NimbleReaderTest, decodeAllNulls) {
  using namespace facebook::nimble;

  auto c0 = makeFlatVector<double>(
      1893, [](auto row) { return row * 1.1; }, nullEvery(1));
  auto c1 = makeFlatVector<double>(
      1893, [](auto row) { return row * 1.1; }, nullEvery(2));
  auto input = makeRowVector({c0, c1});

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Nullable, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  decodeVectorAndCheck({{input}}, readFactors, compressionOptions);
}

TEST_F(NimbleReaderTest, decodeNullableMultiChunks) {
  using namespace facebook::nimble;

  auto c0 = makeRowVector({makeFlatVector<double>(
      1893, [](auto row) { return row + 1.1; }, nullEvery(3))});
  auto c1 = makeRowVector({makeFlatVector<double>(
      2156, [](auto row) { return row + 1.2; })}); // nullEvery(13)
  auto c2 = makeRowVector({makeFlatVector<double>(
      4790, [](auto row) { return row + 1.3; }, nullEvery(19))});

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Nullable, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  decodeVectorAndCheck({{c0, c1, c2}}, readFactors, compressionOptions);
}

TEST_F(NimbleReaderTest, decodeNullableMultiChunksMultiStreams) {
  using namespace facebook::nimble;

  auto c0 = makeRowVector(
      {makeFlatVector<double>(1893, [](auto row) { return row + 1.1; }),
       makeFlatVector<int32_t>(1893, folly::identity, nullEvery(5))});
  auto c1 = makeRowVector(
      {makeFlatVector<double>(
           2156, [](auto row) { return row + 1.2; }, nullEvery(13)),
       makeFlatVector<int32_t>(2156, folly::identity)});
  auto c2 = makeRowVector(
      {makeFlatVector<double>(4790, [](auto row) { return row + 1.3; }),
       makeFlatVector<int32_t>(4790, folly::identity)});

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Nullable, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  decodeVectorAndCheck({{c0, c1, c2}}, readFactors, compressionOptions);
}

TEST_F(NimbleReaderTest, NullableWithOneFilterKeepValues) {
  using namespace facebook::nimble;

  auto input = makeRowVector({makeFlatVector<int32_t>(
      1893, [](auto row) { return row; }, nullEvery(3))});

  auto filter = std::make_shared<common::BigintRange>(1022, 1025, false);
  std::vector<FilterSpec> filters = {{"c0", filter, true}};

  auto expectedOutput =
      makeRowVector({makeFlatVector<int32_t>({1022, 1024, 1025})});

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Nullable, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  decodeVectorAndCheck(
      {{input}}, readFactors, compressionOptions, filters, expectedOutput);
}

TEST_F(NimbleReaderTest, NullableWithMoreFiltersKeepValues) {
  using namespace facebook::nimble;

  auto input = makeRowVector(
      {makeFlatVector<int32_t>(
           1893, [](auto row) { return row; }, nullEvery(3)),
       makeFlatVector<int32_t>(
           1893, [](auto row) { return row; }, nullEvery(3))});

  auto c0Filter = std::make_shared<common::BigintRange>(1022, 1025, false);
  auto c1Filter = std::make_shared<common::BigintRange>(1022, 1024, false);
  std::vector<FilterSpec> filters = {
      {"c0", c0Filter, true}, {"c1", c1Filter, true}};

  auto expectedOutput = makeRowVector(
      {makeFlatVector<int32_t>({1022, 1024}),
       makeFlatVector<int32_t>({1022, 1024})});

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Nullable, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  decodeVectorAndCheck(
      {{input}}, readFactors, compressionOptions, filters, expectedOutput);
}

TEST_F(NimbleReaderTest, NullableWithOneFilterOneNonFilterKeepValues) {
  using namespace facebook::nimble;

  auto input = makeRowVector(
      {makeFlatVector<int32_t>(
           1893, [](auto row) { return row; }, nullEvery(3)),
       makeFlatVector<int32_t>(
           1893, [](auto row) { return row; }, nullEvery(3))});

  auto c0Filter =
      std::make_shared<common::BigintRange>(1022, 1025, false); // 1000,1892
  std::vector<FilterSpec> filters = {{"c0", c0Filter, true}};

  auto expectedOutput = makeRowVector(
      {makeFlatVector<int32_t>({1022, 1024, 1025}),
       makeFlatVector<int32_t>({1022, 1024, 1025})});

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Nullable, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  decodeVectorAndCheck(
      {{input}}, readFactors, compressionOptions, filters, expectedOutput);
}

TEST_F(NimbleReaderTest, NullableWithMoreFiltersMoreNonFiltersKeepValues) {
  using namespace facebook::nimble;

  auto input = makeRowVector(
      {makeFlatVector<int32_t>(
           1893, [](auto row) { return row; }, nullEvery(3)),
       makeFlatVector<int32_t>(
           1893, [](auto row) { return row; }, nullEvery(3)),
       makeFlatVector<int32_t>(
           1893, [](auto row) { return row; }, nullEvery(2)),
       makeFlatVector<int32_t>(
           1893, [](auto row) { return row; }, nullEvery(2))});

  auto c0Filter =
      std::make_shared<common::BigintRange>(1022, 1025, false); // 1000,1892
  auto c1Filter =
      std::make_shared<common::BigintRange>(1022, 1024, false); // 1000,1892
  std::vector<FilterSpec> filters = {
      {"c0", c0Filter, true}, {"c1", c1Filter, true}};

  auto expectedOutput = makeRowVector(
      {makeFlatVector<int32_t>({1022, 1024}),
       makeFlatVector<int32_t>({1022, 1024}),
       makeNullableFlatVector<int32_t>({std::nullopt, std::nullopt}),
       makeNullableFlatVector<int32_t>({std::nullopt, std::nullopt})});

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Nullable, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  decodeVectorAndCheck(
      {{input}}, readFactors, compressionOptions, filters, expectedOutput);
}

TEST_F(NimbleReaderTest, NullableFilterNoResults) {
  using namespace facebook::nimble;

  auto input = makeRowVector(
      {makeFlatVector<int32_t>(
           1893, [](auto row) { return row; }, nullEvery(3)),
       makeFlatVector<int32_t>(
           1893, [](auto row) { return row; }, nullEvery(3)),
       makeFlatVector<int32_t>(
           1893, [](auto row) { return row; }, nullEvery(2)),
       makeFlatVector<int32_t>(
           1893, [](auto row) { return row; }, nullEvery(2))});

  auto c0Filter = std::make_shared<common::BigintRange>(1893, 1944, false);
  auto c1Filter = std::make_shared<common::BigintRange>(1022, 1024, false);
  std::vector<FilterSpec> filters = {
      {"c0", c0Filter, true}, {"c1", c1Filter, true}};
  auto expectedOutput = makeRowVector(
      {makeFlatVector<int32_t>({}),
       makeFlatVector<int32_t>({}),
       makeFlatVector<int32_t>({}),
       makeFlatVector<int32_t>({})});

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Nullable, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  decodeVectorAndCheck(
      {{input}}, readFactors, compressionOptions, filters, expectedOutput);
}

TEST_F(NimbleReaderTest, TrivialWithOneFilterKeepValues) {
  using namespace facebook::nimble;

  auto input = makeRowVector(
      {makeFlatVector<int32_t>(2000, [](auto row) { return row; })});

  auto c0Filter = std::make_shared<common::BigintRange>(1022, 1025, false);
  std::vector<FilterSpec> filters = {{"c0", c0Filter, true}};

  auto expectedOutput =
      makeRowVector({makeFlatVector<int32_t>({1022, 1023, 1024, 1025})});

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Trivial, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  decodeVectorAndCheck(
      {{input}}, readFactors, compressionOptions, filters, expectedOutput);
}

TEST_F(NimbleReaderTest, TrivialWithMoreFiltersMoreNonFiltersKeepValues) {
  using namespace facebook::nimble;

  auto input = makeRowVector(
      {makeFlatVector<int32_t>(1893, [](auto row) { return row; }),
       makeFlatVector<int64_t>(1893, [](auto row) { return row; }),
       makeFlatVector<int32_t>(1893, [](auto row) { return row; }),
       makeFlatVector<int64_t>(1893, [](auto row) { return row; })});

  auto c0Filter = std::make_shared<common::BigintRange>(1022, 1025, false);
  auto c1Filter = std::make_shared<common::BigintRange>(1022, 1024, false);
  std::vector<FilterSpec> filters = {
      {"c0", c0Filter, true}, {"c1", c1Filter, true}};

  auto expectedOutput = makeRowVector(
      {makeFlatVector<int32_t>({1022, 1023, 1024}),
       makeFlatVector<int64_t>({1022, 1023, 1024}),
       makeFlatVector<int32_t>({1022, 1023, 1024}),
       makeFlatVector<int64_t>({1022, 1023, 1024})});

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Trivial, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  decodeVectorAndCheck(
      {{input}}, readFactors, compressionOptions, filters, expectedOutput);
}

TEST_F(NimbleReaderTest, TrivialChunksOneFilterKeepValues) {
  using namespace facebook::nimble;

  auto chunk0 = makeRowVector(
      {makeFlatVector<int32_t>(1200, [](auto row) { return row; })});
  auto chunk1 = makeRowVector({makeFlatVector<int32_t>(
      1800, [&](auto row) { return row + chunk0->size(); })});

  int64_t lower = 1020, upper = 1202;
  auto c0Filter = std::make_shared<common::BigintRange>(lower, upper, false);
  std::vector<FilterSpec> filters = {{"c0", c0Filter, true}};

  auto expectedOutput = makeRowVector({makeFlatVector<int32_t>(
      upper - lower + 1, [&](auto row) { return row + lower; })});

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Trivial, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  decodeVectorAndCheck(
      {{chunk0, chunk1}},
      readFactors,
      compressionOptions,
      filters,
      expectedOutput);
}

TEST_F(NimbleReaderTest, NullableChunksOneFilterKeepValues) {
  using namespace facebook::nimble;

  auto chunk0 = makeRowVector({makeFlatVector<int32_t>(
      1200, [](auto row) { return row; }, nullEvery(2))});
  auto chunk1 = makeRowVector({makeFlatVector<int32_t>(
      1800,
      [&](auto row) { return row + chunk0->size(); },
      nullEvery(2))}); // 1800

  int64_t lower = 1198, upper = 1202;
  auto c0Filter = std::make_shared<common::BigintRange>(lower, upper, false);
  std::vector<FilterSpec> filters = {{"c0", c0Filter, true}};

  auto expectedOutput = makeRowVector({makeFlatVector<int32_t>({1199, 1201})});

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Nullable, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  decodeVectorAndCheck(
      {{chunk0, chunk1}},
      readFactors,
      compressionOptions,
      filters,
      expectedOutput);
}

TEST_F(NimbleReaderTest, NullableChunksFiltersKeepValues) {
  using namespace facebook::nimble;

  auto chunk0 = makeRowVector(
      {makeFlatVector<int32_t>(
           1200, [](auto row) { return row; }, nullEvery(2)),
       makeFlatVector<int32_t>(
           1200, [](auto row) { return row; }, nullEvery(2))});
  auto chunk1 = makeRowVector(
      {makeFlatVector<int32_t>(
           1800, [&](auto row) { return row + chunk0->size(); }, nullEvery(2)),
       makeFlatVector<int32_t>(
           1800,
           [&](auto row) { return row + chunk0->size(); },
           nullEvery(2))});

  int64_t lower = 1198, upper = 1202;
  auto c0Filter = std::make_shared<common::BigintRange>(lower, upper, false);
  auto c1filter =
      std::make_shared<common::BigintRange>(lower - 2, upper + 2, false);
  std::vector<FilterSpec> filters = {
      {"c0", c0Filter, true}, {"c1", c0Filter, true}};

  auto expectedOutput = makeRowVector(
      {makeFlatVector<int32_t>({1199, 1201}),
       makeFlatVector<int32_t>({1199, 1201})});

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Nullable, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  decodeVectorAndCheck(
      {{chunk0, chunk1}},
      readFactors,
      compressionOptions,
      filters,
      expectedOutput);
}

TEST_F(NimbleReaderTest, NullableChunksFiltersNonFiltersKeepValues) {
  using namespace facebook::nimble;

  auto vector = [&](int32_t numValues, int32_t offset = 0) {
    return makeFlatVector<int32_t>(
        numValues, [&](auto row) { return row + offset; }, nullEvery(2));
  };
  auto chunk0 = makeRowVector({
      vector(1200),
      vector(1200),
      vector(1200),
      vector(1200),
  });
  auto chunk1 = makeRowVector({
      vector(1800, chunk0->size()),
      vector(1800, chunk0->size()),
      vector(1800, chunk0->size()),
      vector(1800, chunk0->size()),
  });

  int64_t lower = 1198, upper = 1202;
  auto c0Filter = std::make_shared<common::BigintRange>(lower, upper, false);
  auto c1Filter =
      std::make_shared<common::BigintRange>(lower - 2, upper + 2, false);
  std::vector<FilterSpec> filters = {
      {"c0", c0Filter, true}, {"c1", c1Filter, true}};

  auto expectedOutput = makeRowVector({
      makeFlatVector<int32_t>({1199, 1201}),
      makeFlatVector<int32_t>({1199, 1201}),
      makeFlatVector<int32_t>({1199, 1201}),
      makeFlatVector<int32_t>({1199, 1201}),
  });

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Nullable, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  decodeVectorAndCheck(
      {{chunk0, chunk1}},
      readFactors,
      compressionOptions,
      filters,
      expectedOutput);
}

TEST_F(NimbleReaderTest, TrivialChunksFiltersKeepValues) {
  using namespace facebook::nimble;

  auto chunk0 = makeRowVector(
      {makeFlatVector<int32_t>(1200, [](auto row) { return row; }),
       makeFlatVector<int32_t>(1200, [](auto row) { return row; })});
  auto chunk1 = makeRowVector(
      {makeFlatVector<int32_t>(
           1800, [&](auto row) { return row + chunk0->size(); }),
       makeFlatVector<int32_t>(
           1800, [&](auto row) { return row + chunk0->size(); })});

  int64_t lower = 1020, upper = 1202;
  auto c0Filter = std::make_shared<common::BigintRange>(lower, upper, false);
  auto c1filter =
      std::make_shared<common::BigintRange>(lower - 2, upper + 2, false);
  std::vector<FilterSpec> filters = {
      {"c0", c0Filter, true}, {"c1", c0Filter, true}};

  auto numResults = upper - lower + 1;
  auto expectedOutput = makeRowVector(
      {makeFlatVector<int32_t>(
           numResults, [&](auto row) { return row + lower; }),
       makeFlatVector<int32_t>(
           numResults, [&](auto row) { return row + lower; })});

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Trivial, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  decodeVectorAndCheck(
      {{chunk0, chunk1}},
      readFactors,
      compressionOptions,
      filters,
      expectedOutput);
}

TEST_F(NimbleReaderTest, TrivialChunksFiltersNonFiltersKeepValues) {
  using namespace facebook::nimble;

  auto vector = [&](int32_t numValues, int32_t offset = 0) {
    return makeFlatVector<int32_t>(
        numValues, [&](auto row) { return row + offset; });
  };
  auto chunk0 = makeRowVector({
      vector(1200),
      vector(1200),
      vector(1200),
      vector(1200),
  });
  auto chunk1 = makeRowVector({
      vector(1800, chunk0->size()),
      vector(1800, chunk0->size()),
      vector(1800, chunk0->size()),
      vector(1800, chunk0->size()),
  });

  int64_t lower = 1020, upper = 1202;
  auto c0Filter = std::make_shared<common::BigintRange>(lower, upper, false);
  auto c1Filter =
      std::make_shared<common::BigintRange>(lower - 2, upper + 2, false);
  std::vector<FilterSpec> filters = {
      {"c0", c0Filter, true}, {"c1", c1Filter, true}};

  auto numResults = upper - lower + 1;
  auto expectedOutput = makeRowVector({
      vector(numResults, lower),
      vector(numResults, lower),
      vector(numResults, lower),
      vector(numResults, lower),
  });

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Trivial, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  decodeVectorAndCheck(
      {{chunk0, chunk1}},
      readFactors,
      compressionOptions,
      filters,
      expectedOutput);
}

TEST_F(NimbleReaderTest, TrivialUnalignedChunks) {
  using namespace facebook::nimble;

  auto vector = [&](int32_t numValues, int32_t offset = 0) {
    return makeFlatVector<int32_t>(
        numValues, [&](auto row) { return row + offset; });
  };
  int32_t offset = 0;

  // Group 1
  auto chunk0 = makeRowVector({
      vector(1200),
      vector(1200),
  });
  offset = chunk0->size();
  auto chunk1 = makeRowVector({
      vector(1800, offset),
      vector(1800, offset),
  });

  // Group 2
  auto chunk2 = makeRowVector({
      vector(500),
      vector(500),
  });
  offset = chunk2->size();
  auto chunk3 = makeRowVector({
      vector(1200, offset),
      vector(1200, offset),
  });
  offset += chunk3->size();
  auto chunk4 = makeRowVector({
      vector(1300, offset),
      vector(1300, offset),
  });

  int64_t lower = 1020, upper = 1202;
  auto c0Filter = std::make_shared<common::BigintRange>(lower, upper, false);
  auto c2Filter =
      std::make_shared<common::BigintRange>(lower - 2, upper + 2, false);
  std::vector<FilterSpec> filters = {
      {"c0", c0Filter, true}, {"c2", c2Filter, true}};

  auto numResults = upper - lower + 1;
  auto expectedOutput = makeRowVector({
      vector(numResults, lower),
      vector(numResults, lower),
      vector(numResults, lower),
      vector(numResults, lower),
  });

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Trivial, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  decodeVectorAndCheck(
      {{chunk0, chunk1}, {chunk2, chunk3, chunk4}},
      readFactors,
      compressionOptions,
      filters,
      expectedOutput);
}

TEST_F(NimbleReaderTest, NullablelUnalignedChunks) {
  using namespace facebook::nimble;

  auto vector = [&](int32_t numValues, int32_t nullFreq, int32_t offset = 0) {
    return makeFlatVector<int32_t>(
        numValues, [&](auto row) { return row + offset; }, nullEvery(nullFreq));
  };
  int32_t offset = 0;

  // Group 1
  auto chunk0 = makeRowVector({
      vector(1200, 3),
      vector(1200, 3),
  });
  offset = chunk0->size();
  auto chunk1 = makeRowVector({
      vector(1800, 3, offset),
      vector(1800, 3, offset),
  });

  // Group 2
  auto chunk2 = makeRowVector({
      vector(600, 3),
      vector(600, 3),
  });
  offset = chunk2->size();
  auto chunk3 = makeRowVector({
      vector(1200, 3, offset),
      vector(1200, 3, offset),
  });
  offset += chunk3->size();
  auto chunk4 = makeRowVector({
      vector(1200, 3, offset),
      vector(1200, 3, offset),
  });

  int64_t lower = 1020, upper = 1202;
  auto c0Filter = std::make_shared<common::BigintRange>(lower, upper, false);
  auto c2Filter =
      std::make_shared<common::BigintRange>(lower - 2, upper + 2, false);
  std::vector<FilterSpec> filters = {
      {"c0", c0Filter, true}, {"c2", c2Filter, true}};

  auto numResults = (upper - lower + 1) / 3 * 2;
  auto expectedVector = [&]() {
    auto inputRow = lower;
    return makeFlatVector<int32_t>(numResults, [&](auto row) {
      if (inputRow % 3 == 0) {
        inputRow += 2;
        return inputRow - 1;
      } else {
        return inputRow++;
      }
    });
  };
  auto expectedOutput = makeRowVector({
      expectedVector(),
      expectedVector(),
      expectedVector(),
      expectedVector(),
  });

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Nullable, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  decodeVectorAndCheck(
      {{chunk0, chunk1}, {chunk2, chunk3, chunk4}},
      readFactors,
      compressionOptions,
      filters,
      expectedOutput);
}
} // namespace facebook::velox::wave
