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

#include "dwio/nimble/encodings/EncodingLayout.h"
#include "dwio/nimble/encodings/EncodingLayoutCapture.h"
#include "dwio/nimble/encodings/EncodingSelectionPolicy.h"
#include "dwio/nimble/tablet/TabletReader.h"
#include "velox/experimental/wave/dwio/nimble/NimbleFileFormat.h"
#include "velox/experimental/wave/dwio/nimble/NimbleFormatData.h"
#include "velox/experimental/wave/dwio/nimble/tests/NimbleReaderTestUtil.h"
#include "velox/type/Filter.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::wave {
using namespace facebook::nimble;

namespace {
using namespace facebook::velox;

class NimbleReaderTest : public ::testing::Test,
                         public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::initializeMemoryManager(velox::memory::MemoryManager::Options{});
  }

  void SetUp() override {
    if (int device; cudaGetDevice(&device) != cudaSuccess) {
      GTEST_SKIP() << "No CUDA detected, skipping all tests";
    }
  }

  memory::MemoryPool* leafPool() {
    return pool_.get();
  }

  // Helper function to decode a vector with specified read factors and
  // compression options
  void test(
      const std::vector<std::vector<VectorPtr>>& chunkVectorGroups,
      const std::vector<std::pair<EncodingType, float>>& readFactors,
      const CompressionOptions& compressionOptions =
          {.compressionAcceptRatio = 0.0},
      const std::vector<FilterSpec>& filters = {}) {
    auto streamLoaders = writeToNimbleAndGetStreamLoaders(
        leafPool(), chunkVectorGroups, readFactors, compressionOptions);

    auto input = createInputFromChunkVectorGroups(chunkVectorGroups);

    TestNimbleReader reader(leafPool(), input, streamLoaders, filters);
    auto testResult = reader.read();

    verifier_.verify(input, filters, testResult);
  }

 private:
  NimbleReaderVerifier verifier_{};
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

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Trivial, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  auto streamLoaders = writeToNimbleAndGetStreamLoaders(
      pool, {{c0}}, readFactors, compressionOptions);

  ASSERT_EQ(streamLoaders.size(), 1);
  ASSERT_NE(streamLoaders[0], nullptr);
  NimbleChunkedStream stream(*pool, streamLoaders[0]->getStream());

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

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Nullable, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  auto streamLoaders = writeToNimbleAndGetStreamLoaders(
      pool, {{c0}}, readFactors, compressionOptions);

  ASSERT_EQ(streamLoaders.size(), 1);
  ASSERT_NE(streamLoaders[0], nullptr);
  NimbleChunkedStream stream(*pool, streamLoaders[0]->getStream());

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

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Dictionary, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  auto streamLoaders = writeToNimbleAndGetStreamLoaders(
      pool, {{c0}}, readFactors, compressionOptions);

  ASSERT_EQ(streamLoaders.size(), 1);
  ASSERT_NE(streamLoaders[0], nullptr);
  NimbleChunkedStream stream(*pool, streamLoaders[0]->getStream());

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

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::RLE, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  auto streamLoaders = writeToNimbleAndGetStreamLoaders(
      pool, {{c0}}, readFactors, compressionOptions);

  ASSERT_EQ(streamLoaders.size(), 1);
  ASSERT_NE(streamLoaders[0], nullptr);
  NimbleChunkedStream stream(*pool, streamLoaders[0]->getStream());

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

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::RLE, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  auto streamLoaders = writeToNimbleAndGetStreamLoaders(
      pool, {{c0}}, readFactors, compressionOptions);

  ASSERT_EQ(streamLoaders.size(), 1);
  ASSERT_NE(streamLoaders[0], nullptr);
  NimbleChunkedStream stream(*pool, streamLoaders[0]->getStream());

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

  test({{input}}, readFactors, compressionOptions);
}

TEST_F(NimbleReaderTest, decodeTrivialSingleLevelFloat) {
  using namespace facebook::nimble;

  auto c0 = makeFlatVector<double>(1893, [](auto row) { return row * 1.1; });
  auto input = makeRowVector({c0});

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Trivial, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  test({{input}}, readFactors, compressionOptions);
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
    test({{input}}, readFactors, compressionOptions);
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

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Trivial, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  auto streamLoaders = writeToNimbleAndGetStreamLoaders(
      pool, {{chunk0, chunk1, chunk2}}, readFactors, compressionOptions);

  ASSERT_EQ(streamLoaders.size(), 1);
  ASSERT_NE(streamLoaders[0], nullptr);
  NimbleChunkedStream stream(*pool, streamLoaders[0]->getStream());

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

  test({{chunk1, chunk2, chunk3}}, readFactors, compressionOptions);
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

  test({{chunk1, chunk2, chunk3}}, readFactors, compressionOptions);
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

  test({{input}}, readFactors, compressionOptions);
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

  test({{input}}, readFactors, compressionOptions);
}

TEST_F(NimbleReaderTest, decodeNullableMultiChunks) {
  using namespace facebook::nimble;

  auto c0 = makeRowVector({makeFlatVector<double>(
      1893, [](auto row) { return row + 1.1; }, nullEvery(3))});
  auto c1 = makeRowVector(
      {makeFlatVector<double>(2156, [](auto row) { return row + 1.2; })});
  auto c2 = makeRowVector({makeFlatVector<double>(
      4790, [](auto row) { return row + 1.3; }, nullEvery(19))});

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Nullable, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  test({{c0, c1, c2}}, readFactors, compressionOptions);
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

  test({{c0, c1, c2}}, readFactors, compressionOptions);
}

TEST_F(NimbleReaderTest, NullableWithOneFilterKeepValues) {
  using namespace facebook::nimble;

  auto input = makeRowVector({makeFlatVector<int32_t>(
      1893, [](auto row) { return row; }, nullEvery(3))});

  auto filter = std::make_shared<common::BigintRange>(1022, 1025, false);
  std::vector<FilterSpec> filters = {{"c0", filter, true}};

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Nullable, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  test({{input}}, readFactors, compressionOptions, filters);
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

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Nullable, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  test({{input}}, readFactors, compressionOptions, filters);
}

TEST_F(NimbleReaderTest, NullableWithOneFilterOneNonFilterKeepValues) {
  using namespace facebook::nimble;

  auto input = makeRowVector(
      {makeFlatVector<int32_t>(
           1893, [](auto row) { return row; }, nullEvery(3)),
       makeFlatVector<int32_t>(
           1893, [](auto row) { return row; }, nullEvery(3))});

  auto c0Filter = std::make_shared<common::BigintRange>(1022, 1025, false);
  std::vector<FilterSpec> filters = {{"c0", c0Filter, true}};

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Nullable, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  test({{input}}, readFactors, compressionOptions, filters);
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

  auto c0Filter = std::make_shared<common::BigintRange>(1022, 1025, false);
  auto c1Filter = std::make_shared<common::BigintRange>(1022, 1024, false);
  std::vector<FilterSpec> filters = {
      {"c0", c0Filter, true}, {"c1", c1Filter, true}};

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Nullable, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  test({{input}}, readFactors, compressionOptions, filters);
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

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Nullable, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  test({{input}}, readFactors, compressionOptions, filters);
}

TEST_F(NimbleReaderTest, TrivialWithOneFilterKeepValues) {
  using namespace facebook::nimble;

  auto input = makeRowVector(
      {makeFlatVector<int32_t>(2000, [](auto row) { return row; })});

  auto c0Filter = std::make_shared<common::BigintRange>(1022, 1025, false);
  std::vector<FilterSpec> filters = {{"c0", c0Filter, true}};

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Trivial, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  test({{input}}, readFactors, compressionOptions, filters);
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

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Trivial, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  test({{input}}, readFactors, compressionOptions, filters);
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

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Trivial, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  test({{chunk0, chunk1}}, readFactors, compressionOptions, filters);
}

TEST_F(NimbleReaderTest, NullableChunksOneFilterKeepValues) {
  using namespace facebook::nimble;

  auto chunk0 = makeRowVector({makeFlatVector<int32_t>(
      1200, [](auto row) { return row; }, nullEvery(2))});
  auto chunk1 = makeRowVector({makeFlatVector<int32_t>(
      1800, [&](auto row) { return row + chunk0->size(); }, nullEvery(2))});

  int64_t lower = 1198, upper = 1202;
  auto c0Filter = std::make_shared<common::BigintRange>(lower, upper, false);
  std::vector<FilterSpec> filters = {{"c0", c0Filter, true}};

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Nullable, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  test({{chunk0, chunk1}}, readFactors, compressionOptions, filters);
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

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Nullable, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  test({{chunk0, chunk1}}, readFactors, compressionOptions, filters);
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

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Nullable, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  test({{chunk0, chunk1}}, readFactors, compressionOptions, filters);
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

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Trivial, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  test({{chunk0, chunk1}}, readFactors, compressionOptions, filters);
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

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Trivial, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  test({{chunk0, chunk1}}, readFactors, compressionOptions, filters);
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

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Trivial, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  test(
      {{chunk0, chunk1}, {chunk2, chunk3, chunk4}},
      readFactors,
      compressionOptions,
      filters);
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

  std::vector<std::pair<EncodingType, float>> readFactors = {
      {EncodingType::Nullable, 1.0},
  };
  CompressionOptions compressionOptions = {.compressionAcceptRatio = 0.0};

  test(
      {{chunk0, chunk1}, {chunk2, chunk3, chunk4}},
      readFactors,
      compressionOptions,
      filters);
}
} // namespace facebook::velox::wave
